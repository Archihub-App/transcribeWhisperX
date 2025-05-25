from app.utils.PluginClass import PluginClass
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask import request, send_file
from app.utils import DatabaseHandler
from app.api.records.models import RecordUpdate
from celery import shared_task, current_task
import os
import shutil
from bson.objectid import ObjectId
from app.utils.LogActions import log_actions
from app.api.logs.services import register_log
from dotenv import load_dotenv
import re
from datetime import datetime
import ffmpeg

load_dotenv()

mongodb = DatabaseHandler.DatabaseHandler()
WEB_FILES_PATH = os.environ.get('WEB_FILES_PATH', '')
USER_FILES_PATH = os.environ.get('USER_FILES_PATH', '')
ORIGINAL_FILES_PATH = os.environ.get('ORIGINAL_FILES_PATH', '')
TEMPORAL_FILES_PATH = os.environ.get('TEMPORAL_FILES_PATH', '')
HF_TOKEN = os.environ.get('HF_TOKEN', '')
batch_size = 16
compute_type='float32'

class ExtendedPluginClass(PluginClass):
    def __init__(self, path, import_name, name, description, version, author, type, settings, actions, capabilities=None, **kwargs):
        super().__init__(path, __file__, import_name, name, description, version, author, type, settings, actions = actions, capabilities = None, **kwargs)

    def add_routes(self):
        @self.route('/bulk', methods=['POST'])
        @jwt_required()
        def processing():
            current_user = get_jwt_identity()
            body = request.get_json()

            self.validate_fields(body, 'bulk')
            self.validate_roles(current_user, ['admin', 'processing'])

            task = self.bulk.delay(body, current_user)
            self.add_task_to_user(task.id, 'transcribeWhisperX.bulk', current_user, 'msg')
            
            return {'msg': 'Se agregó la tarea a la fila de procesamientos'}, 201
        
        @self.route('/download', methods=['POST'])
        @jwt_required()
        def download():
            current_user = get_jwt_identity()
            body = request.get_json()
            
            print(body)
            
            self.validate_roles(current_user, ['admin', 'processing', 'editor'])

            task = self.download.delay(body, current_user)
            self.add_task_to_user(task.id, 'transcribeWhisperX.download', current_user, 'file_download')
            
            return {'msg': 'Se agregó la tarea a la fila de procesamientos'}, 201
        
        @self.route('/filedownload/<taskId>', methods=['GET'])
        @jwt_required()
        def file_download(taskId):
            current_user = get_jwt_identity()

            if not self.has_role('admin', current_user) and not self.has_role('processing', current_user) and not self.has_role('editor', current_user):
                return {'msg': 'No tiene permisos suficientes'}, 401
            
            # Buscar la tarea en la base de datos
            task = mongodb.get_record('tasks', {'taskId': taskId})
            # Si la tarea no existe, retornar error
            if not task:
                return {'msg': 'Tarea no existe'}, 404
            
            if task['user'] != current_user and not self.has_role('admin', current_user):
                return {'msg': 'No tiene permisos para obtener la tarea'}, 401

            if task['status'] == 'pending':
                return {'msg': 'Tarea en proceso'}, 400

            if task['status'] == 'failed':
                return {'msg': 'Tarea fallida'}, 400

            if task['status'] == 'completed':
                if task['resultType'] != 'file_download':
                    return {'msg': 'Tarea no es de tipo file_download'}, 400
                
            path = USER_FILES_PATH + task['result']
            response = send_file(path, as_attachment=True, download_name=os.path.basename(path), conditional=False)
            
            response.headers.add("Access-Control-Expose-Headers", "Content-Disposition")
            return response
        
        
    @shared_task(ignore_result=False, name='transcribeWhisperX.download')
    def download(body, user):
        records_filters = {'_id': {'$in': [ObjectId(record) for record in body['records']]}}
        
        records = list(mongodb.get_all_records('records', records_filters, fields={'_id': 1, 'mime': 1, 'filepath': 1, 'processing': 1, 'name': 1, 'displayName': 1}))
        
        if len(records) == 0:
            raise Exception('No se encontraron registros')
        elif len(records) > 1:
            raise Exception('Debe seleccionar solo un registro')
        
        folder_path = USER_FILES_PATH + '/' + user + '/transcribeWhisperX'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        for r in records:
            if 'transcribeWhisperX' in r['processing']:
                result = r['processing']['transcribeWhisperX']['result']

                if body['format'] == 'doc':
                    from docx import Document
                    doc = Document()
                    title = r['displayName'] if 'displayName' in r else r['name']
                    doc.add_heading(title, 0)
                    doc.add_paragraph(result['text'])
                    path = os.path.join(USER_FILES_PATH, user, 'transcribeWhisperX', str(r['_id']) + '.docx')
                    doc.save(path)
                    return '/' + user + '/transcribeWhisperX/' + str(r['_id']) + '.docx'
                elif body['format'] == 'pdf':
                    from docx import Document
                    doc = Document()
                    title = r['displayName'] if 'displayName' in r else r['name']
                    doc.add_heading(title, 0)
                    doc.add_paragraph(result['text'])
                    temp_path = os.path.join(TEMPORAL_FILES_PATH, str(r['_id']) + '.docx')
                    doc.save(temp_path)
                    
                    try:
                        from app.plugins.filesProcessing.utils.DocumentProcessing import convert_to_pdf_with_libreoffice
                    except Exception as e:
                        raise Exception('Error al importar el módulo del plugin para el procesamiento de documentos: ' + str(e))
                    
                    output_pdf = os.path.join(USER_FILES_PATH, user, 'transcribeWhisperX', str(r['_id']) + '.pdf')
                    convert_to_pdf_with_libreoffice(temp_path, output_pdf)
                    shutil.move(os.path.join(TEMPORAL_FILES_PATH, str(r['_id']) + '.pdf'), output_pdf)
                    os.remove(temp_path)
                    return '/' + user + '/transcribeWhisperX/' + str(r['_id']) + '.pdf'
                elif body['format'] == 'srt':
                    def millis_to_srt_time(millis):
                        # Convert millis to an integer
                        millis = int(millis * 1000)
                        seconds, ms = divmod(millis, 1000)
                        minutes, sec = divmod(seconds, 60)
                        hours, minutes = divmod(minutes, 60)
                        return f"{hours:02d}:{minutes:02d}:{sec:02d},{ms:03d}"
                    
                    segments = result['segments']
                    srt = ''
                    for i, segment in enumerate(segments):
                        start_str = millis_to_srt_time(segment['start'])
                        end_str = millis_to_srt_time(segment['end'])
                        srt += str(i + 1) + '\n'
                        srt += start_str + ' --> ' + end_str + '\n'
                        srt += segment['text'] + '\n\n'
                    path = os.path.join(USER_FILES_PATH, user, 'transcribeWhisperX', str(r['_id']) + '.srt')
                    with open(path, 'w') as f:
                        f.write(srt)
                    return '/' + user + '/transcribeWhisperX/' + str(r['_id']) + '.srt'
                else:
                    raise Exception('Formato no válido')
                          
    @shared_task(ignore_result=False, name='transcribeWhisperX.bulk', queue='high')
    def bulk(body, user):
        current_task.update_state(state='PROGRESS', meta={
            'status': 'Iniciando procesamiento de transcripción',
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        id_process = []

        import torch
        if 'gpu' in body and body['gpu']:
            device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        else:
            device = torch.device('cpu')

        if 'records' not in body:
            filters = {
                'post_type': body['post_type']
            }
            
            if isinstance(body['post_type'], list):
                filters['post_type'] = {'$in': body['post_type']}   

            if 'parent' in body:
                if body['parent'] and len(body['resources']) == 0:
                    filters = {'$or': [{'parents.id': body['parent'], 'post_type': filters['post_type']}, {'_id': ObjectId(body['parent'])}]}
            
            if 'resources' in body:
                if body['resources']:
                    if len(body['resources']) > 0:
                        filters = {'_id': {'$in': [ObjectId(resource) for resource in body['resources']]}, **filters}
                
            # obtenemos los recursos
            resources = list(mongodb.get_all_records('resources', filters, fields={'_id': 1}))
            resources = [str(resource['_id']) for resource in resources]

            records_filters = {
                'parent.id': {'$in': resources},
                'processing.fileProcessing': {'$exists': True},
                '$or':[{'processing.fileProcessing.type': 'audio'}, {'processing.fileProcessing.type': 'video'}]
            }
        else:
            records_filters = {'_id': {'$in': [ObjectId(record) for record in body['records']]}}

        if 'overwrite' in body and body['overwrite']:
            records_filters = {"$or": [{"processing.fileProcessing": {"$exists": False}, **records_filters}, {"processing.fileProcessing": {"$exists": True}, **records_filters}]}
        else:
            records_filters['processing.transcribeWhisperX'] = {'$exists': False}
        
        records = list(mongodb.get_all_records('records', records_filters, fields={'_id': 1, 'mime': 1, 'filepath': 1, 'processing': 1}))
        
        current_task.update_state(state='PROGRESS', meta={
            'status': 'Cargando los modelos de transcripción',
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        if len(records) > 0:
            print(body)
            import whisper
            model = whisper.load_model(body['model'], device=device)
            
            print('Modelo cargado')
            if body['diarize']:
                import whisperx
                from whisperx import diarize
                diarize_model = diarize.DiarizationPipeline(use_auth_token=HF_TOKEN, device=device)
            if body['denoise']:
                from df.enhance import enhance, init_df, load_audio, save_audio
                model_denoise, df_state, sr, _ = init_df()
                
        current_task.update_state(state='PROGRESS', meta={
            'status': 'Modelo cargado, procesando transcripción',
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        for r in records:
            current_task.update_state(state='PROGRESS', meta={
                'status': 'Procesando transcripción del audio: ' + str(records.index(r) + 1) + ' de ' + str(len(records)),
                'progress': (records.index(r) + 1) / len(records) * 100,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            file_path = os.path.join(ORIGINAL_FILES_PATH, r['filepath'])
            
            if body['denoise']:
                if r['mime'] != 'audio/wav':
                    temporal_file_path = os.path.join(TEMPORAL_FILES_PATH, r['filepath'])
                    temporal_file_path = os.path.splitext(temporal_file_path)[0] + '.wav'
                    if not os.path.exists(os.path.dirname(temporal_file_path)):
                        os.makedirs(os.path.dirname(temporal_file_path))
                    
                    try:
                        (
                            ffmpeg
                            .input(file_path)
                            .output(temporal_file_path, format='wav', acodec='pcm_s16le', ac=1, ar='48000')
                            .overwrite_output()
                            .run()
                        )
                    except Exception as e:
                        raise Exception('Error al convertir el audio a WAV')
                    
                    audio, _ = load_audio(temporal_file_path, sr=df_state.sr())
                    enhanced_audio = enhance(model_denoise, df_state, audio)
                    save_audio(temporal_file_path, enhanced_audio, df_state.sr())
                    file_path = temporal_file_path
                    
                else:
                    file_path = os.path.join(ORIGINAL_FILES_PATH, r['filepath'])
            
            audio = whisper.load_audio(file_path)
            if body['language'] == 'auto':
                result = model.transcribe(audio)
            else:
                result = model.transcribe(audio, language=body['language'])
# 
            if body['diarize']:
                try:
                    current_task.update_state(state='PROGRESS', meta={
                        'status': 'Procesando segmentación del audio: ' + str(records.index(r) + 1) + ' de ' + str(len(records)),
                        'progress': (records.index(r) + 1) / len(records) * 100,
                        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    diarize_segments = diarize_model(audio)
                    result = whisperx.assign_word_speakers(diarize_segments, result)
                except Exception as e:
                    print(str(e))
                    pass

                
                if 'speaker' in result['segments'][0]:
                    current_speaker = result['segments'][0]['speaker']
                else:
                    current_speaker = ''
                text = current_speaker + ": " + result['segments'][0]['text']

                for segment in result['segments']:
                    segment_text = segment['text']
                    pattern = r'\s*(transcribed by.*|subtitles by.*|by.*\.com|by.*\.org|http.*|.com*)$'
                    if re.search(pattern, segment_text):
                        segment_text = ''
                        segment['text'] = segment_text
                    # si el segmento actual tiene el mismo speaker que el anterior
                    if 'speaker' in segment:
                        if segment['speaker'] == current_speaker:
                            # sumar el texto del segmento actual al anterior
                            text += ' ' + segment['text']
                        else:
                            # si no, actualizar el speaker actual
                            current_speaker = segment['speaker']
                            # y agregar el nuevo texto
                            text += '\n\n' + current_speaker + ": " + segment['text']
                    else:
                        text += ' ' + segment['text']

                result['text'] = text.replace('SPEAKER_', 'PERSONA_')

            if body['denoise']:
                if r['mime'] != 'audio/wav':
                    os.remove(temporal_file_path)
                    
                    
            current_task.update_state(state='PROGRESS', meta={
                'status': 'Guardando procesamiento de ' + str(records.index(r) + 1) + ' de ' + str(len(records)),
                'progress': (records.index(r) + 1) / len(records) * 100,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

            update = {
                'processing': r['processing'],
                'updatedAt': datetime.now(),
                'updatedBy': user if user else 'system'
            }

            update['processing']['transcribeWhisperX'] = {
                'type': 'av_transcribe',
                'result': result,
            }
            update = RecordUpdate(**update)
            mongodb.update_record('records', {'_id': r['_id']}, update)
            id_process.append(r['_id'])

        # Registrar el log
        register_log(user, log_actions['av_transcribe'], {'form': body, 'ids': id_process})

        instance = ExtendedPluginClass('transcribeWhisperX','', **plugin_info)
        instance.clear_cache()
        return f'Se procesaron {len(records)} registros'
        
general_settings = [
    {
        'type': 'checkbox',
        'label': 'Sobreescribir procesamientos existentes',
        'id': 'overwrite',
        'default': False,
        'required': False,
    },
    {
        'type': 'checkbox',
        'label': 'Limpiar audio con DeepFilterNet',
        'id': 'denoise',
        'default': False,
        'required': False,
        'instructions': 'Si el audio original está en un formato diferente a WAV, se convertirá a WAV y se limpiará el audio con DeepFilterNet. Si el audio original ya está en WAV, no se convertirá y se limpiará el audio con DeepFilterNet.',
    },
    {
        'type': 'checkbox',
        'label': 'Separar parlantes',
        'id': 'diarize',
        'default': False,
        'required': False,
    },
    {
        'type': 'checkbox',
        'label': 'Usar GPU (si está disponible)',
        'id': 'gpu',
        'default': False,
        'required': False,
    },
    {
        'type': 'select',
        'label': 'Tamaño del modelo',
        'id': 'model',
        'default': 'turbo',
        'options': [
            {'value': 'tiny', 'label': 'Muy pequeño'},
            {'value': 'small', 'label': 'Pequeño'},
            {'value': 'medium', 'label': 'Mediano'},
            {'value': 'large', 'label': 'Grande'},
            {'value': 'turbo', 'label': 'Turbo'},
        ],
        'required': False,
    },
    {
        'type': 'select',
        'label': 'Idioma de la transcripción',
        'id': 'language',
        'default': 'auto',
        'options': [
            {'value': 'auto', 'label': 'Automático'},
            {'value': 'es', 'label': 'Español'},
            {'value': 'en', 'label': 'Inglés'},
            {'value': 'fr', 'label': 'Francés'},
            {'value': 'de', 'label': 'Alemán'},
            {'value': 'it', 'label': 'Italiano'},
            {'value': 'pt', 'label': 'Portugués'},
            
        ],
        'required': False,
    }
]
    
plugin_info = {
    'name': 'Transcripción automática',
    'description': 'Plugin para la transcripción automática de audios y videos usando el modelo WhisperX',
    'version': '0.1',
    'author': 'Néstor Andrés Peña',
    'type': ['bulk'],
    'settings': {
        'settings_bulk': [
            {
                'type':  'instructions',
                'title': 'Instrucciones',
                'text': 'Este plugin permite la transcripción automática de audios y videos usando el modelo WhisperX. Para ello, debe seleccionar el tipo de contenido y el recurso padre. El plugin procesará todos los archivos de audio y video de los recursos hijos del recurso padre seleccionado. Si el recurso padre no está seleccionado, el plugin procesará todos los archivos de audio y video de todos los recursos del tipo de contenido seleccionado.',
            },
            *general_settings
        ]
    },
    'actions': [
        {
            'placement': 'detail_record',
            'record_type': ['audio', 'video'],
            'label': 'Transcribir con Whisper',
            'roles': ['admin', 'processing', 'editor'],
            'endpoint': 'bulk',
            'icon': 'Transcribe',
            'extraOpts': [
                *general_settings
            ]
        },
        {
            'placement': 'detail_record',
            'record_type': ['audio', 'video'],
            'label': 'Descargar transcripción',
            'roles': ['admin', 'processing', 'editor'],
            'endpoint': 'download',
            'icon': 'Download,Transcribe',
            'extraOpts': [
                {
                    'type': 'select',
                    'label': 'Formato del archivo',
                    'id': 'format',
                    'default': 'pdf',
                    'options': [
                        {'value': 'pdf', 'label': 'PDF'},
                        {'value': 'doc', 'label': 'DOC'},
                        {'value': 'srt', 'label': 'SRT'},
                    ],
                    'required': False,
                }
            ]
        }
    ]
}