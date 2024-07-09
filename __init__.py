from app.utils.PluginClass import PluginClass
from flask_jwt_extended import jwt_required, get_jwt_identity
from flask import request
from app.utils import DatabaseHandler
from app.api.records.models import RecordUpdate
from celery import shared_task
import os
from bson.objectid import ObjectId
from dotenv import load_dotenv
import re

load_dotenv()

mongodb = DatabaseHandler.DatabaseHandler()
WEB_FILES_PATH = os.environ.get('WEB_FILES_PATH', '')
ORIGINAL_FILES_PATH = os.environ.get('ORIGINAL_FILES_PATH', '')
HF_TOKEN = os.environ.get('HF_TOKEN', '')
batch_size = 16
compute_type='float32'

class ExtendedPluginClass(PluginClass):
    def __init__(self, path, import_name, name, description, version, author, type, settings):
        super().__init__(path, __file__, import_name, name, description, version, author, type, settings)

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
        
    @shared_task(ignore_result=False, name='transcribeWhisperX.bulk', queue='high')
    def bulk(body, user):
        import torch
        device = 'cuda' if torch.cuda.is_available() else 'cpu'

        filters = {
            'post_type': body['post_type']
        }

        if body['parent'] and len(body['resources']) == 0:
            filters = {'$or': [{'parents.id': body['parent'], 'post_type': body['post_type']}, {'_id': ObjectId(body['parent'])}], **filters}
        
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
        if body['overwrite']:
            records_filters['processing.transcribeWhisperX'] = {'$exists': True}
        else:
            records_filters['processing.transcribeWhisperX'] = {'$exists': False}
        
        records = list(mongodb.get_all_records('records', records_filters, fields={'_id': 1, 'mime': 1, 'filepath': 1, 'processing': 1}))

        if len(records) > 0:
            import whisper
            model = whisper.load_model(body['model'], device=device)
            if body['diarize']:
                import whisperx
                diarize_model = whisperx.DiarizationPipeline(use_auth_token=HF_TOKEN, device=device)

        for r in records:
            file_path = os.path.join(ORIGINAL_FILES_PATH, r['filepath'])
            audio = whisper.load_audio(file_path)
            if body['language'] == 'auto':
                result = model.transcribe(audio)
            else:
                result = model.transcribe(audio, language=body['language'])
# 
            if body['diarize']:
                try:
                    diarize_segments = diarize_model(audio)
                    result = whisperx.assign_word_speakers(diarize_segments, result)
                except:
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


            update = {
                'processing': r['processing']
            }

            update['processing']['transcribeWhisperX'] = {
                'type': 'av_transcribe',
                'result': result
            }
            update = RecordUpdate(**update)
            mongodb.update_record('records', {'_id': r['_id']}, update)

        instance = ExtendedPluginClass('transcribeWhisperX','', **plugin_info)
        instance.clear_cache()
        return 'Transcripción automática finalizada'
        
    
plugin_info = {
    'name': 'Transcripción automática',
    'description': 'Plugin para la transcripción automática de audios y videos usando el modelo WhisperX',
    'version': '0.1',
    'author': 'Néstor Andrés Peña',
    'type': ['bulk'],
    'settings': {
        'settings': [

        ],
        'settings_bulk': [
            {
                'type':  'instructions',
                'title': 'Instrucciones',
                'text': 'Este plugin permite la transcripción automática de audios y videos usando el modelo WhisperX. Para ello, debe seleccionar el tipo de contenido y el recurso padre. El plugin procesará todos los archivos de audio y video de los recursos hijos del recurso padre seleccionado. Si el recurso padre no está seleccionado, el plugin procesará todos los archivos de audio y video de todos los recursos del tipo de contenido seleccionado.',
            },
            {
                'type': 'checkbox',
                'label': 'Sobreescribir procesamientos existentes',
                'id': 'overwrite',
                'default': False,
                'required': False,
            },
            {
                'type': 'checkbox',
                'label': 'Separar parlantes',
                'id': 'diarize',
                'default': False,
                'required': False,
            },
            {
                'type': 'select',
                'label': 'Tamaño del modelo',
                'id': 'model',
                'default': 'small',
                'options': [
                    {'value': 'tiny', 'label': 'Muy pequeño'},
                    {'value': 'small', 'label': 'Pequeño'},
                    {'value': 'medium', 'label': 'Mediano'},
                    {'value': 'large', 'label': 'Grande'},
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
    }
}