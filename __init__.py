"""transcribeWhisperX — automatic speech transcription for audio and video.

WHAT IT DOES. Selects audio/video records, transcribes them with Whisper, and
optionally attributes each segment to a speaker with pyannote. The result is
stored as an ``av_transcribe`` processing entry, which is what the record
viewer's transcription tab reads and what ``liquidText`` starts from. A second
route exports one record's transcript as DOCX, PDF or SRT.

THE PROCESSING KEY IS THE CONTRACT WITH DATA ALREADY ON DISK. It is
``transcribeWhisperX`` — the same string as this package's directory, its route
prefix, and its dotted task names. Nothing here may rename it: the key is where
finished transcripts live, ``records/transcription.py`` resolves a record's
transcript by the slug the client sends, and the bulk filter uses the key's
absence to decide a record still needs transcribing. Renaming it would leave
existing transcripts unreachable under a name nothing looks up *and* make every
already-transcribed record look untranscribed, so the next run would repeat the
GPU work and store the answer somewhere new.

HEAVY IMPORTS ARE FUNCTION-LOCAL, DELIBERATELY. torch, whisper and pyannote are
imported inside the task body, never at module scope. Mounting a plugin imports
its package, and that happens in the web process as well as the worker — a
module-level ``import torch`` would load a multi-hundred-megabyte CUDA stack
into every API process to serve routes that never touch it.

EXPORT FORMAT IS AN ALLOWLIST. ``format`` reaches a file extension and a code
path, so it indexes a fixed tuple and is refused before anything is written.
"""

from __future__ import annotations

import datetime
import logging
import os
import re

from celery import current_task, shared_task
from fastapi import Body, Depends
from fastapi.responses import JSONResponse

from archihub.core.i18n import gettext as _
from archihub.core.responses import json_response
from archihub.core.security.jwt import CurrentUser
from archihub.plugins.framework import data as plugin_data
from archihub.plugins.framework.base import (
    ArchiPlugin,
    BrokerUnavailable,
    object_ids,
    queue,
    require_roles,
    task_result_file,
)

logger = logging.getLogger(__name__)

SLUG = "transcribeWhisperX"

#: Dotted task names are the wire contract. A message already sitting in Redis
#: and every row in the `tasks` collection resolves by this exact string.
TASK_BULK = "transcribeWhisperX.bulk"
TASK_DOWNLOAD = "transcribeWhisperX.download"

#: Where a finished transcript is stored on the record. See the module note.
PROCESSING_KEY = "transcribeWhisperX"

#: The processing kind a transcript declares. `records/transcription.py`
#: refuses an entry that says anything else.
TRANSCRIPTION_TYPE = "av_transcribe"

#: Export formats, indexed by the request and never pasted into a path.
FORMATS = ("doc", "pdf", "srt")

#: Whisper hallucinates boilerplate on silence - subtitle-site credits and bare
#: URLs. Dropped from the transcript rather than shown as speech.
HALLUCINATION = re.compile(
    r"^\s*(transcribed by.*|subtitles by.*|by.*\.com|by.*\.org|http.*|.com*)$",
    re.IGNORECASE,
)

#: Wall-clock ceiling on the audio extraction. Without one a malformed input
#: can hold a worker slot indefinitely.
FFMPEG_TIMEOUT_SECONDS = 1800

#: The kinds of file this plugin can read, as `fileProcessing` labels them.
MEDIA_TYPES = ("audio", "video")


class TranscribeWhisperX(ArchiPlugin):
    """Transcription over a selection of audio and video records."""

    def add_routes(self) -> None:
        @self.router.post(
            "/bulk",
            status_code=201,
            responses={
                201: {"description": "Queued"},
                400: {"description": "The selection is not valid"},
                503: {"description": "Queue unavailable"},
            },
        )
        def transcribe(
            body: dict = Body(...),
            current_user: CurrentUser = Depends(require_roles("admin", "processing")),
        ) -> JSONResponse:
            """Queue transcription over a selection of records."""
            error = self.validate_settings_fields(body, "bulk")
            if error:
                return json_response({"msg": error}, 400)

            try:
                queue(
                    bulk_task,
                    TASK_BULK,
                    current_user.username,
                    "msg",
                    body,
                    current_user.username,
                )
            except BrokerUnavailable:
                return json_response({"msg": _("The task queue is unavailable")}, 503)

            return json_response(
                {"msg": _("The task was added to the processing queue")}, 201
            )

        @self.router.post(
            "/download",
            status_code=201,
            responses={
                201: {"description": "Queued"},
                400: {"description": "Unsupported format"},
                503: {"description": "Queue unavailable"},
            },
        )
        def download(
            body: dict = Body(...),
            current_user: CurrentUser = Depends(
                require_roles("admin", "processing", "editor")
            ),
        ) -> JSONResponse:
            """Queue an export of one record's transcript.

            The format is checked HERE rather than in the task: a typo should
            come back as a 400, not as a background job that fails minutes later
            with the reason buried in a task row.
            """
            if body.get("format") not in FORMATS:
                return json_response({"msg": _("Unsupported format")}, 400)

            try:
                queue(
                    download_task,
                    TASK_DOWNLOAD,
                    current_user.username,
                    "file_download",
                    body,
                    current_user.username,
                )
            except BrokerUnavailable:
                return json_response({"msg": _("The task queue is unavailable")}, 503)

            return json_response(
                {"msg": _("The task was added to the processing queue")}, 201
            )

        @self.router.get(
            "/filedownload/{task_id}",
            responses={
                200: {"description": "The exported transcript"},
                404: {"description": "Unknown task, or its file is gone"},
            },
        )
        def file_download(
            task_id: str,
            current_user: CurrentUser = Depends(
                require_roles("admin", "processing", "editor")
            ),
        ):
            """Download a completed export."""
            from archihub.api.users.services import has_role
            from archihub.core.responses import file_response

            result, status_code = task_result_file(
                task_id,
                current_user.username,
                is_admin=has_role(current_user.username, "admin"),
            )
            if status_code != 200:
                return json_response(result, status_code)

            return file_response(
                result,
                download_name=os.path.basename(str(result)),
                as_attachment=True,
            )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


def _mongo():
    from archihub.infra.mongo import get_mongo

    return get_mongo()


def _progress(status: str, progress: float | None = None) -> None:
    """Report progress, when running inside a real task.

    Guarded because the task bodies are called directly from tests, where
    ``current_task`` is unbound and updating state raises.
    """
    if current_task is None or getattr(current_task, "request", None) is None:
        return
    meta = {"status": status, "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    if progress is not None:
        meta["progress"] = progress
    try:
        current_task.update_state(state="PROGRESS", meta=meta)
    except Exception:  # pragma: no cover - progress must never fail the job
        logger.debug("Could not report progress", exc_info=True)


def record_filters(body: dict) -> dict:
    """Which records a bulk run covers.

    Either an explicit list of record ids, or every media record filed under the
    resources a content-type/parent selection matches. Extracted from the task
    body so the selection can be asserted without transcribing anything.
    """
    if body.get("records"):
        return {"_id": {"$in": object_ids(body["records"], "records")}}

    post_type = body.get("post_type")
    if not post_type:
        raise ValueError(_("No content type was specified"))

    type_clause = {"$in": post_type} if isinstance(post_type, list) else post_type
    resource_filters: dict = {"post_type": type_clause}

    resources = body.get("resources") or []
    parent = body.get("parent")

    if parent and not resources:
        resource_filters = {
            "$or": [
                {"parents.id": parent, "post_type": type_clause},
                {"_id": object_ids([parent], "parent")[0]},
            ]
        }
    elif resources:
        resource_filters = {
            "_id": {"$in": object_ids(resources, "resources")},
            **resource_filters,
        }

    matched = _mongo().get_all_records("resources", resource_filters, fields={"_id": 1})
    resource_ids = [str(resource["_id"]) for resource in matched]

    return {
        "parent.id": {"$in": resource_ids},
        "processing.fileProcessing": {"$exists": True},
        "processing.fileProcessing.type": {"$in": list(MEDIA_TYPES)},
    }


def _apply_overwrite(filters: dict, overwrite: bool) -> dict:
    """Restrict a selection to records that still need transcribing.

    ``overwrite`` means "do them all again". Otherwise a record that already
    carries a transcript is skipped, which is the only thing standing between a
    re-run and hours of repeated GPU work.
    """
    if overwrite:
        return filters
    return {**filters, f"processing.{PROCESSING_KEY}": {"$exists": False}}


def extraction_command(source, destination, denoise: bool) -> list[str]:
    """The ffmpeg argument list, built as a list and never a shell string.

    Whisper wants 16 kHz mono PCM. ``afftdn`` is ffmpeg's own FFT denoiser,
    applied before transcription when the operator asks for it.

    Split out from the runner so the arguments can be asserted in tests without
    transcoding anything.
    """
    command = [
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y",
        "-i", str(source),
    ]
    if denoise:
        command += ["-af", "afftdn=nf=-25"]
    command += ["-vn", "-acodec", "pcm_s16le", "-ac", "1", "-ar", "16000",
                "-f", "wav", str(destination)]
    return command


def _extract_audio(source, destination, denoise: bool) -> None:
    """Decode a media file to the WAV shape Whisper reads."""
    import subprocess

    try:
        subprocess.run(
            extraction_command(source, destination, denoise),
            check=True,
            capture_output=True,
            timeout=FFMPEG_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg is not installed on this host") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Audio extraction timed out after {FFMPEG_TIMEOUT_SECONDS}s") from exc
    except subprocess.CalledProcessError as exc:
        # ffmpeg's stderr names paths on the server. Logged, never returned.
        detail = (exc.stderr or b"").decode("utf-8", "replace")
        logger.error("ffmpeg failed extracting audio from %s: %s", source, detail)
        raise RuntimeError("Could not extract audio from this file") from exc


def attribute_speakers(segments: list, diarization) -> None:
    """Tag each transcript segment with the speaker who talks over most of it.

    Whisper's segmentation and pyannote's turns are independent, so a segment is
    assigned to whichever speaker holds the largest overlap with it rather than
    to whoever happens to start first. Mutates ``segments`` in place.
    """
    for segment in segments:
        start = segment.get("start", 0)
        end = segment.get("end", 0)

        overlaps: dict = {}
        for turn, _track, speaker in diarization.itertracks(yield_label=True):
            duration = min(end, turn.end) - max(start, turn.start)
            if duration > 0:
                overlaps[speaker] = overlaps.get(speaker, 0) + duration

        if overlaps:
            dominant = max(overlaps, key=overlaps.get)
            segment["speaker"] = dominant.replace("SPEAKER_", "PERSONA_")
        else:
            segment["speaker"] = "PERSONA_UNKNOWN"


def assemble_text(segments: list, diarize: bool) -> str:
    """Rebuild the flat transcript, dropping hallucinations and marking turns.

    A speaker label is written once per turn rather than once per segment, and
    each segment that opens a turn is tagged so the SRT export can prepend the
    same label. Mutates ``segments``: a dropped segment keeps its timings but
    loses its text, which is what stops it rendering in the viewer.
    """
    pieces: list[str] = []
    current_speaker = None

    for segment in segments:
        text = segment.get("text", "")
        if HALLUCINATION.search(text):
            segment["text"] = ""
            continue

        if diarize and "speaker" in segment:
            if segment["speaker"] != current_speaker:
                current_speaker = segment["speaker"]
                pieces.append(f"\n\n{current_speaker}: {text}")
                segment["speaker_tag"] = f"[{current_speaker}] "
            else:
                pieces.append(f" {text}")
        else:
            pieces.append(f" {text}")

    return "".join(pieces).strip()


@shared_task(ignore_result=False, name=TASK_BULK, queue="high")
def bulk_task(body: dict, user: str) -> str:
    """Transcribe every selected record that does not already have a transcript."""
    import torch

    _progress("Starting transcription")

    filters = _apply_overwrite(record_filters(body), bool(body.get("overwrite")))
    records = list(
        _mongo().get_all_records(
            "records", filters, fields={"_id": 1, "mime": 1, "filepath": 1}
        )
    )

    if not records:
        return _("Transcription finished for {count} records", count=0)

    diarize = bool(body.get("diarize"))
    denoise = bool(body.get("denoise"))
    use_gpu = bool(body.get("gpu")) and torch.cuda.is_available()
    device = torch.device("cuda" if use_gpu else "cpu")

    _progress("Loading the transcription models")

    import whisper

    model = whisper.load_model(body.get("model") or "turbo", device=device)

    diarize_model = None
    if diarize:
        from pyannote.audio import Pipeline

        diarize_model = Pipeline.from_pretrained(
            "pyannote/speaker-diarization-3.1",
            token=os.environ.get("HF_TOKEN", ""),
        )

    _progress("Models loaded, processing records")

    from archihub.core import files as filestore
    from archihub.core.settings import get_settings

    settings = get_settings()
    transcribed = 0
    failed = 0
    processed_ids = []

    for position, record in enumerate(records, start=1):
        if use_gpu:
            torch.cuda.empty_cache()

        _progress(
            f"Processing file {position} of {len(records)}",
            progress=position / len(records) * 100,
        )

        try:
            source = filestore.resolve_within(
                settings.original_files_path, str(record.get("filepath") or "")
            )
        except Exception:
            logger.error("Record %s has no usable file path", record.get("_id"))
            failed += 1
            continue

        temporary = None
        try:
            audio_path = source
            if record.get("mime") != "audio/wav" or denoise:
                temporary = os.path.join(
                    settings.temporal_files_path, f"{record['_id']}.wav"
                )
                os.makedirs(os.path.dirname(temporary), exist_ok=True)
                _extract_audio(source, temporary, denoise)
                audio_path = temporary

            audio = whisper.load_audio(str(audio_path))
            language = body.get("language")
            if language and language != "auto":
                result = model.transcribe(audio, language=language)
            else:
                result = model.transcribe(audio)

            if diarize and diarize_model is not None:
                _progress(
                    f"Separating speakers for file {position} of {len(records)}",
                    progress=position / len(records) * 100,
                )
                try:
                    attribute_speakers(
                        result.get("segments") or [],
                        _diarize(diarize_model, audio_path, torch),
                    )
                except Exception:
                    # A transcript without speakers is still worth keeping;
                    # losing it because attribution failed is not a trade
                    # anyone would choose.
                    logger.exception(
                        "Speaker separation failed for record %s", record["_id"]
                    )

            result["text"] = assemble_text(result.get("segments") or [], diarize)

            plugin_data.store_processing_result(
                str(record["_id"]),
                PROCESSING_KEY,
                {"type": TRANSCRIPTION_TYPE, "result": result},
            )
            processed_ids.append(record["_id"])
            transcribed += 1

        except Exception:
            # One unreadable file must not end the run. Counted, so the
            # operator is told the number rather than only the successes.
            logger.exception("Could not transcribe record %s", record.get("_id"))
            failed += 1
        finally:
            if temporary and os.path.exists(temporary):
                os.remove(temporary)

    _audit(user, body, processed_ids)

    if failed:
        logger.warning(
            "transcribeWhisperX: %d of %d records failed", failed, len(records)
        )
    return _("Transcription finished for {count} records", count=transcribed)


def _diarize(pipeline, audio_path, torch):
    """Run the diarisation pipeline over a decoded waveform."""
    import soundfile

    data, sample_rate = soundfile.read(str(audio_path))
    waveform = torch.from_numpy(data).float()
    waveform = waveform.unsqueeze(0) if waveform.ndim == 1 else waveform.transpose(0, 1)

    output = pipeline({"waveform": waveform, "sample_rate": sample_rate})
    # pyannote 4 wraps the annotation; 3 returns it directly.
    return getattr(output, "speaker_diarization", output)


def _audit(user: str, body: dict, record_ids: list) -> None:
    from archihub.api.logs.services import register_log

    try:
        register_log(user, "av_transcribe", {"form": body, "ids": record_ids})
    except Exception:
        logger.warning("Could not write the transcription audit entry", exc_info=True)


@shared_task(ignore_result=False, name=TASK_DOWNLOAD)
def download_task(body: dict, user: str) -> str:
    """Export one record's transcript, returning the path relative to the user root."""
    from archihub.core import files as filestore
    from archihub.core.settings import get_settings

    fmt = body.get("format")
    if fmt not in FORMATS:
        raise ValueError(f"Unsupported format: {fmt!r}")

    record_ids = object_ids(body.get("records") or [], "records")
    if len(record_ids) != 1:
        raise ValueError("Select exactly one record to export")

    record = _mongo().get_record(
        "records",
        {"_id": record_ids[0]},
        fields={"_id": 1, "processing": 1, "name": 1, "displayName": 1},
    )
    if not record:
        raise ValueError("Record not found")

    entry = (record.get("processing") or {}).get(PROCESSING_KEY) or {}
    result = entry.get("result") or {}
    if not result.get("text"):
        raise ValueError("This record has not been transcribed")

    settings = get_settings()
    # `user` reaches a directory name. It is an authenticated username rather
    # than free text, but it is still data and is resolved, never concatenated.
    directory = filestore.resolve_within(settings.user_files_path, user, SLUG)
    directory.mkdir(parents=True, exist_ok=True)

    title = record.get("displayName") or record.get("name") or str(record["_id"])
    stem = str(record["_id"])

    if fmt == "srt":
        destination = directory / f"{stem}.srt"
        destination.write_text(to_srt(result.get("segments") or []), encoding="utf-8")
    elif fmt == "doc":
        destination = directory / f"{stem}.docx"
        write_docx(result["text"], title, destination)
    else:
        destination = _export_pdf(result["text"], title, stem, directory, settings)

    return f"/{user}/{SLUG}/{destination.name}"


def srt_timestamp(seconds: float) -> str:
    """``HH:MM:SS,mmm``, the only timestamp form SRT accepts."""
    total_ms = int(round(float(seconds) * 1000))
    total_ms = max(total_ms, 0)
    whole, ms = divmod(total_ms, 1000)
    minutes, sec = divmod(whole, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02d}:{minutes:02d}:{sec:02d},{ms:03d}"


def to_srt(segments: list) -> str:
    """Render segments as an SRT subtitle file.

    A segment emptied as a hallucination is skipped rather than written as a
    blank cue, which some players render as a flash of empty subtitle.
    """
    blocks = []
    index = 0
    for segment in segments:
        text = (segment.get("text") or "").strip()
        if not text:
            continue
        index += 1
        label = segment.get("speaker_tag", "")
        blocks.append(
            f"{index}\n"
            f"{srt_timestamp(segment.get('start', 0))} --> "
            f"{srt_timestamp(segment.get('end', 0))}\n"
            f"{label}{text}\n"
        )
    return "\n".join(blocks)


def write_docx(text: str, title: str, path) -> None:
    """A titled DOCX, one paragraph per blank-line-separated block."""
    from docx import Document

    document = Document()
    document.add_heading(title, 0)
    for block in text.split("\n\n"):
        document.add_paragraph(block.strip())
    document.save(str(path))


def _export_pdf(text: str, title: str, stem: str, directory, settings):
    """DOCX first, then LibreOffice. Cleans up after itself either way.

    The conversion is a capability looked up through the plugin registry, so it
    follows ``filesProcessing``'s activation and refuses with a sentence naming
    what to switch on. Importing that package directly would run its code
    whether or not it is active.
    """
    from pathlib import Path

    from archihub.core import files as filestore
    from archihub.plugins.framework import interop

    temporary = filestore.resolve_within(settings.temporal_files_path, f"{stem}.docx")
    Path(temporary).parent.mkdir(parents=True, exist_ok=True)
    write_docx(text, title, temporary)

    destination = directory / f"{stem}.pdf"
    try:
        interop.convert_to_pdf(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)

    if not destination.is_file():
        raise RuntimeError("PDF conversion produced no file")
    return destination


# ---------------------------------------------------------------------------
# Declaration
# ---------------------------------------------------------------------------

general_settings = [
    {
        "type": "checkbox",
        "label": "Sobreescribir procesamientos existentes",
        "id": "overwrite",
        "default": False,
        "required": False,
    },
    {
        "type": "checkbox",
        "label": "Limpiar audio de fondo (FFmpeg)",
        "id": "denoise",
        "default": False,
        "required": False,
        "instructions": (
            "Si el audio tiene ruido de fondo estático, se filtrará usando el "
            "reductor FFT nativo de FFmpeg antes de transcribir."
        ),
    },
    {
        "type": "checkbox",
        "label": "Separar parlantes (Pyannote)",
        "id": "diarize",
        "default": False,
        "required": False,
    },
    {
        "type": "checkbox",
        "label": "Usar GPU (si está disponible)",
        "id": "gpu",
        "default": False,
        "required": False,
    },
    {
        "type": "select",
        "label": "Tamaño del modelo Whisper",
        "id": "model",
        "default": "turbo",
        "options": [
            {"value": "tiny", "label": "Muy pequeño"},
            {"value": "small", "label": "Pequeño"},
            {"value": "medium", "label": "Mediano"},
            {"value": "large-v3", "label": "Grande"},
            {"value": "turbo", "label": "Turbo"},
            {"value": "large-v3-turbo", "label": "Turbo Grande"},
        ],
        "required": False,
    },
    {
        "type": "select",
        "label": "Idioma de la transcripción",
        "id": "language",
        "default": "auto",
        "options": [
            {"value": "auto", "label": "Automático"},
            {"value": "es", "label": "Español"},
            {"value": "en", "label": "Inglés"},
            {"value": "fr", "label": "Francés"},
            {"value": "de", "label": "Alemán"},
            {"value": "it", "label": "Italiano"},
            {"value": "pt", "label": "Portugués"},
        ],
        "required": False,
    },
]

plugin_info = {
    "name": "Transcripción Whisper",
    "description": (
        "Plugin para la transcripción automática usando Whisper y separación de "
        "audio con Pyannote."
    ),
    "version": "0.2",
    "author": "Néstor Andrés Peña",
    "type": ["bulk"],
    "settings": {
        "settings_bulk": [
            {
                "type": "instructions",
                "title": "Instrucciones",
                "text": (
                    "Este plugin procesará todos los archivos de audio y video de "
                    "los recursos hijos del recurso padre seleccionado. Utiliza "
                    "Whisper para el texto y Pyannote para la separación de canales."
                ),
            },
            *general_settings,
        ]
    },
    "actions": [
        {
            "placement": "detail_record",
            "record_type": ["audio", "video"],
            "label": "Transcribir con Whisper",
            "roles": ["admin", "processing", "editor"],
            "endpoint": "bulk",
            "icon": "Transcribe",
            "extraOpts": [*general_settings],
        },
        {
            "placement": "detail_record",
            "record_type": ["audio", "video"],
            "label": "Descargar transcripción",
            "roles": ["admin", "processing", "editor"],
            "endpoint": "download",
            "icon": "Download,Transcribe",
            "extraOpts": [
                {
                    "type": "select",
                    "label": "Formato del archivo",
                    "id": "format",
                    "default": "pdf",
                    "options": [
                        {"value": "pdf", "label": "PDF"},
                        {"value": "doc", "label": "DOC"},
                        {"value": "srt", "label": "SRT"},
                    ],
                    "required": False,
                }
            ],
        },
    ],
}


def build() -> TranscribeWhisperX:
    return TranscribeWhisperX(SLUG, plugin_info, module_file=__file__)
