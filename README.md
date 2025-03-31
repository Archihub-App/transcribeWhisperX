# Transcripción de voz a texto usando Whisper

Este plugin permite transcribir audio a texto usando el modelo [Whisper de OpenAI](https://cdn.openai.com/papers/whisper.pdf) y [DeepFilterNet](https://github.com/Rikorose/DeepFilterNet) para limpiar el audio. Para ello, se debe tener instalado ArchiHUB y configurar el plugin siguiendo los pasos en la [guía de instalación de un plugin](https://archihub-app.github.io/archihub.github.io/es/install_plugin/).

## Uso de GPU

El plugin permite usar GPU para acelerar la transcrición de voz a texto. Para ello, en la interfaz de configuración del plugin, se debe activar la opción "Usar GPU (si está disponible)". Además, se debe tener en cuenta que el nodo de procesamiento de _Celery_ debe estar configurado para usar GPU. Para más información, consultar la [documentación de los nodos de procesamiento](https://archihub-app.github.io/archihub.github.io/es/nodos/#nodos-de-procesamiento-para-tareas-que-requieren-gpu).
