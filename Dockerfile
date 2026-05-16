# Procesador Python para deteccion de anomalias en logs (Cap. III - Fase 4).
#
# Construye en dos pasos para soportar entornos sin conexion:
#   1) Instala dependencias preferentemente desde ./wheels/ (descargadas previamente)
#   2) Cae a PyPI solo si falta algun paquete (online)
#
# Para construir SIN INTERNET (Cuba offline):
#   docker build -t tecopos-processor:dev --build-arg PIP_NO_INDEX=--no-index .
#
# Para construir online (default):
#   docker build -t tecopos-processor:dev .

FROM python:3.11-slim

ARG PIP_NO_INDEX=""

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Crear usuario no-root (buena practica de seguridad).
RUN useradd --create-home --shell /bin/bash app

# Copiar wheels pre-descargados (puede estar vacio si vas a instalar online).
COPY wheels/ /tmp/wheels/
COPY requirements.txt /tmp/requirements.txt

# Instalar: prefiere wheels locales, fallback a PyPI si PIP_NO_INDEX vacio.
RUN pip install ${PIP_NO_INDEX} --find-links /tmp/wheels -r /tmp/requirements.txt && \
    rm -rf /tmp/wheels /tmp/requirements.txt

# Copiar codigo fuente (cuando lo crees en Fase 4).
COPY --chown=app:app . /app

USER app

# El procesador expone metricas Prometheus en /metrics.
EXPOSE 8000

CMD ["python", "-m", "processor.main"]
