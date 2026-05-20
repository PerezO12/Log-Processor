# Procesador Python de deteccion de anomalias en logs (Cap. III).
#
# Soporta build offline (entornos sin internet) via wheels pre-descargados:
#   docker build -t tecopos-processor:prod --build-arg PIP_NO_INDEX=--no-index .
#
# Build online (default, usa PyPI como fallback):
#   docker build -t tecopos-processor:prod .

FROM python:3.11-slim

ARG PIP_NO_INDEX=""

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PROCESSOR_ENV=production

WORKDIR /app

# Usuario no-root: si el contenedor se compromete, no tiene acceso root al host.
RUN useradd --create-home --shell /bin/bash app

# Wheels pre-descargados (directorio puede estar vacio si se instala online).
COPY wheels/ /tmp/wheels/
COPY requirements.txt /tmp/requirements.txt

# Prefiere wheels locales; cae a PyPI solo si PIP_NO_INDEX esta vacio.
RUN pip install ${PIP_NO_INDEX} --find-links /tmp/wheels -r /tmp/requirements.txt && \
    rm -rf /tmp/wheels /tmp/requirements.txt

COPY --chown=app:app . /app

USER app

# Prometheus /metrics
EXPOSE 8000

# Healthcheck: el endpoint /metrics debe responder en <5s.
# Si falla 3 veces seguidas k8s/docker marca el pod como unhealthy.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/metrics')" || exit 1

CMD ["python", "-m", "processor.main"]
