# Procesador Python de deteccion de anomalias en logs.
#
# Build offline (usa wheels/ pre-descargados — no requiere internet):
#   docker build -t tecopos-processor:production .
#
# Build con PyPI (para CI/CD con internet disponible):
#   docker build --build-arg USE_PYPI=1 -t tecopos-processor:production .

FROM python:3.11-slim

# USE_PYPI=1 -> pip descarga de PyPI (default, requiere internet en build).
# USE_PYPI=0 -> pip solo usa los .whl en wheels/ (build offline sin internet).
ARG USE_PYPI=1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    # [DevOps] PROCESSOR_ENV controla el perfil de deteccion:
    #   production -> ciclo 5 min, historial 7 dias, k=3.0  (recomendado)
    #   development -> ciclo 2 min, historial 3 dias, k=2.5  (pruebas)
    # Sobreescribir en el deployment con la variable de entorno PROCESSOR_ENV.
    PROCESSOR_ENV=production

WORKDIR /app

# Usuario sin privilegios para ejecutar el proceso.
RUN useradd --create-home --shell /bin/bash app

COPY wheels/ /tmp/wheels/
COPY requirements.txt /tmp/requirements.txt

# Las lineas con "# dev" (ipython, pytest) se excluyen del build de produccion.
RUN grep -v "# dev" /tmp/requirements.txt > /tmp/requirements-prod.txt && \
    if [ "$USE_PYPI" = "1" ]; then \
        pip install --find-links /tmp/wheels -r /tmp/requirements-prod.txt; \
    else \
        pip install --no-index --find-links /tmp/wheels -r /tmp/requirements-prod.txt; \
    fi && \
    rm -rf /tmp/wheels /tmp/requirements.txt /tmp/requirements-prod.txt

COPY --chown=app:app . /app

USER app

EXPOSE 8000

CMD ["python", "-m", "processor.main"]
