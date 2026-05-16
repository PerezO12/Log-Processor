"""Metricas Prometheus expuestas por el procesador.

Las definimos a nivel de modulo (idiomatico en prometheus_client). La
funcion `start_metrics_server` recibe el `MetricsConfig` por inyeccion
y arranca el HTTP server.

Estas metricas alimentan el "Dashboard 3 — Procesador" del Cap. III.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from processor.settings import MetricsConfig


# ----------------------------------------------------------------------------
# Drain
# ----------------------------------------------------------------------------
DRAIN_TEMPLATES = Gauge(
    "drain_templates_total",
    "Numero de templates aprendidos por servicio",
    ["service"],
)

# ----------------------------------------------------------------------------
# Logs
# ----------------------------------------------------------------------------
LOGS_PROCESSED = Counter(
    "logs_processed_total",
    "Numero de logs procesados",
    ["service", "level"],
)

# ----------------------------------------------------------------------------
# Anomalias
# ----------------------------------------------------------------------------
ANOMALIES_DETECTED = Counter(
    "anomalies_detected_total",
    "Anomalias detectadas",
    ["service", "direction"],
)

# Visualiza el k efectivo por servicio (defaults vs override) en Grafana.
SERVICE_THRESHOLD_K = Gauge(
    "service_threshold_k",
    "Coeficiente k efectivo por servicio (defaults + overrides)",
    ["service"],
)

# ----------------------------------------------------------------------------
# Ciclo
# ----------------------------------------------------------------------------
CYCLE_DURATION = Histogram(
    "processor_cycle_duration_seconds",
    "Duracion de un ciclo completo",
)

# ----------------------------------------------------------------------------
# Errores por modulo Y por servicio (error boundaries granulares)
# ----------------------------------------------------------------------------
ERRORS = Counter(
    "processor_errors_total",
    "Errores en el procesador",
    ["module", "service"],
)


def start_metrics_server(cfg: MetricsConfig) -> None:
    """Levanta el HTTP server de Prometheus en background."""
    if not cfg.enabled:
        return
    start_http_server(cfg.port, addr=cfg.host)
