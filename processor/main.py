"""Procesador de deteccion de anomalias en logs (Cap. III).

Ejecuta un ciclo cada W minutos:
    1. Consulta Loki para cada servicio habilitado.
    2. Pre-parser por perfil (resuelto desde config).
    3. Drain extrae template_id.
    4. Calcula umbral mu +/- k*sigma sobre H dias de historico (SQLite).
    5. DBSCAN clusteriza anomalias co-ocurrentes.
    6. Publica a AlertManager (o stdout si no responde).
    7. Persiste las frecuencias de esta ventana en SQLite.

Diseno:
    - Inyeccion de dependencias: `Processor.__init__(settings, dry_run)`.
    - Error boundaries por servicio: un crash de un servicio incrementa
      ERRORS{module, service} pero no tumba el ciclo.
    - `structlog.contextvars.bound_contextvars` envuelve cada servicio,
      todos los logs heredan {service, cycle_id} sin pasarlos a mano.
    - max_workers=1 en APScheduler: no hay ciclos solapados ni contencion
      en SQLite.
    - Graceful shutdown: espera el ciclo en curso, flushea estado.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
import uuid
from collections import Counter as Counter_dict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import structlog
from apscheduler.schedulers.background import BackgroundScheduler

from processor.alert_publisher import AlertPublisher
from processor.dbscan_cluster import DBSCANClusterer
from processor.drain_parser import DrainParser
from processor.history import HistoryStore
from processor.loki_client import LokiClient
from processor.metrics import (
    ANOMALIES_DETECTED,
    CYCLE_DURATION,
    DRAIN_TEMPLATES,
    ERRORS,
    LOGS_PROCESSED,
    SERVICE_THRESHOLD_K,
    start_metrics_server,
)
from processor.pre_parser import PreParser
from processor.settings import Settings, load_settings
from processor.threshold import Anomaly, TemplateFrequency, detect_anomalies


# ----------------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------------
def configure_logging(settings: Settings) -> None:
    level_name = settings.logging.level.upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(message)s", stream=sys.stdout)

    # Silenciar loggers ruidosos de terceros (no son nuestros logs estructurados).
    for noisy in ("drain3", "httpx", "httpcore", "apscheduler"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]
    if settings.logging.format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
    )


# ----------------------------------------------------------------------------
# Procesador
# ----------------------------------------------------------------------------
class Processor:
    """Orquesta el ciclo Loki -> pre-parser -> Drain -> threshold -> DBSCAN -> alert."""

    def __init__(self, settings: Settings, dry_run: bool = False):
        self._settings = settings
        self._dry_run = dry_run
        self.log = structlog.get_logger(__name__)

        # Componentes (DI). Todos reciben `settings` o sub-cfg.
        self._loki = LokiClient(settings)
        self._drain = DrainParser(settings)
        self._history = HistoryStore(settings.history)
        self._pre_parser = PreParser(settings)
        self._clusterer = DBSCANClusterer(settings)
        self._publisher = AlertPublisher(settings, dry_run=dry_run)

        # Resolver cacheado: nombre_servicio -> (k, min_obs).
        self._thresholds: Dict[str, Tuple[float, int]] = {
            svc.name: (svc.threshold_k, svc.min_observations)
            for svc in settings.enabled_services()
        }
        # Publicar el k efectivo como gauge desde el arranque.
        for svc in settings.enabled_services():
            SERVICE_THRESHOLD_K.labels(service=svc.name).set(svc.threshold_k)

    # ------------------------------------------------------------------
    # Resolver para threshold.detect_anomalies
    # ------------------------------------------------------------------
    def _resolve(self, service: str) -> Tuple[float, int]:
        return self._thresholds.get(
            service,
            (self._settings.processor.defaults.threshold_k,
             self._settings.processor.defaults.min_observations),
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    def shutdown(self) -> None:
        self.log.info("processor_shutting_down")
        try:
            self._drain.save_all()
        except Exception as e:
            self.log.warning("drain_save_failed", error=str(e))
        try:
            self._history.close()
        except Exception as e:
            self.log.warning("history_close_failed", error=str(e))
        try:
            self._loki.close()
        except Exception as e:
            self.log.warning("loki_close_failed", error=str(e))

    # ------------------------------------------------------------------
    # Ciclo
    # ------------------------------------------------------------------
    def run_cycle(self) -> None:
        cycle_id = uuid.uuid4().hex[:8]
        cycle_start = time.monotonic()
        window_end = datetime.now(tz=timezone.utc)

        with structlog.contextvars.bound_contextvars(cycle_id=cycle_id):
            self.log.info("cycle_start", services=list(self._thresholds.keys()))
            try:
                current_freqs = self._process_all_services()
                self._update_template_metrics()
                anomalies = self._detect_anomalies_across_services(current_freqs)
                clustered = self._clusterer.cluster(anomalies)
                self._publisher.publish(clustered)
                self._update_anomaly_metrics(clustered)
                self._persist_window(window_end, current_freqs)
                self._prune_history()
            except Exception as e:
                ERRORS.labels(module="main", service="").inc()
                self.log.exception("cycle_error", error=str(e))
            finally:
                elapsed = time.monotonic() - cycle_start
                CYCLE_DURATION.observe(elapsed)
                self.log.info("cycle_done", elapsed_sec=round(elapsed, 2))

    # ------------------------------------------------------------------
    # Pasos del ciclo
    # ------------------------------------------------------------------
    def _process_all_services(self) -> List[TemplateFrequency]:
        all_freqs: List[TemplateFrequency] = []
        W = self._settings.processor.schedule_interval_minutes
        for service in self._thresholds.keys():
            with structlog.contextvars.bound_contextvars(service=service):
                try:
                    all_freqs.extend(self._process_one_service(service, W))
                except Exception as e:
                    ERRORS.labels(module="pipeline", service=service).inc()
                    self.log.warning("service_pipeline_failed", error=str(e))
        return all_freqs

    def _process_one_service(self, service: str, window_min: int) -> List[TemplateFrequency]:
        try:
            logs = self._loki.fetch_window(service, window_min)
        except Exception as e:
            ERRORS.labels(module="loki_client", service=service).inc()
            self.log.warning("fetch_failed", error=str(e))
            return []
        self.log.debug("logs_fetched", count=len(logs))

        counts: Counter_dict = Counter_dict()
        templates: Dict[int, str] = {}
        for entry in logs:
            parsed = self._pre_parser.parse(service, entry.line)
            LOGS_PROCESSED.labels(service=service, level=parsed.level).inc()
            if parsed.level == "unknown":
                continue
            result = self._drain.add(service, parsed.message)
            if result is None:
                continue
            cid = result["cluster_id"]
            counts[cid] += 1
            templates[cid] = result["template_mined"]

        return [
            TemplateFrequency(service=service, template_id=cid, template_str=templates[cid], count=cnt)
            for cid, cnt in counts.items()
        ]

    def _detect_anomalies_across_services(
        self, current: List[TemplateFrequency]
    ) -> List[Anomaly]:
        H_days = self._settings.processor.history_days
        all_history: List[List[TemplateFrequency]] = []
        for service in self._thresholds.keys():
            all_history.extend(self._history.load_history(service, H_days))
        return detect_anomalies(current, all_history, self._resolve)

    def _persist_window(self, ts: datetime, freqs: List[TemplateFrequency]) -> None:
        try:
            self._history.record_window(ts, freqs)
        except Exception as e:
            ERRORS.labels(module="history", service="").inc()
            self.log.warning("history_record_failed", error=str(e))

    def _prune_history(self) -> None:
        H_days = self._settings.processor.history_days
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=H_days)
        try:
            self._history.prune(cutoff)
        except Exception as e:
            ERRORS.labels(module="history", service="").inc()
            self.log.warning("history_prune_failed", error=str(e))

    def _update_template_metrics(self) -> None:
        for service in self._thresholds.keys():
            DRAIN_TEMPLATES.labels(service=service).set(self._drain.template_count(service))

    def _update_anomaly_metrics(self, clustered) -> None:
        for c in clustered:
            ANOMALIES_DETECTED.labels(
                service=c.anomaly.service,
                direction=c.anomaly.direction,
            ).inc()


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Procesador de deteccion de anomalias.")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Corre un ciclo, imprime las alertas que enviaria, y sale.",
    )
    p.add_argument(
        "--config",
        default="config.yaml",
        help="Ruta a config.yaml (default: config.yaml).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    settings = load_settings(args.config)
    configure_logging(settings)
    log = structlog.get_logger(__name__)

    log.info(
        "processor_starting",
        W_min=settings.processor.schedule_interval_minutes,
        H_days=settings.processor.history_days,
        defaults_k=settings.processor.defaults.threshold_k,
        services=[s.name for s in settings.enabled_services()],
        dry_run=args.dry_run,
    )

    # Healthcheck Loki antes de arrancar el scheduler.
    with LokiClient(settings) as c:
        if not c.ready():
            log.error("loki_not_ready", url=settings.loki.url)
            sys.exit(1)
    log.info("loki_ready", url=settings.loki.url)

    # Metrics server
    start_metrics_server(settings.metrics)
    log.info("metrics_listening", port=settings.metrics.port)

    proc = Processor(settings, dry_run=args.dry_run)

    if args.dry_run:
        proc.run_cycle()
        proc.shutdown()
        return

    # Scheduler
    scheduler = BackgroundScheduler(
        timezone="UTC",
        executors={"default": {"type": "threadpool", "max_workers": 1}},
    )
    scheduler.add_job(
        proc.run_cycle,
        "interval",
        minutes=settings.processor.schedule_interval_minutes,
        next_run_time=datetime.now(tz=timezone.utc),  # primer ciclo inmediato
    )
    scheduler.start()
    log.info(
        "scheduler_started",
        interval_min=settings.processor.schedule_interval_minutes,
    )

    def _shutdown(signum, frame):
        log.info("shutdown_received", signal=signum)
        try:
            scheduler.shutdown(wait=True)
        finally:
            proc.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("processor_running")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
