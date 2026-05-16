"""Publicador de anomalias a AlertManager (API v2).

En sandbox AlertManager no esta desplegado: el cliente recibe error de
conexion y caemos a un log estructurado en stdout. No es una falla
critica del procesador.

Tambien soporta `dry_run=True`: no envia nada, devuelve las alertas como
dicts para inspeccion (usado por `python -m processor.main --dry-run`).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import httpx
import structlog

from processor.dbscan_cluster import ClusteredAnomaly
from processor.settings import Settings

log = structlog.get_logger(__name__)


class AlertPublisher:
    def __init__(self, settings: Settings, dry_run: bool = False):
        cfg = settings.alertmanager
        self._url = cfg.url.rstrip("/")
        self._path = cfg.webhook_path
        self._timeout = cfg.timeout_seconds
        self._dry_run = dry_run

    def publish(self, anomalies: List[ClusteredAnomaly]) -> List[dict]:
        """Publica anomalias y devuelve los payloads construidos."""
        if not anomalies:
            return []
        payload = [self._build_alert(c) for c in anomalies]

        if self._dry_run:
            log.info("dry_run_alerts", count=len(payload), alerts=payload)
            return payload

        try:
            with httpx.Client(timeout=self._timeout) as client:
                r = client.post(f"{self._url}{self._path}", json=payload)
                if r.status_code >= 400:
                    log.warning(
                        "alertmanager_rejected",
                        status=r.status_code,
                        body=r.text[:200],
                    )
                else:
                    log.info("alerts_published", count=len(payload))
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            log.warning("alertmanager_unreachable", error=str(e))
            self._log_local(anomalies)
        return payload

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _build_alert(self, c: ClusteredAnomaly) -> dict:
        a = c.anomaly
        severity = "warning" if abs(a.z_score) < 5 else "critical"
        return {
            "labels": {
                "alertname": "LogAnomaly",
                "service": a.service,
                "template_id": str(a.template_id),
                "direction": a.direction,
                "cluster_id": str(c.cluster_id),
                "severity": severity,
            },
            "annotations": {
                "summary": f"Frecuencia anomala en {a.service}",
                "description": (
                    f"Plantilla '{a.template_str}' tiene frecuencia {a.current} "
                    f"(historico mu={a.mean:.1f}, sigma={a.stddev:.1f}, "
                    f"z={a.z_score:.2f})"
                ),
                "template": a.template_str,
            },
            "startsAt": datetime.now(tz=timezone.utc).isoformat(),
        }

    def _log_local(self, anomalies: List[ClusteredAnomaly]) -> None:
        """Fallback cuando AlertManager no responde: log estructurado."""
        for c in anomalies:
            a = c.anomaly
            log.warning(
                "anomaly_detected",
                service=a.service,
                template_id=a.template_id,
                template=a.template_str,
                current=a.current,
                mean=round(a.mean, 2),
                stddev=round(a.stddev, 2),
                z_score=round(a.z_score, 2),
                direction=a.direction,
                cluster_id=c.cluster_id,
            )
