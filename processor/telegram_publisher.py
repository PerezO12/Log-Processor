"""Notificaciones push via Telegram Bot API.

Diseno:
    - Canal paralelo a AlertPublisher. No lo reemplaza.
    - Un solo mensaje por ciclo agrupando todas las anomalias (evita spam).
    - Filtro por severidad (warning | critical).
    - Falla en silencio (log warning) si Telegram esta inalcanzable; nunca
      tumba el ciclo del procesador.
    - Respeta `--dry-run`: imprime el mensaje en stdout, no envia.

Configuracion minima (env vars):
    PROCESSOR_TELEGRAM__ENABLED=true
    PROCESSOR_TELEGRAM__BOT_TOKEN=<token>
    PROCESSOR_TELEGRAM__CHAT_ID=<chat_id>
"""
from __future__ import annotations

import html
from typing import List

import httpx
import structlog

from processor.dbscan_cluster import ClusteredAnomaly
from processor.settings import Settings

log = structlog.get_logger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 4000          # margen seguro bajo el limite de 4096 de Telegram
CRITICAL_Z_THRESHOLD = 5.0


class TelegramPublisher:
    """Envia anomalias como notificacion HTML por Telegram."""

    def __init__(self, settings: Settings, dry_run: bool = False):
        cfg = settings.telegram
        self._token = cfg.bot_token
        self._chat_id = cfg.chat_id
        self._timeout = cfg.timeout_seconds
        self._min_severity = cfg.min_severity
        self._dry_run = dry_run

        # Habilitado solo si esta activado Y tiene credenciales.
        self._enabled = cfg.enabled and bool(self._token) and bool(self._chat_id)
        if cfg.enabled and not self._enabled:
            log.warning(
                "telegram_enabled_but_credentials_missing",
                hint="Set PROCESSOR_TELEGRAM__BOT_TOKEN and PROCESSOR_TELEGRAM__CHAT_ID",
            )

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    def publish(self, anomalies: List[ClusteredAnomaly]) -> None:
        if not self._enabled or not anomalies:
            return
        filtered = [c for c in anomalies if self._severity_passes(c)]
        if not filtered:
            return

        text = self._format_batch(filtered)
        if self._dry_run:
            log.info("telegram_dry_run", chars=len(text), preview=text[:200])
            return
        self._send(text, len(filtered))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _severity_passes(self, c: ClusteredAnomaly) -> bool:
        if self._min_severity == "critical":
            return abs(c.anomaly.z_score) >= CRITICAL_Z_THRESHOLD
        return True

    def _format_batch(self, anomalies: List[ClusteredAnomaly]) -> str:
        n = len(anomalies)
        header = f"<b>Anomalia{'s' if n > 1 else ''} detectada{'s' if n > 1 else ''}: {n}</b>"
        lines: List[str] = [header, ""]
        total_len = len(header) + 1

        for idx, c in enumerate(anomalies):
            a = c.anomaly
            sev = "CRIT" if abs(a.z_score) >= CRITICAL_Z_THRESHOLD else "WARN"
            tpl = html.escape(a.template_str[:120])
            block = (
                f"[{sev}] <b>{html.escape(a.service)}</b> ({a.direction})\n"
                f"  plantilla: <code>{tpl}</code>\n"
                f"  frecuencia: <b>{a.current}</b>  "
                f"(mu={a.mean:.1f}, sigma={a.stddev:.1f}, z={a.z_score:.2f})\n"
                f"  cluster: {c.cluster_id}\n"
            )
            if total_len + len(block) > MAX_MESSAGE_LENGTH:
                lines.append(f"...y {n - idx} mas (mensaje truncado)")
                break
            lines.append(block)
            total_len += len(block) + 1

        return "\n".join(lines)

    def _send(self, text: str, count: int) -> None:
        url = TELEGRAM_API.format(token=self._token)
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            with httpx.Client(timeout=self._timeout) as client:
                r = client.post(url, json=payload)
                if r.status_code >= 400:
                    log.warning("telegram_rejected", status=r.status_code, body=r.text[:200])
                else:
                    log.info("telegram_sent", count=count)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            log.warning("telegram_unreachable", error=str(e))
