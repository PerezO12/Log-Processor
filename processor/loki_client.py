"""Cliente HTTP para Loki. Encapsula las queries LogQL.

Recibe `Settings` por inyeccion (DI). Implementa reintentos con backoff
exponencial via tenacity para tolerar fallos transitorios del tunel SSH
o de Loki bajo carga.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from processor.settings import Settings

log = structlog.get_logger(__name__)


@dataclass
class LogEntry:
    """Una linea de log devuelta por Loki."""
    timestamp_ns: int           # timestamp en nanosegundos (nativo de Loki)
    line: str                   # contenido crudo del log
    labels: dict                # labels del stream: {service, level, app, ...}

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)


class LokiClient:
    """Cliente sincrono para Loki."""

    def __init__(self, settings: Settings):
        cfg = settings.loki
        self._cfg = cfg
        self.base_url = cfg.url.rstrip("/")
        self._stream_label = cfg.stream_label
        self._client = httpx.Client(timeout=cfg.timeout_seconds)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "LokiClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def ready(self) -> bool:
        """Verifica que Loki responde a /ready."""
        r = self._client.get(f"{self.base_url}/ready")
        return r.status_code == 200 and r.text.strip() == "ready"

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def query_range(
        self,
        logql: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[LogEntry]:
        """Ejecuta una query LogQL para un rango temporal."""
        limit = limit or self._cfg.max_lines
        params = {
            "query": logql,
            "start": int(start.timestamp() * 1e9),
            "end": int(end.timestamp() * 1e9),
            "limit": limit,
            "direction": "forward",
            "step": self._cfg.query_step,
        }
        url = f"{self.base_url}/loki/api/v1/query_range"
        log.debug("loki_query", logql=logql, start=start.isoformat(), end=end.isoformat())
        r = self._call(url, params)
        return self._parse_streams(r.json())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _call(self, url: str, params: dict) -> httpx.Response:
        r = self._client.get(url, params=params)
        r.raise_for_status()
        return r

    @staticmethod
    def _parse_streams(payload: dict) -> List[LogEntry]:
        result: List[LogEntry] = []
        for stream in payload.get("data", {}).get("result", []):
            labels = stream.get("stream", {})
            for ts, line in stream.get("values", []):
                result.append(LogEntry(timestamp_ns=int(ts), line=line, labels=labels))
        result.sort(key=lambda e: e.timestamp_ns)
        return result

    # ------------------------------------------------------------------
    # Helpers de alto nivel
    # ------------------------------------------------------------------
    def fetch_window(self, service: str, window_minutes: int) -> List[LogEntry]:
        """Trae logs del servicio para los ultimos `window_minutes`."""
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(minutes=window_minutes)
        return self.query_range(f'{{{self._stream_label}="{service}"}}', start, end)

    def fetch_history(
        self, service: str, history_days: int, window_minutes: int
    ) -> List[LogEntry]:
        """Trae logs historicos del servicio para `history_days` dias."""
        end = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
        start = end - timedelta(days=history_days)
        return self.query_range(f'{{{self._stream_label}="{service}"}}', start, end)
