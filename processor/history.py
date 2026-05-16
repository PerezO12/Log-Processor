"""Persistencia del historico de frecuencias en SQLite.

Justificacion:
    - Sin persistencia, cada reinicio del procesador pierde mu/sigma y
      necesita N ciclos para reconstruir la base estadistica. Con SQLite,
      el primer ciclo tras reiniciar ya tiene historico cargado.
    - SQLite esta en stdlib: cero dependencia externa.
    - PRAGMA journal_mode=WAL: escritura atomica, sobrevive crashes.
    - PRAGMA user_version: schema versioning para migraciones.
"""
from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import structlog

from processor.settings import HistoryConfig
from processor.threshold import TemplateFrequency

log = structlog.get_logger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS freq (
    service       TEXT    NOT NULL,
    template_id   INTEGER NOT NULL,
    template_str  TEXT    NOT NULL,
    window_ts     INTEGER NOT NULL,
    count         INTEGER NOT NULL,
    PRIMARY KEY (service, template_id, window_ts)
);
CREATE INDEX IF NOT EXISTS idx_freq_service_ts
    ON freq (service, window_ts);
"""


class HistoryStore:
    """Almacen de frecuencias historicas en SQLite."""

    def __init__(self, cfg: HistoryConfig):
        self._cfg = cfg
        self._path = Path(cfg.path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._open()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _open(self) -> sqlite3.Connection:
        conn = self._connect(self._path)
        try:
            current_version = conn.execute("PRAGMA user_version").fetchone()[0]
        except sqlite3.DatabaseError:
            current_version = -1

        # Schema version mismatch -> backup + recrear. Nunca eliminar silencioso.
        if current_version not in (0, self._cfg.schema_version):
            log.warning(
                "history_schema_mismatch",
                file=str(self._path),
                found=current_version,
                expected=self._cfg.schema_version,
            )
            conn.close()
            backup = self._path.with_suffix(
                f".bak-{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S')}"
            )
            shutil.move(str(self._path), str(backup))
            log.warning("history_backed_up", backup=str(backup))
            conn = self._connect(self._path)

        conn.executescript(SCHEMA_SQL)
        conn.execute(f"PRAGMA user_version = {self._cfg.schema_version}")
        conn.commit()
        return conn

    @staticmethod
    def _connect(path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "HistoryStore":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Operaciones
    # ------------------------------------------------------------------
    def record_window(
        self, window_ts: datetime, frequencies: List[TemplateFrequency]
    ) -> None:
        """Guarda las frecuencias de una ventana en una sola transaccion."""
        if not frequencies:
            return
        ts = int(window_ts.timestamp())
        rows = [
            (tf.service, tf.template_id, tf.template_str, ts, tf.count)
            for tf in frequencies
        ]
        with self._conn:
            self._conn.executemany(
                """INSERT INTO freq(service, template_id, template_str, window_ts, count)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(service, template_id, window_ts)
                   DO UPDATE SET count = excluded.count,
                                 template_str = excluded.template_str""",
                rows,
            )

    def load_history(
        self, service: str, days: int, now: datetime | None = None
    ) -> List[List[TemplateFrequency]]:
        """Carga el historico de los ultimos `days` dias para un servicio.

        Returns:
            Lista de ventanas, donde cada ventana es la lista de
            TemplateFrequency observadas en ese instante. Las ventanas
            estan ordenadas por timestamp ascendente.
        """
        now = now or datetime.now(tz=timezone.utc)
        cutoff = int((now - timedelta(days=days)).timestamp())
        cur = self._conn.execute(
            """SELECT window_ts, template_id, template_str, count
                 FROM freq
                WHERE service = ? AND window_ts >= ?
                ORDER BY window_ts ASC""",
            (service, cutoff),
        )
        by_window: Dict[int, List[TemplateFrequency]] = {}
        for ts, tid, tstr, cnt in cur:
            by_window.setdefault(ts, []).append(
                TemplateFrequency(service=service, template_id=tid, template_str=tstr, count=cnt)
            )
        return [by_window[ts] for ts in sorted(by_window)]

    def prune(self, older_than: datetime) -> int:
        """Borra registros mas viejos que `older_than`. Devuelve filas borradas."""
        cutoff = int(older_than.timestamp())
        cur = self._conn.execute("DELETE FROM freq WHERE window_ts < ?", (cutoff,))
        deleted = cur.rowcount
        if deleted:
            log.debug("history_pruned", deleted=deleted, cutoff=cutoff)
        return deleted
