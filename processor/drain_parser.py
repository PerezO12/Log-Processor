"""Wrapper de drain3 con persistencia por servicio.

Cada servicio mantiene su propio TemplateMiner persistido en
`drain_state/<service>.bin`. Mezclar formatos heterogeneos en un solo
arbol Drain degrada la calidad de las plantillas, por eso separamos.

Recibe `Settings` por inyeccion (DI).
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import structlog
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

from processor.settings import Settings

log = structlog.get_logger(__name__)


class DrainParser:
    """Mantiene un TemplateMiner por servicio, con persistencia en disco."""

    def __init__(self, settings: Settings):
        self._cfg = settings.drain
        self._miners: Dict[str, TemplateMiner] = {}
        self._calls: Dict[str, int] = {}
        self._state_dir = Path(self._cfg.state_save_path)
        self._state_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Build miner
    # ------------------------------------------------------------------
    def _build_config(self) -> TemplateMinerConfig:
        cfg = TemplateMinerConfig()
        cfg.drain_depth = self._cfg.depth
        cfg.drain_sim_th = self._cfg.similarity_threshold
        cfg.drain_max_children = self._cfg.max_children
        cfg.drain_extra_delimiters = self._cfg.extra_delimiters
        cfg.snapshot_interval_minutes = 5
        cfg.snapshot_compress_state = True
        return cfg

    def _get_miner(self, service: str) -> TemplateMiner:
        if service in self._miners:
            return self._miners[service]
        state_file = self._state_dir / f"{service}.bin"
        persistence = FilePersistence(str(state_file))
        miner = TemplateMiner(persistence, self._build_config())
        self._miners[service] = miner
        self._calls[service] = 0
        log.debug("drain_miner_created", service=service, state=str(state_file))
        return miner

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    def add(self, service: str, message: str) -> Optional[dict]:
        """Procesa un mensaje y devuelve el resultado de drain3.

        Returns:
            dict con keys change_type, cluster_id, cluster_size, template_mined.
            None si el mensaje esta vacio.
        """
        if not message or not message.strip():
            return None
        miner = self._get_miner(service)
        result = miner.add_log_message(message.strip())
        self._calls[service] += 1
        if self._calls[service] % self._cfg.state_save_interval == 0:
            miner.save_state(snapshot_reason="periodic")
        return result

    def get_template(self, service: str, cluster_id: int) -> Optional[str]:
        """Devuelve la plantilla canonica de un cluster_id."""
        miner = self._miners.get(service)
        if not miner:
            return None
        cluster = miner.drain.id_to_cluster.get(cluster_id)
        return cluster.get_template() if cluster else None

    def template_count(self, service: str) -> int:
        miner = self._miners.get(service)
        return len(miner.drain.clusters) if miner else 0

    def save_all(self) -> None:
        for service, miner in self._miners.items():
            miner.save_state(snapshot_reason="shutdown")
            log.debug("drain_saved", service=service)
