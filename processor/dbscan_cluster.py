"""Clustering de anomalias co-ocurrentes con DBSCAN (Cap. II II.3.3 etapa 3).

Objetivo: distinguir entre:
    - Spike aislado de un servicio (cluster_id = -1, "ruido" de DBSCAN)
    - Patron sistemico donde varios servicios fallan en la misma ventana
      con magnitud similar (cluster_id >= 0)

Features simples por anomalia: [|z_score|, direccion_codificada (1=up, -1=down)].
DBSCAN no necesita feature engineering profundo en esta etapa porque la
entrada ya esta filtrada a "anomalias relevantes" por `threshold.py`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import numpy as np
from sklearn.cluster import DBSCAN

from processor.settings import Settings
from processor.threshold import Anomaly


@dataclass
class ClusteredAnomaly:
    """Anomalia con su cluster_id asignado por DBSCAN."""
    anomaly: Anomaly
    cluster_id: int             # -1 = ruido, 0+ = miembro de grupo


class DBSCANClusterer:
    """Agrupa anomalias con DBSCAN. Stateless excepto la config inyectada."""

    def __init__(self, settings: Settings):
        cfg = settings.dbscan
        self._eps = cfg.eps
        self._min_samples = cfg.min_samples
        self._metric = cfg.metric

    def cluster(self, anomalies: List[Anomaly]) -> List[ClusteredAnomaly]:
        if not anomalies:
            return []
        if len(anomalies) < self._min_samples:
            # No alcanzan a formar cluster; todas son "ruido" individual.
            return [ClusteredAnomaly(a, cluster_id=-1) for a in anomalies]

        features = np.array(
            [[abs(a.z_score), 1.0 if a.direction == "up" else -1.0] for a in anomalies]
        )
        db = DBSCAN(eps=self._eps, min_samples=self._min_samples, metric=self._metric).fit(features)
        return [
            ClusteredAnomaly(a, cluster_id=int(lbl))
            for a, lbl in zip(anomalies, db.labels_)
        ]
