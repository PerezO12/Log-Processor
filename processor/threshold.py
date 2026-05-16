"""Deteccion de anomalias por umbrales dinamicos (Cap. II II.3.3).

Para cada plantilla observada en la ventana actual, calcula mu y sigma
sobre las ventanas historicas y marca anomalia cuando:

    f(t) > mu + k*sigma     (pico de frecuencia)
    f(t) < mu - k*sigma     (caida sospechosa)

`k` y `min_observations` se resuelven por servicio (defaults + overrides
del config.yaml) via un callable inyectado. Asi esta funcion permanece
pura, sin dependencias del modulo de settings, y es trivialmente testable.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Callable, List, Tuple


# ----------------------------------------------------------------------------
# Datos del dominio
# ----------------------------------------------------------------------------
@dataclass
class TemplateFrequency:
    """Frecuencia de una plantilla en una ventana W."""
    service: str
    template_id: int
    template_str: str
    count: int


@dataclass
class Anomaly:
    """Anomalia detectada respecto a una linea base mu/sigma."""
    service: str
    template_id: int
    template_str: str
    current: int
    mean: float
    stddev: float
    z_score: float
    direction: str          # "up" | "down"


# ----------------------------------------------------------------------------
# Algoritmo
# ----------------------------------------------------------------------------
ThresholdResolver = Callable[[str], Tuple[float, int]]
"""Funcion `service_name -> (threshold_k, min_observations)`."""


def detect_anomalies(
    current: List[TemplateFrequency],
    history: List[List[TemplateFrequency]],
    resolver: ThresholdResolver,
) -> List[Anomaly]:
    """Detecta anomalias comparando frecuencias actuales contra el historico.

    Args:
        current: frecuencias observadas en la ventana W actual.
        history: lista de ventanas previas (cada elemento = frecuencias de
                 una ventana). Sirve para calcular mu y sigma por plantilla.
        resolver: callback que devuelve (k, min_obs) efectivos por servicio.

    Returns:
        Lista de anomalias. Vacia si no hay base estadistica suficiente.
    """
    # Re-organiza el historico por clave (servicio, template_id)
    hist_by_key: dict = {}
    for window in history:
        for tf in window:
            hist_by_key.setdefault((tf.service, tf.template_id), []).append(tf.count)

    anomalies: List[Anomaly] = []
    for tf in current:
        k, min_obs = resolver(tf.service)
        samples = hist_by_key.get((tf.service, tf.template_id), [])
        if len(samples) < min_obs:
            continue
        mean = statistics.mean(samples)
        stddev = statistics.stdev(samples) if len(samples) > 1 else 0.0
        if stddev == 0.0:
            # Frecuencia historicamente constante: cualquier cambio es
            # "infinitamente" anomalo. Usamos un finito grande (no float inf)
            # para mantener numericamente estable a DBSCAN aguas abajo.
            if tf.count != mean:
                z_sentinel = 1000.0 if tf.count > mean else -1000.0
                anomalies.append(_build(tf, mean, 0.0, z_sentinel, tf.count > mean))
            continue
        z = (tf.count - mean) / stddev
        if z > k:
            anomalies.append(_build(tf, mean, stddev, z, up=True))
        elif z < -k:
            anomalies.append(_build(tf, mean, stddev, z, up=False))
    return anomalies


def _build(tf: TemplateFrequency, mean: float, stddev: float, z: float, up: bool) -> Anomaly:
    return Anomaly(
        service=tf.service,
        template_id=tf.template_id,
        template_str=tf.template_str,
        current=tf.count,
        mean=mean,
        stddev=stddev,
        z_score=z,
        direction="up" if up else "down",
    )
