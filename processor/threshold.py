"""Deteccion de anomalias por umbrales dinamicos (Cap. II II.3.3).

Para cada plantilla observada en la ventana actual, calcula mu y sigma
sobre las ventanas historicas y marca anomalia cuando:

    f(t) > mu + k*sigma     (pico de frecuencia)
    f(t) < mu - k*sigma     (caida sospechosa)

Filtros anti-ruido adicionales (aplicados antes del umbral z):
    min_count  -- ignora plantillas cuya media historica sea menor que este
                  valor; evita que logs de arranque/config con media=1 generen
                  alertas por cualquier fluctuacion trivial.
    min_delta  -- ignora cambios cuyo valor absoluto |current - mean| sea
                  menor que este umbral; elimina el ruido de Poisson natural
                  en ventanas de 1 min (p.ej. "3 vs 2" no merece alerta).

`k`, `min_observations`, `min_count` y `min_delta` se resuelven por servicio
(defaults + overrides del config.yaml) via ThresholdParams inyectado. Asi
esta funcion permanece pura y es trivialmente testable.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Callable, List


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
# Parametros de umbral por servicio
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class ThresholdParams:
    """Parametros efectivos de deteccion para un servicio concreto.

    Attributes:
        k:                Multiplicador de sigma para el umbral (mu ± k*sigma).
        min_observations: Minimo de ventanas historicas requeridas antes de
                          empezar a comparar.
        min_count:        Media historica minima para monitorear una plantilla.
                          Plantillas con media < min_count se ignoran — suelen
                          ser logs de arranque o de configuracion con variance
                          estadisticamente irreal.
        min_delta:        Cambio absoluto minimo |current - mean| para reportar.
                          Suprime el ruido de Poisson natural en ventanas cortas
                          (p.ej. 3 vs 2 en 1 min no es una anomalia real).
    """
    k: float
    min_observations: int
    min_count: float = 0.0
    min_delta: int = 0


ThresholdResolver = Callable[[str], ThresholdParams]
"""Funcion `service_name -> ThresholdParams`."""


# ----------------------------------------------------------------------------
# Algoritmo
# ----------------------------------------------------------------------------
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
        resolver: callback que devuelve ThresholdParams efectivos por servicio.

    Returns:
        Lista de anomalias. Vacia si no hay base estadistica suficiente o
        todas las desviaciones quedan por debajo de los filtros anti-ruido.
    """
    # Re-organiza el historico por clave (servicio, template_id)
    hist_by_key: dict = {}
    for window in history:
        for tf in window:
            hist_by_key.setdefault((tf.service, tf.template_id), []).append(tf.count)

    anomalies: List[Anomaly] = []
    for tf in current:
        params = resolver(tf.service)
        samples = hist_by_key.get((tf.service, tf.template_id), [])
        if len(samples) < params.min_observations:
            continue

        mean = statistics.mean(samples)

        # Filtro 1: plantilla de muy bajo trafico (arranque, config, etc.)
        # Ignoramos si la media historica esta por debajo del umbral minimo
        # Y la observacion actual tambien lo esta — un spike a 100 en una
        # plantilla con media=1 si merece atencion.
        if params.min_count > 0 and mean < params.min_count and tf.count < params.min_count:
            continue

        stddev = statistics.stdev(samples) if len(samples) > 1 else 0.0

        if stddev == 0.0:
            # Frecuencia historicamente constante: cualquier cambio es
            # "infinitamente" anomalo. Usamos un finito grande (no float inf)
            # para mantener numericamente estable a DBSCAN aguas abajo.
            if tf.count != mean:
                delta = abs(tf.count - mean)
                # Filtro 2 (sentinel): exigir cambio absoluto minimo tambien
                # en el caso sigma=0, para no alertar por "2 vs 1".
                if params.min_delta > 0 and delta < params.min_delta:
                    continue
                z_sentinel = 1000.0 if tf.count > mean else -1000.0
                anomalies.append(_build(tf, mean, 0.0, z_sentinel, tf.count > mean))
            continue

        z = (tf.count - mean) / stddev
        if z > params.k:
            # Filtro 2: cambio absoluto minimo para reducir ruido de Poisson
            if params.min_delta > 0 and abs(tf.count - mean) < params.min_delta:
                continue
            anomalies.append(_build(tf, mean, stddev, z, up=True))
        elif z < -params.k:
            if params.min_delta > 0 and abs(tf.count - mean) < params.min_delta:
                continue
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
