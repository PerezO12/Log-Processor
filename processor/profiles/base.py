"""Contrato base para los perfiles de parsing (Strategy pattern).

Cada perfil implementa esta interfaz y se registra en `registry.py` con
`@register`. `pre_parser.PreParser` consulta el registro segun el tipo
declarado en `config.yaml` y obtiene una instancia parametrizada via
`from_spec(spec)`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional, Protocol, runtime_checkable

from processor.settings import ProfileSpec


# ----------------------------------------------------------------------------
# Normalizacion de niveles
# ----------------------------------------------------------------------------

# Niveles numericos al estilo bunyan / pino.
# Bunyan: 10=TRACE 20=DEBUG 30=INFO 40=WARN 50=ERROR 60=FATAL
# Pino usa los mismos valores. Redondeamos a la decena inferior para
# tolerar valores no estandar (p.ej. 35 → 30 → info).
_NUMERIC_LEVELS = {
    10: "debug",   # TRACE  → debug
    20: "debug",   # DEBUG
    30: "info",
    40: "warn",
    50: "error",
    60: "error",   # FATAL  → error
}

# Aliases de texto no canonicos → forma canonica.
# Las formas canonicas son: info | warn | error | debug
_STRING_ALIASES = {
    "trace":    "debug",
    "verbose":  "debug",
    "warning":  "warn",
    "critical": "error",
    "fatal":    "error",
    "severe":   "error",
    "notice":   "info",
    "log":      "info",   # NestJS LOG cuando no pasa por level_map
}


def normalize_level(raw: Any) -> str:
    """Convierte cualquier representacion de nivel al canonico.

    Formatos soportados:
        - String canonico: "info", "warn", "error", "debug"  → devuelve tal cual
        - String en mayusculas/mixto: "ERROR", "Warning"      → normaliza
        - Alias no estandar: "fatal", "critical", "verbose"   → mapea
        - Entero bunyan/pino: 30, 40, 50                       → mapea por decena
        - String numerico: "30", "50"                          → idem

    Devuelve "unknown" si el valor no es reconocible.
    """
    # Intentar conversion numerica primero (int o string de int)
    try:
        n = int(raw)
        bucket = (n // 10) * 10          # 35 → 30, 55 → 50
        return _NUMERIC_LEVELS.get(bucket) or _NUMERIC_LEVELS.get(n, "unknown")
    except (ValueError, TypeError):
        pass

    s = str(raw).lower().strip()
    if s in ("info", "warn", "error", "debug"):
        return s
    return _STRING_ALIASES.get(s, "unknown")


# ----------------------------------------------------------------------------
# Tipo de salida del parser
# ----------------------------------------------------------------------------

@dataclass
class ParsedLog:
    """Resultado de un pre-parser, comun a los tres perfiles."""
    level: str                          # info, warn, error, debug, unknown
    message: str                        # texto limpio que Drain procesa
    profile: str                        # nombre del perfil que produjo este resultado
    extras: Dict[str, Any] = None       # campos opcionales segun perfil (context, label, logger, ...)

    def __post_init__(self):
        if self.extras is None:
            self.extras = {}


@runtime_checkable
class BaseProfile(Protocol):
    """Contrato que toda implementacion de perfil debe cumplir.

    `name` es la clave bajo la cual se registra en el registry y que el
    YAML usa como `profiles.<x>.type`. `from_spec` es el factory que
    construye una instancia parametrizada a partir del `ProfileSpec`.
    """

    name: ClassVar[str]

    @classmethod
    def from_spec(cls, spec: ProfileSpec) -> "BaseProfile":
        ...

    def parse(self, line: str) -> ParsedLog:
        ...
