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
