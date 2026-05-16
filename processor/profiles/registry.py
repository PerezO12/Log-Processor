"""Registro global de perfiles de parsing.

Cada clase que herede de BaseProfile se registra con `@register` para
quedar accesible por su `name`. `pre_parser.PreParser` busca aqui la
clase correspondiente al tipo declarado en `config.yaml`.
"""
from __future__ import annotations

from typing import Dict, Type

from processor.profiles.base import BaseProfile


PROFILE_REGISTRY: Dict[str, Type[BaseProfile]] = {}


def register(cls: Type[BaseProfile]) -> Type[BaseProfile]:
    """Decorador: registra una clase de perfil bajo su atributo `name`."""
    key = getattr(cls, "name", None)
    if not key:
        raise ValueError(f"{cls.__name__} must define a class attribute `name`")
    if key in PROFILE_REGISTRY:
        raise ValueError(f"profile `{key}` already registered by {PROFILE_REGISTRY[key]}")
    PROFILE_REGISTRY[key] = cls
    return cls
