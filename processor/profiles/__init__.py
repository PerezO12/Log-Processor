"""Perfiles de parsing (Strategy pattern).

Importar este paquete carga todas las clases concretas y dispara su
registro (@register) en `PROFILE_REGISTRY`. Para anadir un perfil nuevo:

    1. Crear processor/profiles/mi_perfil.py con clase decorada @register
    2. Importar aqui (`from . import mi_perfil`)
    3. Declarar en config.yaml: `profiles.<x>: {type: mi_perfil, ...}`
"""
from processor.profiles import regex, json_path, fallback  # noqa: F401 (side-effect: registry)
from processor.profiles.base import BaseProfile, ParsedLog
from processor.profiles.registry import PROFILE_REGISTRY, register

__all__ = ["BaseProfile", "ParsedLog", "PROFILE_REGISTRY", "register"]
