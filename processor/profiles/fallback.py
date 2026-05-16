"""Perfil de respaldo para servicios sin perfil declarado.

PreParser instancia este perfil de forma defensiva: si por alguna razon
llega una linea para un servicio que no esta en `config.processor.services`
(e.g. label inesperado en Loki), se rutea aqui en vez de crashear.
"""
from __future__ import annotations

from typing import ClassVar

from processor.profiles.base import BaseProfile, ParsedLog
from processor.profiles.registry import register
from processor.settings import ProfileSpec


@register
class FallbackProfile:
    name: ClassVar[str] = "fallback"

    @classmethod
    def from_spec(cls, spec: ProfileSpec) -> "FallbackProfile":
        return cls()

    def parse(self, line: str) -> ParsedLog:
        return ParsedLog(level="unknown", message=line, profile=self.name)
