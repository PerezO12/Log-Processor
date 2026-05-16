"""Perfil basado en regex con grupos nombrados.

Sirve hoy para NestJS built-in. Para cualquier formato de texto plano
futuro basta declarar otro `profiles.<x>: {type: regex, pattern: ..., level_map: ...}`.

El pattern debe contener al menos los grupos nombrados:
    - `level`   (obligatorio): nivel crudo, mapeado por `level_map`
    - `message` (obligatorio): contenido a procesar por Drain

Grupos adicionales (e.g. `context`) se guardan en ParsedLog.extras.
"""
from __future__ import annotations

import re
from typing import ClassVar, Dict, Pattern

from processor.profiles.base import BaseProfile, ParsedLog
from processor.profiles.registry import register
from processor.settings import ProfileSpec


@register
class RegexProfile:
    name: ClassVar[str] = "regex"

    def __init__(self, pattern: Pattern[str], level_map: Dict[str, str]):
        self._pattern = pattern
        self._level_map = level_map

    @classmethod
    def from_spec(cls, spec: ProfileSpec) -> "RegexProfile":
        if not spec.pattern:
            raise ValueError("regex profile requires `pattern` in config")
        compiled = re.compile(spec.pattern)
        if "level" not in compiled.groupindex or "message" not in compiled.groupindex:
            raise ValueError(
                "regex profile pattern must contain named groups `level` and `message`"
            )
        return cls(compiled, spec.level_map)

    def parse(self, line: str) -> ParsedLog:
        m = self._pattern.match(line)
        if not m:
            # Linea que no encaja (stack trace, multilinea, etc.) -> unknown.
            # Drain igual la veria como ruido; preferimos no procesarla.
            return ParsedLog(level="unknown", message=line, profile=self.name)
        raw_level = m.group("level")
        extras = {
            k: v for k, v in m.groupdict().items()
            if k not in ("level", "message") and v is not None
        }
        return ParsedLog(
            level=self._level_map.get(raw_level, raw_level.lower()),
            message=m.group("message"),
            profile=self.name,
            extras=extras,
        )
