"""Perfil para logs JSON (kafkajs, Winston, Pino, etc.).

Una sola clase parametrizable sirve para cualquier formato JSON donde
el nivel y el mensaje vivan en campos top-level. Los nombres se declaran
en `config.yaml`:

    profiles:
      kafkajs_json:
        type: json
        level_field: level
        message_field: message
        extra_fields: [logger, groupId, broker]
"""
from __future__ import annotations

import json
from typing import ClassVar, List

from processor.profiles.base import BaseProfile, ParsedLog, normalize_level
from processor.profiles.registry import register
from processor.settings import ProfileSpec


@register
class JsonProfile:
    name: ClassVar[str] = "json"

    def __init__(
        self,
        level_field: str,
        message_field: str,
        extra_fields: List[str],
    ):
        self._level_field = level_field
        self._message_field = message_field
        self._extra_fields = extra_fields

    @classmethod
    def from_spec(cls, spec: ProfileSpec) -> "JsonProfile":
        if not spec.level_field or not spec.message_field:
            raise ValueError(
                "json profile requires `level_field` and `message_field` in config"
            )
        return cls(spec.level_field, spec.message_field, spec.extra_fields)

    def parse(self, line: str) -> ParsedLog:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            # No es JSON parseable; deja que la linea cruda pase como unknown
            # (Drain la veria pero su `level` sera ignorado por el pipeline).
            return ParsedLog(level="unknown", message=line, profile=self.name)
        if not isinstance(obj, dict):
            return ParsedLog(level="unknown", message=line, profile=self.name)

        level_raw = obj.get(self._level_field, "unknown")
        message = obj.get(self._message_field, "")
        extras = {k: obj[k] for k in self._extra_fields if k in obj}

        return ParsedLog(
            level=normalize_level(level_raw),
            message=str(message),
            profile=self.name,
            extras=extras,
        )
