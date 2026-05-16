"""Router servicio -> perfil de parsing.

Recibe `Settings` por inyeccion de dependencias. Al instanciar construye
un dict `{service_name: profile_instance}` consultando el registry. Cada
llamada a `parse()` es un lookup O(1).

Agregar un servicio NO requiere modificar este archivo: basta una entrada
en `config.processor.services` apuntando a un perfil existente.
"""
from __future__ import annotations

from typing import Dict

import structlog

from processor.profiles import PROFILE_REGISTRY, BaseProfile, ParsedLog
from processor.profiles.fallback import FallbackProfile
from processor.settings import Settings

log = structlog.get_logger(__name__)


class PreParser:
    """Selecciona el perfil correcto para cada servicio configurado."""

    def __init__(self, settings: Settings):
        self._by_service: Dict[str, BaseProfile] = {}
        self._fallback: BaseProfile = FallbackProfile()
        self._build(settings)

    def _build(self, settings: Settings) -> None:
        for svc in settings.processor.services:
            if not svc.enabled:
                continue
            spec = settings.profiles[svc.profile]
            cls = PROFILE_REGISTRY.get(spec.type)
            if cls is None:
                # No deberia ocurrir gracias al validator de Settings, pero
                # mantenemos el fallback defensivo.
                log.warning(
                    "unknown_profile_type",
                    service=svc.name,
                    profile=svc.profile,
                    type=spec.type,
                )
                self._by_service[svc.name] = self._fallback
                continue
            self._by_service[svc.name] = cls.from_spec(spec)
            log.debug(
                "profile_bound",
                service=svc.name,
                profile=svc.profile,
                type=spec.type,
            )

    def parse(self, service: str, line: str) -> ParsedLog:
        return self._by_service.get(service, self._fallback).parse(line)

    def supported_services(self) -> list[str]:
        return list(self._by_service.keys())
