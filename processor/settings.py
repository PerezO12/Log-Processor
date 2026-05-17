"""Configuracion del procesador (Cap. III, RNF-04).

Carga `config.yaml` con validacion pydantic y permite override por
variables de entorno con prefijo `PROCESSOR_` y separador `__` para anidados.

Diseno:
    - Modelos pydantic representan la estructura del YAML 1-a-1.
    - `Settings` valida que cada `service.profile` exista en `profiles:`
      (cross-validation con @model_validator) — falla rapido en arranque.
    - `Settings.resolve_service(name)` combina defaults globales + overrides
      del servicio + spec del perfil, devolviendo un `ResolvedService`. El
      resto del codigo consulta este resolver, no navega la config a mano.
    - `load_settings()` es una factory pura, sin singleton module-global.
      Solo `main()` la llama; el resto de modulos reciben `Settings`
      inyectado (Dependency Inversion Principle, SOLID).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


# ----------------------------------------------------------------------------
# Sub-modelos
# ----------------------------------------------------------------------------
class LokiConfig(BaseModel):
    url: str = "http://localhost:13100"
    query_step: str = "60s"
    max_lines: int = 10000
    timeout_seconds: int = 30


class AlertManagerConfig(BaseModel):
    url: str = "http://alertmanager:9093"
    webhook_path: str = "/api/v2/alerts"
    timeout_seconds: int = 10


class TelegramConfig(BaseModel):
    """Notificaciones push via Telegram Bot API.

    Para activar:
        1. Crear bot en @BotFather, copiar el token.
        2. Iniciar conversacion con el bot (/start).
        3. Obtener chat_id visitando
           https://api.telegram.org/bot<TOKEN>/getUpdates
        4. Exportar:
           PROCESSOR_TELEGRAM__ENABLED=true
           PROCESSOR_TELEGRAM__BOT_TOKEN=<token>
           PROCESSOR_TELEGRAM__CHAT_ID=<chat_id>
    """
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    timeout_seconds: int = 10
    min_severity: Literal["warning", "critical"] = "warning"


class ProcessorDefaults(BaseModel):
    threshold_k: float = 3.0
    min_observations: int = 10
    min_count: float = 0.0      # media minima para monitorear una plantilla
    min_delta: int = 0          # cambio absoluto minimo para reportar anomalia
    monitor_levels: List[str] = Field(
        default_factory=lambda: ["warn", "error"],
    )
    """Niveles de log que se envian a Drain para deteccion.

    Valores posibles: info, warn, error, debug.
    Por defecto solo warn+error para evitar ruido de logs operacionales INFO.
    Anadir 'info' por servicio si hay patrones INFO criticos de negocio
    (p.ej. 'Payment processed' o 'User authenticated') que quieras monitorear.
    """


class ServiceOverrides(BaseModel):
    threshold_k: Optional[float] = None
    min_observations: Optional[int] = None
    min_count: Optional[float] = None
    min_delta: Optional[int] = None
    monitor_levels: Optional[List[str]] = None


class ServiceConfig(BaseModel):
    name: str
    profile: str
    enabled: bool = True
    overrides: ServiceOverrides = Field(default_factory=ServiceOverrides)


class ProcessorConfig(BaseModel):
    schedule_interval_minutes: int = 5
    history_days: int = 7
    defaults: ProcessorDefaults = Field(default_factory=ProcessorDefaults)
    services: List[ServiceConfig] = Field(default_factory=list)


class ProfileSpec(BaseModel):
    """Spec declarativa de un perfil de parsing.

    Los campos relevantes dependen de `type`:
        - type=regex: usa `pattern` y `level_map`
        - type=json:  usa `level_field`, `message_field`, `extra_fields`
        - type=fallback: ninguno
    """
    type: Literal["regex", "json", "fallback"]
    pattern: Optional[str] = None
    level_map: Dict[str, str] = Field(default_factory=dict)
    level_field: Optional[str] = None
    message_field: Optional[str] = None
    extra_fields: List[str] = Field(default_factory=list)


class DrainConfig(BaseModel):
    depth: int = 4
    similarity_threshold: float = 0.4
    max_children: int = 100
    extra_delimiters: List[str] = Field(default_factory=list)
    state_save_path: str = "./drain_state"
    state_save_interval: int = 5


class DBSCANConfig(BaseModel):
    eps: float = 0.5
    min_samples: int = 2
    metric: str = "euclidean"


class HistoryConfig(BaseModel):
    backend: Literal["sqlite", "memory"] = "sqlite"
    path: str = "./drain_state/history.db"
    flush_every_cycles: int = 1
    schema_version: int = 1


class MetricsConfig(BaseModel):
    enabled: bool = True
    port: int = 8000
    host: str = "0.0.0.0"


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


# ----------------------------------------------------------------------------
# Resolver: vista derivada por servicio
# ----------------------------------------------------------------------------
@dataclass(frozen=True)
class ResolvedService:
    """Valores efectivos de un servicio tras aplicar defaults + overrides."""
    name: str
    profile_name: str
    profile_spec: ProfileSpec
    threshold_k: float
    min_observations: int
    min_count: float
    min_delta: int
    monitor_levels: List[str]
    enabled: bool


# ----------------------------------------------------------------------------
# Modelo root
# ----------------------------------------------------------------------------
class Settings(BaseSettings):
    """Configuracion root del procesador."""

    model_config = SettingsConfigDict(
        env_prefix="PROCESSOR_",
        env_nested_delimiter="__",
        case_sensitive=False,
        env_file=".env",                # carga .env si existe (gitignored)
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        """Prioridad: env vars > YAML (init) > defaults. (RNF-04)

        Por defecto pydantic-settings pone `init` por encima de `env`. Lo
        invertimos para que las variables de entorno PROCESSOR_* puedan
        sobreescribir cualquier valor declarado en config.yaml.
        """
        return env_settings, dotenv_settings, init_settings, file_secret_settings

    loki: LokiConfig = Field(default_factory=LokiConfig)
    alertmanager: AlertManagerConfig = Field(default_factory=AlertManagerConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    processor: ProcessorConfig = Field(default_factory=ProcessorConfig)
    profiles: Dict[str, ProfileSpec] = Field(default_factory=dict)
    drain: DrainConfig = Field(default_factory=DrainConfig)
    dbscan: DBSCANConfig = Field(default_factory=DBSCANConfig)
    history: HistoryConfig = Field(default_factory=HistoryConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @model_validator(mode="after")
    def _check_service_profiles_exist(self) -> "Settings":
        """Cada `service.profile` debe existir en `profiles:`. Falla rapido."""
        unknown = [
            svc.name for svc in self.processor.services
            if svc.profile not in self.profiles
        ]
        if unknown:
            raise ValueError(
                f"Services reference unknown profiles: {unknown}. "
                f"Available profiles: {list(self.profiles.keys())}"
            )
        return self

    def resolve_service(self, name: str) -> ResolvedService:
        """Combina defaults + overrides + profile spec para un servicio.

        Raises:
            KeyError: si el servicio no esta declarado en `processor.services`.
        """
        for svc in self.processor.services:
            if svc.name != name:
                continue
            d = self.processor.defaults
            o = svc.overrides
            return ResolvedService(
                name=svc.name,
                profile_name=svc.profile,
                profile_spec=self.profiles[svc.profile],
                threshold_k=o.threshold_k if o.threshold_k is not None else d.threshold_k,
                min_observations=(
                    o.min_observations if o.min_observations is not None else d.min_observations
                ),
                min_count=o.min_count if o.min_count is not None else d.min_count,
                min_delta=o.min_delta if o.min_delta is not None else d.min_delta,
                monitor_levels=o.monitor_levels if o.monitor_levels is not None else d.monitor_levels,
                enabled=svc.enabled,
            )
        raise KeyError(f"service not declared: {name}")

    def enabled_services(self) -> List[ResolvedService]:
        """Devuelve todos los servicios habilitados con valores efectivos."""
        return [
            self.resolve_service(svc.name)
            for svc in self.processor.services
            if svc.enabled
        ]


# ----------------------------------------------------------------------------
# Perfiles de entorno
# ----------------------------------------------------------------------------
#
# PROCESSOR_ENV controla el perfil activo. Prioridad completa (mayor primero):
#   1. Variables de entorno PROCESSOR_*  (siempre ganan sobre todo)
#   2. config.yaml (valores explicitos del usuario)
#   3. Perfil PROCESSOR_ENV (local | development | production)
#   4. Defaults de los modelos pydantic
#
# Uso:
#   PROCESSOR_ENV=local         iteracion rapida, todos los niveles, DEBUG
#   PROCESSOR_ENV=development   balance sensibilidad/ruido, INFO
#   PROCESSOR_ENV=production    conservador, solo warn+error, JSON

_ENV_PROFILES: Dict[str, Dict[str, Any]] = {
    "local": {
        # Iteracion rapida: detecta en 3 ciclos, muy sensible, verbose
        "processor": {
            "schedule_interval_minutes": 1,
            "history_days": 1,
            "defaults": {
                "threshold_k": 2.0,
                "min_observations": 3,
                "min_count": 1.0,
                "min_delta": 1,
                "monitor_levels": ["info", "warn", "error"],
            },
        },
        "logging": {"level": "DEBUG", "format": "console"},
    },
    "development": {
        # Balance: detecta en 5 ciclos, algo de ruido tolerable
        "processor": {
            "schedule_interval_minutes": 2,
            "history_days": 3,
            "defaults": {
                "threshold_k": 2.5,
                "min_observations": 5,
                "min_count": 1.5,
                "min_delta": 2,
                "monitor_levels": ["warn", "error"],
            },
        },
        "logging": {"level": "INFO", "format": "console"},
    },
    "production": {
        # Conservador: base solida, solo anomalias reales, JSON para Loki
        "processor": {
            "schedule_interval_minutes": 5,
            "history_days": 7,
            "defaults": {
                "threshold_k": 3.0,
                "min_observations": 10,
                "min_count": 3.0,
                "min_delta": 3,
                "monitor_levels": ["warn", "error"],
            },
        },
        "logging": {"level": "INFO", "format": "json"},
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Fusiona `override` sobre `base` recursivamente. `override` gana en conflictos.

    Regla: si ambos lados tienen un dict para la misma clave, se fusionan.
    En cualquier otro caso, override prevalece (o se agrega si solo esta en override).
    """
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# ----------------------------------------------------------------------------
# Factory
# ----------------------------------------------------------------------------
def _load_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(config_path: str = "config.yaml") -> Settings:
    """Carga config.yaml, aplica perfil PROCESSOR_ENV y overrides de env vars.

    Prioridad de fuentes (mayor primero, RNF-04):
        1. Variables de entorno PROCESSOR_*  (siempre ganan)
        2. config.yaml  (topologia de servicios, overrides por servicio)
        3. Perfil PROCESSOR_ENV  (algoritmo defaults segun entorno)
        4. Defaults de los modelos pydantic

    El perfil actua como base: si config.yaml declara un valor para la misma
    clave que el perfil, config.yaml gana. Esto permite que los overrides por
    servicio (p.ej. gateway threshold_k=2.5) se respeten en todos los entornos.
    """
    yaml_data = _load_yaml(config_path)

    env_name = os.environ.get("PROCESSOR_ENV", "production").lower()
    profile = _ENV_PROFILES.get(env_name, _ENV_PROFILES["production"])

    # Perfil es la BASE: yaml sobreescribe solo donde tiene valores explicitos.
    # Asi los overrides por servicio de config.yaml siempre se respetan.
    merged = _deep_merge(base=profile, override=yaml_data)

    return Settings(**merged)
