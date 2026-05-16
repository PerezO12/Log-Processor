# IMPLEMENTACION.md — Procesador de detección de anomalías

> **Documento autónomo** para implementar el procesador Python de la tesis.
> Diseñado para que tanto un humano como una IA (Claude, Copilot, etc.) puedan
> ejecutarlo de inicio a fin sin contexto previo.

---

## Sección 0 — Cómo usar este documento

### Para una persona

Leer secuencialmente. Cada **sección 4.x** implementa un módulo Python concreto.
En cada uno: leer **Objetivo**, copiar el **Código completo** al archivo indicado,
ejecutar el **Checkpoint** para verificar que el módulo funciona aislado. No avanzar
si el checkpoint falla.

### Para una IA

Ejecutar de principio a fin como tarea autónoma. Pausar **solo** en los checkpoints
para verificar el comando de validación. Si un checkpoint falla, revisar la sección
de **Troubleshooting** al final. No inventar archivos no listados en la estructura.

### Glosario rápido

| Símbolo | Significado |
|---|---|
| **W** | Ventana de análisis temporal (5 min por defecto) |
| **H** | Histórico para calcular umbral (7 días por defecto) |
| **k** | Coeficiente de desviaciones estándar (3.0 = regla 3σ) |
| **μ** | Media de frecuencia histórica de una plantilla |
| **σ** | Desviación estándar de frecuencia histórica |
| **template_id** | ID numérico que Drain asigna a un patrón de log |
| **template_str** | Plantilla canónica con `<*>` en lugar de tokens variables |
| **Loki** | Almacén de logs (Grafana Labs) — fuente de datos |
| **LogQL** | Lenguaje de queries de Loki |
| **Perfil** | Formato de log de un servicio (A=NestJS, JSON kafkajs, C=Winston JSON) |

---

## Sección 1 — Contexto y arquitectura

### Qué hace este procesador

Cada **W minutos** (5 por defecto):

1. **Consulta Loki** vía LogQL para los servicios configurados
2. **Pre-parser** limpia cada línea según el perfil del servicio (extrae level, context, message)
3. **Drain** asigna `template_id` a cada mensaje agrupando por similitud
4. **Frecuencia** cuenta ocurrencias por (servicio, template_id) en la ventana W
5. **Umbral dinámico** compara la frecuencia actual contra μ ± kσ del histórico H
6. **DBSCAN** agrupa anomalías co-ocurrentes para distinguir spike aislado vs patrón sistémico
7. **Publica** las anomalías confirmadas a AlertManager (o stdout en sandbox)
8. **Expone** métricas Prometheus en `/metrics` (puerto 8000)

### Diagrama de flujo de datos

```
                ┌──────────────────────────────────────────────┐
                │   Loki  http://localhost:13100 (SSH tunnel)  │
                │   (logs simulados del sandbox Hetzner)       │
                └──────────────────┬───────────────────────────┘
                                   │ LogQL queries (cada W min)
                                   ▼
              ┌────────────────────────────────────────────┐
              │           loki_client.py                   │
              │  - query_range para ventana W              │
              │  - query_range para histórico H            │
              └──────────────────┬─────────────────────────┘
                                 │ List[LogEntry]
                                 ▼
              ┌────────────────────────────────────────────┐
              │            pre_parser.py                   │
              │  Routing por servicio → perfil correcto    │
              │  - Perfil A NestJS  (regex)                │
              │  - Perfil JSON kafkajs (json.loads)        │
              │  - Perfil C Winston (json.loads)           │
              │  Output: {level, context?, message}        │
              └──────────────────┬─────────────────────────┘
                                 │ List[Parsed]
                                 ▼
              ┌────────────────────────────────────────────┐
              │           drain_parser.py                  │
              │  drain3.add_log_message(msg) → template_id │
              │  Persistencia: ./drain_state/<service>.bin │
              └──────────────────┬─────────────────────────┘
                                 │ {(service, template_id): count}
                                 ▼
              ┌────────────────────────────────────────────┐
              │            threshold.py                    │
              │  Calcula μ, σ del histórico H              │
              │  Detecta: f(t) > μ + kσ  ó  f(t) < μ − kσ  │
              └──────────────────┬─────────────────────────┘
                                 │ List[Anomaly]
                                 ▼
              ┌────────────────────────────────────────────┐
              │          dbscan_cluster.py                 │
              │  Agrupa anomalías por similitud temporal   │
              │  Output: anomalías con cluster_id          │
              └──────────────────┬─────────────────────────┘
                                 │
                                 ▼
              ┌────────────────────────────────────────────┐
              │          alert_publisher.py                │
              │  POST a AlertManager (o log stdout)        │
              └────────────────────────────────────────────┘

                                 ┌────────────────┐
                                 │  metrics.py    │  ← actualizada en cada paso
                                 │  /metrics:8000 │
                                 └────────────────┘
```

### Mapa de módulos y dependencias

```
main.py
  ├─ settings.py         (carga config — nadie depende de él, todos lo usan)
  ├─ loki_client.py      (depende de settings)
  ├─ pre_parser.py       (sin dependencias internas)
  ├─ drain_parser.py     (depende de settings)
  ├─ threshold.py        (depende de settings)
  ├─ dbscan_cluster.py   (depende de settings)
  ├─ alert_publisher.py  (depende de settings)
  └─ metrics.py          (sin dependencias internas)
```

### Referencias a la tesis

- **Cap. II II.3.3** — pipeline de procesamiento (W, H, k, parámetros Drain)
- **RNF-04** — todos los parámetros configurables vía env vars
- **Cap. III** — implementación del procesador (este documento es la guía)

---

## Sección 2 — Setup del entorno de desarrollo

> **Workflow elegido**: PC del autor (Windows) con venv local, queries hacia el
> Loki del sandbox Hetzner vía SSH tunnel. Iteración rápida sin tocar el server.

### 2.1 Requisitos previos

- **Python 3.11+** instalado (`python --version`)
- **OpenSSH client** (viene con Windows 10/11)
- **Acceso SSH al sandbox**: `root@204.168.195.31` con password (DevOps de TECOPOS)
- **PowerShell** (o cualquier shell con SSH)
- **Editor**: VSCode recomendado con extensión Python

### 2.2 Levantar SSH tunnel hacia Loki del sandbox

En una terminal de PowerShell (dejarla abierta durante todo el desarrollo):

```powershell
ssh -L 13100:localhost:3100 root@204.168.195.31
```

> **Por qué `13100` y no `3100`?** Windows tiene los puertos 3000-3999 reservados
> por Hyper-V/Docker Desktop. Usar puertos altos (>10000) evita el conflicto
> "bind: Permission denied".

**Verificar tunnel (en otra terminal):**

```powershell
curl http://localhost:13100/ready
```

Salida esperada:
```
ready
```

**Verificar que llegan datos del sandbox:**

```powershell
curl "http://localhost:13100/loki/api/v1/query?query={service=`"tecoposv1`"}&limit=5"
```

Debe devolver un JSON con `"status":"success"` y al menos 1 resultado.

### 2.3 Crear venv y instalar dependencias (offline desde wheels)

En PowerShell, dentro de la carpeta del proyecto:

```powershell
cd E:\TESIS\ia\processor

# Crear venv (solo la primera vez)
python -m venv .venv

# Activar venv
.\.venv\Scripts\Activate.ps1
# Si PowerShell bloquea el script:
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
#   y luego volver a activar

# Instalar dependencias desde wheels locales (sin internet)
pip install --no-index --find-links wheels -r requirements.txt
```

**Verificar instalación:**

```powershell
python -c "import drain3, sklearn, httpx, pydantic, apscheduler, structlog, prometheus_client; print('OK')"
```

Salida esperada:
```
OK
```

### 2.4 Verificar el config.yaml local

El archivo `config.yaml` ya tiene los valores correctos para desarrollo:

```yaml
loki:
  url: "http://localhost:13100"   # apunta al SSH tunnel

processor:
  schedule_interval_minutes: 5
  history_days: 7
  threshold_k: 3.0
  min_observations: 10
  services:
    - tecoposv1
    - tecoposv2
    - gateway
    - midas-dev
    - control
    - controlbeta

logging:
  level: "DEBUG"
  format: "console"
```

> **Para iteración rápida durante desarrollo**, bajar:
> `schedule_interval_minutes: 1`, `history_days: 1`, `min_observations: 5`.
> Esto hace que el procesador detecte anomalías en minutos en vez de horas.

---

## Sección 3 — Estructura de carpetas a crear

Después de implementar todos los módulos, la estructura es:

```
E:\TESIS\ia\processor\
├── processor\                       ← paquete Python (CREAR)
│   ├── __init__.py
│   ├── settings.py
│   ├── loki_client.py
│   ├── pre_parser.py
│   ├── drain_parser.py
│   ├── threshold.py
│   ├── dbscan_cluster.py
│   ├── alert_publisher.py
│   ├── metrics.py
│   └── main.py
├── tests\                           ← tests (CREAR)
│   ├── __init__.py
│   ├── test_pre_parser.py
│   └── test_drain.py
├── drain_state\                     ← se crea en runtime, NO commit
├── .venv\                           ← venv local
├── config.yaml                      ← ya existe
├── requirements.txt                 ← ya existe
├── Dockerfile                       ← ya existe
├── wheels\                          ← ya existe
├── README.md                        ← ya existe
├── IMPLEMENTACION.md                ← este archivo
└── IMPLEMENTACION-old.md.bak        ← backup del viejo
```

**Crear las carpetas vacías:**

```powershell
mkdir processor, tests, drain_state -Force
New-Item processor\__init__.py -ItemType File -Force
New-Item tests\__init__.py -ItemType File -Force
```

---

## Sección 4 — Implementación de los 9 módulos

Cada módulo sigue este patrón:
1. **Objetivo**
2. **Dependencias**
3. **Código completo** (copy-paste al archivo indicado)
4. **Checkpoint** (comando de validación)

---

### 4.1 — `processor/settings.py`

**Objetivo:** cargar `config.yaml` con validación pydantic. Permitir sobreescribir
cualquier campo con env vars (prefijo `PROCESSOR_`, separador `__`).

**Dependencias internas:** ninguna.

**Archivo:** `processor/settings.py`

```python
"""Configuracion del procesador, cargada desde config.yaml + env vars.

Cualquier valor del YAML puede sobreescribirse con una env var del mismo
nombre en MAYUSCULAS con prefijo PROCESSOR_ y separador __ para anidados.

Ejemplos:
    PROCESSOR_LOKI__URL=http://otro:3100  sobreescribe loki.url
    PROCESSOR_PROCESSOR__THRESHOLD_K=2.5  sobreescribe processor.threshold_k
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LokiConfig(BaseModel):
    url: str = "http://localhost:13100"
    query_step: str = "60s"
    max_lines: int = 10000
    timeout_seconds: int = 30


class AlertManagerConfig(BaseModel):
    url: str = "http://alertmanager:9093"
    webhook_path: str = "/api/v2/alerts"
    timeout_seconds: int = 10


class ProcessorConfig(BaseModel):
    schedule_interval_minutes: int = 5
    history_days: int = 7
    threshold_k: float = 3.0
    min_observations: int = 10
    services: List[str] = Field(default_factory=list)


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


class MetricsConfig(BaseModel):
    enabled: bool = True
    port: int = 8000
    host: str = "0.0.0.0"


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


class Settings(BaseSettings):
    """Configuracion root. Carga config.yaml por defecto."""

    model_config = SettingsConfigDict(
        env_prefix="PROCESSOR_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    loki: LokiConfig = Field(default_factory=LokiConfig)
    alertmanager: AlertManagerConfig = Field(default_factory=AlertManagerConfig)
    processor: ProcessorConfig = Field(default_factory=ProcessorConfig)
    drain: DrainConfig = Field(default_factory=DrainConfig)
    dbscan: DBSCANConfig = Field(default_factory=DBSCANConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_settings(config_path: str = "config.yaml") -> Settings:
    """Carga config.yaml y permite override desde env vars."""
    yaml_data = {}
    path = Path(config_path)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f) or {}
    return Settings(**yaml_data)


# Instancia global usada por el resto de modulos
settings = load_settings()
```

**Checkpoint:**

```powershell
python -c "from processor.settings import settings; print(settings.loki.url, settings.processor.services)"
```

Salida esperada:
```
http://localhost:13100 ['tecoposv1', 'tecoposv2', 'gateway', 'midas-dev', 'control', 'controlbeta']
```

---

### 4.2 — `processor/loki_client.py`

**Objetivo:** cliente HTTP a Loki. Métodos para query_range (ventana W) y para
histórico (H). Maneja paginación y errores con tenacity.

**Dependencias internas:** `settings`.

**Archivo:** `processor/loki_client.py`

```python
"""Cliente HTTP para Loki. Encapsula las queries LogQL."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from processor.settings import settings

log = structlog.get_logger(__name__)


@dataclass
class LogEntry:
    """Una linea de log devuelta por Loki."""
    timestamp_ns: int           # timestamp en nanosegundos (Loki nativo)
    line: str                   # contenido crudo del log
    labels: dict                # labels del stream: {service, level, app, ...}

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)


class LokiClient:
    """Cliente sincrono para Loki."""

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or settings.loki.url).rstrip("/")
        self.timeout = settings.loki.timeout_seconds
        self.max_lines = settings.loki.max_lines
        self._client = httpx.Client(timeout=self.timeout)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def ready(self) -> bool:
        """Verifica que Loki responde."""
        r = self._client.get(f"{self.base_url}/ready")
        return r.status_code == 200 and r.text.strip() == "ready"

    def query_range(
        self,
        logql: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[LogEntry]:
        """Ejecuta una query LogQL para un rango temporal.

        Args:
            logql: query LogQL ej. '{service="tecoposv1"}'
            start, end: rango temporal en UTC
            limit: maximo de lineas (default: settings.loki.max_lines)

        Returns:
            Lista de LogEntry en orden cronologico.
        """
        limit = limit or self.max_lines
        params = {
            "query": logql,
            "start": int(start.timestamp() * 1e9),
            "end": int(end.timestamp() * 1e9),
            "limit": limit,
            "direction": "forward",
            "step": settings.loki.query_step,
        }
        url = f"{self.base_url}/loki/api/v1/query_range"
        log.debug("loki_query", logql=logql, start=start.isoformat(), end=end.isoformat())

        r = self._call(url, params)
        return self._parse_streams(r.json())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _call(self, url: str, params: dict) -> httpx.Response:
        r = self._client.get(url, params=params)
        r.raise_for_status()
        return r

    @staticmethod
    def _parse_streams(payload: dict) -> List[LogEntry]:
        """Convierte la respuesta de Loki en List[LogEntry]."""
        result: List[LogEntry] = []
        data = payload.get("data", {})
        for stream in data.get("result", []):
            labels = stream.get("stream", {})
            for ts, line in stream.get("values", []):
                result.append(
                    LogEntry(
                        timestamp_ns=int(ts),
                        line=line,
                        labels=labels,
                    )
                )
        result.sort(key=lambda e: e.timestamp_ns)
        return result

    def fetch_window(self, service: str, window_minutes: int) -> List[LogEntry]:
        """Obtiene logs de un servicio para los ultimos `window_minutes`."""
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(minutes=window_minutes)
        logql = f'{{service="{service}"}}'
        return self.query_range(logql, start, end)

    def fetch_history(
        self, service: str, history_days: int, window_minutes: int
    ) -> List[LogEntry]:
        """Obtiene logs historicos de un servicio para `history_days` dias."""
        end = datetime.now(tz=timezone.utc) - timedelta(minutes=window_minutes)
        start = end - timedelta(days=history_days)
        logql = f'{{service="{service}"}}'
        return self.query_range(logql, start, end, limit=self.max_lines)
```

**Checkpoint:**

```powershell
python -c "from processor.loki_client import LokiClient; c = LokiClient(); print('ready:', c.ready()); logs = c.fetch_window('tecoposv1', 5); print('logs:', len(logs)); c.close()"
```

Salida esperada (asumiendo SSH tunnel activo):
```
ready: True
logs: 50
```

(El número de logs varía según cuánto tiempo lleve corriendo el sandbox.)

---

### 4.3 — `processor/pre_parser.py`

**Objetivo:** routing por servicio → perfil correcto → extracción de
{level, context?, message}. Devuelve el mensaje "limpio" que Drain procesa.

**Dependencias internas:** ninguna.

**Archivo:** `processor/pre_parser.py`

```python
"""Pre-parser por perfil de servicio.

Tres perfiles soportados:
- Perfil A (NestJS built-in): gateway, tecoposv2, midas-dev
- Perfil JSON (kafkajs):       control, controlbeta
- Perfil C (Winston JSON):     tecoposv1
Fallback: devuelve la linea cruda con level=unknown.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Dict, Optional

# --- Tabla de routing servicio -> perfil ---
PROFILE_A_NESTJS = {"gateway", "tecoposv2", "midas-dev"}
PROFILE_JSON_KAFKAJS = {"control", "controlbeta"}
PROFILE_C_WINSTON = {"tecoposv1"}

# --- Regex Perfil A NestJS ---
# Ejemplo: "[Nest] 46  - 05/12/2026, 8:06:34 PM   ERROR [GrpcErrorHandler] gRPC error: ..."
NESTJS_RE = re.compile(
    r"^\[Nest\]\s+\d+.*?\s+"
    r"(?P<level>LOG|WARN|ERROR|DEBUG|VERBOSE)\s+"
    r"\[(?P<context>[^\]]+)\]\s+"
    r"(?P<message>.+)$"
)

# Mapeo de niveles NestJS al estandar
NESTJS_LEVEL_MAP = {
    "LOG": "info",
    "WARN": "warn",
    "ERROR": "error",
    "DEBUG": "debug",
    "VERBOSE": "debug",
}


@dataclass
class ParsedLog:
    """Resultado del pre-parser."""
    level: str                    # info, warn, error, debug, unknown
    message: str                  # texto limpio para Drain
    context: Optional[str] = None # solo Perfil A
    label: Optional[str] = None   # solo Perfil C
    profile: str = "fallback"     # A, JSON, C, fallback


def parse(service: str, line: str) -> ParsedLog:
    """Punto de entrada: ruta segun el servicio y aplica el parser correcto."""
    if service in PROFILE_A_NESTJS:
        return _parse_nestjs(line)
    if service in PROFILE_JSON_KAFKAJS:
        return _parse_kafkajs_json(line)
    if service in PROFILE_C_WINSTON:
        return _parse_winston_json(line)
    return _fallback(line)


def _parse_nestjs(line: str) -> ParsedLog:
    """Perfil A — NestJS built-in: "[Nest] PID - ts LEVEL [ctx] msg"."""
    m = NESTJS_RE.match(line)
    if not m:
        # Stack traces o lineas sin [Nest] caen aqui
        return ParsedLog(
            level="unknown",
            message=line,
            profile="A",
        )
    return ParsedLog(
        level=NESTJS_LEVEL_MAP.get(m.group("level"), "unknown"),
        context=m.group("context"),
        message=m.group("message"),
        profile="A",
    )


def _parse_kafkajs_json(line: str) -> ParsedLog:
    """Perfil JSON — kafkajs:
    {"level":"INFO","logger":"kafkajs","message":"...","timestamp":"..."}
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return ParsedLog(level="unknown", message=line, profile="JSON")
    level = str(obj.get("level", "unknown")).lower()
    return ParsedLog(
        level=level,
        message=str(obj.get("message", "")),
        profile="JSON",
    )


def _parse_winston_json(line: str) -> ParsedLog:
    """Perfil C — Winston JSON:
    {"level":"info","message":"...","timestamp":"...","label":"..."}
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return ParsedLog(level="unknown", message=line, profile="C")
    return ParsedLog(
        level=str(obj.get("level", "unknown")).lower(),
        message=str(obj.get("message", "")),
        label=obj.get("label"),
        profile="C",
    )


def _fallback(line: str) -> ParsedLog:
    """Servicio desconocido: devolver linea cruda."""
    return ParsedLog(level="unknown", message=line, profile="fallback")
```

**Checkpoint:**

```powershell
python -c "
from processor.pre_parser import parse
print(parse('gateway', '[Nest] 46  - 05/12/2026, 8:06:34 PM   ERROR [GrpcErrorHandler] gRPC error: bad'))
print(parse('control', '{\"level\":\"INFO\",\"logger\":\"kafkajs\",\"message\":\"hi\"}'))
print(parse('tecoposv1', '{\"level\":\"warn\",\"message\":\"slow\",\"label\":\"x\"}'))
print(parse('unknown-svc', 'something raw'))
"
```

Salida esperada:
```
ParsedLog(level='error', message='gRPC error: bad', context='GrpcErrorHandler', label=None, profile='A')
ParsedLog(level='info', message='hi', context=None, label=None, profile='JSON')
ParsedLog(level='warn', message='slow', context=None, label='x', profile='C')
ParsedLog(level='unknown', message='something raw', context=None, label=None, profile='fallback')
```

---

### 4.4 — `processor/drain_parser.py`

**Objetivo:** wrapper sobre drain3 con persistencia en disco por servicio.
Devuelve el `template_id` para cada mensaje.

**Dependencias internas:** `settings`.

**Archivo:** `processor/drain_parser.py`

```python
"""Wrapper de drain3 con persistencia por servicio."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import structlog
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

from processor.settings import settings

log = structlog.get_logger(__name__)


class DrainParser:
    """Mantiene un TemplateMiner por servicio, con persistencia en disco."""

    def __init__(self):
        self._miners: Dict[str, TemplateMiner] = {}
        self._calls: Dict[str, int] = {}    # contador para save_interval
        self._state_dir = Path(settings.drain.state_save_path)
        self._state_dir.mkdir(parents=True, exist_ok=True)

    def _build_config(self) -> TemplateMinerConfig:
        cfg = TemplateMinerConfig()
        cfg.drain_depth = settings.drain.depth
        cfg.drain_sim_th = settings.drain.similarity_threshold
        cfg.drain_max_children = settings.drain.max_children
        cfg.drain_extra_delimiters = settings.drain.extra_delimiters
        cfg.snapshot_interval_minutes = 5
        cfg.snapshot_compress_state = True
        return cfg

    def _get_miner(self, service: str) -> TemplateMiner:
        """Devuelve (creando si hace falta) un TemplateMiner para el servicio."""
        if service in self._miners:
            return self._miners[service]
        state_file = self._state_dir / f"{service}.bin"
        persistence = FilePersistence(str(state_file))
        miner = TemplateMiner(persistence, self._build_config())
        self._miners[service] = miner
        self._calls[service] = 0
        log.debug("drain_miner_created", service=service, state=str(state_file))
        return miner

    def add(self, service: str, message: str) -> Optional[dict]:
        """Procesa un mensaje y devuelve el resultado de drain3.

        Returns:
            dict con keys: change_type, cluster_id, cluster_size,
                           template_mined, cluster_count
            o None si el mensaje esta vacio.
        """
        if not message or not message.strip():
            return None
        miner = self._get_miner(service)
        result = miner.add_log_message(message.strip())
        self._calls[service] += 1
        if self._calls[service] % settings.drain.state_save_interval == 0:
            miner.save_state(snapshot_reason="periodic")
        return result

    def get_template(self, service: str, cluster_id: int) -> Optional[str]:
        """Devuelve la plantilla canonica para un cluster_id."""
        miner = self._miners.get(service)
        if not miner:
            return None
        cluster = miner.drain.id_to_cluster.get(cluster_id)
        if not cluster:
            return None
        return cluster.get_template()

    def template_count(self, service: str) -> int:
        """Numero de templates aprendidos para el servicio."""
        miner = self._miners.get(service)
        if not miner:
            return 0
        return len(miner.drain.clusters)

    def save_all(self):
        """Persiste el estado de todos los miners."""
        for service, miner in self._miners.items():
            miner.save_state(snapshot_reason="shutdown")
            log.debug("drain_saved", service=service)
```

**Checkpoint:**

```powershell
python -c "
from processor.drain_parser import DrainParser
d = DrainParser()
for msg in ['login failed for user 123', 'login failed for user 456', 'login failed for user 789']:
    r = d.add('test-svc', msg)
    print(r['cluster_id'], r['template_mined'])
print('total templates:', d.template_count('test-svc'))
"
```

Salida esperada (los 3 mensajes deben agruparse en 1 cluster):
```
1 login failed for user <*>
1 login failed for user <*>
1 login failed for user <*>
total templates: 1
```

---

### 4.5 — `processor/threshold.py`

**Objetivo:** calcular μ y σ de la frecuencia de cada plantilla en el histórico,
y decidir si la frecuencia actual es anomalía (`f(t) > μ + kσ` o `f(t) < μ − kσ`).

**Dependencias internas:** `settings`.

**Archivo:** `processor/threshold.py`

```python
"""Calculo de umbrales dinamicos mu +/- k*sigma."""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import List, Tuple

from processor.settings import settings


@dataclass
class TemplateFrequency:
    """Frecuencia de una plantilla en una ventana."""
    service: str
    template_id: int
    template_str: str
    count: int               # numero de ocurrencias en la ventana actual


@dataclass
class Anomaly:
    """Anomalia detectada respecto a una linea base."""
    service: str
    template_id: int
    template_str: str
    current: int            # frecuencia actual f(t)
    mean: float             # mu del historico
    stddev: float           # sigma del historico
    z_score: float          # (current - mean) / stddev
    direction: str          # "up" o "down"


def detect_anomalies(
    current: List[TemplateFrequency],
    history: List[List[TemplateFrequency]],
) -> List[Anomaly]:
    """Detecta anomalias comparando frecuencias actuales contra el historico.

    Args:
        current: frecuencias en la ventana actual W
        history: lista de listas — cada elemento es las frecuencias de
                 una ventana W previa (forman la base para mu y sigma)

    Returns:
        Lista de Anomaly. Vacia si no hay anomalias o si no hay suficiente historico.
    """
    k = settings.processor.threshold_k
    min_obs = settings.processor.min_observations

    # Reorganizar history por (service, template_id)
    hist_by_key: dict = {}
    for window in history:
        for tf in window:
            key = (tf.service, tf.template_id)
            hist_by_key.setdefault(key, []).append(tf.count)

    anomalies: List[Anomaly] = []
    for tf in current:
        key = (tf.service, tf.template_id)
        samples = hist_by_key.get(key, [])
        if len(samples) < min_obs:
            continue                                  # no hay base estadistica
        mean = statistics.mean(samples)
        stddev = statistics.stdev(samples) if len(samples) > 1 else 0.0
        if stddev == 0:
            continue                                  # frecuencia constante, sin anomalia posible
        z = (tf.count - mean) / stddev
        if z > k:
            anomalies.append(
                Anomaly(
                    service=tf.service,
                    template_id=tf.template_id,
                    template_str=tf.template_str,
                    current=tf.count,
                    mean=mean,
                    stddev=stddev,
                    z_score=z,
                    direction="up",
                )
            )
        elif z < -k:
            anomalies.append(
                Anomaly(
                    service=tf.service,
                    template_id=tf.template_id,
                    template_str=tf.template_str,
                    current=tf.count,
                    mean=mean,
                    stddev=stddev,
                    z_score=z,
                    direction="down",
                )
            )
    return anomalies
```

**Checkpoint:**

```powershell
python -c "
from processor.threshold import TemplateFrequency, detect_anomalies
current = [TemplateFrequency('test', 1, 'login failed', 200)]
history = [[TemplateFrequency('test', 1, 'login failed', n)] for n in [10, 12, 9, 11, 10, 8, 11, 9, 12, 10, 11]]
print(detect_anomalies(current, history))
"
```

Salida esperada:
```
[Anomaly(service='test', template_id=1, template_str='login failed', current=200, mean=10.27..., stddev=1.27..., z_score=149..., direction='up')]
```

(z_score gigante porque 200 está muy lejos de la media ~10).

---

### 4.6 — `processor/dbscan_cluster.py`

**Objetivo:** agrupar anomalías co-ocurrentes con DBSCAN para distinguir spike
aislado vs patrón sistémico. Features simples: [z_score, timestamp].

**Dependencias internas:** `settings`.

**Archivo:** `processor/dbscan_cluster.py`

```python
"""Clustering de anomalias con DBSCAN.

Agrupa anomalias detectadas en la misma ventana para distinguir entre:
- Spike aislado (cluster_id = -1, ruido)
- Patron co-ocurrente entre varios servicios (cluster_id >= 0)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import numpy as np
from sklearn.cluster import DBSCAN

from processor.settings import settings
from processor.threshold import Anomaly


@dataclass
class ClusteredAnomaly:
    """Anomalia con su cluster_id asignado por DBSCAN."""
    anomaly: Anomaly
    cluster_id: int                  # -1 = ruido, 0+ = grupo


def cluster_anomalies(anomalies: List[Anomaly]) -> List[ClusteredAnomaly]:
    """Agrupa anomalias por similitud de |z_score| y direccion.

    Estrategia simple para una sola ventana:
    - Features: [|z_score|, direccion_codificada (1=up, -1=down)]
    - DBSCAN con eps y min_samples del config
    - Si hay <2 anomalias, todas son "ruido" individual (cluster_id=-1)
    """
    if not anomalies:
        return []
    if len(anomalies) < settings.dbscan.min_samples:
        return [ClusteredAnomaly(a, cluster_id=-1) for a in anomalies]

    features = np.array(
        [
            [abs(a.z_score), 1.0 if a.direction == "up" else -1.0]
            for a in anomalies
        ]
    )
    db = DBSCAN(
        eps=settings.dbscan.eps,
        min_samples=settings.dbscan.min_samples,
        metric=settings.dbscan.metric,
    ).fit(features)
    return [
        ClusteredAnomaly(a, cluster_id=int(lbl))
        for a, lbl in zip(anomalies, db.labels_)
    ]
```

**Checkpoint:**

```powershell
python -c "
from processor.threshold import Anomaly
from processor.dbscan_cluster import cluster_anomalies
anomalies = [Anomaly('s1', 1, 't1', 100, 10, 1, 90, 'up'), Anomaly('s2', 2, 't2', 100, 10, 1, 90, 'up'), Anomaly('s3', 3, 't3', 5, 50, 1, -45, 'down')]
print(cluster_anomalies(anomalies))
"
```

Salida esperada (las 2 anomalías "up" agrupadas, la "down" como ruido):
```
[ClusteredAnomaly(anomaly=Anomaly(... direction='up'), cluster_id=0), ClusteredAnomaly(anomaly=Anomaly(... direction='up'), cluster_id=0), ClusteredAnomaly(anomaly=Anomaly(... direction='down'), cluster_id=-1)]
```

---

### 4.7 — `processor/alert_publisher.py`

**Objetivo:** publicar anomalías a AlertManager vía webhook. Fallback a stdout
si AlertManager no responde (modo sandbox).

**Dependencias internas:** `settings`.

**Archivo:** `processor/alert_publisher.py`

```python
"""Publicador de anomalias.

Intenta enviar a AlertManager. Si falla (404, timeout, etc.), loguea por stdout.
En sandbox o cuando no se usa AlertManager, esto es esperado y NO es un error
critico — el objetivo es ver las anomalias detectadas.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import httpx
import structlog

from processor.dbscan_cluster import ClusteredAnomaly
from processor.settings import settings

log = structlog.get_logger(__name__)


class AlertPublisher:
    def __init__(self):
        self.base_url = settings.alertmanager.url.rstrip("/")
        self.path = settings.alertmanager.webhook_path
        self.timeout = settings.alertmanager.timeout_seconds

    def publish(self, anomalies: List[ClusteredAnomaly]) -> None:
        """Publica anomalias. Una alerta por (service, template_id)."""
        if not anomalies:
            return
        payload = [self._build_alert(c) for c in anomalies]
        try:
            with httpx.Client(timeout=self.timeout) as client:
                r = client.post(f"{self.base_url}{self.path}", json=payload)
                if r.status_code >= 400:
                    log.warning(
                        "alertmanager_rejected",
                        status=r.status_code,
                        body=r.text[:200],
                    )
                else:
                    log.info("alerts_published", count=len(payload))
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            log.warning("alertmanager_unreachable", error=str(e))
            # Fallback: log local
            self._log_local(anomalies)

    def _build_alert(self, c: ClusteredAnomaly) -> dict:
        a = c.anomaly
        return {
            "labels": {
                "alertname": "LogAnomaly",
                "service": a.service,
                "template_id": str(a.template_id),
                "direction": a.direction,
                "cluster_id": str(c.cluster_id),
                "severity": "warning" if abs(a.z_score) < 5 else "critical",
            },
            "annotations": {
                "summary": f"Frecuencia anomala en {a.service}",
                "description": (
                    f"Plantilla '{a.template_str}' tiene frecuencia {a.current} "
                    f"(historico mu={a.mean:.1f}, sigma={a.stddev:.1f}, z={a.z_score:.2f})"
                ),
                "template": a.template_str,
            },
            "startsAt": datetime.now(tz=timezone.utc).isoformat(),
        }

    def _log_local(self, anomalies: List[ClusteredAnomaly]) -> None:
        """Si AlertManager no responde, dejamos constancia en stdout."""
        for c in anomalies:
            a = c.anomaly
            log.warning(
                "anomaly_detected",
                service=a.service,
                template_id=a.template_id,
                template=a.template_str,
                current=a.current,
                mean=round(a.mean, 2),
                stddev=round(a.stddev, 2),
                z_score=round(a.z_score, 2),
                direction=a.direction,
                cluster_id=c.cluster_id,
            )
```

**Checkpoint:**

```powershell
python -c "
from processor.threshold import Anomaly
from processor.dbscan_cluster import ClusteredAnomaly
from processor.alert_publisher import AlertPublisher
a = Anomaly('gateway', 5, 'login failed for <*>', 200, 10, 2, 95, 'up')
ca = ClusteredAnomaly(a, cluster_id=0)
AlertPublisher().publish([ca])
"
```

Salida esperada (AlertManager no responde en sandbox → fallback a stdout):
```
2026-... [warning] alertmanager_unreachable error=...
2026-... [warning] anomaly_detected service=gateway template_id=5 ...
```

---

### 4.8 — `processor/metrics.py`

**Objetivo:** exponer métricas Prometheus. Las actualizan los demás módulos.
Endpoint en `:8000/metrics`.

**Dependencias internas:** `settings`.

**Archivo:** `processor/metrics.py`

```python
"""Metricas Prometheus expuestas por el procesador."""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from processor.settings import settings

# Templates aprendidos por servicio (gauge, actualizado en cada ciclo)
DRAIN_TEMPLATES = Gauge(
    "drain_templates_total",
    "Numero de templates aprendidos por servicio",
    ["service"],
)

# Logs procesados por servicio (counter incremental)
LOGS_PROCESSED = Counter(
    "logs_processed_total",
    "Numero de logs procesados",
    ["service", "level"],
)

# Anomalias detectadas (counter)
ANOMALIES_DETECTED = Counter(
    "anomalies_detected_total",
    "Anomalias detectadas",
    ["service", "direction"],
)

# Duracion de cada ciclo (histogram)
CYCLE_DURATION = Histogram(
    "processor_cycle_duration_seconds",
    "Duracion de un ciclo completo",
)

# Errores por modulo (counter)
ERRORS = Counter(
    "processor_errors_total",
    "Errores en el procesador",
    ["module"],
)


def start_metrics_server():
    """Levanta el HTTP server de Prometheus en background."""
    if not settings.metrics.enabled:
        return
    start_http_server(settings.metrics.port, addr=settings.metrics.host)
```

**Checkpoint:**

```powershell
python -c "
from processor.metrics import start_metrics_server, DRAIN_TEMPLATES
start_metrics_server()
DRAIN_TEMPLATES.labels(service='tecoposv1').set(5)
import time; print('listening on :8000/metrics for 3 seconds'); time.sleep(3)
"
```

En otra terminal mientras corre:
```powershell
curl http://localhost:8000/metrics | Select-String drain_templates
```

Salida esperada:
```
drain_templates_total{service="tecoposv1"} 5.0
```

---

### 4.9 — `processor/main.py`

**Objetivo:** orquestador. Cada `W` minutos: trae logs, parsea, drainea, calcula
umbrales, clusteriza, publica. Mantiene métricas Prometheus actualizadas.

**Dependencias internas:** todas las anteriores.

**Archivo:** `processor/main.py`

```python
"""Procesador de deteccion de anomalias en logs (Cap. III).

Ejecuta un ciclo cada W minutos:
    1. Consulta Loki para cada servicio
    2. Pre-parser por perfil (A NestJS / JSON kafkajs / C Winston)
    3. Drain extrae template_id
    4. Calcula umbral mu +/- k*sigma sobre H dias de historico
    5. DBSCAN clusteriza anomalias
    6. Publica a AlertManager (fallback: stdout)
"""
from __future__ import annotations

import logging
import signal
import sys
import time
from collections import Counter as Counter_dict
from datetime import datetime, timedelta, timezone
from typing import List

import structlog
from apscheduler.schedulers.background import BackgroundScheduler

from processor.alert_publisher import AlertPublisher
from processor.dbscan_cluster import cluster_anomalies
from processor.drain_parser import DrainParser
from processor.loki_client import LokiClient
from processor.metrics import (
    ANOMALIES_DETECTED,
    CYCLE_DURATION,
    DRAIN_TEMPLATES,
    ERRORS,
    LOGS_PROCESSED,
    start_metrics_server,
)
from processor.pre_parser import parse
from processor.settings import settings
from processor.threshold import (
    Anomaly,
    TemplateFrequency,
    detect_anomalies,
)


# ----------------------------------------------------------------------------
# Setup de logging
# ----------------------------------------------------------------------------
def configure_logging():
    level_name = settings.logging.level.upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(level=level, format="%(message)s", stream=sys.stdout)

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]
    if settings.logging.format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
    )


configure_logging()
log = structlog.get_logger(__name__)


# ----------------------------------------------------------------------------
# Logica del ciclo
# ----------------------------------------------------------------------------
class Processor:
    def __init__(self):
        self.loki = LokiClient()
        self.drain = DrainParser()
        self.publisher = AlertPublisher()
        # Historico en memoria: por servicio, lista de Counters por ventana
        self._history: dict = {svc: [] for svc in settings.processor.services}

    def shutdown(self):
        self.drain.save_all()
        self.loki.close()

    def run_cycle(self):
        """Un ciclo completo de procesamiento."""
        cycle_start = time.monotonic()
        log.info("cycle_start", services=settings.processor.services)
        try:
            current_frequencies = self._process_current_window()
            self._update_metrics()
            anomalies = self._detect_anomalies(current_frequencies)
            clustered = cluster_anomalies(anomalies)
            self.publisher.publish(clustered)
            self._update_anomaly_metrics(clustered)
        except Exception as e:
            ERRORS.labels(module="main").inc()
            log.exception("cycle_error", error=str(e))
        finally:
            elapsed = time.monotonic() - cycle_start
            CYCLE_DURATION.observe(elapsed)
            log.info("cycle_done", elapsed_sec=round(elapsed, 2))

    def _process_current_window(self) -> List[TemplateFrequency]:
        """Procesa la ventana W actual y devuelve las frecuencias."""
        all_frequencies: List[TemplateFrequency] = []
        W = settings.processor.schedule_interval_minutes
        for service in settings.processor.services:
            try:
                logs = self.loki.fetch_window(service, W)
            except Exception as e:
                ERRORS.labels(module="loki_client").inc()
                log.warning("fetch_failed", service=service, error=str(e))
                continue
            log.debug("logs_fetched", service=service, count=len(logs))

            # Pre-parser + Drain
            counts: Counter_dict = Counter_dict()
            templates: dict = {}
            for entry in logs:
                parsed = parse(service, entry.line)
                if parsed.level == "unknown":
                    LOGS_PROCESSED.labels(service=service, level="unknown").inc()
                    continue
                LOGS_PROCESSED.labels(service=service, level=parsed.level).inc()
                result = self.drain.add(service, parsed.message)
                if result is None:
                    continue
                cid = result["cluster_id"]
                counts[cid] += 1
                templates[cid] = result["template_mined"]

            # Convertir a TemplateFrequency
            for cid, count in counts.items():
                tf = TemplateFrequency(
                    service=service,
                    template_id=cid,
                    template_str=templates[cid],
                    count=count,
                )
                all_frequencies.append(tf)

            # Anadir esta ventana al historico en memoria (limitado)
            self._add_to_history(service, [tf for tf in all_frequencies if tf.service == service])

        return all_frequencies

    def _add_to_history(self, service: str, frequencies: List[TemplateFrequency]):
        H_days = settings.processor.history_days
        W_min = settings.processor.schedule_interval_minutes
        max_windows = (H_days * 24 * 60) // W_min
        hist = self._history.setdefault(service, [])
        hist.append(frequencies)
        while len(hist) > max_windows:
            hist.pop(0)

    def _detect_anomalies(
        self, current: List[TemplateFrequency]
    ) -> List[Anomaly]:
        # Aplanar el historico de todos los servicios para detect_anomalies
        all_history: List[List[TemplateFrequency]] = []
        for service, windows in self._history.items():
            # Tomamos todas las ventanas EXCEPTO la actual (que ya esta en current)
            all_history.extend(windows[:-1] if windows else [])
        return detect_anomalies(current, all_history)

    def _update_metrics(self):
        for service in settings.processor.services:
            DRAIN_TEMPLATES.labels(service=service).set(
                self.drain.template_count(service)
            )

    def _update_anomaly_metrics(self, clustered):
        for c in clustered:
            ANOMALIES_DETECTED.labels(
                service=c.anomaly.service,
                direction=c.anomaly.direction,
            ).inc()


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------
def main():
    log.info(
        "processor_starting",
        W_min=settings.processor.schedule_interval_minutes,
        H_days=settings.processor.history_days,
        k=settings.processor.threshold_k,
        services=settings.processor.services,
    )

    # Healthcheck Loki antes de empezar
    with LokiClient() as c:
        if not c.ready():
            log.error("loki_not_ready", url=settings.loki.url)
            sys.exit(1)
    log.info("loki_ready", url=settings.loki.url)

    # Servidor Prometheus
    start_metrics_server()
    log.info("metrics_listening", port=settings.metrics.port)

    # Scheduler
    proc = Processor()
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        proc.run_cycle,
        "interval",
        minutes=settings.processor.schedule_interval_minutes,
        next_run_time=datetime.now(tz=timezone.utc),  # primer ciclo inmediato
    )
    scheduler.start()
    log.info(
        "scheduler_started",
        interval_min=settings.processor.schedule_interval_minutes,
    )

    # Graceful shutdown
    def _shutdown(signum, frame):
        log.info("shutdown_received", signal=signum)
        scheduler.shutdown(wait=False)
        proc.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("processor_running")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
```

**Checkpoint:**

```powershell
python -m processor.main
```

Salida esperada (primeros segundos):
```
2026-... [info] processor_starting W_min=5 H_days=7 k=3.0 services=['tecoposv1', ...]
2026-... [info] loki_ready url=http://localhost:13100
2026-... [info] metrics_listening port=8000
2026-... [info] scheduler_started interval_min=5
2026-... [info] processor_running
2026-... [info] cycle_start services=[...]
2026-... [debug] logs_fetched service=tecoposv1 count=...
2026-... [info] cycle_done elapsed_sec=...
```

En otra terminal:
```powershell
curl http://localhost:8000/metrics | Select-String -Pattern "drain_templates|anomalies_detected|logs_processed" | Select-Object -First 20
```

Debe mostrar counters por servicio.

`Ctrl+C` para parar limpiamente.

---

## Sección 5 — Testing

### 5.1 Smoke test E2E

Ya cubierto en el checkpoint de 4.9 — corre el procesador completo, verifica
que conecta, procesa, expone métricas.

### 5.2 Tests unitarios mínimos

**Archivo:** `tests/test_pre_parser.py`

```python
"""Tests del pre_parser para los 3 perfiles + fallback."""
from processor.pre_parser import parse


def test_nestjs_error_line():
    line = "[Nest] 46  - 05/12/2026, 8:06:34 PM   ERROR [GrpcErrorHandler] gRPC error: bad"
    p = parse("gateway", line)
    assert p.profile == "A"
    assert p.level == "error"
    assert p.context == "GrpcErrorHandler"
    assert p.message == "gRPC error: bad"


def test_nestjs_log_line():
    line = "[Nest] 1  - 05/12/2026, 8:06:34 PM     LOG [Bootstrap] starting"
    p = parse("tecoposv2", line)
    assert p.level == "info"
    assert p.context == "Bootstrap"


def test_nestjs_stack_trace_passes_as_unknown():
    line = "    at Object.callback (/usr/src/app/node_modules/x.js:1:1)"
    p = parse("gateway", line)
    assert p.level == "unknown"
    assert p.profile == "A"


def test_kafkajs_json():
    line = '{"level":"WARN","logger":"kafkajs","message":"rebalancing"}'
    p = parse("control", line)
    assert p.profile == "JSON"
    assert p.level == "warn"
    assert p.message == "rebalancing"


def test_winston_json():
    line = '{"level":"info","message":"payment ok","timestamp":"2026-05-12","label":"kafka"}'
    p = parse("tecoposv1", line)
    assert p.profile == "C"
    assert p.level == "info"
    assert p.label == "kafka"


def test_fallback_unknown_service():
    p = parse("xyz", "random line")
    assert p.profile == "fallback"
    assert p.level == "unknown"
```

**Archivo:** `tests/test_drain.py`

```python
"""Tests del wrapper de drain3."""
import shutil
from pathlib import Path

from processor.drain_parser import DrainParser
from processor.settings import settings


def setup_function():
    """Limpiar el directorio de estado antes de cada test."""
    p = Path(settings.drain.state_save_path)
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)


def test_similar_messages_one_template():
    d = DrainParser()
    msgs = [
        "user 1234 logged in",
        "user 5678 logged in",
        "user 9999 logged in",
    ]
    for m in msgs:
        d.add("test", m)
    assert d.template_count("test") == 1


def test_different_messages_multiple_templates():
    d = DrainParser()
    d.add("test", "payment success for order 1")
    d.add("test", "database connection failed")
    d.add("test", "kafka rebalance start")
    assert d.template_count("test") == 3
```

**Ejecutar tests:**

```powershell
pip install pytest
pytest tests/ -v
```

Salida esperada:
```
tests/test_pre_parser.py::test_nestjs_error_line PASSED
tests/test_pre_parser.py::test_nestjs_log_line PASSED
... (todos PASSED)
```

---

## Sección 6 — Iteración rápida durante desarrollo

Para ver anomalías rápido (sin esperar 7 días de histórico), bajar
estos valores en `config.yaml`:

```yaml
processor:
  schedule_interval_minutes: 1     # ciclo cada minuto
  history_days: 1                  # 1 dia de historico suficiente
  min_observations: 5              # ventanas minimas para calcular sigma

logging:
  level: "DEBUG"
  format: "console"
```

**Inducir anomalía artificialmente:**

```bash
# En el server Hetzner (otra sesion SSH)
kubectl delete pod -n tecopos-observability log-replay-gateway
# Espera 30s — el pod se recrea con nuevo arranque (mas logs INFO de bootstrap)
# El procesador debe detectar el spike en 1-2 ciclos
```

**Ver el resultado:**

```powershell
# Mientras el procesador corre, en otra terminal:
curl http://localhost:8000/metrics | Select-String anomalies_detected
```

---

## Sección 7 — Build de imagen Docker y deploy en sandbox

Cuando el procesador esté validado en local, construir y desplegar al sandbox:

### 7.1 Build de la imagen (en Windows con Docker Desktop)

```powershell
cd E:\TESIS\ia\processor
docker build -t tecopos-log-processor:dev .
```

### 7.2 Exportar e importar al server

```powershell
docker save tecopos-log-processor:dev -o processor.tar
scp processor.tar root@204.168.195.31:/tmp/
ssh root@204.168.195.31 "k3s ctr images import /tmp/processor.tar && rm /tmp/processor.tar"
```

### 7.3 Aplicar manifiestos en el server

```powershell
ssh root@204.168.195.31
# Una vez dentro:
cd /opt/tecopos-obs/kubernetes
kubectl apply -f 50-processor-configmap.yaml \
              -f 51-processor-pvc.yaml \
              -f 52-processor-deployment.yaml \
              -f 53-processor-service.yaml
kubectl logs -f -l app=processor -n tecopos-observability
```

Detalle completo en `observability/despliegue/2-PASOS-SERVIDOR.md`.

---

## Sección 8 — Migración a producción TECOPOS

Cuando el procesador esté funcional en sandbox, DevOps lo despliega en el
cluster k3s real de TECOPOS. Los cambios son mínimos (~5 líneas en 3 archivos):

- `image:` → registry interno de TECOPOS
- `LOKI__URL` → `http://172.16.0.21:3100`
- `storageClassName:` → `local-path` o `longhorn`

Detalle completo en `observability/para-devops/README.md`.

---

## Sección 9 — Troubleshooting

### "0 logs encontrados" en algún servicio

1. Verificar SSH tunnel: `curl http://localhost:13100/ready`
2. Verificar que el servicio existe en Loki:
   ```powershell
   curl "http://localhost:13100/loki/api/v1/label/service/values"
   ```
3. Verificar formato exacto del nombre del servicio (sin guiones extra, sin espacios).

### "Drain crashea al cargar estado"

Probable causa: cambio de versión de drain3 o config incompatible. Solución:

```powershell
Remove-Item -Recurse -Force drain_state
# Y reiniciar el procesador — reaprende desde cero
```

### `anomalies_detected_total = 0` siempre

Casos:
- **Histórico insuficiente**: bajar `min_observations` a 5 y esperar 5 ciclos
- **σ muy alto**: el tráfico real es ruidoso, k=3 es muy estricto. Bajar a `2.0`
- **Tráfico constante**: los log-replay pods emiten patrones muy regulares. Inducir anomalía como en Sección 6.

### "AlertManager unreachable" en logs

**Es esperado en sandbox** — no hay AlertManager desplegado. El procesador hace
fallback a stdout y loguea las anomalías ahí. Buscar `anomaly_detected` en
los logs del procesador para verlas.

### Memoria del procesador crece sin parar

Causas posibles:
- `max_lines` muy alto + retención larga
- Drain state corrupto

Solución temporal: reiniciar y verificar `max_lines: 10000`.

### "Permission denied" al activar venv en PowerShell

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### El SSH tunnel se cae

Recrear:
```powershell
ssh -L 13100:localhost:3100 root@204.168.195.31
```

Para que no se caiga, en el server:
```bash
# Mantener kubectl port-forward vivo dentro de tmux
tmux attach -t pf
# Si no existe:
tmux new -s pf
kubectl port-forward -n tecopos-observability svc/loki 3100:3100 --address=127.0.0.1
# Ctrl+B luego D para desacoplar
```

---

## Archivos de referencia (fuera de este documento)

| Archivo | Uso |
|---|---|
| `config.yaml` | Configuración del procesador (ya existe, no tocar estructura) |
| `requirements.txt` | Dependencias (ya pinneadas) |
| `Dockerfile` | Build de imagen (ya funcional) |
| `wheels/` | Wheels offline para build sin internet |
| `observability/kubernetes/alloy-patch-devops.alloy` | Define los perfiles de log que parseamos aquí |
| `observability/kubernetes/40-log-replay-pods.yaml` | Formato exacto de los logs que vamos a consumir |
| `observability/trazas.md` | Ejemplos reales de cada perfil |
| `observability/despliegue/2-PASOS-SERVIDOR.md` | Pasos del SSH tunnel y deploy en server |
| `observability/para-devops/README.md` | Migración a producción |

---

## Checklist final

Antes de considerar el procesador terminado, verificar:

- [ ] Las 9 implementaciones de la Sección 4 están escritas
- [ ] Cada checkpoint de la Sección 4 pasa
- [ ] `pytest tests/ -v` pasa al 100%
- [ ] `python -m processor.main` corre sin errores por al menos 10 min
- [ ] `/metrics` muestra `drain_templates_total > 0` para los 6 servicios
- [ ] El procesador detecta una anomalía artificialmente inducida (Sección 6)
- [ ] La imagen Docker se construye sin errores: `docker build -t tecopos-log-processor:dev .`
- [ ] El deploy en el sandbox funciona (Sección 7)

Cuando todos pasan → estás listo para la migración a producción TECOPOS.
