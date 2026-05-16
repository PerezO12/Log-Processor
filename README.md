# Procesador de detección de anomalías en logs

Procesador Python que consume logs desde **Loki** vía LogQL, extrae plantillas con el algoritmo **Drain**, calcula **umbrales dinámicos μ ± kσ** sobre un histórico configurable, agrupa anomalías co-ocurrentes con **DBSCAN**, y publica alertas a **AlertManager**. Expone métricas en `/metrics` (Prometheus).

Implementación del Cap. III de la tesis. Diseñado para ser **genérico, escalable, configurable y profesional**: agregar un servicio nuevo o un formato de log distinto se hace por configuración, sin tocar código.

---

## Tabla de contenidos

- [Arquitectura](#arquitectura)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Cómo levantar el proyecto](#cómo-levantar-el-proyecto)
- [Configuración](#configuración)
- [Cómo probarlo](#cómo-probarlo)
- [Métricas Prometheus](#métricas-prometheus)
- [Notificaciones por Telegram](#notificaciones-por-telegram)
- [Decisiones de diseño](#decisiones-de-diseño)
- [Despliegue en Docker / Kubernetes](#despliegue-en-docker--kubernetes)
- [Troubleshooting](#troubleshooting)

---

## Arquitectura

```
   ┌──────────────────────────────────────────────┐
   │  Loki  (http://localhost:13100 vía SSH)      │
   └──────────────────┬───────────────────────────┘
                      │ LogQL cada W minutos
                      ▼
        ┌──────────────────────────────────┐
        │     loki_client.py               │
        └──────────────────┬───────────────┘
                           ▼
        ┌──────────────────────────────────┐
        │     pre_parser.py  (router)      │  ──► profiles/  (Strategy)
        │                                  │      ├─ regex.py  (NestJS, ...)
        │                                  │      ├─ json_path.py (kafkajs, Winston, ...)
        │                                  │      └─ fallback.py
        └──────────────────┬───────────────┘
                           ▼
        ┌──────────────────────────────────┐
        │     drain_parser.py              │  ──► drain3 (1 miner por servicio,
        │                                  │      persistencia en drain_state/<svc>.bin)
        └──────────────────┬───────────────┘
                           ▼
        ┌──────────────────────────────────┐
        │     threshold.py                 │  ──► history.py (SQLite WAL)
        │     f(t) ⋛ μ ± k·σ por servicio  │
        └──────────────────┬───────────────┘
                           ▼
        ┌──────────────────────────────────┐
        │     dbscan_cluster.py            │
        │     agrupa anomalías             │
        └──────────────────┬───────────────┘
                           ▼
        ┌──────────────────────────────────┐
        │     alert_publisher.py           │  ──►  AlertManager (POST v2)
        │                                  │       ó stdout estructurado (fallback)
        └──────────────────────────────────┘

         ┌─────────────────────────────────┐
         │  metrics.py  →  :8000/metrics   │  (Prometheus)
         └─────────────────────────────────┘
```

---

## Estructura del proyecto

```
processor/
├── processor/                       paquete Python
│   ├── settings.py                  pydantic + cross-validation + resolver
│   ├── loki_client.py               cliente HTTP a Loki con retry
│   ├── pre_parser.py                router servicio → perfil (O(1))
│   ├── profiles/                    Strategy pattern
│   │   ├── base.py                  Protocol + ParsedLog
│   │   ├── registry.py              PROFILE_REGISTRY + @register
│   │   ├── regex.py                 RegexProfile (NestJS, etc.)
│   │   ├── json_path.py             JsonProfile (kafkajs, Winston, etc.)
│   │   └── fallback.py              FallbackProfile
│   ├── drain_parser.py              drain3 wrapper, 1 miner/servicio
│   ├── threshold.py                 detect_anomalies (función pura, resolver inyectado)
│   ├── dbscan_cluster.py            DBSCANClusterer
│   ├── history.py                   SQLite WAL para μ/σ persistente
│   ├── alert_publisher.py           AlertManager v2 + dry-run + stdout fallback
│   ├── metrics.py                   Prometheus gauges/counters/histograms
│   └── main.py                      orquestador + scheduler + CLI
├── tests/                           pytest (27 tests)
│   ├── test_settings_validation.py
│   ├── test_profiles.py
│   ├── test_threshold.py
│   └── test_history.py
├── drain_state/                     (runtime) drain miners + history.db
├── wheels/                          (offline) wheels pre-descargados
├── config.yaml                      configuración del procesador
├── requirements.txt                 dependencias pinneadas
├── Dockerfile                       build online u offline
├── LICENSE
└── README.md
```

---

## Cómo levantar el proyecto

### Requisitos

- **Python 3.11+** (`python --version`)
- **OpenSSH client** (incluido en Windows 10/11, Linux y macOS)
- **Acceso al servidor con Loki** (sandbox Hetzner: `root@204.168.195.31`)
- Editor recomendado: **VSCode** con extensión Python

### 1. Clonar y entrar al directorio

```bash
cd processor
```

### 2. Crear y activar entorno virtual

**Windows PowerShell:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# Si PowerShell bloquea el script:
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

**Linux/macOS:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Instalar dependencias

**Con conexión a internet:**
```bash
pip install -r requirements.txt
```

**Sin internet (desde wheels pre-descargados):**
```bash
pip install --no-index --find-links wheels -r requirements.txt
```

Verifica la instalación:
```bash
python -c "import drain3, sklearn, httpx, pydantic, apscheduler, structlog, prometheus_client; print('OK')"
```

### 4. Abrir túnel SSH hacia Loki (solo si Loki es remoto)

En una terminal aparte, dejarla abierta durante todo el desarrollo:

```bash
ssh -L 13100:localhost:3100 root@204.168.195.31
```

> Se usa `13100` en local porque Windows tiene los puertos `3000-3999` reservados por Hyper-V/Docker Desktop. Cualquier puerto alto sirve.

Verifica que el túnel está vivo:
```bash
curl http://localhost:13100/ready
# debe responder: ready
```

### 5. Ejecutar el procesador

**Dry-run** (un único ciclo, imprime alertas hipotéticas y sale — útil para validar configuración):
```bash
python -m processor.main --dry-run
```

**Modo normal** (ciclo cada W minutos, Ctrl+C para detener):
```bash
python -m processor.main
```

Salida esperada (modo normal):
```
[info] processor_starting     W_min=5 H_days=7 defaults_k=3.0 services=[...]
[info] loki_ready             url=http://localhost:13100
[info] metrics_listening      port=8000
[info] scheduler_started      interval_min=5
[info] cycle_start            cycle_id=a2184344
[info] cycle_done             cycle_id=a2184344 elapsed_sec=1.91
```

---

## Configuración

Toda la configuración vive en **`config.yaml`**. Cualquier campo puede sobreescribirse con variables de entorno con prefijo `PROCESSOR_` y separador `__` para anidados (RNF-04 — env vars tienen mayor prioridad que el YAML).

**Ejemplos:**

```bash
# Cambiar URL de Loki
export PROCESSOR_LOKI__URL=http://loki.internal:3100

# Hacer el procesador más sensible (k=2.0 en vez de 3.0)
export PROCESSOR_PROCESSOR__DEFAULTS__THRESHOLD_K=2.0

# Cambiar a logs JSON para producción
export PROCESSOR_LOGGING__FORMAT=json
```

### Agregar un servicio nuevo (sin tocar código)

Editar `config.yaml`:

```yaml
processor:
  services:
    - {name: apollo, profile: nestjs}                    # nuevo, default k=3.0
    - {name: hermes-server, profile: kafkajs_json,       # nuevo
       overrides: {threshold_k: 4.0}}                    # con k más permisivo
```

### Agregar un formato de log nuevo

1. Crear `processor/profiles/mi_formato.py` con clase decorada `@register`:

```python
from processor.profiles.base import ParsedLog
from processor.profiles.registry import register
from processor.settings import ProfileSpec

@register
class MiProfile:
    name = "mi_formato"
    @classmethod
    def from_spec(cls, spec: ProfileSpec): ...
    def parse(self, line: str) -> ParsedLog: ...
```

2. Importarlo en `processor/profiles/__init__.py` para que dispare el registro.
3. Declararlo en `config.yaml`:
```yaml
profiles:
  mi_formato:
    type: mi_formato
    # campos específicos de tu formato
```

---

## Cómo probarlo

### Tests unitarios

```bash
pytest tests/ -v
```

Salida esperada (27 tests cubren settings cross-validation, perfiles, threshold con resolver, history SQLite):

```
tests/test_history.py ............ 6 passed
tests/test_profiles.py ............ 11 passed
tests/test_settings_validation.py ............ 5 passed
tests/test_threshold.py ............ 5 passed
============ 27 passed in 0.28s ============
```

### Smoke test E2E (un ciclo real contra Loki)

```bash
# Con el túnel SSH activo:
python -m processor.main --dry-run
```

Debe procesar todos los servicios habilitados, imprimir las alertas hipotéticas en JSON y salir 0.

### Verificar métricas en vivo

En una terminal, ejecuta el procesador en modo normal:
```bash
python -m processor.main
```

En otra, después de ~15s:
```bash
curl -s http://localhost:8000/metrics | grep -E "drain_templates_total|service_threshold_k|logs_processed_total"
```

Deberías ver contadores como:
```
drain_templates_total{service="gateway"} 5.0
service_threshold_k{service="gateway"} 2.5
service_threshold_k{service="midas-dev"} 4.0
logs_processed_total{level="error",service="gateway"} 24.0
```

### Iteración rápida durante desarrollo

Para ver anomalías sin esperar 7 días de histórico, baja temporalmente estos valores (en `config.yaml` o vía env vars):

```yaml
processor:
  schedule_interval_minutes: 1     # ciclo cada minuto
  history_days: 1                  # 1 día de histórico
  defaults:
    min_observations: 5            # mínimas ventanas para calcular σ

logging:
  level: "DEBUG"
  format: "console"
```

Para forzar una anomalía artificialmente, reiniciar un pod en el sandbox:
```bash
ssh root@204.168.195.31 "kubectl delete pod -n tecopos-observability log-replay-gateway"
```

El procesador detectará el spike en 1-2 ciclos. Verás `anomalies_detected_total{service="gateway"}` incrementar.

---

## Métricas Prometheus

Todas en `http://localhost:8000/metrics`:

| Métrica | Tipo | Labels | Descripción |
|---|---|---|---|
| `drain_templates_total` | Gauge | `service` | Plantillas aprendidas por Drain |
| `logs_processed_total` | Counter | `service`, `level` | Logs procesados por nivel |
| `anomalies_detected_total` | Counter | `service`, `direction` | Anomalías up/down |
| `service_threshold_k` | Gauge | `service` | k efectivo por servicio (visualiza overrides) |
| `processor_cycle_duration_seconds` | Histogram | — | Duración de cada ciclo |
| `processor_errors_total` | Counter | `module`, `service` | Errores por módulo y servicio |

---

## Notificaciones por Telegram

Canal opcional paralelo a AlertManager. **Un solo mensaje por ciclo** (no spamea), con todas las anomalías agrupadas. Falla silenciosa si Telegram no responde.

### Setup (5 minutos)

**1. Crear bot.** En Telegram, abre [@BotFather](https://t.me/BotFather):
```
/newbot
<nombre del bot, ej: tecopos-anomaly-bot>
<username, debe terminar en _bot>
```
BotFather te devuelve un token tipo `123456789:ABCdefGhIJklmNoPQrsTUvwxYZ`.

**2. Obtener `chat_id`.** Abre conversación con tu bot (búscalo por username), envía `/start`. Luego visita en el navegador:
```
https://api.telegram.org/bot<TU_TOKEN>/getUpdates
```
Busca `"chat":{"id":<chat_id>,...}` en la respuesta. Es un número entero (positivo para chats privados, negativo para grupos).

**3. Exportar env vars.**

Linux/macOS:
```bash
export PROCESSOR_TELEGRAM__ENABLED=true
export PROCESSOR_TELEGRAM__BOT_TOKEN="123456789:ABC..."
export PROCESSOR_TELEGRAM__CHAT_ID="123456789"
```

Windows PowerShell:
```powershell
$env:PROCESSOR_TELEGRAM__ENABLED="true"
$env:PROCESSOR_TELEGRAM__BOT_TOKEN="123456789:ABC..."
$env:PROCESSOR_TELEGRAM__CHAT_ID="123456789"
```

**4. Arrancar el procesador** — cuando detecte anomalías recibirás un mensaje por chat.

### Configuración avanzada

En `config.yaml`:

```yaml
telegram:
  enabled: false                  # global on/off
  timeout_seconds: 10
  min_severity: "warning"         # "warning" (todas) o "critical" (|z|>=5 solamente)
```

> El token y chat_id **nunca deben ir en `config.yaml`** — usa env vars para mantenerlos fuera de git.

### Formato del mensaje (HTML)

```
Anomalias detectadas: 3

[CRIT] gateway (up)
  plantilla: login failed for <*>
  frecuencia: 200 (mu=10.0, sigma=2.0, z=95.00)
  cluster: 0

[CRIT] tecoposv1 (up)
  plantilla: db connection lost
  frecuencia: 50 (mu=5.0, sigma=1.0, z=45.00)
  cluster: 0

[WARN] midas-dev (down)
  plantilla: silence
  frecuencia: 0 (mu=100.0, sigma=10.0, z=-10.00)
  cluster: -1
```

### Probar la integración antes de producirla

```bash
export PROCESSOR_TELEGRAM__ENABLED=true
export PROCESSOR_TELEGRAM__BOT_TOKEN="..."
export PROCESSOR_TELEGRAM__CHAT_ID="..."
python -m processor.main --dry-run
```

En `--dry-run` el publisher imprime el mensaje en stdout (`telegram_dry_run`) **sin enviarlo realmente** — ideal para validar formato y filtro de severidad.

---

## Decisiones de diseño

| Decisión | Por qué |
|---|---|
| **Strategy + Registry para perfiles** | Open/Closed Principle. Agregar un formato = 1 clase + 1 entry YAML. |
| **Misma `JsonProfile` para kafkajs y Winston** | `level_field`, `message_field` declarativos. Una clase, N formatos. |
| **Inyección de dependencias (`Settings` por constructor)** | DIP (SOLID). Sin `settings` global. Testable. |
| **Cross-validation en `Settings`** | Falla en startup, no en producción. |
| **Per-service overrides de `threshold_k` / `min_observations`** | dev (ruidoso) y prod (estable) viven en el mismo binario. |
| **Histórico en SQLite con WAL + `user_version`** | Atómico, recuperable de crashes, migrable. Cero bootstrap tras reinicio. |
| **`max_workers=1` en APScheduler** | Sin ciclos solapados, sin contención en SQLite. |
| **`contextvars` con `cycle_id` y `service`** | Trazabilidad estructurada — cada log se puede filtrar por ciclo. |
| **Modo `--dry-run`** | Defensa académica sin spam a AlertManager. |
| **Env vars > YAML > defaults** | RNF-04 (Configurabilidad sin recompilación). |
| **Fallback a stdout si AlertManager down** | Sandbox no tiene AlertManager y no debe fallar el procesador. |

---

## Despliegue en Docker / Kubernetes

### Build local

```bash
# Online
docker build -t tecopos-processor:dev .

# Offline (Cuba)
docker build -t tecopos-processor:dev --build-arg PIP_NO_INDEX=--no-index .
```

### Ejecutar contenedor (apuntando a Loki del túnel SSH)

```bash
docker run --rm \
  -p 8000:8000 \
  -e PROCESSOR_LOKI__URL=http://host.docker.internal:13100 \
  -v $(pwd)/drain_state:/app/drain_state \
  tecopos-processor:dev
```

### Despliegue en k3s del sandbox

Manifiestos en `../observability/kubernetes/` (`50-processor-*.yaml`). Pasos detallados en `../observability/despliegue/2-PASOS-SERVIDOR.md`.

---

## Troubleshooting

### "0 logs encontrados" en algún servicio

1. Verificar túnel SSH: `curl http://localhost:13100/ready`
2. Verificar que el servicio existe en Loki:
   ```bash
   curl "http://localhost:13100/loki/api/v1/label/service/values"
   ```
3. Verificar nombre exacto del servicio (sin guiones extra ni espacios).

### "Drain crashea al cargar estado"

Probable causa: cambio de versión de drain3 o config incompatible.

```bash
rm -rf drain_state/*.bin
# Reiniciar el procesador — reaprende desde cero
```

### `anomalies_detected_total = 0` siempre

- **Histórico insuficiente** — bajar `min_observations` a 5 y esperar 5 ciclos.
- **σ muy alto** — bajar `threshold_k` a 2.0 (o usar override por servicio).
- **Tráfico constante** — los log-replay pods emiten patrones muy regulares. Induce una anomalía artificial (ver [Iteración rápida](#iteración-rápida-durante-desarrollo)).

### "AlertManager unreachable" en logs

**Esperado en sandbox** — no hay AlertManager desplegado. El procesador hace fallback a stdout estructurado. Buscar `anomaly_detected` en los logs para ver las anomalías.

### `max_entries_limit_per_query` exceeded

Loki del sandbox limita queries a 5000 entries. Si necesitas más, edita `loki.max_lines` en `config.yaml` (default ya es 5000).

### "Permission denied" al activar venv en PowerShell

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
```

---

## Referencias académicas

- **Cap. II II.3.3** — diseño del pipeline (W, H, k, parámetros Drain).
- **Cap. III** — implementación (este componente).
- **RNF-04** — configurabilidad sin modificar código.
- **Zhu et al. (2019)** — algoritmo Drain.
- **Jiang et al. (2024)** — evaluación de log parsing.
- **Ester et al. (1996)** — DBSCAN.

---

## Licencia

Ver [LICENSE](LICENSE).
