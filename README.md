# tecopos-processor

Servicio de detección de anomalías en los logs de TECOPOS. Lee los streams de logs desde Loki, extrae patrones recurrentes con [Drain3](https://github.com/logpai/Drain3), detecta outliers estadísticos con un modelo μ ± kσ, y envía alertas por Telegram.

## Cómo funciona

Cada ciclo (configurable, por defecto 5 min) el procesador:
1. Consulta Loki para cada servicio monitorizado
2. Agrupa líneas de log en plantillas (Drain3)
3. Compara la frecuencia actual contra la media histórica ± k·desviación estándar
4. Agrupa anomalías co-ocurrentes con DBSCAN
5. Envía un mensaje de Telegram si se superan los umbrales configurados

## Requisitos

- Python 3.11+
- Acceso a una instancia de Loki
- Token de bot de Telegram + chat ID (opcional pero recomendado)

## Configuración

Todos los ajustes están en `config.yaml`. Cualquier valor puede sobreescribirse con una variable de entorno usando el prefijo `PROCESSOR_` y `__` como separador de campos anidados.

| Variable | Por defecto | Descripción |
|---|---|---|
| `PROCESSOR_LOKI__URL` | — | **Requerido.** Endpoint de Loki |
| `PROCESSOR_ENV` | `production` | Perfil: `local`, `development`, `production` |
| `PROCESSOR_LOKI__STREAM_LABEL` | `service_name` | Label de Loki que identifica el servicio |
| `PROCESSOR_TELEGRAM__ENABLED` | `false` | Activar notificaciones Telegram |
| `PROCESSOR_TELEGRAM__BOT_TOKEN` | — | Token del bot de Telegram |
| `PROCESSOR_TELEGRAM__CHAT_ID` | — | Chat ID de Telegram |
| `PROCESSOR_TELEGRAM__MIN_SEVERITY` | `warning` | `warning` = todas · `critical` = solo z >= 5 |
| `PROCESSOR_ALERTMANAGER__ENABLED` | `false` | Activar envío a AlertManager |
| `PROCESSOR_ALERTMANAGER__URL` | — | Endpoint de AlertManager (requerido si enabled=true) |
| `PROCESSOR_METRICS__PORT` | `8000` | Puerto donde se exponen las métricas Prometheus |

### Perfiles de entorno

| Perfil | Ciclo | Historial | k | Observaciones mín. |
|---|---|---|---|---|
| `local` | 1 min | 1 día | 2.0 | 3 |
| `development` | 2 min | 3 días | 2.5 | 5 |
| `production` | 5 min | 7 días | 3.0 | 10 |

## Desarrollo local

```bash
python -m venv .venv
# Windows
.venv\Scripts\Activate.ps1
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # editar con la URL de Loki y las credenciales de Telegram
python -m processor.main
```

## Docker

```bash
# Build
docker build -t tecopos-processor:local .

# Ejecutar (credenciales como variables de entorno, nunca en la imagen)
docker run --rm \
  -e PROCESSOR_ENV=local \
  -e PROCESSOR_LOKI__URL=http://host.docker.internal:13100 \
  -e PROCESSOR_TELEGRAM__ENABLED=true \
  -e PROCESSOR_TELEGRAM__BOT_TOKEN=<token> \
  -e PROCESSOR_TELEGRAM__CHAT_ID=<chat_id> \
  -p 8000:8000 \
  tecopos-processor:local
```

Las métricas Prometheus se exponen en `http://localhost:8000/metrics`.

## Agregar un servicio nuevo

Solo editar `config.yaml` — sin cambios en el código:

```yaml
processor:
  services:
    - name: mi-nuevo-servicio
      profile: nestjs        # nestjs | kafkajs_json | winston_json
```

## Estructura del proyecto

```
processor/
├── processor/
│   ├── main.py               # scheduler y orquestación
│   ├── settings.py           # modelo de configuración (pydantic-settings)
│   ├── loki_client.py        # cliente HTTP para Loki
│   ├── profiles/             # parsers de logs (NestJS, Kafkajs, Winston)
│   ├── drain_parser.py       # wrapper de Drain3
│   ├── threshold.py          # detección de anomalías (media +/- k*sigma)
│   ├── dbscan_cluster.py     # clustering de co-ocurrencias
│   ├── alert_publisher.py    # webhook AlertManager
│   ├── telegram_publisher.py # notificaciones Telegram
│   └── metrics.py            # contadores Prometheus
├── config.yaml
├── requirements.txt
└── Dockerfile
```
