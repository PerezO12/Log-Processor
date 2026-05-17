# Procesador de anomalías en logs

> Un sistema que aprende qué es "normal" en tu infraestructura y te avisa cuando algo se rompe — sin que tengas que configurar una regla por cada tipo de error posible.

---

## La idea en una frase

**Imagina un vigilante que lee todos los logs de tus servicios, aprende sus patrones normales y te manda un mensaje de Telegram cuando algo se comporta de manera inusual.**

No necesitas decirle qué buscar. Él lo aprende solo.

---

## El problema que resuelve

Si tienes microservicios en producción, generás millones de líneas de log por día. Algo así:

```
[gateway]     User garcia authenticated successfully
[gateway]     GraphQL query executed: getDashboard (342ms)
[midas-dev]   Payment processed: order #8821, amount 150.00 CUP
[tecoposv2]   Webhook received from tropipay
[gateway]     User perez authenticated successfully
[control]     Kafka consumer group synced
...
```

¿Cómo sabes cuándo algo está mal? Hoy en día, tienes dos opciones:

1. **Mirar Grafana manualmente** — imposible 24/7
2. **Configurar alertas explícitas** — tienes que adivinar de antemano cada tipo de error posible

El problema es que los errores más graves son exactamente los que no anticipaste.

Este procesador resuelve eso. Aprende solo que `gateway` normalmente procesa unas 80 queries por minuto. Si de pronto procesa 4, algo pasó. Si procesa 800, algo también pasó. Y te avisa.

---

## Qué hace, paso a paso

Cada ciertos minutos (configurable: 1 min en desarrollo, 5 en producción), el procesador:

```
1. Lee los logs recientes de cada servicio desde Loki
       ↓
2. Los entiende: "este es un error de NestJS", "este es un log JSON de Kafka"
       ↓
3. Agrupa mensajes similares en categorías (plantillas)
       ↓
4. Compara: ¿cuántas veces apareció esta categoría hoy vs. el histórico?
       ↓
5. Si hay una desviación estadística significativa → anomalía detectada
       ↓
6. Agrupa las anomalías relacionadas (¿varios servicios fallando juntos?)
       ↓
7. Manda una notificación a Telegram con el resumen
```

### Un ejemplo concreto

El servicio `gateway` normalmente registra unas 3 queries lentas de GraphQL por minuto. Un martes a las 11pm:

```
[gateway] Slow GraphQL query: took 4823ms - getDashboard
[gateway] Slow GraphQL query: took 3901ms - getDashboard
[gateway] Slow GraphQL query: took 5122ms - getDashboard
[gateway] Slow GraphQL query: took 4456ms - getDashboard
[gateway] Slow GraphQL query: took 3788ms - getDashboard    ← 5 en total
```

El procesador sabe que el promedio es 3 y la desviación estándar es 0.5. Con 5 ocurrencias:

```
z = (5 - 3) / 0.5 = 4.0  →  muy por encima del umbral  →  ANOMALÍA
```

Y llega este mensaje al Telegram del equipo:

```
🚨 1 anomalía detectada · 23:14

🔴 gateway ↑ CRÍTICO
   Slow GraphQL query: took <*> - getDashboard
   5 ocurrencias · ~1.7× lo habitual (esperado ~3)
```

En producción normal, este mensaje significaría "la base de datos se está poniendo lenta, actúa antes de que los usuarios lo noten."

---

## Los componentes explicados

El sistema tiene cuatro piezas de inteligencia. Acá va cada una explicada desde cero.

---

### 🔍 Pieza 1: El pre-parser (entender el formato)

Cada servicio escribe sus logs de manera diferente:

```
# NestJS (como gateway y tecoposv2):
[Nest] 46  - 12/05/2025, 8:06:34 PM   ERROR [HttpExceptionFilter] Request failed

# JSON de kafkajs (como control):
{"level":"ERROR","logger":"kafkajs","message":"Broker disconnected","timestamp":"..."}

# JSON de Winston (como tecoposv1):
{"level":"error","message":"Payment timeout","label":"paymentGateway"}
```

El pre-parser actúa como un traductor. No importa el formato de entrada — siempre produce la misma estructura de salida:

```python
{
  "level": "error",           # normalizado: info / warn / error / debug
  "message": "Request failed" # el texto limpio que se analiza
}
```

Esto es importante porque las etapas siguientes no necesitan saber si el servicio usa NestJS, Kafka o Winston. Solo reciben `{level, message}`.

---

### 🌳 Pieza 2: Drain3 (categorizar mensajes similares)

Este es el corazón del sistema. Su trabajo: agrupar miles de líneas de log distintas en decenas de categorías (llamadas "plantillas").

**¿Por qué es necesario?**

Sin categorización, cada línea es única y no puedes sacar estadísticas:
```
Payment timeout after 2341ms
Payment timeout after 5677ms
Payment timeout after 891ms
```

Son tres líneas distintas, pero el mismo problema. Drain las convierte en:
```
Payment timeout after <*>ms     ← plantilla, aparece 3 veces
```

Ahora sí puedes preguntar: ¿cuántas veces apareció "Payment timeout" esta semana?

**¿Cómo lo hace?**

Drain usa un árbol de decisión basado en la estructura de las palabras. Para cada mensaje:

1. Mira cuántas palabras tiene
2. Compara las primeras palabras con clusters existentes
3. Si encuentra uno que comparte al menos el 40% de palabras → lo agrega ahí
4. Si no → crea un cluster nuevo

```
Mensaje: "Payment timeout after 2341ms"
         ↓
¿Existe cluster con "Payment timeout after <?>":  Sí
         ↓
Agregar al cluster_id=15
         ↓
Actualizar plantilla: "Payment timeout after <*>ms"
         ↓
count del cluster_id=15 en este ciclo: 3
```

Al final del ciclo, en vez de 5000 líneas brutas, tenemos algo como:
```
cluster_id=3   "User <*> authenticated successfully"         → 87 veces
cluster_id=7   "PostgreSQL connection established"           → 12 veces
cluster_id=15  "Payment timeout after <*>ms"                → 3 veces
cluster_id=22  "Invalid credentials for user <*>"            → 5 veces
```

**El estado de Drain se guarda en disco** (`drain_state/<servicio>.bin`). Si reinicias el procesador, no pierde el aprendizaje de plantillas — arranca donde quedó.

---

### 📊 Pieza 3: El detector μ ± kσ (¿es esto normal?)

Para cada plantilla que Drain identifica, el detector pregunta: **¿cuántas veces solía aparecer antes? ¿Y cuántas apareció ahora?**

La historia se guarda en SQLite — una base de datos liviana que vive en `drain_state/history.db`. Para cada `(servicio, plantilla, ventana_de_tiempo)` guarda el conteo.

**El cálculo:**

```
Historial de "Payment timeout" en los últimos 7 días:
[2, 1, 3, 2, 2, 1, 2, 3, 1, 2, ...]

media (μ) = 1.9
desviación estándar (σ) = 0.7

Ciclo actual: 14 ocurrencias

z = (14 - 1.9) / 0.7 = 17.3  →  esto es ANÓMALO
```

El z-score mide cuántas "desviaciones estándar" se aleja el valor actual de lo normal. Con un umbral `k=3.0` (el de producción): si `z > 3` → anomalía hacia arriba, si `z < -3` → anomalía hacia abajo.

**Casos especiales:**

- Si una plantilla siempre tuvo exactamente el mismo conteo (σ=0) y de pronto cambia, cualquier cambio es anómalo. El sistema lo marca como "NUEVO" en la notificación.
- Si una plantilla aparece muy poco en el histórico (menos de 10 ventanas), el sistema espera antes de empezar a detectar anomalías. No vale generar alertas con estadísticas de 2 puntos.

**Los filtros anti-ruido** (porque estadística pura genera ruido):

El detector aplica tres filtros adicionales antes de reportar una anomalía:

| Filtro | Qué hace | Ejemplo de lo que elimina |
|--------|----------|--------------------------|
| `monitor_levels` | Solo analiza ciertos niveles de log | Ignora "PostgreSQL connection established" (INFO) en servicios configurados solo con warn+error |
| `min_count` | Ignora plantillas con promedio muy bajo | "AppModule initialized" aparece 1 vez por arranque — no tiene sentido monitorearla |
| `min_delta` | Ignora cambios absolutos pequeños | "3 webhooks en vez de 2" — statísticamente puede ser anómalo, operacionalmente no importa |

Sin estos filtros, el sistema mandaría 8-10 alertas por ciclo. Con ellos: 1-3 alertas, todas relevantes.

---

### 🔗 Pieza 4: DBSCAN (¿varios servicios fallando juntos?)

Hay dos tipos de incidentes:

- **Aislado**: solo `gateway` tiene un problema
- **Sistémico**: `gateway` + `tecoposv2` + `midas-dev` fallan a la misma vez

El segundo tipo indica un problema en infraestructura compartida: la base de datos, la red, un servicio del que todos dependen. DBSCAN detecta este patrón.

Toma todas las anomalías de un ciclo y las agrupa según su magnitud y dirección:

```
gateway    → spike de autenticaciones fallidas   z=8.2, dirección=arriba
tecoposv2  → spike de timeouts de conexión       z=7.9, dirección=arriba
midas-dev  → caída en pagos procesados           z=6.1, dirección=abajo
```

Las dos primeras tienen magnitudes muy similares y la misma dirección → las agrupa como "co-ocurrentes". En el mensaje de Telegram aparecen marcadas con `↳ co-ocurre con otras de este ciclo`.

Si ves ese indicador, la causa probable no está en un servicio individual sino en algo compartido.

---

## La arquitectura completa

```
┌──────────────────────────────────────────────────────┐
│                   INFRAESTRUCTURA                     │
│                                                        │
│  Kubernetes / Docker                                   │
│  ├── 6 microservicios (tecoposv1, tecoposv2, ...)     │
│  │     └── escriben logs a stdout                     │
│  ├── Grafana Alloy  → recoge los logs                 │
│  └── Loki           → los almacena y expone via API   │
└──────────────────────────────────────────────────────┘
                          │
                          │ HTTP (LogQL)
                          ▼
┌──────────────────────────────────────────────────────┐
│              PROCESADOR (este proyecto)               │
│                                                        │
│  Cada W minutos:                                       │
│                                                        │
│  LokiClient ──► PreParser ──► Drain3 ──► Detector     │
│                                            │          │
│                                          DBSCAN        │
│                                            │          │
│                               ┌───────────┴────────┐  │
│                               ▼                    ▼  │
│                           Telegram            AlertManager│
│                           (push directo)      (webhook)   │
│                                                        │
│  SQLite: historial de frecuencias por plantilla        │
│  Prometheus /metrics: contadores para dashboards       │
└──────────────────────────────────────────────────────┘
```

---

## Los servicios monitoreados

El sistema monitorea 6 microservicios de TECOPOS, cada uno con su formato de log:

| Servicio | Tecnología | Formato de log | Particularidad |
|----------|-----------|----------------|----------------|
| `tecoposv1` | Express + Winston | JSON estructurado | Errores de negocio vienen como INFO |
| `tecoposv2` | NestJS | Formato Nest nativo | Webhooks de pago frecuentes |
| `gateway` | NestJS | Formato Nest nativo | Más tráfico, monitorea autenticación como INFO |
| `midas-dev` | NestJS | Formato Nest nativo | Muy ruidoso (webhooks de pago), k más alto |
| `control` | NestJS + kafkajs | JSON mixto | Eventos de Kafka en JSON puro |
| `controlbeta` | NestJS + kafkajs | JSON mixto | Igual que control, versión beta |

---

## Cómo se configura

### El entorno (`.env`)

El parámetro más importante es `PROCESSOR_ENV`. Define el comportamiento completo del detector:

```bash
# Para desarrollo: ve anomalías en 3 minutos, muy verboso
PROCESSOR_ENV=local

# Para staging: balance entre sensibilidad y ruido
PROCESSOR_ENV=development

# Para producción: conservador, pocas falsas alarmas
PROCESSOR_ENV=production
```

Qué cambia con cada entorno:

| | local | development | production |
|--|-------|-------------|------------|
| Tiempo hasta primera detección | ~3 min | ~10 min | ~50 min |
| Sensibilidad (k) | 2.0 (alta) | 2.5 | 3.0 (baja) |
| Historia consultada | 1 día | 3 días | 7 días |
| Logs analizados | info + warn + error | warn + error | warn + error |
| Formato del log propio | consola coloreada | consola | JSON |

Para Telegram, en `.env`:
```bash
PROCESSOR_TELEGRAM__ENABLED=true
PROCESSOR_TELEGRAM__BOT_TOKEN=tu-token-aqui
PROCESSOR_TELEGRAM__CHAT_ID=tu-chat-id
```

### La topología (`config.yaml`)

El `config.yaml` define **qué** servicios monitorear y sus particularidades — no los parámetros del algoritmo (esos los maneja el perfil de entorno):

```yaml
processor:
  services:
    - name: gateway
      profile: nestjs
      overrides:
        threshold_k: 3.5              # gateway tiene más tráfico → umbral más alto
        monitor_levels: [info, warn, error]  # necesita ver autenticación (INFO)

    - name: midas-dev
      profile: nestjs
      overrides:
        threshold_k: 4.0              # muy ruidoso → ignorar variaciones pequeñas
```

---

## Cómo leer una notificación

```
🚨 3 anomalías detectadas · 23:47
```
→ Hay 3 comportamientos inusuales en este ciclo

```
🔴 gateway ↑ CRÍTICO
   Slow GraphQL query: took <*> - getDashboard
   5 ocurrencias · ~2.5× lo habitual (esperado ~2)
```
→ **Rojo**: z-score alto, muy alejado de lo normal  
→ **↑**: está subiendo (hay más de lo normal)  
→ La plantilla agrupada (el `<*>` reemplaza el valor variable de tiempo)  
→ Cuántas veces pasó y cuánto es el ratio respecto al promedio histórico

```
🆕 tecoposv2 ↑ NUEVO
   AppModule dependencies initialized <*>
   2 ocurrencias (antes constante en ~1)
```
→ **Nuevo**: esta plantilla siempre tuvo el mismo valor y ahora cambió  
→ Útil para detectar reinicios inesperados (el módulo se reinicializó 2 veces)

```
🟡 tecoposv1 ↑ atención
   Auth token <*> for user <*>
   10 ocurrencias · ~1.7× lo habitual (esperado ~6)
   ↳ co-ocurre con otras de este ciclo
```
→ **Amarillo**: anómalo pero no crítico  
→ `↳ co-ocurre`: está agrupado con otras anomalías del mismo ciclo → problema sistémico

```
📊 1 crítica · 1 con atención · 1 nueva
🔗 1 cluster co-ocurrente (2 anomalías agrupadas)
```
→ Resumen: cuántas hay de cada tipo y si forman grupos

---

## Cómo arrancar

### Requisitos previos

- Python 3.11+
- Acceso a Loki (en local: SSH tunnel al sandbox)
- Credenciales de Telegram (opcional pero recomendado)

### Instalación

```powershell
# Crear entorno virtual
python -m venv .venv
.venv\Scripts\Activate.ps1

# Instalar dependencias (desde wheels offline si no hay internet)
pip install --no-index --find-links wheels -r requirements.txt
# o con internet:
pip install -r requirements.txt
```

### Configurar

```powershell
# Copiar el template de configuración
copy .env.example .env

# Editar .env: poner PROCESSOR_ENV=local y las credenciales de Telegram
notepad .env
```

### Levantar el tunnel SSH (para usar Loki del sandbox)

```powershell
ssh -L 13100:localhost:3100 root@204.168.195.31
```

### Ejecutar

```powershell
# Modo normal (corre indefinidamente, ciclo cada W minutos)
.venv\Scripts\python.exe -m processor.main

# Modo dry-run: corre un ciclo, muestra qué alertaría, no manda nada
.venv\Scripts\python.exe -m processor.main --dry-run

# Ver las métricas en Prometheus
# http://localhost:8000/metrics
```

---

## La estructura del código

```
processor/
├── processor/               ← El paquete Python
│   ├── main.py              ← Punto de entrada y orquestador del ciclo
│   ├── settings.py          ← Configuración: YAML + env vars + perfiles
│   ├── loki_client.py       ← Cliente HTTP para consultar Loki
│   ├── pre_parser.py        ← Router: elige el perfil correcto por servicio
│   ├── profiles/            ← Implementaciones de cada perfil de log
│   │   ├── regex.py         ← Para NestJS (expresión regular)
│   │   ├── json_path.py     ← Para kafkajs y Winston (JSON)
│   │   └── fallback.py      ← Para formatos desconocidos
│   ├── drain_parser.py      ← Wrapper de Drain3 con persistencia por servicio
│   ├── threshold.py         ← El detector μ ± kσ con filtros anti-ruido
│   ├── dbscan_cluster.py    ← Clustering de anomalías co-ocurrentes
│   ├── history.py           ← Persistencia del histórico en SQLite
│   ├── alert_publisher.py   ← Envío a AlertManager
│   ├── telegram_publisher.py← Formato y envío de notificaciones Telegram
│   └── metrics.py           ← Contadores y gauges para Prometheus
│
├── drain_state/             ← Se crea al ejecutar (no commitear)
│   ├── gateway.bin          ← Estado de Drain para gateway
│   ├── tecoposv1.bin        ← Estado de Drain para tecoposv1
│   └── history.db           ← SQLite con el histórico de frecuencias
│
├── config.yaml              ← Topología de servicios y sus particularidades
├── .env.example             ← Template de variables de entorno
├── .env                     ← Tu configuración local (gitignored)
├── requirements.txt         ← Dependencias Python
├── Dockerfile               ← Para deployment en Kubernetes
│
├── ALGORITMO.md             ← Documentación técnica profunda del algoritmo
└── DEFENSA_TRIBUNAL.md      ← Guía de preparación para la defensa oral
```

---

## Preguntas frecuentes

**¿Por qué no usa machine learning?**

Porque para este problema el modelo estadístico es suficiente y tiene ventajas importantes: es interpretable (puedes ver exactamente por qué se disparó una alerta), no requiere datos etiquetados para entrenar, y se actualiza automáticamente con cada ciclo sin necesidad de reentrenamiento.

**¿Por qué no basta con Grafana + reglas manuales?**

Grafana detecta lo que ya sabes que puede fallar. Este sistema detecta lo que no anticipaste. Además, Grafana requiere configurar una regla por cada tipo de error — con decenas de plantillas por servicio, eso no escala. El procesador no necesita saber de antemano qué va a fallar.

**¿Qué pasa si reinio el procesador?**

El estado de Drain (las plantillas aprendidas) y el histórico de frecuencias (el SQLite) sobreviven al reinicio porque están en disco. El procesador continúa exactamente donde quedó.

**¿Qué pasa si borro `drain_state/`?**

El procesador arranca de cero. Necesitará `min_observations` ciclos (3 en local, 10 en producción) para acumular historia antes de empezar a detectar. Durante esos ciclos, generará muchas alertas "NUEVO" porque todo es nuevo para él.

**¿Genera muchas falsas alarmas?**

Con `PROCESSOR_ENV=production` y los filtros activos: 1-3 alertas por hora en condiciones normales, 5-15 durante un incidente real. Con `local` y todo activado: puede generar más ruido — está diseñado para eso, para que puedas ver que funciona rápido.

**¿Cuánta memoria y CPU usa?**

Muy poco. El ciclo completo de 6 servicios procesa hasta 30.000 líneas de log en ~15 segundos. Entre ciclos no consume nada. El SQLite con 7 días de historia ocupa < 50MB.

---

## Glosario

| Término | Qué significa aquí |
|---------|-------------------|
| **Plantilla** | Un patrón de mensaje de log con los valores variables reemplazados por `<*>`. Ejemplo: `"Query took <*>ms"` |
| **Drain** | Algoritmo que agrupa mensajes similares en plantillas automáticamente |
| **μ (mu)** | Promedio histórico de veces que aparece una plantilla por ciclo |
| **σ (sigma)** | Desviación estándar del histórico — qué tan variable es normalmente |
| **k** | El multiplicador de sensibilidad. k=3 significa "alerta si está a más de 3 sigmas de la media" |
| **z-score** | Cuántas sigmas se aleja el valor actual de la media: `z = (actual - μ) / σ` |
| **DBSCAN** | Algoritmo de clustering que agrupa anomalías que ocurren juntas en el mismo ciclo |
| **Cluster co-ocurrente** | Varias anomalías de distintos servicios que el DBSCAN agrupa — indica problema sistémico |
| **W** | Ancho de la ventana de análisis (cada cuántos minutos corre el procesador) |
| **H** | Días de historia que se consultan para calcular μ y σ |
| **Loki** | Base de datos de logs (parte del stack de Grafana Labs) |
| **LogQL** | Lenguaje de query de Loki, similar a SQL pero para logs |
| **Sentinel z=±1000** | Valor especial que se usa cuando σ=0 (patrón constante que cambió) para evitar división por cero |
