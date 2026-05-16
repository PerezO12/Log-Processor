# Procesador Python — Fase 4

Esta carpeta es el **stub** del componente más importante de la tesis: el procesador que aplica Drain (extracción de plantillas) + umbrales dinámicos + DBSCAN sobre los logs almacenados en Loki, generando alertas hacia AlertManager.

> **Estado actual:** sólo estructura mínima e infraestructura para construir el contenedor. La implementación real (`main.py`, `drain_parser.py`, etc.) se desarrollará en la próxima fase.

## Por qué existe ahora

Aunque la lógica del procesador no está escrita, hemos creado:

1. **`requirements.txt`** con todas las dependencias pinneadas → para descargarlas mientras hay conexión.
2. **`Dockerfile`** que soporta build online y offline → listo para cuando tengamos código.
3. **`DESCARGA-OFFLINE.md`** con los comandos exactos para pre-descargar wheels y la imagen base de Docker.

Esto te permite, **estando con conexión hoy**, dejar todo cacheado para poder iterar después sin internet.

## Estructura

```
processor/
├── README.md                  # este archivo
├── DESCARGA-OFFLINE.md        # como descargar todo offline (LEELA AHORA)
├── requirements.txt           # dependencias pinneadas
├── Dockerfile                 # imagen del procesador
├── .dockerignore
├── .gitignore
└── wheels/                    # vacia; se llena con `pip download`
    └── .gitkeep
```

## Qué hará el procesador (resumen del Cap. II II.3.3)

Cada `W = 5 minutos` el bucle ejecuta:

1. **Consulta Loki** vía LogQL para la ventana actual.
2. **Pre-parser por perfil** (A: NestJS texto plano, B: Winston texto colorize, C: Winston JSON).
3. **Drain** extrae `template_id` y plantilla canónica con `<*>` para tokens variables.
4. **Frecuencias** por (`service`, `template_id`).
5. **Umbrales dinámicos:** `f(t) > μ + k·σ` o `f(t) < μ - k·σ` con `k=3` y ventana histórica `H=7d`.
6. **DBSCAN** agrupa anomalías para distinguir ruido aislado de patrones recurrentes.
7. **Webhook → AlertManager** para anomalías confirmadas.

Parámetros (W, H, k, eps, min_samples, depth) externalizados en `config.yaml` (RNF-04).

## Dependencias clave

| Paquete | Para qué |
|---|---|
| `drain3` | Implementación oficial del algoritmo Drain (Cap. II) |
| `scikit-learn` | DBSCAN |
| `numpy`, `pandas`, `scipy` | Estadísticas (μ, σ, ventanas) |
| `httpx` | Cliente async para LogQL y webhooks |
| `apscheduler` | Bucle periódico cada W minutos |
| `pydantic`, `pydantic-settings` | Validación del config |
| `structlog` | Logs JSON propios del procesador |
| `prometheus-client` | Endpoint `/metrics` para alimentar Dashboard 3 |
| `tenacity` | Reintentos con backoff (RF-09 robustez) |

## Próximos pasos (en orden)

1. **AHORA, con conexión:** seguir `DESCARGA-OFFLINE.md` paso a paso.
2. **Más adelante:** crear `main.py` y los módulos de implementación.
3. **Validación:** correr el procesador local contra el Loki de la Fase 1 con datos del `log-generator`.
4. **Producción:** convertir a manifiesto k3s (`observability/kubernetes/40-processor-deployment.yaml`) y desplegar en TECOPOS.

## Referencias

- `../../../plan.md` sección "Paso 4: Procesador Python (el corazón del trabajo de tesis)"
- Cap. II, sección II.3.3 "Diseño del flujo de procesamiento"
- Zhu et al. (2019) — algoritmo Drain
- Jiang et al. (2024) — evaluación de log parsing
