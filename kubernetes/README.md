# Manifiestos de despliegue (k3s) — Stack de observabilidad TECOPOS

Manifiestos de Kubernetes que despliegan el prototipo de detección de anomalías
sobre un clúster **k3s** en el namespace `tecopos-observability`. Materializan la
arquitectura de cuatro capas descrita en el Capítulo II de la memoria:

| Capa | Componente | Manifiestos |
|------|-----------|-------------|
| 1 · Recolección | Grafana Alloy (DaemonSet) | `30-alloy-configmap.yaml`, `31-alloy-daemonset.yaml` |
| 2 · Almacenamiento | Grafana Loki | `10-loki-pvc.yaml`, `11-loki-configmap.yaml`, `12-loki-deployment.yaml`, `13-loki-service.yaml` |
| 3 · Procesamiento | Procesador Python | `50-processor-configmap.yaml`, `51-processor-pvc.yaml`, `52-processor-deployment.yaml`, `53-processor-service.yaml` |
| 4 · Visualización | Grafana | `20-grafana-configmap.yaml`, `21-grafana-deployment.yaml`, `22-grafana-service.yaml` |

`00-namespace.yaml` crea el namespace y `40-log-replay-pods.yaml` despliega pods
de simulación que emiten logs con los formatos reales de cada servicio (perfiles
NestJS, Winston JSON y kafkajs) para validar el flujo en el entorno de prueba.

## Orden de aplicación

Los archivos están numerados según el orden de despliegue:

```bash
kubectl apply -f .
```

## Notas

- **Sin credenciales.** El token del bot de Telegram y la contraseña de Grafana
  **no** están en estos manifiestos: se inyectan en tiempo de ejecución desde
  objetos `Secret` (`processor-secrets`, `grafana-admin`) creados fuera del
  repositorio. Las IPs de producción son parámetros a definir con el equipo de
  DevOps.
- La imagen del procesador (`52-processor-deployment.yaml`) usa el marcador
  `<DOCKER_HUB_USER>`; sustitúyelo por el usuario del registro correspondiente.
- `StorageClass: local-path` es la opción por defecto de k3s; en producción
  multi-nodo puede sustituirse por `longhorn` para replicación entre nodos.
