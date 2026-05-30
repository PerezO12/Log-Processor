"""Notificaciones push via Telegram Bot API.

Diseno:
    - Canal paralelo a AlertPublisher. No lo reemplaza.
    - Un solo mensaje por ciclo agrupando todas las anomalias (evita spam).
    - Filtro por severidad (warning | critical).
    - Falla en silencio (log warning) si Telegram esta inalcanzable; nunca
      tumba el ciclo del procesador.
    - Respeta `--dry-run`: imprime el mensaje en stdout, no envia.

Configuracion minima (env vars):
    PROCESSOR_TELEGRAM__ENABLED=true
    PROCESSOR_TELEGRAM__BOT_TOKEN=<token>
    PROCESSOR_TELEGRAM__CHAT_ID=<chat_id>
"""
from __future__ import annotations

import html
import os
import platform
import socket
from datetime import datetime
from typing import List, Optional

import httpx
import structlog

from processor.dbscan_cluster import ClusteredAnomaly
from processor.settings import Settings

log = structlog.get_logger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 4000          # margen seguro bajo el limite de 4096 de Telegram
CRITICAL_Z_THRESHOLD = 5.0
SENTINEL_Z = 999.0                 # umbral que distingue el z=±1000 generado por sigma=0
TEMPLATE_TRUNCATE = 140            # mas legible que 120


class TelegramPublisher:
    """Envia anomalias como notificacion HTML por Telegram."""

    def __init__(self, settings: Settings, dry_run: bool = False):
        cfg = settings.telegram
        self._token = cfg.bot_token
        self._chat_id = cfg.chat_id
        self._timeout = cfg.timeout_seconds
        self._min_severity = cfg.min_severity
        self._dry_run = dry_run

        # Habilitado solo si esta activado Y tiene credenciales.
        self._enabled = cfg.enabled and bool(self._token) and bool(self._chat_id)
        if cfg.enabled and not self._enabled:
            log.warning(
                "telegram_enabled_but_credentials_missing",
                hint="Set PROCESSOR_TELEGRAM__BOT_TOKEN and PROCESSOR_TELEGRAM__CHAT_ID",
            )

    # ------------------------------------------------------------------
    # Notificación de arranque
    # ------------------------------------------------------------------
    def notify_startup(self, services: List[str], env: Optional[str] = None) -> None:
        """Envía un mensaje único a Telegram cuando el procesador arranca.

        Incluye contexto del runtime (hostname, CPU, RAM disponible) útil para
        operadores que reciben la alerta y para la tesis (evidencia de
        despliegue real sobre la infraestructura).
        """
        if not self._enabled:
            return

        text = self._format_startup(services, env)
        if self._dry_run:
            log.info("telegram_dry_run_startup", chars=len(text), preview=text[:200])
            return

        try:
            self._send_raw(text)
            log.info("telegram_startup_sent", services=len(services))
        except Exception as e:  # noqa: BLE001 — nunca tumbar el arranque
            log.warning("telegram_startup_failed", error=str(e))

    @staticmethod
    def _read_system_info() -> dict:
        """Obtiene CPU/RAM del host usando solo módulos estándar.

        En contenedores Linux lee `/proc/meminfo` y `os.cpu_count()`.
        En entornos donde `/proc` no existe (Windows nativo), retorna
        solo los datos disponibles.
        """
        info: dict = {
            "hostname": socket.gethostname(),
            "system": f"{platform.system()} {platform.release()}",
            "cpus": os.cpu_count() or "?",
            "python": platform.python_version(),
        }
        # RAM total (solo Linux/contenedores)
        try:
            with open("/proc/meminfo", encoding="ascii") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        info["ram_gb"] = round(kb / (1024 * 1024), 2)
                    elif line.startswith("MemAvailable:"):
                        kb = int(line.split()[1])
                        info["ram_free_gb"] = round(kb / (1024 * 1024), 2)
        except (FileNotFoundError, OSError):
            pass
        return info

    def _format_startup(self, services: List[str], env: Optional[str]) -> str:
        sysinfo = self._read_system_info()
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        env_label = f" · <code>{html.escape(env)}</code>" if env else ""

        lines: List[str] = [
            f"🟢 <b>Procesador TECOPOS iniciado</b>{env_label}",
            f"   🕒 {now}",
            "",
            "<b>📦 Runtime</b>",
            f"   🖥 host: <code>{html.escape(str(sysinfo['hostname']))}</code>",
            f"   🐧 sistema: <code>{html.escape(str(sysinfo['system']))}</code>",
            f"   🧠 CPUs: <code>{sysinfo['cpus']}</code>",
        ]
        if "ram_gb" in sysinfo:
            ram_line = f"   💾 RAM: <code>{sysinfo['ram_gb']} GB"
            if "ram_free_gb" in sysinfo:
                ram_line += f" ({sysinfo['ram_free_gb']} GB libres)"
            ram_line += "</code>"
            lines.append(ram_line)
        lines.append(f"   🐍 Python: <code>{sysinfo['python']}</code>")
        lines.append("")
        lines.append(f"<b>🛠 Servicios monitorizados ({len(services)})</b>")
        # Mostrar todos los servicios (cortando si fuesen demasiados)
        shown = services[:20]
        services_block = ", ".join(f"<code>{html.escape(s)}</code>" for s in shown)
        if len(services) > 20:
            services_block += f" …y {len(services) - 20} más"
        lines.append(f"   {services_block}")
        lines.append("")
        lines.append("<i>El procesador comenzará a notificar anomalías en los próximos ciclos.</i>")
        return "\n".join(lines)

    def _send_raw(self, text: str) -> None:
        """Envío directo sin filtros (usado por notify_startup)."""
        url = TELEGRAM_API.format(token=self._token)
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        with httpx.Client(timeout=self._timeout) as client:
            r = client.post(url, json=payload)
            if r.status_code >= 400:
                raise RuntimeError(f"telegram_rejected status={r.status_code} body={r.text[:200]}")

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------
    def publish(self, anomalies: List[ClusteredAnomaly]) -> None:
        if not self._enabled or not anomalies:
            return
        filtered = [c for c in anomalies if self._severity_passes(c)]
        if not filtered:
            return

        text = self._format_batch(filtered)
        if self._dry_run:
            log.info("telegram_dry_run", chars=len(text), preview=text[:200])
            return
        self._send(text, len(filtered))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _severity_passes(self, c: ClusteredAnomaly) -> bool:
        if self._min_severity == "critical":
            return abs(c.anomaly.z_score) >= CRITICAL_Z_THRESHOLD
        return True

    # ------------------------------------------------------------------
    # Helpers de presentacion (mensaje legible, sin jerga estadistica)
    # ------------------------------------------------------------------
    @staticmethod
    def _classify(anomaly) -> tuple:
        """Devuelve (icono, etiqueta_corta, prioridad_orden)."""
        z = abs(anomaly.z_score)
        # Sentinel: sigma=0 + cambio -> "primera variacion" (no es CRITICO real)
        if z >= SENTINEL_Z:
            return ("🆕", "NUEVO", 1)
        if z >= CRITICAL_Z_THRESHOLD:
            return ("🔴", "CRÍTICO", 0)
        return ("🟡", "atención", 2)

    @staticmethod
    def _context_hint(anomaly, z: float) -> str:
        """Una linea de guia accionable segun tipo y direccion de la anomalia."""
        direction = anomaly.direction
        if z >= SENTINEL_Z:
            if direction == "up":
                return "❓ <i>Patrón nunca visto — ¿nuevo deploy, bug o cambio de config?</i>"
            return "❓ <i>Patrón desaparecido — ¿ruta eliminada o servicio detenido?</i>"
        if z >= CRITICAL_Z_THRESHOLD:
            if direction == "up":
                return "⚡ <i>Spike extremo — revisar errores en cascada o sobrecarga</i>"
            return "⚡ <i>Caída extrema — posible servicio caído o ruta bloqueada</i>"
        # atención
        if direction == "up":
            return "👁 <i>Frecuencia elevada — monitorear si escala en los próximos ciclos</i>"
        return "👁 <i>Frecuencia baja — verificar que el servicio responde con normalidad</i>"

    @staticmethod
    def _human_frequency(anomaly) -> str:
        """Describe la frecuencia en lenguaje natural (sin mu/sigma/z)."""
        cur, mu, sigma, direction = anomaly.current, anomaly.mean, anomaly.stddev, anomaly.direction

        # Caso sigma=0: el patron era constante hasta ahora
        if sigma == 0.0:
            if direction == "up":
                if mu == 0:
                    return f"aparece <b>{cur}</b> veces (patrón nuevo, sin histórico)"
                return f"<b>{cur}</b> ocurrencias (antes constante en ~{mu:.0f})"
            # down
            if cur == 0:
                return f"silencio total (antes constante en ~{mu:.0f})"
            return f"<b>{cur}</b> ocurrencias (antes constante en ~{mu:.0f})"

        # Caso normal: ratio en lenguaje humano
        if direction == "up":
            ratio = cur / max(mu, 0.1)
            return f"<b>{cur}</b> ocurrencias · ~{ratio:.1f}× lo habitual (esperado ~{mu:.0f})"
        # down
        if cur == 0:
            return f"silencio total (esperado ~{mu:.0f})"
        ratio = mu / max(cur, 0.1)
        return f"<b>{cur}</b> ocurrencias · ~{ratio:.1f}× menos (esperado ~{mu:.0f})"

    @staticmethod
    def _truncate_html(s: str, n: int = TEMPLATE_TRUNCATE) -> str:
        # Reemplazar <*> de Drain por [...] para mayor legibilidad en Telegram.
        # html.escape viene despues para no escapar los corchetes.
        clean = s.replace("<*>", "[…]")
        clean = clean if len(clean) <= n else clean[: n - 1] + "…"
        return html.escape(clean)

    @staticmethod
    def _summary_footer(anomalies: List[ClusteredAnomaly]) -> str:
        crit = novel = warn = 0
        for c in anomalies:
            z = abs(c.anomaly.z_score)
            if z >= SENTINEL_Z:
                novel += 1
            elif z >= CRITICAL_Z_THRESHOLD:
                crit += 1
            else:
                warn += 1

        parts: List[str] = []
        if crit:
            parts.append(f"{crit} crítica{'s' if crit > 1 else ''}")
        if warn:
            parts.append(f"{warn} con atención")
        if novel:
            parts.append(f"{novel} nueva{'s' if novel > 1 else ''}")
        line = "📊 " + " · ".join(parts) if parts else ""

        # Resumen de co-ocurrencias
        clusters: dict = {}
        for c in anomalies:
            if c.cluster_id >= 0:
                clusters.setdefault(c.cluster_id, 0)
                clusters[c.cluster_id] += 1
        if clusters:
            n_clusters = len(clusters)
            n_grouped = sum(clusters.values())
            line += (
                f"\n🔗 {n_clusters} cluster co-ocurrente"
                f"{'s' if n_clusters > 1 else ''} "
                f"({n_grouped} anomalías agrupadas)"
            )
        return line

    def _format_batch(self, anomalies: List[ClusteredAnomaly]) -> str:
        # Ordenar por severidad para que lo importante salga arriba
        sorted_anoms = sorted(anomalies, key=lambda c: self._classify(c.anomaly)[2])

        n = len(sorted_anoms)
        now = datetime.now().strftime("%H:%M")
        header = (
            f"🚨 <b>{n} anomalía{'s' if n > 1 else ''} detectada"
            f"{'s' if n > 1 else ''}</b> · {now}"
        )
        lines: List[str] = [header, ""]
        total_len = len(header) + 1

        for idx, c in enumerate(sorted_anoms):
            a = c.anomaly
            z = abs(a.z_score)
            icon, label, _ = self._classify(a)
            arrow = "↑" if a.direction == "up" else "↓"
            tpl = self._truncate_html(a.template_str)
            freq = self._human_frequency(a)
            hint = self._context_hint(a, z)

            cluster_line = ""
            if c.cluster_id >= 0:
                cluster_line = "\n   🔗 <i>Falla junto a otros servicios → revisar infraestructura común</i>"

            block = (
                f"{icon} <b>{html.escape(a.service)}</b> {arrow} <i>{label}</i>\n"
                f"   <code>{tpl}</code>\n"
                f"   {freq}\n"
                f"   {hint}"
                f"{cluster_line}\n"
            )
            if total_len + len(block) > MAX_MESSAGE_LENGTH - 200:  # margen para footer
                lines.append(f"…y {n - idx} más (mensaje truncado)")
                break
            lines.append(block)
            total_len += len(block) + 1

        # Footer (solo si hay mas de 1 anomalia para evitar redundancia)
        if n > 1:
            footer = self._summary_footer(sorted_anoms)
            if footer:
                lines.append(footer)

        return "\n".join(lines)

    def _send(self, text: str, count: int) -> None:
        url = TELEGRAM_API.format(token=self._token)
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            with httpx.Client(timeout=self._timeout) as client:
                r = client.post(url, json=payload)
                if r.status_code >= 400:
                    log.warning("telegram_rejected", status=r.status_code, body=r.text[:200])
                else:
                    log.info("telegram_sent", count=count)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            log.warning("telegram_unreachable", error=str(e))
