"""
Scraper de movimientos de tarjeta de crédito del Banco Falabella usando Playwright.
El banco usa Shadow DOM — se traversa con JS para extraer datos y cerrar modales.
Paginación: botones ‹ 1 › (flechas SVG, no texto).
"""
import os
import sys
import logging
import asyncio
import re
import hashlib
from datetime import date
from typing import List, Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page
import psycopg2
import psycopg2.extras

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
for _noisy in ("urllib3", "asyncio", "playwright"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

DEBUG_DIR = Path("debug")

# ------------------------------------------------------------------ #
# JavaScript helpers                                                   #
# ------------------------------------------------------------------ #

JS_CLOSE_MODAL = """
() => {
    function findClose(root) {
        const btns = Array.from(root.querySelectorAll('button'));
        for (const b of btns) {
            const text = b.textContent.trim();
            const aria = (b.getAttribute('aria-label') || '').toLowerCase();
            if (['×','✕','✖','x'].includes(text) ||
                ['close','cerrar','dismiss'].includes(aria) ||
                b.classList.toString().toLowerCase().includes('close')) {
                return b;
            }
        }
        for (const el of root.querySelectorAll('*')) {
            if (el.shadowRoot) {
                const b = findClose(el.shadowRoot);
                if (b) return b;
            }
        }
        return null;
    }
    const btn = findClose(document);
    if (btn) { btn.click(); return 'button'; }
    const backdrop = document.querySelector('.cdk-overlay-backdrop, [class*="backdrop"]');
    if (backdrop) { backdrop.click(); return 'backdrop'; }
    // Fallback: click fuera del modal
    const modal = document.querySelector('#modalDetailTransaction');
    if (modal) { document.body.dispatchEvent(new MouseEvent('click', {bubbles: true})); return 'body'; }
    return null;
}
"""

JS_HAS_BLOCKING_BACKDROP = """
() => {
    const els = Array.from(document.querySelectorAll('div.backdrop, .cdk-overlay-backdrop'));
    return els.some(el => {
        const r = el.getBoundingClientRect();
        const s = getComputedStyle(el);
        return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' &&
               s.display !== 'none' && s.opacity !== '0';
    });
}
"""

# Cierra el modal promocional buscando su botón ×. Nunca clickea 'Acepto': ese botón
# inscribe al usuario en el programa CMR Puntos y autoriza comunicaciones comerciales.
JS_CLICK_PROMO_CLOSE = """
() => {
    for (const b of Array.from(document.querySelectorAll('button'))) {
        const cls = (b.className || '').toString().toLowerCase();
        const aria = (b.getAttribute('aria-label') || '').toLowerCase();
        const text = (b.textContent || '').trim();
        const isClose = cls.includes('close') || ['close','cerrar','dismiss'].includes(aria) ||
                        ['×','✕','✖','x'].includes(text) ||
                        !!b.querySelector('svg.icon-close, use[*|href$="#icon-close"]');
        if (!isClose) continue;
        if (/cerrar sesi/i.test(aria) || /cerrar sesi/i.test(text)) continue;  // no cerrar sesión
        const r = b.getBoundingClientRect();
        if (r.width === 0 || r.height === 0) continue;
        b.click();
        return cls || aria || text || 'close';
    }
    return null;
}
"""

JS_EXTRACT_FIELDS = """
() => {
    const LABELS = {
        'Cuotas': 'modal_cuotas',
        'Rubro': 'rubro',
        'Comercio': 'comercio',
        'Código autorización': 'codigo_autorizacion',
        'Fecha': 'fecha_compra',
        'Hora': 'hora',
        'Pais': 'pais',
        'País': 'pais',
        'Origen de la compra': 'origen',
    };

    function findLeaf(root, text) {
        for (const el of root.querySelectorAll('*')) {
            if (el.children.length === 0 && el.textContent.trim() === text) return el;
            if (el.shadowRoot) {
                const found = findLeaf(el.shadowRoot, text);
                if (found) return found;
            }
        }
        return null;
    }

    function findModalRoot(anchorEl, secondLabel) {
        let container = anchorEl.parentElement;
        for (let i = 0; i < 20 && container; i++) {
            const hasSecond = Array.from(container.querySelectorAll('*'))
                .some(el => el.children.length === 0 && el.textContent.trim() === secondLabel);
            if (hasSecond) return container;
            container = container.parentElement;
        }
        return null;
    }

    const anchorPairs = [['Rubro', 'Hora'], ['Hora', 'Fecha'], ['Comercio', 'Fecha'], ['Fecha', 'Hora']];
    let modal = null;
    for (const [a1, a2] of anchorPairs) {
        const el = findLeaf(document, a1);
        if (el) { modal = findModalRoot(el, a2); }
        if (modal) break;
    }
    if (!modal) return {};

    function getField(container, label) {
        for (const el of container.querySelectorAll('*')) {
            const text = el.textContent.trim();
            const matches = text === label || (text.startsWith(label) && el.children.length <= 2);
            if (!matches) continue;
            const childHasLabel = Array.from(el.children).some(c => c.textContent.trim().startsWith(label));
            if (childHasLabel) continue;
            const next = el.nextElementSibling;
            if (next && next.textContent.trim()) return next.textContent.trim();
            const parent = el.parentElement;
            if (parent) return parent.textContent.replace(label, '').trim();
        }
        return null;
    }

    const result = {};
    for (const [label, key] of Object.entries(LABELS)) {
        const v = getField(modal, label);
        if (v) result[key] = v;
    }

    // Dump de todos los pares label→valor del modal (para debug).
    // No se filtra por elementos hoja para capturar labels que tengan íconos/spans hijos.
    const allPairs = {};
    const candidateEls = Array.from(modal.querySelectorAll('*')).filter(el => {
        const t = el.textContent.trim();
        return t && t.length < 60;
    });
    for (const el of candidateEls) {
        const t = el.textContent.trim();
        const childHasSameText = Array.from(el.children).some(c => c.textContent.trim() === t);
        if (childHasSameText) continue;
        const next = el.nextElementSibling;
        if (next && next.textContent.trim()) {
            allPairs[t] = next.textContent.trim();
        }
    }
    result['_debug_pairs'] = allPairs;

    return result;
}
"""


# Retorna el bounding rect del botón › (siguiente página) o null
JS_NEXT_PAGE_RECT = """
() => {
    // Busca botones de paginación atravesando shadow roots.
    // btn-move = estructura nueva (2026-03+); btn-pagination = estructura antigua.
    function findPagBtns(root) {
        const btns = [];
        for (const el of root.querySelectorAll('*')) {
            if (el.tagName === 'BUTTON' &&
                (el.classList.contains('btn-move') || el.classList.contains('btn-pagination')))
                btns.push(el);
            if (el.shadowRoot) btns.push(...findPagBtns(el.shadowRoot));
        }
        return btns;
    }
    const pagBtns = findPagBtns(document);
    if (pagBtns.length === 0) return null;
    // El ÚLTIMO btn-move en el DOM siempre es ›; si está disabled, no hay más páginas
    const nextBtn = pagBtns[pagBtns.length - 1];
    if (nextBtn.disabled || nextBtn.hasAttribute('disabled')) return null;
    const r = nextBtn.getBoundingClientRect();
    if (r.width > 0 && r.height > 0) {
        return { x: r.x + r.width / 2, y: r.y + r.height / 2, width: r.width, height: r.height };
    }
    return null;
}
"""

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")

# ------------------------------------------------------------------ #
# Pestaña "Movimientos facturados" (estados de cuenta cerrados)        #
# ------------------------------------------------------------------ #

# Helpers compartidos: la app vive detrás de shadow roots, así que hay que traversarlos.
_JS_WALK = """
    const find = (pred) => {
        const out = [];
        (function walk(root) {
            for (const el of root.querySelectorAll('*')) {
                if (pred(el)) out.push(el);
                if (el.shadowRoot) walk(el.shadowRoot);
            }
        })(document);
        return out;
    };
    const q = (sel) => {
        const out = [];
        (function walk(root) {
            try { out.push(...root.querySelectorAll(sel)); } catch (e) {}
            for (const el of root.querySelectorAll('*')) if (el.shadowRoot) walk(el.shadowRoot);
        })(document);
        return out;
    };
"""

JS_CLICK_BILLED_TAB = """
(label) => {
""" + _JS_WALK + """
    const tabs = find(el => el.tagName === 'LABEL' && el.textContent.trim() === label);
    if (!tabs.length) return false;
    tabs[0].click();
    return true;
}
"""

# Tres estados distintos, y la diferencia importa:
#   con_filas    → hay detalle, se puede leer
#   sin_detalle  → "Aún no tienes movimientos": el banco todavía no publicó el estado de cuenta
#                  (normal los primeros días tras el cierre) → NO marcar, reintentar mañana
#   error_carga  → "No pudimos cargar...": falla del backend, hay botón Reintentar
JS_BILLED_STATE = """
() => {
""" + _JS_WALK + """
    const inv = find(el => el.tagName === 'APP-INVOICED-MOVEMENTS')[0];
    if (!inv) return 'sin_pestana';
    if (q('app-invoiced-movements table tbody tr').length) return 'con_filas';
    const txt = inv.textContent.replace(/\\s+/g, ' ');
    if (/No pudimos cargar/i.test(txt)) return 'error_carga';
    if (/no tienes movimientos/i.test(txt)) return 'sin_detalle';
    return 'cargando';
}
"""

JS_CLICK_BILLED_RETRY = """
() => {
""" + _JS_WALK + """
    const btns = find(el => el.tagName === 'BUTTON' && el.textContent.trim() === 'Reintentar');
    if (!btns.length) return false;
    btns[0].click();
    return true;
}
"""

# El header no se actualiza al cambiar de estado de cuenta en el dropdown, así que su fecha se
# usa solo para validar: si no coincide con el período pedido, se ignora el monto.
JS_BILLED_HEADER = """
() => {
""" + _JS_WALK + """
    const inv = find(el => el.tagName === 'APP-INVOICED-MOVEMENTS')[0];
    if (!inv) return null;
    const txt = inv.textContent.replace(/\\s+/g, ' ');
    const fecha = txt.match(/Fecha de facturaci[oó]n\\s*(\\d{2}\\/\\d{2}\\/\\d{4})/i);
    const monto = txt.match(/Monto facturado\\s*\\$?\\s*([\\d.,]+)/i);
    return { fecha_facturacion: fecha ? fecha[1] : '', monto_facturado: monto ? monto[1] : '' };
}
"""

JS_BILLED_LOADING = """
() => {
""" + _JS_WALK + """
    return find(el => el.tagName === 'APP-LOADER').some(el => el.getClientRects().length > 0);
}
"""

BILLED_STATEMENT_SELECT = 'select[name="dateOnChange"]'



def _parse_monto(s: str) -> Optional[float]:
    """Convierte '$ -1.234' o '-$3.712.410' a float preservando el signo."""
    if not s or str(s).strip() in ("", "nan", "None"):
        return None
    s = str(s).strip().replace("$", "").replace(" ", "").replace(".", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_date(s: str) -> Optional[date]:
    """Parsea 'dd/mm/yyyy' a date object o None."""
    if not s or str(s).strip() in ("", "nan", "None"):
        return None
    try:
        from datetime import datetime
        return datetime.strptime(str(s).strip(), "%d/%m/%Y").date()
    except (ValueError, TypeError):
        return None


def _make_tx_hash(fecha_compra_raw: str, descripcion: str, monto_raw: str) -> str:
    key = f"{fecha_compra_raw}|{descripcion}|{monto_raw}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


class FalabellaScraper:
    ROW_SELECTOR = "app-last-movements table tbody tr"
    # Pestaña "Movimientos facturados": otro componente Angular, pero por dentro usa el mismo
    # `app-movements-table` que la vista de últimos movimientos, así que las seis columnas caen
    # en los mismos índices y `_read_row` sirve sin cambios.
    BILLED_ROW_SELECTOR = "app-invoiced-movements table tbody tr"
    BILLED_TAB_LABEL = "Movimientos facturados"

    def __init__(self, headless: bool = False, debug_mode: bool = False):
        self.username = os.getenv("FALABELLA_USER")
        self.password = os.getenv("FALABELLA_PASSWORD")
        if not self.username or not self.password:
            raise ValueError("Configura FALABELLA_USER y FALABELLA_PASSWORD en el .env")

        self.headless = headless
        self.debug_mode = debug_mode
        self.max_per_page: int = 0  # 0 = sin límite
        self._run_status: str = "success"

        # Conexión a Supabase PostgreSQL
        self.db_conn = psycopg2.connect(
            os.environ["DATABASE_URL"],
            cursor_factory=psycopg2.extras.RealDictCursor,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

        self.periodo_facturacion: str = ""
        # Selector de filas del pase en curso. El pase de facturados lo sobrescribe y lo restaura.
        self.row_selector: str = self.ROW_SELECTOR
        self.backfill_periodo: str = ""      # 'YYYY-MM' forzado por CLI; "" = automático
        self.backfill_dry_run: bool = False
        self.backfill_max_new: int = 25      # tope de filas nuevas por pase
        self.backfill_min_overlap: float = 0.5
        self._in_backfill: bool = False      # lo lee _upsert_to_db para sellar la procedencia
        self.existing_keys: set = self._load_existing_keys()
        self.incomplete_keys: set = self._load_incomplete_keys()
        self.pending_hashes: dict = {}  # {(desc_norm, monto_norm): tx_hash} — cargado antes de _reset_pending

        # Contadores de la ejecución actual
        self.run_id: Optional[int] = None
        self._cnt_paginas = 0
        self._cnt_procesados = 0
        self._cnt_nuevos = 0
        self._cnt_actualizados = 0
        self._cnt_pendientes = 0

        (DEBUG_DIR / "screenshots").mkdir(parents=True, exist_ok=True)

    def __del__(self):
        try:
            if hasattr(self, "db_conn") and self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
        except Exception:
            pass

    @staticmethod
    def _normalize_auth(val) -> str:
        """Normaliza codigo_autorizacion a string entero limpio (ej: '599387.0' → '599387')."""
        s = str(val).strip()
        if not s or s in ("nan", "None", ""):
            return ""
        try:
            return str(int(float(s)))
        except (ValueError, TypeError):
            return s

    def _start_run(self) -> None:
        """Registra el inicio de la ejecución en scraper_runs."""
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                """
                INSERT INTO scraper_runs (started_at, status, headless)
                VALUES (NOW(), 'running', %s) RETURNING id
                """,
                (self.headless,),
            )
            self.run_id = cur.fetchone()["id"]
            self.db_conn.commit()
            cur.close()
            logger.info(f"Run #{self.run_id} iniciado")
        except Exception as e:
            self.db_conn.rollback()
            logger.warning(f"No se pudo registrar inicio de run: {e}")

    def _finish_run(self, status: str = "success", error: Optional[str] = None) -> None:
        """Actualiza el registro de la ejecución con los resultados finales."""
        self._run_status = status
        if not self.run_id:
            return
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                """
                UPDATE scraper_runs SET
                    finished_at  = NOW(),
                    status       = %s,
                    paginas      = %s,
                    procesados   = %s,
                    nuevos       = %s,
                    actualizados = %s,
                    pendientes   = %s,
                    error_message = %s,
                    periodo      = %s
                WHERE id = %s
                """,
                (
                    status,
                    self._cnt_paginas,
                    self._cnt_procesados,
                    self._cnt_nuevos,
                    self._cnt_actualizados,
                    self._cnt_pendientes,
                    error,
                    self.periodo_facturacion or None,
                    self.run_id,
                ),
            )
            self.db_conn.commit()
            cur.close()
            logger.info(
                f"Run #{self.run_id} {status} — "
                f"{self._cnt_nuevos} nuevos, {self._cnt_actualizados} actualizados, "
                f"{self._cnt_pendientes} pendientes, {self._cnt_paginas} páginas"
            )
        except Exception as e:
            self.db_conn.rollback()
            logger.warning(f"No se pudo registrar fin de run: {e}")

    def _movement_key(self, m: Dict) -> tuple:
        """Clave de checkpoint: (fecha, descripcion, monto, num_cuotas).
        Usa fecha de la tabla (no fecha_compra del modal) para ser consistente
        con lo que se lee antes de abrir el detalle."""
        num_cuotas = str(m.get("num_cuotas", "") or "").strip()
        monto_norm = str(int(abs(_parse_monto(str(m.get("monto", "") or "")) or 0)))
        return (str(m.get("fecha", "")), str(m.get("descripcion", "")), monto_norm, num_cuotas)

    def _load_existing_keys(self) -> set:
        """Carga claves de movimientos confirmados desde la DB."""
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                "SELECT fecha, descripcion, monto, num_cuotas FROM movimientos WHERE pendiente = FALSE"
            )
            rows = cur.fetchall()
            cur.close()
            keys = set()
            for row in rows:
                fecha_row = row.get("fecha")
                fecha_str = fecha_row.strftime("%d/%m/%Y") if fecha_row else ""
                desc = str(row.get("descripcion", "") or "")
                monto_norm = str(int(abs(float(row.get("monto") or 0))))
                num_cuotas = str(row.get("num_cuotas", "") or "").strip()
                keys.add((fecha_str, desc, monto_norm, num_cuotas))
            return keys
        except Exception:
            return set()

    def _load_incomplete_keys(self) -> set:
        """Claves de filas confirmadas que faltan rubro o comercio del modal.
        No se reintenta por `codigo_autorizacion IS NULL`: las filas del bache de
        abril–mayo 2026 (cuando el banco no servía el campo) nunca podrán obtenerlo,
        así que se re-procesarían en cada run sin resultado."""
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                """
                SELECT fecha, descripcion, monto, num_cuotas FROM movimientos
                WHERE pendiente = FALSE
                  AND (
                    -- Solo se reintenta si falta rubro o comercio (ver docstring: reintentar
                    -- por auth NULL re-procesaría para siempre las filas del bache abr–may 2026).
                    rubro IS NULL OR rubro = '' OR comercio IS NULL OR comercio = ''
                  )
                """
            )
            rows = cur.fetchall()
            cur.close()
            incomplete = set()
            for row in rows:
                fecha = row.get("fecha")
                fecha_str = fecha.strftime("%d/%m/%Y") if fecha else ""
                if not fecha_str:
                    continue
                desc = str(row.get("descripcion", "") or "")
                monto_norm = str(int(abs(float(row.get("monto") or 0))))
                num_cuotas = str(row.get("num_cuotas", "") or "").strip()
                incomplete.add((fecha_str, desc, monto_norm, num_cuotas))
            if incomplete:
                logger.info(f"Checkpoint: {len(incomplete)} filas incompletas serán re-procesadas")
            return incomplete
        except Exception:
            return set()

    def _save_pending_hashes(self) -> dict:
        """Guarda {(desc_norm, monto_norm): tx_hash} de todos los pendientes antes de borrarlos.
        Permite migrar clasificaciones/splits cuando el tx_hash cambia al confirmarse."""
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                "SELECT descripcion, monto, tx_hash FROM movimientos "
                "WHERE pendiente = TRUE AND tx_hash IS NOT NULL"
            )
            rows = cur.fetchall()
            cur.close()
            mapping = {}
            for row in rows:
                desc = str(row["descripcion"] or "").rstrip("*").strip().upper()
                monto = str(int(abs(float(row["monto"] or 0))))
                mapping[(desc, monto)] = row["tx_hash"]
            if mapping:
                logger.info(f"Pendientes guardados para migración: {len(mapping)}")
            return mapping
        except Exception:
            return {}

    def _reset_pending(self) -> None:
        """Elimina todos los pendientes de la DB al inicio del run para re-agregarlos frescos.

        Excluye las filas escritas por un backfill: un estado de cuenta no tiene pendientes, así
        que una fila con `backfill_run_id` marcada como pendiente sería un error de detección —
        y borrarla la perdería para siempre, porque el período ya quedó marcado como completado.
        """
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                "DELETE FROM movimientos WHERE pendiente = TRUE AND backfill_run_id IS NULL"
            )
            n = cur.rowcount
            self.db_conn.commit()
            cur.close()
            if n > 0:
                logger.info(f"Reseteados {n} pendientes para re-procesar")
        except Exception:
            self.db_conn.rollback()

    def _upsert_to_db(self, movement: Dict) -> None:
        """Parsea y hace upsert del movimiento en la tabla movimientos."""
        # --- Parseo y limpieza ---
        monto_raw = str(movement.get("monto", "") or "")
        monto = _parse_monto(monto_raw)

        valor_cuota_raw = str(movement.get("valor_cuota", "") or "").strip()
        valor_cuota = _parse_monto(valor_cuota_raw) if valor_cuota_raw not in ("", "nan", "None") else None

        monto_periodo = valor_cuota if valor_cuota is not None else monto

        fecha_compra_raw = str(movement.get("fecha_compra", "") or "").strip()
        fecha_compra = _parse_date(fecha_compra_raw)

        fecha_raw = str(movement.get("fecha", "") or "").strip()
        fecha = _parse_date(fecha_raw)

        # periodo: desde periodo_facturacion "dd/mm/yyyy" → "YYYY-MM"
        periodo_fac = str(movement.get("periodo_facturacion", "") or "").strip()
        periodo = None
        if periodo_fac:
            d = _parse_date(periodo_fac)
            if d:
                periodo = d.strftime("%Y-%m")

        descripcion = str(movement.get("descripcion", "") or "")
        pendiente = bool(movement.get("pendiente", False))

        # Normalizar codigo_autorizacion antes del hash para poder incluirlo
        auth_raw = movement.get("codigo_autorizacion", "") or ""
        codigo_autorizacion = self._normalize_auth(auth_raw) or None

        # tx_hash: para cualquier movimiento SIN codigo_autorizacion (pendientes y confirmados).
        # Cuando hay codigo_autorizacion, el conflict target es (codigo_autorizacion, num_cuotas)
        # y tx_hash no se usa — se guarda NULL para no ocupar la constraint UNIQUE.
        # Dar tx_hash a pendientes permite guardar clasificaciones que persisten al confirmarse.
        # Normalizar descripción antes del hash: el banco usa mixed-case con asterisco final en
        # pendientes ("COMPRA SumUp * Isidora*") y mayúsculas sin asterisco en confirmados
        # ("COMPRA SUMUP * ISIDORA") — la misma transacción daría hashes distintos sin normalizar.
        fecha_para_hash = fecha_compra_raw if fecha_compra_raw else f"{fecha_raw}|{periodo_fac}"
        desc_para_hash = descripcion.rstrip("*").strip().upper()
        potential_hash = _make_tx_hash(fecha_para_hash, desc_para_hash, monto_raw)

        if not codigo_autorizacion:
            tx_hash = potential_hash
        else:
            tx_hash = None

        params = {
            "fecha": fecha,
            "descripcion": descripcion,
            "persona": str(movement.get("persona", "") or "").strip() or None,
            "monto": monto,
            "monto_periodo": monto_periodo,
            "pendiente": pendiente,
            "rubro": str(movement.get("rubro", "") or "").strip() or None,
            "comercio": str(movement.get("comercio", "") or "").strip() or None,
            "codigo_autorizacion": codigo_autorizacion,
            "fecha_compra": fecha_compra,
            "hora": str(movement.get("hora", "") or "").strip() or None,
            "pais": str(movement.get("pais", "") or "").strip() or None,
            "origen": str(movement.get("origen", "") or "").strip() or None,
            "periodo_facturacion": periodo_fac or None,
            "periodo": periodo,
            "num_cuotas": str(movement.get("num_cuotas", "") or "").strip() or None,
            "valor_cuota": valor_cuota,
            "tx_hash": tx_hash,
            # Procedencia: NULL en el pase normal. Solo se sella al INSERTAR (nunca en el
            # DO UPDATE), para que el rollback `DELETE ... WHERE backfill_run_id = N` borre
            # exactamente lo que el backfill creó y no filas que ya existían.
            "backfill_run_id": self.run_id if self._in_backfill else None,
        }

        cur = self.db_conn.cursor()
        try:
            if codigo_autorizacion:
                # Eliminar fila previa sin codigo_autorizacion (guardada con tx_hash) que
                # corresponde a esta misma transacción. Ocurre cuando el modal no cargó en
                # un run anterior y ahora sí tiene auth code: sin este DELETE quedarían
                # dos filas para la misma transacción (duplicados).
                if potential_hash:
                    # Acotado al período: todas las cuotas de una compra comparten `tx_hash`,
                    # así que sin el filtro un backfill de un mes viejo —o el pase normal al
                    # procesar la cuota de un período con auth code recién disponible— borraría
                    # las filas de los otros períodos de la misma compra.
                    cur.execute(
                        "DELETE FROM movimientos "
                        " WHERE tx_hash = %s AND codigo_autorizacion IS NULL"
                        "   AND periodo IS NOT DISTINCT FROM %s",
                        (potential_hash, periodo),
                    )
                cur.execute(
                    """
                    INSERT INTO movimientos
                        (fecha, descripcion, persona, monto, monto_periodo, pendiente,
                         rubro, comercio, codigo_autorizacion, fecha_compra, hora,
                         pais, origen, periodo_facturacion, periodo, num_cuotas,
                         valor_cuota, tx_hash, backfill_run_id, updated_at)
                    VALUES
                        (%(fecha)s, %(descripcion)s, %(persona)s, %(monto)s, %(monto_periodo)s,
                         %(pendiente)s, %(rubro)s, %(comercio)s, %(codigo_autorizacion)s,
                         %(fecha_compra)s, %(hora)s, %(pais)s, %(origen)s,
                         %(periodo_facturacion)s, %(periodo)s, %(num_cuotas)s,
                         %(valor_cuota)s, %(tx_hash)s, %(backfill_run_id)s, NOW())
                    ON CONFLICT (codigo_autorizacion, num_cuotas) DO UPDATE SET
                        fecha             = EXCLUDED.fecha,
                        descripcion       = EXCLUDED.descripcion,
                        persona           = EXCLUDED.persona,
                        monto             = EXCLUDED.monto,
                        monto_periodo     = EXCLUDED.monto_periodo,
                        pendiente         = EXCLUDED.pendiente,
                        rubro             = EXCLUDED.rubro,
                        comercio          = EXCLUDED.comercio,
                        fecha_compra      = EXCLUDED.fecha_compra,
                        hora              = EXCLUDED.hora,
                        pais              = EXCLUDED.pais,
                        origen            = EXCLUDED.origen,
                        periodo_facturacion = EXCLUDED.periodo_facturacion,
                        periodo           = EXCLUDED.periodo,
                        valor_cuota       = EXCLUDED.valor_cuota,
                        tx_hash           = EXCLUDED.tx_hash,
                        updated_at        = NOW()
                    """,
                    params,
                )
            else:
                cur.execute(
                    """
                    INSERT INTO movimientos
                        (fecha, descripcion, persona, monto, monto_periodo, pendiente,
                         rubro, comercio, codigo_autorizacion, fecha_compra, hora,
                         pais, origen, periodo_facturacion, periodo, num_cuotas,
                         valor_cuota, tx_hash, backfill_run_id, updated_at)
                    VALUES
                        (%(fecha)s, %(descripcion)s, %(persona)s, %(monto)s, %(monto_periodo)s,
                         %(pendiente)s, %(rubro)s, %(comercio)s, %(codigo_autorizacion)s,
                         %(fecha_compra)s, %(hora)s, %(pais)s, %(origen)s,
                         %(periodo_facturacion)s, %(periodo)s, %(num_cuotas)s,
                         %(valor_cuota)s, %(tx_hash)s, %(backfill_run_id)s, NOW())
                    ON CONFLICT (tx_hash, periodo) WHERE tx_hash IS NOT NULL DO UPDATE SET
                        fecha             = EXCLUDED.fecha,
                        descripcion       = EXCLUDED.descripcion,
                        persona           = EXCLUDED.persona,
                        monto             = EXCLUDED.monto,
                        monto_periodo     = EXCLUDED.monto_periodo,
                        pendiente         = EXCLUDED.pendiente,
                        rubro             = EXCLUDED.rubro,
                        comercio          = EXCLUDED.comercio,
                        hora              = EXCLUDED.hora,
                        pais              = EXCLUDED.pais,
                        origen            = EXCLUDED.origen,
                        periodo_facturacion = EXCLUDED.periodo_facturacion,
                        periodo           = EXCLUDED.periodo,
                        num_cuotas        = EXCLUDED.num_cuotas,
                        valor_cuota       = EXCLUDED.valor_cuota,
                        updated_at        = NOW()
                    """,
                    params,
                )

            # Migrar clasificaciones y splits cuando pendiente → confirmado cambia la llave.
            # Casos: (1) tx_hash cambia porque fecha_compra no estaba en el modal del pendiente;
            #        (2) banco vuelve a estructura antigua y aparece codigo_autorizacion.
            if not pendiente:
                desc_norm = descripcion.rstrip("*").strip().upper()
                monto_norm = str(int(abs(_parse_monto(monto_raw) or 0)))
                # El hash real que tenía el pendiente en DB (puede diferir de potential_hash
                # si fecha_compra no estaba disponible cuando se scrapeó como pendiente).
                old_hash = self.pending_hashes.pop((desc_norm, monto_norm), None)
                # Fallback: si no está en pending_hashes, usar potential_hash (cubre el caso en
                # que el banco sirve auth code y potential_hash coincide con el pendiente).
                if not old_hash and codigo_autorizacion and potential_hash:
                    old_hash = potential_hash

                if old_hash:
                    if codigo_autorizacion:
                        # tx_hash → codigo_autorizacion: migrar clasificación y splits
                        cur.execute(
                            """
                            UPDATE clasificaciones
                               SET codigo_autorizacion = %s, tx_hash = NULL, updated_at = NOW()
                             WHERE tx_hash = %s
                               AND NOT EXISTS (
                                   SELECT 1 FROM clasificaciones WHERE codigo_autorizacion = %s
                               )
                            """,
                            (codigo_autorizacion, old_hash, codigo_autorizacion),
                        )
                        migrated_cls = cur.rowcount
                        cur.execute(
                            """
                            UPDATE splits
                               SET codigo_autorizacion = %s, tx_hash = NULL
                             WHERE tx_hash = %s
                               AND NOT EXISTS (
                                   SELECT 1 FROM splits WHERE codigo_autorizacion = %s
                               )
                            """,
                            (codigo_autorizacion, old_hash, codigo_autorizacion),
                        )
                        migrated_spl = cur.rowcount
                    elif tx_hash and old_hash != tx_hash:
                        # tx_hash cambió (fecha_compra diferente entre pendiente y confirmado)
                        cur.execute(
                            """
                            UPDATE clasificaciones
                               SET tx_hash = %s, updated_at = NOW()
                             WHERE tx_hash = %s
                               AND NOT EXISTS (
                                   SELECT 1 FROM clasificaciones WHERE tx_hash = %s
                               )
                            """,
                            (tx_hash, old_hash, tx_hash),
                        )
                        migrated_cls = cur.rowcount
                        cur.execute(
                            """
                            UPDATE splits
                               SET tx_hash = %s
                             WHERE tx_hash = %s
                               AND NOT EXISTS (
                                   SELECT 1 FROM splits WHERE tx_hash = %s
                               )
                            """,
                            (tx_hash, old_hash, tx_hash),
                        )
                        migrated_spl = cur.rowcount
                    else:
                        migrated_cls = migrated_spl = 0

                    if migrated_cls or migrated_spl:
                        new_id = codigo_autorizacion or tx_hash
                        logger.info(
                            f"Migración pendiente→confirmado: {old_hash} → {new_id} "
                            f"({migrated_cls} cls, {migrated_spl} splits) — {descripcion}"
                        )

            self.db_conn.commit()
        except Exception:
            self.db_conn.rollback()
            raise
        finally:
            cur.close()

    async def _screenshot(self, page: Page, name: str, *, error: bool = False) -> None:
        if self.debug_mode or error:
            await page.screenshot(path=str(DEBUG_DIR / "screenshots" / f"{name}.png"))

    async def _dismiss_service_popup(self, page: Page) -> None:
        """Cierra el popup 'En estos momentos no lo podemos atender' si aparece.
        El popup está dentro de Shadow DOM, requiere traversal con JS.

        El botón se busca por texto ('Entendido') **y** por contexto: el sitio público
        tiene otros dos 'Entendido' (el aviso de cookies y `#btn-login-client-nuevo`,
        dentro del drawer de login) que no deben clickearse.
        """
        JS_POPUP = """
        (mode) => {
            const NEEDLE = 'no lo podemos atender';
            function inServicePopup(el) {
                let n = el;
                for (let i = 0; i < 8 && n && n.tagName !== 'BODY'; i++) {
                    if ((n.textContent || '').toLowerCase().includes(NEEDLE)) return true;
                    const root = n.getRootNode();
                    n = n.parentElement || (root && root.host) || null;
                }
                return false;
            }
            function findButton(root) {
                for (const el of root.querySelectorAll('*')) {
                    if (el.tagName === 'BUTTON' &&
                        el.textContent.trim() === 'Entendido' &&
                        inServicePopup(el)) {
                        return el;
                    }
                    if (el.shadowRoot) {
                        const found = findButton(el.shadowRoot);
                        if (found) return found;
                    }
                }
                return null;
            }
            const btn = findButton(document);
            if (!btn) return false;
            if (mode === 'click') btn.click();
            return true;
        }
        """
        try:
            await page.wait_for_function(JS_POPUP, arg="probe", timeout=5000)
            dismissed = await page.evaluate(JS_POPUP, "click")
            if dismissed:
                logger.info("Popup de servicio no disponible cerrado")
        except Exception:
            pass

    async def _dismiss_blocking_modals(self, page: Page, attempts: int = 3) -> bool:
        """Cierra los modales post-login que dejan un backdrop interceptando los clics.

        Desde 2026-08-12 el banco muestra `app-popup-terms-conditions-optin` (opt-in de
        CMR Puntos): su `div.backdrop` (z-index 1000) bloquea el click en la tarjeta y
        cualquier click posterior. Se cierra con la × (`button.close-misdocumentos`),
        nunca con 'Acepto' — eso inscribiría al usuario en el programa.

        Devuelve True si no queda ningún backdrop bloqueando.
        """
        CLOSE_SELECTORS = [
            "#optin button.close-misdocumentos",
            "app-popup-terms-conditions-optin button[class*='close']",
            "app-marketing button",
        ]

        # El modal puede tardar en montarse tras el login.
        try:
            await page.wait_for_function(JS_HAS_BLOCKING_BACKDROP, timeout=5000)
        except Exception:
            return True  # nunca apareció

        for _ in range(attempts):
            if not await page.evaluate(JS_HAS_BLOCKING_BACKDROP):
                return True

            closed = None
            for sel in CLOSE_SELECTORS:
                try:
                    await page.locator(sel).first.click(timeout=2000)
                    closed = sel
                    break
                except Exception:
                    continue
            if not closed:
                closed = await page.evaluate(JS_CLICK_PROMO_CLOSE)
            if closed:
                logger.info(f"Modal bloqueante cerrado ({closed})")
            else:
                await page.keyboard.press("Escape")
            await page.wait_for_timeout(1200)  # animación de cierre

        if await page.evaluate(JS_HAS_BLOCKING_BACKDROP):
            logger.warning("El backdrop sigue visible tras intentar cerrar los modales")
            await self._screenshot(page, "backdrop_bloqueado", error=True)
            return False
        return True

    # ------------------------------------------------------------------ #
    # Login                                                                #
    # ------------------------------------------------------------------ #

    async def _login_with_retry(self, page: Page, max_attempts: int = 3) -> bool:
        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                logger.info(f"Reintentando login (intento {attempt}/{max_attempts})...")
                await page.wait_for_timeout(30000)
            if await self.login(page):
                return True
        return False

    async def login(self, page: Page) -> bool:
        await page.goto("https://www.bancofalabella.cl/", wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)

        await self._dismiss_service_popup(page)

        # Desde 2026-08-18 el sitio público es un Next.js: el header ya no tiene
        # `#main-header__sub-content` y el formulario vive en un drawer que monta sus
        # inputs solo al clickear "Mi Cuenta". Las clases son CSS-modules con hash
        # (cambian en cada deploy), así que solo se usan anclas estables: el id
        # `#main-header`, el texto del botón y los ids `#document` / `#pass`.
        login_btn = page.locator("#main-header button:visible", has_text="Mi Cuenta").first
        try:
            await login_btn.wait_for(state="visible", timeout=20000)
        except Exception:
            await self._screenshot(page, "login_btn_not_found", error=True)
            logger.error("No apareció el botón 'Mi Cuenta' en el header")
            return False

        rut_field = page.locator("#document:visible").first
        for attempt in (1, 2):
            if await rut_field.is_visible():
                break
            await login_btn.click()
            try:
                await rut_field.wait_for(state="visible", timeout=6000)
                break
            except Exception:
                # Next.js hidrata el header después del SSR: el primer click puede caer
                # antes de que el handler esté montado.
                logger.info(f"El drawer de login no abrió (intento {attempt}/2)")
                await page.wait_for_timeout(1500)

        if not await rut_field.is_visible():
            await self._screenshot(page, "login_rut_not_found", error=True)
            logger.error("No apareció el campo RUT tras hacer click en 'Mi Cuenta'")
            return False

        # El campo formatea el RUT solo y tiene maxlength=10: hay que escribirlo sin puntos.
        await rut_field.click()
        await rut_field.type(self.username.replace(".", "").strip(), delay=80)

        pass_field = page.locator("#pass:visible").first
        await pass_field.click()
        await pass_field.type(self.password, delay=80)

        try:
            # El submit del drawer se habilita cuando ambos campos son válidos. Se ancla
            # al form que contiene #pass para no depender de las clases con hash.
            submit = page.locator("form:has(#pass) button[type='submit']:not([disabled])")
            await submit.wait_for(state="visible", timeout=20000)
            await submit.click()
        except Exception:
            await self._screenshot(page, "login_submit_failed", error=True)
            logger.error("No apareció el botón de login habilitado")
            return False

        try:
            await page.wait_for_selector("//span[normalize-space(text())='Hola']", timeout=20000)
            logger.info("Login exitoso")
            return True
        except Exception:
            await self._dismiss_service_popup(page)
            try:
                await page.wait_for_selector("//span[normalize-space(text())='Hola']", timeout=10000)
                logger.info("Login exitoso (tras cerrar popup)")
                return True
            except Exception:
                pass
            logger.error("Login fallido")
            await self._screenshot(page, "login_failed", error=True)
            return False

    # ------------------------------------------------------------------ #
    # Navegación                                                           #
    # ------------------------------------------------------------------ #

    async def navigate_to_movements(self, page: Page) -> bool:

        await self._dismiss_blocking_modals(page)

        card_link = page.locator(
            "a.div-product",
            has=page.locator("div.product-name", has_text="CMR Mastercard"),
        ).first
        try:
            await card_link.wait_for(state="visible", timeout=15000)
        except Exception:
            logger.warning("Usando primera tarjeta disponible...")
            card_link = page.locator("a.div-product").first
            await card_link.wait_for(state="visible", timeout=10000)

        try:
            await card_link.click(timeout=15000)
        except Exception:
            # Un modal apareció después del dismiss: cerrar y reintentar.
            logger.warning("Click en la tarjeta bloqueado — cerrando modales y reintentando")
            await self._dismiss_blocking_modals(page)
            try:
                await card_link.click(timeout=10000)
            except Exception:
                logger.warning("Click bloqueado de nuevo — usando click directo en el DOM")
                await card_link.evaluate("el => el.click()")

        try:
            # Siempre la pestaña por defecto: el override de `row_selector` del pase de
            # facturados no debe afectar la navegación inicial.
            await page.wait_for_selector(self.ROW_SELECTOR, timeout=30000)
            logger.info("Tabla cargada")
            await self._screenshot(page, "movements_loaded")
            self.periodo_facturacion = await self._extract_periodo(page)
            if self.periodo_facturacion:
                logger.info(f"Período de facturación: {self.periodo_facturacion}")
            return True
        except Exception:
            await self._screenshot(page, "movements_table_not_found", error=True)
            logger.error("No apareció la tabla")
            return False

    async def _extract_periodo(self, page: Page) -> str:
        """Extrae la fecha de próxima facturación desde la página de movimientos.
        El texto aparece junto: 'Próxima facturación 19/03/2026'
        """
        return await page.evaluate("""
        () => {
            function findText(root, needle) {
                for (const el of root.querySelectorAll('*')) {
                    if (el.textContent.includes(needle) && el.children.length <= 2)
                        return el;
                    if (el.shadowRoot) {
                        const found = findText(el.shadowRoot, needle);
                        if (found) return found;
                    }
                }
                return null;
            }
            const el = findText(document, 'Próxima facturación') ||
                       findText(document, 'Proxima facturacion');
            if (!el) return '';
            const match = el.textContent.match(/\\d{2}\\/\\d{2}\\/\\d{4}/);
            return match ? match[0] : '';
        }
        """)

    # ------------------------------------------------------------------ #
    # Backfill del período facturado                                       #
    # ------------------------------------------------------------------ #

    def _closed_periodo(self) -> Optional[str]:
        """'YYYY-MM' del período recién cerrado, derivado de 'Próxima facturación' menos un mes.

        Se deriva del label del banco y no del reloj: el valor cambia justo cuando el banco rota
        la fecha de próxima facturación, que *es* la definición de "el ciclo cerró". Sin
        aritmética de fechas ni zonas horarias.
        """
        d = _parse_date(self.periodo_facturacion)
        if not d:
            return None
        y, m = (d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12)
        return f"{y:04d}-{m:02d}"

    def _backfill_done(self, periodo: str) -> Optional[bool]:
        """True = ya se completó, False = falta, None = no se pudo determinar.

        None significa **no hacer nada**: si no se puede leer la marca (por ejemplo porque falta
        la migración 007), correr el pase a ciegas lo repetiría en cada corrida.
        """
        cur = self.db_conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM scraper_runs "
                " WHERE backfill_periodo = %s AND status = 'success' LIMIT 1",
                (periodo,),
            )
            return cur.fetchone() is not None
        except Exception as e:
            logger.warning(f"No se pudo leer la marca de backfill ({e}) — se omite el pase")
            return None
        finally:
            cur.close()
            self.db_conn.rollback()

    def _mark_backfill_done(self, periodo: str) -> None:
        if not self.run_id:
            return
        cur = self.db_conn.cursor()
        try:
            cur.execute(
                "UPDATE scraper_runs SET backfill_periodo = %s WHERE id = %s",
                (periodo, self.run_id),
            )
            self.db_conn.commit()
            logger.info(f"Período {periodo} marcado como completado desde facturados")
        except Exception as e:
            self.db_conn.rollback()
            logger.warning(f"No se pudo marcar el backfill de {periodo}: {e}")
        finally:
            cur.close()

    @staticmethod
    def _desc_alias(descripcion: str) -> str:
        """Descripción normalizada, sin el prefijo de cuotas.

        La misma cuota se llama distinto según la pestaña: "Últimos movimientos" la muestra como
        `COMPRA CUOTAS SIN INTERES MERCADO PAGO` y "Movimientos facturados" como
        `COMPRA EN CUOTAS MERCADO PAGO`. Además la fecha difiere en un día entre las dos vistas,
        así que la clave natural `(fecha, descripcion, monto, cuotas)` no puede matchearlas.
        """
        d = " ".join(str(descripcion or "").rstrip("*").strip().upper().split())
        for prefijo in ("COMPRA CUOTAS SIN INTERES ", "COMPRA EN CUOTAS "):
            if d.startswith(prefijo):
                return d[len(prefijo):]
        return d

    def _alias_key(self, movement: Dict[str, Any]) -> tuple:
        """Clave tolerante a la diferencia de descripción y fecha entre las dos pestañas:
        (descripción sin prefijo de cuotas, monto, cuotas). Se usa solo dentro de un período,
        y solo para NO insertar — nunca para escribir."""
        monto_norm = str(int(abs(_parse_monto(str(movement.get("monto", "") or "")) or 0)))
        cuotas = str(movement.get("num_cuotas", "") or "").strip()
        return (self._desc_alias(movement.get("descripcion", "")), monto_norm, cuotas)

    def _load_period_index(self, periodo: str) -> tuple:
        """(auth_keys, alias_keys) de un período: la identidad que ya está guardada.

        `auth_keys` es la llave única real de la tabla `(codigo_autorizacion, num_cuotas)` y es la
        guarda dura antes de escribir. `alias_keys` es la tolerante, para clasificar bien las
        candidatas en el reporte y no abrir modales al vacío.
        """
        auth_keys, alias_keys = set(), set()
        cur = self.db_conn.cursor()
        try:
            cur.execute(
                "SELECT descripcion, monto, num_cuotas, codigo_autorizacion "
                "  FROM movimientos WHERE periodo = %s",
                (periodo,),
            )
            for r in cur.fetchall():
                cuotas = str(r["num_cuotas"] or "").strip()
                if r["codigo_autorizacion"]:
                    auth_keys.add((self._normalize_auth(r["codigo_autorizacion"]), cuotas))
                monto_norm = str(int(abs(float(r["monto"] or 0))))
                alias_keys.add((self._desc_alias(r["descripcion"]), monto_norm, cuotas))
        except Exception as e:
            logger.warning(f"No se pudo indexar el período {periodo}: {e}")
        finally:
            cur.close()
            self.db_conn.rollback()
        return auth_keys, alias_keys

    def _backfill_row_ok(self, movement: Dict[str, Any]) -> tuple:
        """(ok, motivo) — filtros de sanidad sobre una fila del estado de cuenta."""
        if not DATE_RE.match(str(movement.get("fecha", "") or "")):
            return False, "sin fecha en la tabla"
        cuotas = str(movement.get("num_cuotas", "") or "").strip()
        en_cuotas = cuotas not in ("", "01/01", "01 de 01", "/")
        if en_cuotas and not str(movement.get("valor_cuota", "") or "").strip():
            # monto_periodo caería al monto total de la compra e inflaría el período.
            return False, f"cuota {cuotas} sin 'Cuota a pagar'"
        return True, ""

    async def _billed_wait_load(self, page: Page, gone_tries: int = 25) -> None:
        """Espera el ciclo de carga del componente: loader visible → loader oculto.

        Hace falta porque al cambiar de estado de cuenta el DOM del anterior sigue montado unos
        segundos: leer el estado antes de que arranque la carga devuelve el resultado viejo.
        """
        for _ in range(12):  # hasta 6s a que ARRANQUE la carga
            if await page.evaluate(JS_BILLED_LOADING):
                break
            await page.wait_for_timeout(500)
        for _ in range(gone_tries):  # hasta 50s a que TERMINE
            if not await page.evaluate(JS_BILLED_LOADING):
                return
            await page.wait_for_timeout(2000)

    async def _billed_state(self, page: Page, tries: int = 25, confirm: int = 3) -> str:
        """Estado de la vista. `con_filas` se acepta al toque; un estado vacío o de error hay
        que verlo `confirm` veces seguidas para creerlo (evita leer el render anterior)."""
        state, repetido, previo = "cargando", 0, ""
        for _ in range(tries):
            state = await page.evaluate(JS_BILLED_STATE)
            if state == "con_filas":
                return state
            repetido = repetido + 1 if state == previo else 1
            previo = state
            if state != "cargando" and repetido >= confirm:
                return state
            await page.wait_for_timeout(2000)
        return state

    async def _billed_settle(self, page: Page, retries: int = 3) -> str:
        """Estado final de la vista, usando el botón Reintentar del banco si la carga falló."""
        state = await self._billed_state(page)
        for i in range(retries):
            if state != "error_carga":
                break
            logger.info(f"La vista de facturados falló al cargar — Reintentar ({i + 1}/{retries})")
            if not await page.evaluate(JS_CLICK_BILLED_RETRY):
                break
            await page.wait_for_timeout(4000)
            state = await self._billed_state(page)
        return state

    async def _open_billed_tab(self, page: Page) -> bool:
        if not await page.evaluate(JS_CLICK_BILLED_TAB, self.BILLED_TAB_LABEL):
            logger.error(f"No se encontró la pestaña '{self.BILLED_TAB_LABEL}'")
            return False
        # El componente monta el dropdown de estados de cuenta unos segundos después del click;
        # leer las opciones antes devuelve una lista vacía.
        try:
            await page.locator(BILLED_STATEMENT_SELECT).first.wait_for(
                state="attached", timeout=30000
            )
        except Exception:
            logger.error("La pestaña de facturados no montó el selector de estados de cuenta")
            await self._screenshot(page, "backfill_sin_selector", error=True)
            return False
        return True

    async def _billed_statements(self, page: Page) -> List[str]:
        """Etiquetas 'dd/mm/yyyy' de los estados de cuenta que ofrece el dropdown."""
        try:
            opts = await page.locator(f"{BILLED_STATEMENT_SELECT} option").all_text_contents()
        except Exception:
            return []
        return [o.strip() for o in opts if o.strip()]

    async def _billed_statements_ready(self, page: Page, tries: int = 15) -> List[str]:
        """Igual que `_billed_statements`, pero espera a que el dropdown termine de poblarse.

        El componente lo monta en dos etapas: primero con el estado de cuenta vigente como única
        opción, y unos segundos después con los 12 que ofrece el banco. Leerlo antes daría una
        lista de un solo elemento y el período pedido parecería no existir.
        """
        previa: List[str] = []
        for _ in range(tries):
            actual = await self._billed_statements(page)
            if len(actual) > 1 and actual == previa:
                return actual
            previa = actual
            await page.wait_for_timeout(1000)
        return previa

    async def _load_statement(self, page: Page, label: str, force_reload: bool = False) -> str:
        """Selecciona un estado de cuenta y devuelve el estado final de la vista."""
        sel = page.locator(BILLED_STATEMENT_SELECT)
        if force_reload:
            # select_option no dispara la recarga si el valor no cambia, y hace falta volver a
            # la página 1 para la fase 2: se pasa por otro estado de cuenta y se vuelve.
            otros = [l for l in await self._billed_statements_ready(page) if l != label]
            if otros:
                await sel.select_option(label=otros[0])
                await self._billed_wait_load(page)
        await sel.select_option(label=label)
        await self._billed_wait_load(page)
        state = await self._billed_settle(page)

        d = _parse_date(label)
        value = await sel.input_value()
        if not d or not str(value).startswith(d.strftime("%Y-%m-%d")):
            logger.error(f"No quedó seleccionado el estado de cuenta {label} (select={value})")
            return "seleccion_fallida"
        return state

    async def _billed_collect(self, page: Page) -> List[tuple]:
        """Fase 1: recorre las páginas leyendo SOLO celdas de tabla. No abre modales ni escribe.

        Devuelve [(nro_pagina, indice_fila, movimiento, clave), ...].
        """
        collected: List[tuple] = []
        page_num, prev_sig = 0, ""
        while True:
            page_num += 1
            total = await self._count_rows(page)
            sig = (
                await page.locator(self.row_selector).nth(0).inner_text() if total else ""
            )
            if sig and sig == prev_sig:
                logger.info("  Página repetida — fin del recorrido")
                break
            prev_sig = sig

            leidas = 0
            for i in range(total):
                mv = await self._read_row(page, i)
                if not mv.get("descripcion", "").strip() and not mv.get("monto", "").strip():
                    continue
                collected.append((page_num, i, mv, self._movement_key(mv)))
                leidas += 1
            logger.info(f"  Página {page_num}: {leidas} filas")

            if not await self._has_next_page(page):
                break
            await self._go_next_page(page)
        return collected

    async def _reconcile_log(self, page: Page, periodo: str, label: str) -> None:
        """Loguea el total del banco contra el de la DB. Nunca corta la corrida: pagos y
        reversas hacen que la igualdad se rompa legítimamente."""
        header = await page.evaluate(JS_BILLED_HEADER) or {}
        cur = self.db_conn.cursor()
        try:
            cur.execute(
                "SELECT count(*) n, coalesce(sum(monto_periodo), 0) neto, "
                "       coalesce(sum(monto_periodo) FILTER (WHERE monto_periodo > 0), 0) cargos "
                "  FROM movimientos WHERE periodo = %s",
                (periodo,),
            )
            row = cur.fetchone() or {}
            msg = (f"Reconciliación {periodo}: DB {row.get('n')} filas, "
                   f"cargos {row.get('cargos')}, neto {row.get('neto')}")
            if header.get("fecha_facturacion") == label and header.get("monto_facturado"):
                msg += f" — banco informa monto facturado {header['monto_facturado']}"
            logger.info(msg)
        except Exception as e:
            logger.warning(f"No se pudo reconciliar {periodo}: {e}")
        finally:
            cur.close()
            self.db_conn.rollback()

    def _report_backfill_dupes(self) -> None:
        """Detecta (sin borrar) posibles duplicados: un pendiente que coincide en descripción y
        monto con una fila recién insertada. Es el hueco residual del camino por tx_hash."""
        if not self.run_id:
            return
        cur = self.db_conn.cursor()
        try:
            cur.execute(
                """
                SELECT b.id AS id_facturado, b.fecha, b.descripcion, b.monto,
                       p.id AS id_pendiente, p.periodo AS periodo_pendiente
                  FROM movimientos b
                  JOIN movimientos p
                    ON p.pendiente
                   AND upper(btrim(rtrim(p.descripcion, '*'))) =
                       upper(btrim(rtrim(b.descripcion, '*')))
                   AND round(abs(p.monto)) = round(abs(b.monto))
                 WHERE b.backfill_run_id = %s
                """,
                (self.run_id,),
            )
            dupes = cur.fetchall()
            for d in dupes:
                logger.warning(
                    "Posible duplicado tras el backfill: facturada id=%s (%s %s) coincide con "
                    "pendiente id=%s del período %s — revisar a mano",
                    d["id_facturado"], d["fecha"], d["descripcion"],
                    d["id_pendiente"], d["periodo_pendiente"],
                )
        except Exception as e:
            logger.warning(f"No se pudo revisar duplicados del backfill: {e}")
        finally:
            cur.close()
            self.db_conn.rollback()

    async def backfill_period(self, page: Page, periodo: str) -> int:
        """Completa desde "Movimientos facturados" las filas que faltan de un período cerrado.

        Devuelve la cantidad de filas insertadas, o **-1** si no se pudo completar el período
        (el banco no publicó el detalle todavía, o una guarda abortó el pase). -1 significa
        *no marcar*: la corrida de mañana reintenta.
        """
        if not await self._open_billed_tab(page):
            return -1

        labels = await self._billed_statements_ready(page)
        match = [l for l in labels
                 if (_parse_date(l) and _parse_date(l).strftime("%Y-%m") == periodo)]
        if not match:
            logger.warning(
                f"backfill {periodo}: el banco no ofrece ese estado de cuenta "
                f"(disponibles: {', '.join(labels) or 'ninguno'})"
            )
            return -1
        label = match[0]

        state = await self._load_statement(page, label)
        if state == "sin_detalle":
            logger.info(
                f"backfill {periodo}: el banco todavía no publicó el detalle del estado de "
                f"cuenta {label} — se reintenta en la próxima corrida"
            )
            return -1
        if state != "con_filas":
            logger.warning(f"backfill {periodo}: la vista quedó en estado '{state}'")
            await self._screenshot(page, f"backfill_{periodo}_{state}", error=True)
            return -1

        saved_selector = self.row_selector
        self.row_selector = self.BILLED_ROW_SELECTOR
        try:
            logger.info(f"backfill {periodo}: leyendo el estado de cuenta {label}")
            collected = await self._billed_collect(page)
            total = len(collected)
            if total == 0:
                logger.warning(f"backfill {periodo}: 0 filas leídas")
                return -1

            auth_keys, alias_keys = self._load_period_index(periodo)

            conocidas = sum(1 for c in collected if c[3] in self.existing_keys)
            por_alias = [
                c for c in collected
                if c[3] not in self.existing_keys and self._alias_key(c[2]) in alias_keys
            ]
            solape = (conocidas + len(por_alias)) / total
            logger.info(
                f"backfill {periodo}: {total} filas en el estado de cuenta, "
                f"{conocidas} ya en la DB por clave natural, {len(por_alias)} por descripción "
                f"alternativa (solape {solape:.0%})"
            )
            for c in por_alias:
                logger.info(
                    f"  ya está con otra descripción/fecha: {c[2].get('fecha')} | "
                    f"{c[2].get('descripcion')} | {c[2].get('monto')} | {c[2].get('num_cuotas')}"
                )
            if solape < self.backfill_min_overlap:
                logger.error(
                    f"backfill {periodo}: solape {solape:.0%} bajo el mínimo "
                    f"({self.backfill_min_overlap:.0%}) — probablemente la vista cambió de "
                    f"formato y las claves no matchean. Abortado sin escribir."
                )
                return -1

            alias_por_saltar = {(c[0], c[1]) for c in por_alias}
            candidatas, descartadas = [], []
            for c in collected:
                if c[3] in self.existing_keys or (c[0], c[1]) in alias_por_saltar:
                    continue
                ok, motivo = self._backfill_row_ok(c[2])
                (candidatas if ok else descartadas).append((c, motivo))
            for (c, motivo) in descartadas:
                logger.warning(
                    f"  descartada ({motivo}): {c[2].get('fecha')} "
                    f"{c[2].get('descripcion')} {c[2].get('monto')}"
                )
            if not candidatas:
                logger.info(f"backfill {periodo}: no falta ninguna fila")
                await self._reconcile_log(page, periodo, label)
                return 0
            if len(candidatas) > self.backfill_max_new:
                logger.error(
                    f"backfill {periodo}: {len(candidatas)} filas nuevas superan el tope de "
                    f"{self.backfill_max_new} — abortado sin escribir"
                )
                return -1

            logger.info(f"backfill {periodo}: {len(candidatas)} filas faltantes")
            for (c, _) in candidatas:
                mv = c[2]
                logger.info(
                    f"  falta: {mv.get('fecha')} | {mv.get('descripcion')} | "
                    f"{mv.get('monto')} | cuotas {mv.get('num_cuotas')} | "
                    f"cuota a pagar {mv.get('valor_cuota')}"
                )
            if self.backfill_dry_run:
                logger.info(f"backfill {periodo}: dry-run, no se escribió nada")
                return 0

            # --- fase 2: volver a la página 1 y abrir el modal solo de las faltantes ---
            if await self._load_statement(page, label, force_reload=True) != "con_filas":
                logger.error(f"backfill {periodo}: no se pudo recargar el estado de cuenta")
                return -1

            objetivo: Dict[int, List[tuple]] = {}
            for (c, _) in candidatas:
                objetivo.setdefault(c[0], []).append((c[1], c[3]))
            ultima_pagina = max(objetivo)

            insertadas, salteadas = 0, 0
            page_num = 0
            self._in_backfill = True
            try:
                while True:
                    page_num += 1
                    for (idx, key) in objetivo.get(page_num, []):
                        mv = await self._read_row(page, idx)
                        if self._movement_key(mv) != key:
                            logger.warning(
                                f"  fila {idx} de la página {page_num} no coincide con la "
                                f"fase 1 — salteada"
                            )
                            salteadas += 1
                            continue
                        detail = await self._open_and_read_detail(page, idx)
                        detail.pop("modal_cuotas", None)
                        mv.update(detail)

                        # Guarda dura: `(codigo_autorizacion, num_cuotas)` es la llave única real
                        # de la tabla. Si ya está, esta fila es un movimiento que la DB tiene con
                        # otra descripción o fecha — insertarlo reescribiría la fila buena.
                        auth = self._normalize_auth(mv.get("codigo_autorizacion", ""))
                        cuotas = str(mv.get("num_cuotas", "") or "").strip()
                        if auth and (auth, cuotas) in auth_keys:
                            logger.info(
                                f"  = ya existe con auth {auth} cuotas {cuotas} "
                                f"({mv.get('descripcion')}) — no se toca"
                            )
                            continue
                        # Un estado de cuenta no tiene pendientes: si el modal falló, la cascada
                        # de extract_all_movements marcaría pendiente y el reset de mañana la
                        # borraría, con el período ya marcado como hecho.
                        mv["pendiente"] = False
                        mv["periodo_facturacion"] = label
                        self._upsert_to_db(mv)
                        self.existing_keys.add(self._movement_key(mv))
                        insertadas += 1
                        self._cnt_procesados += 1
                        self._cnt_nuevos += 1
                        logger.info(
                            f"  + {mv.get('fecha')} {mv.get('descripcion')} "
                            f"{mv.get('monto')} (auth={mv.get('codigo_autorizacion') or '∅'})"
                        )
                    if page_num >= ultima_pagina or not await self._has_next_page(page):
                        break
                    await self._go_next_page(page)
            finally:
                self._in_backfill = False

            self._report_backfill_dupes()
            await self._reconcile_log(page, periodo, label)
            logger.info(f"backfill {periodo}: {insertadas} filas insertadas")
            if salteadas:
                # Quedó al menos una fila sin escribir: el período NO está completo, así que no
                # se marca. La corrida de mañana reintenta y, por ser insert-only, no duplica.
                logger.warning(
                    f"backfill {periodo}: {salteadas} filas quedaron sin escribir — el período "
                    f"no se marca y se reintenta en la próxima corrida"
                )
                return -1
            return insertadas
        finally:
            self.row_selector = saved_selector

    # ------------------------------------------------------------------ #
    # Filas                                                                #
    # ------------------------------------------------------------------ #

    async def _count_rows(self, page: Page) -> int:
        return await page.locator(self.row_selector).count()

    async def _read_row(self, page: Page, index: int) -> Dict[str, Any]:
        row = page.locator(self.row_selector).nth(index)
        cells = await row.locator("td").all()
        texts = [await c.inner_text() for c in cells]

        first = texts[0].strip() if texts else ""
        base = {
            "descripcion": texts[1] if len(texts) > 1 else "",
            "persona": texts[2] if len(texts) > 2 else "",
            "monto": texts[3] if len(texts) > 3 else "",
            "num_cuotas": texts[4].strip() if len(texts) > 4 else "",
            "valor_cuota": texts[5].strip() if len(texts) > 5 else "",
        }
        # Estructura nueva (2026-03+): todos los movimientos tienen fecha; pendiente se detecta
        # desde el modal (campo 'Cuotas' ausente). Estructura antigua: pendientes sin fecha —
        # DATE_RE lo marca pendiente=True aquí, confirmado en extract_all_movements via modal.
        if DATE_RE.match(first):
            return {"pendiente": False, "fecha": first, **base}
        else:
            return {"pendiente": True, "fecha": "", **base}

    # ------------------------------------------------------------------ #
    # Detalle                                                              #
    # ------------------------------------------------------------------ #

    async def _close_modal(self, page: Page) -> None:
        """Cierra el modal con Escape + JS y espera fija."""
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)
        await page.evaluate(JS_CLOSE_MODAL)
        await page.wait_for_timeout(1200)  # espera fija para animación de cierre

    async def _click_row(self, page: Page, index: int) -> bool:
        """Intenta clickear la fila; si está bloqueada cierra el modal y reintenta una vez."""
        row = page.locator(self.row_selector).nth(index)
        try:
            await row.click(timeout=8000)
            return True
        except Exception:
            logger.warning(f"    Fila {index} bloqueada — cerrando modal y reintentando")
            await self._close_modal(page)
        try:
            row = page.locator(self.row_selector).nth(index)
            await row.click(timeout=8000)
            return True
        except Exception:
            logger.warning(f"    Fila {index} no clickeable tras reintento — saltando")
            return False

    async def _open_and_read_detail(self, page: Page, index: int) -> Dict[str, str]:
        if not await self._click_row(page, index):
            return {}

        # Esperar a que el modal esté en el DOM
        try:
            await page.wait_for_selector("#modalDetailTransaction", timeout=5000)
        except Exception:
            pass

        # Esperar a que el modal tenga contenido real.
        # Acepta tanto estructura nueva ('Comercio') como antigua ('Código autorización').
        try:
            await page.wait_for_function(
                """() => {
                    function hasLabel(root, text) {
                        for (const el of root.querySelectorAll('*')) {
                            if (el.textContent.trim() === text) {
                                const childHasIt = Array.from(el.children).some(c => c.textContent.trim() === text);
                                if (!childHasIt) return true;
                            }
                            if (el.shadowRoot && hasLabel(el.shadowRoot, text)) return true;
                        }
                        return false;
                    }
                    return hasLabel(document, 'Comercio') || hasLabel(document, 'Código autorización');
                }""",
                timeout=8000,
            )
        except Exception:
            pass  # si no aparece, intentamos extraer igual

        if self.debug_mode:
            await self._screenshot(page, f"detail_open_{index:03d}")

        detail = await page.evaluate(JS_EXTRACT_FIELDS)

        if not detail:
            logger.warning(f"    Fila {index} — detalle vacío")
        elif self.debug_mode:
            pairs = detail.pop("_debug_pairs", {})
            logger.info(f"    Fila {index} — todos los campos del modal: {pairs}")
        else:
            detail.pop("_debug_pairs", None)

        await self._close_modal(page)
        return detail or {}

    # ------------------------------------------------------------------ #
    # Paginación — botones ‹ 1 › con íconos SVG                         #
    # ------------------------------------------------------------------ #

    async def _next_page_rect(self, page: Page) -> Optional[Dict]:
        """Retorna el bounding rect del botón › o None si no hay siguiente página."""
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(400)

        return await page.evaluate(JS_NEXT_PAGE_RECT)

    async def _has_next_page(self, page: Page) -> bool:
        rect = await self._next_page_rect(page)
        if self.debug_mode:
            await self._screenshot(page, "pagination_check")
        return rect is not None

    async def _go_next_page(self, page: Page) -> None:
        rect = await self._next_page_rect(page)
        if not rect:
            logger.warning("No se encontró el botón ›")
            return

        # Capturar texto de la primera fila antes de navegar
        first_before = await page.locator(self.row_selector).nth(0).inner_text()

        x, y = rect["x"], rect["y"]
        await page.mouse.click(x, y)

        # Esperar a que la primera fila cambie (Angular actualizó el contenido)
        try:
            await page.wait_for_function(
                f"""() => {{
                    const row = document.querySelector('{self.row_selector}');
                    return row && row.innerText !== {repr(first_before)};
                }}""",
                timeout=10000,
            )
        except Exception:
            await page.wait_for_timeout(2000)

        await page.evaluate("window.scrollTo(0, 0)")

    # ------------------------------------------------------------------ #
    # Loop principal                                                       #
    # ------------------------------------------------------------------ #

    async def extract_all_movements(self, page: Page) -> List[Dict[str, Any]]:
        all_movements = []
        page_num = 0
        prev_page_signature: str = ""

        while True:
            page_num += 1
            total_rows = await self._count_rows(page)
            logger.info(f"Página {page_num}: {total_rows} movimientos")

            # Detectar loop: si la primera fila es igual a la página anterior, salimos
            first_row_text = await page.locator(self.row_selector).nth(0).inner_text() if total_rows > 0 else ""
            if first_row_text and first_row_text == prev_page_signature:
                logger.info("Página repetida detectada — extracción completa")
                break
            prev_page_signature = first_row_text

            if self.debug_mode:
                await self._screenshot(page, f"page_{page_num:02d}_start")

            limit = self.max_per_page if self.max_per_page > 0 else total_rows
            new_on_page = 0

            for i in range(min(limit, total_rows)):
                movement = await self._read_row(page, i)

                # Saltear filas vacías (separadores de tabla Angular)
                if not movement.get("descripcion", "").strip() and not movement.get("monto", "").strip():
                    continue

                key = self._movement_key(movement)

                # Saltar solo si es confirmado (tiene num_cuotas), ya está en DB y no está incompleto.
                # Pendientes nunca tienen num_cuotas → siempre se procesan sin importar existing_keys.
                if movement.get("num_cuotas") and key in self.existing_keys and key not in self.incomplete_keys:
                    logger.info(f"  [{i+1}/{total_rows}] Ya procesado — {movement.get('descripcion', '?')}")
                    continue
                if movement.get("num_cuotas") and key in self.incomplete_keys:
                    logger.info(f"  [{i+1}/{total_rows}] Incompleto, re-descargando — {movement.get('descripcion', '?')}")

                detail = await self._open_and_read_detail(page, i)
                modal_cuotas = detail.pop("modal_cuotas", None)
                has_auth = bool(detail.get("codigo_autorizacion"))
                modal_loaded = bool(detail)  # True si el modal entregó algún campo útil
                has_date = bool(DATE_RE.match(str(movement.get("fecha", "") or "")))

                # Detección de pendiente según estructura activa:
                # - Estructura antigua (has_auth): ambos pendientes Y confirmados tienen auth
                #   code → el único discriminador es la fecha en la tabla (sin fecha = pendiente)
                # - Estructura nueva (modal_loaded, sin auth): 'Cuotas' presente = confirmado
                # - Fallback (modal no cargó): DATE_RE sobre fecha de tabla
                if has_auth:
                    movement["pendiente"] = not has_date
                elif modal_loaded:
                    movement["pendiente"] = not bool(modal_cuotas)
                else:
                    movement["pendiente"] = not has_date
                movement.update(detail)
                status = "pendiente" if movement["pendiente"] else "confirmado"
                logger.info(
                    f"  [{i+1}/{total_rows}] ({status}) "
                    f"{movement.get('descripcion', '?')} — {movement.get('monto', '?')}"
                )
                movement["periodo_facturacion"] = self.periodo_facturacion

                is_new = key not in self.existing_keys
                self._upsert_to_db(movement)
                self.existing_keys.add(self._movement_key(movement))
                all_movements.append(movement)
                new_on_page += 1
                self._cnt_procesados += 1
                if movement["pendiente"]:
                    self._cnt_pendientes += 1
                elif is_new:
                    self._cnt_nuevos += 1
                else:
                    self._cnt_actualizados += 1

            self._cnt_paginas += 1
            if await self._has_next_page(page):
                logger.info("Navegando a página siguiente...")
                await self._go_next_page(page)
            else:
                logger.info(f"Extracción completa: {len(all_movements)} movimientos procesados")
                break

        return all_movements

    # ------------------------------------------------------------------ #
    # Run                                                                  #
    # ------------------------------------------------------------------ #

    async def _maybe_backfill(self, page: Page) -> None:
        """Corre el backfill del período cerrado si corresponde. Nunca hace fallar la corrida."""
        periodo = self.backfill_periodo
        forzado = bool(periodo)
        if not forzado:
            if self.max_per_page:
                return  # una corrida truncada por --limit no debe consumir la marca
            periodo = self._closed_periodo()
            if not periodo:
                return
            if self._backfill_done(periodo) is not False:
                return  # ya hecho, o no se pudo determinar (fail closed)
        try:
            insertadas = await self.backfill_period(page, periodo)
            if insertadas >= 0 and not self.backfill_dry_run:
                self._mark_backfill_done(periodo)
        except Exception as e:
            logger.warning(f"backfill {periodo} falló: {type(e).__name__}: {e}")
            await self._screenshot(page, f"backfill_{periodo}_error", error=True)

    async def run(self) -> List[Dict[str, Any]]:
        self._start_run()
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                locale="es-CL",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = await context.new_page()
            try:
                if not await self._login_with_retry(page):
                    self._finish_run("error", "Login fallido tras 3 intentos")
                    return []
                if not await self.navigate_to_movements(page):
                    self._finish_run("error", "Navegación a movimientos fallida")
                    return []
                self.pending_hashes = self._save_pending_hashes()
                self._reset_pending()
                movements = await self.extract_all_movements(page)
                # El backfill va al final: si falla, la corrida del día ya está guardada.
                await self._maybe_backfill(page)
                self._finish_run("success")
                return movements
            except Exception as e:
                await self._screenshot(page, "unexpected_error", error=True)
                self._finish_run("error", str(e))
                raise
            finally:
                await context.close()
                await browser.close()


def main(debug_mode: bool = False, headless: bool = False, max_per_page: int = 0,
         backfill_periodo: str = "", backfill_dry_run: bool = False, **_):
    if backfill_periodo and not re.fullmatch(r"\d{4}-\d{2}", backfill_periodo):
        logger.error(f"--backfill-periodo espera 'YYYY-MM', recibió '{backfill_periodo}'")
        sys.exit(2)
    scraper = FalabellaScraper(headless=headless, debug_mode=debug_mode)
    scraper.max_per_page = max_per_page
    scraper.backfill_periodo = backfill_periodo
    scraper.backfill_dry_run = backfill_dry_run
    movements = asyncio.run(scraper.run())

    if scraper._run_status == "error":
        sys.exit(1)

    if movements:
        logger.info(f"{len(movements)} movimientos nuevos guardados en Supabase")
        for m in movements[:5]:
            cols = ["fecha", "descripcion", "persona", "monto", "rubro", "hora", "pais", "origen"]
            row_str = " | ".join(f"{c}={m.get(c, '')}" for c in cols if m.get(c))
            logger.info(f"  {row_str}")
        return movements

    logger.info("Sin movimientos nuevos")
    return None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument('--limit', type=int, default=0, help='Máx movimientos por página (0=todos)')
    parser.add_argument('--backfill-periodo', default='',
                        help="Fuerza el backfill de un período cerrado ('YYYY-MM')")
    parser.add_argument('--backfill-dry-run', action='store_true',
                        help='Lista lo que falta sin escribir en la DB')
    args = parser.parse_args()
    main(debug_mode=args.debug, headless=args.headless, max_per_page=args.limit,
         backfill_periodo=args.backfill_periodo, backfill_dry_run=args.backfill_dry_run)
