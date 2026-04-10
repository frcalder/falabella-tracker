"""
Scraper de movimientos de tarjeta de crédito del Banco Falabella usando Playwright.
El banco usa Shadow DOM — se traversa con JS para extraer datos y cerrar modales.
Paginación: botones ‹ 1 › (flechas SVG, no texto).
"""
import os
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
logging.getLogger("root").setLevel(logging.CRITICAL)  # silencia warnings de hashlib/OpenSSL

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

JS_EXTRACT_FIELDS = """
() => {
    const LABELS = {
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

    // Dump de todos los pares label→valor del modal (para debug)
    const allPairs = {};
    const leafEls = Array.from(modal.querySelectorAll('*')).filter(el => el.children.length === 0 && el.textContent.trim());
    for (const el of leafEls) {
        const next = el.nextElementSibling;
        if (next && next.textContent.trim() && next.children.length === 0) {
            allPairs[el.textContent.trim()] = next.textContent.trim();
        }
    }
    result['_debug_pairs'] = allPairs;

    return result;
}
"""


# Retorna el bounding rect del botón › (siguiente página) o null
JS_NEXT_PAGE_RECT = """
() => {
    // Busca btn-pagination atravesando shadow roots (el componente está en shadow DOM)
    function findPagBtns(root) {
        const btns = [];
        for (const el of root.querySelectorAll('*')) {
            if (el.tagName === 'BUTTON' && el.classList.contains('btn-pagination')) btns.push(el);
            if (el.shadowRoot) btns.push(...findPagBtns(el.shadowRoot));
        }
        return btns;
    }
    const pagBtns = findPagBtns(document);
    if (pagBtns.length === 0) return null;
    // El ÚLTIMO btn-pagination en el DOM siempre es ›; si está disabled, no hay más páginas
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

    def __init__(self, headless: bool = False, debug_mode: bool = False):
        self.username = os.getenv("FALABELLA_USER")
        self.password = os.getenv("FALABELLA_PASSWORD")
        if not self.username or not self.password:
            raise ValueError("Configura FALABELLA_USER y FALABELLA_PASSWORD en el .env")

        self.headless = headless
        self.debug_mode = debug_mode
        self.max_per_page: int = 0  # 0 = sin límite

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
        self.existing_keys: set = self._load_existing_keys()
        self.incomplete_keys: set = self._load_incomplete_keys()

        # Contadores de la ejecución actual
        self.run_id: Optional[int] = None
        self._cnt_paginas = 0
        self._cnt_procesados = 0
        self._cnt_nuevos = 0
        self._cnt_actualizados = 0
        self._cnt_pendientes = 0

        if debug_mode:
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
        """Claves fallback de filas confirmadas con campos del modal incompletos.
        Incluye: filas con auth code pero sin rubro/comercio, y filas sin auth code
        (podrían tenerlo en una re-visita del modal)."""
        try:
            cur = self.db_conn.cursor()
            cur.execute(
                """
                SELECT fecha, descripcion, monto, num_cuotas FROM movimientos
                WHERE pendiente = FALSE
                  AND (
                    codigo_autorizacion IS NULL
                    OR (rubro IS NULL OR rubro = '' OR comercio IS NULL OR comercio = '')
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

    def _reset_pending(self) -> None:
        """Elimina todos los pendientes de la DB al inicio del run para re-agregarlos frescos."""
        try:
            cur = self.db_conn.cursor()
            cur.execute("DELETE FROM movimientos WHERE pendiente = TRUE")
            n = cur.rowcount
            self.db_conn.commit()
            cur.close()
            if n > 0:
                # Remover claves de pendientes del checkpoint (fecha vacía)
                self.existing_keys = {k for k in self.existing_keys
                                      if not (isinstance(k, tuple) and k[0] == "")}
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

        num_cuotas_raw = str(movement.get("num_cuotas", "") or "").strip()

        # tx_hash: solo para confirmadas SIN codigo_autorizacion (fallback de identificación).
        # Cuando hay codigo_autorizacion, el conflict target es (codigo_autorizacion, num_cuotas)
        # y tx_hash no se usa — se guarda NULL para no ocupar la constraint UNIQUE.
        # potential_hash: calculado siempre para limpiar filas antiguas que usaban tx_hash
        # cuando la misma tx. ahora tiene codigo_autorizacion (evita duplicados en re-scrape).
        if not pendiente:
            fecha_para_hash = fecha_compra_raw if fecha_compra_raw else f"{fecha_raw}|{periodo_fac}"
            potential_hash = _make_tx_hash(fecha_para_hash, descripcion, monto_raw)
        else:
            potential_hash = None

        if not pendiente and not codigo_autorizacion:
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
        }

        cur = self.db_conn.cursor()
        try:
            if codigo_autorizacion:
                # Eliminar fila previa sin codigo_autorizacion (guardada con tx_hash) que
                # corresponde a esta misma transacción. Ocurre cuando el modal no cargó en
                # un run anterior y ahora sí tiene auth code: sin este DELETE quedarían
                # dos filas para la misma transacción (duplicados).
                if potential_hash:
                    cur.execute(
                        "DELETE FROM movimientos WHERE tx_hash = %s AND codigo_autorizacion IS NULL",
                        (potential_hash,),
                    )
                cur.execute(
                    """
                    INSERT INTO movimientos
                        (fecha, descripcion, persona, monto, monto_periodo, pendiente,
                         rubro, comercio, codigo_autorizacion, fecha_compra, hora,
                         pais, origen, periodo_facturacion, periodo, num_cuotas,
                         valor_cuota, tx_hash, updated_at)
                    VALUES
                        (%(fecha)s, %(descripcion)s, %(persona)s, %(monto)s, %(monto_periodo)s,
                         %(pendiente)s, %(rubro)s, %(comercio)s, %(codigo_autorizacion)s,
                         %(fecha_compra)s, %(hora)s, %(pais)s, %(origen)s,
                         %(periodo_facturacion)s, %(periodo)s, %(num_cuotas)s,
                         %(valor_cuota)s, %(tx_hash)s, NOW())
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
                         valor_cuota, tx_hash, updated_at)
                    VALUES
                        (%(fecha)s, %(descripcion)s, %(persona)s, %(monto)s, %(monto_periodo)s,
                         %(pendiente)s, %(rubro)s, %(comercio)s, %(codigo_autorizacion)s,
                         %(fecha_compra)s, %(hora)s, %(pais)s, %(origen)s,
                         %(periodo_facturacion)s, %(periodo)s, %(num_cuotas)s,
                         %(valor_cuota)s, %(tx_hash)s, NOW())
                    ON CONFLICT (tx_hash) DO UPDATE SET
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
            self.db_conn.commit()
        except Exception:
            self.db_conn.rollback()
            raise
        finally:
            cur.close()

    async def _screenshot(self, page: Page, name: str) -> None:
        if self.debug_mode:
            await page.screenshot(path=str(DEBUG_DIR / "screenshots" / f"{name}.png"))

    # ------------------------------------------------------------------ #
    # Login                                                                #
    # ------------------------------------------------------------------ #

    async def login(self, page: Page) -> bool:
        await page.goto("https://www.bancofalabella.cl/", wait_until="networkidle")
        await page.click("//div[@id='main-header__sub-content']/div[3]/button[3]")

        await page.wait_for_selector("input[placeholder='RUT']:visible", timeout=15000)
        rut_field = page.locator("input[placeholder='RUT']:visible").first
        await rut_field.fill(self.username)

        pass_field = page.locator("input[placeholder='Clave Internet']:visible").first
        await pass_field.fill(self.password)

        try:
            submit = page.locator("button#desktop-login:not([disabled])")
            await submit.wait_for(state="visible", timeout=20000)
            await submit.click()
        except Exception:
            await self._screenshot(page, "login_submit_failed")
            logger.error("No apareció el botón de login")
            return False

        try:
            await page.wait_for_selector("//span[normalize-space(text())='Hola']", timeout=20000)
            logger.info("Login exitoso")
            return True
        except Exception:
            logger.error("Login fallido")
            await self._screenshot(page, "login_failed")
            return False

    # ------------------------------------------------------------------ #
    # Navegación                                                           #
    # ------------------------------------------------------------------ #

    async def navigate_to_movements(self, page: Page) -> bool:

        try:
            backdrop = page.locator("#background-shadow.backdrop.visible")
            await backdrop.wait_for(state="visible", timeout=5000)
            try:
                await page.locator("app-marketing button").first.click(timeout=3000)
            except Exception:
                await backdrop.click(force=True)
            await backdrop.wait_for(state="hidden", timeout=5000)
        except Exception:
            pass

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

        await card_link.click()

        try:
            await page.wait_for_selector(self.ROW_SELECTOR, timeout=30000)
            logger.info("Tabla cargada")
            await self._screenshot(page, "movements_loaded")
            self.periodo_facturacion = await self._extract_periodo(page)
            if self.periodo_facturacion:
                logger.info(f"Período de facturación: {self.periodo_facturacion}")
            return True
        except Exception:
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
    # Filas                                                                #
    # ------------------------------------------------------------------ #

    async def _count_rows(self, page: Page) -> int:
        return await page.locator(self.ROW_SELECTOR).count()

    async def _read_row(self, page: Page, index: int) -> Dict[str, Any]:
        row = page.locator(self.ROW_SELECTOR).nth(index)
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
        row = page.locator(self.ROW_SELECTOR).nth(index)
        try:
            await row.click(timeout=8000)
            return True
        except Exception:
            logger.warning(f"    Fila {index} bloqueada — cerrando modal y reintentando")
            await self._close_modal(page)
        try:
            row = page.locator(self.ROW_SELECTOR).nth(index)
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

        # Esperar activamente a que "Código autorización" aparezca en el shadow DOM (hasta 8s)
        try:
            await page.wait_for_function(
                """() => {
                    function hasLabel(root, text) {
                        for (const el of root.querySelectorAll('*')) {
                            if (el.children.length === 0 && el.textContent.trim() === text) return true;
                            if (el.shadowRoot && hasLabel(el.shadowRoot, text)) return true;
                        }
                        return false;
                    }
                    return hasLabel(document, 'Código autorización');
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
        first_before = await page.locator(self.ROW_SELECTOR).nth(0).inner_text()

        x, y = rect["x"], rect["y"]
        await page.mouse.click(x, y)

        # Esperar a que la primera fila cambie (Angular actualizó el contenido)
        try:
            await page.wait_for_function(
                f"""() => {{
                    const row = document.querySelector('{self.ROW_SELECTOR}');
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
            first_row_text = await page.locator(self.ROW_SELECTOR).nth(0).inner_text() if total_rows > 0 else ""
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

                # Saltar solo si ya está confirmado (con fecha), completo y no incompleto
                if movement.get("fecha") and key in self.existing_keys and key not in self.incomplete_keys:
                    logger.info(f"  [{i+1}/{total_rows}] Ya procesado — {movement.get('descripcion', '?')}")
                    continue
                if movement.get("fecha") and key in self.incomplete_keys:
                    logger.info(f"  [{i+1}/{total_rows}] Incompleto, re-descargando — {movement.get('descripcion', '?')}")

                status = "pendiente" if movement["pendiente"] else "confirmado"
                logger.info(
                    f"  [{i+1}/{total_rows}] ({status}) "
                    f"{movement.get('descripcion', '?')} — {movement.get('monto', '?')}"
                )
                detail = await self._open_and_read_detail(page, i)
                if not movement["fecha"] and "fecha_compra" in detail:
                    movement["fecha"] = detail["fecha_compra"]
                movement.update(detail)
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

    async def run(self) -> List[Dict[str, Any]]:
        self._start_run()
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(viewport={"width": 1280, "height": 900}, locale="es-CL")
            page = await context.new_page()
            try:
                if not await self.login(page):
                    self._finish_run("error", "Login fallido")
                    return []
                if not await self.navigate_to_movements(page):
                    self._finish_run("error", "Navegación a movimientos fallida")
                    return []
                self._reset_pending()
                movements = await self.extract_all_movements(page)
                self._finish_run("success")
                return movements
            except Exception as e:
                self._finish_run("error", str(e))
                raise
            finally:
                await context.close()
                await browser.close()


def main(debug_mode: bool = False, headless: bool = False, max_per_page: int = 0, **_):
    scraper = FalabellaScraper(headless=headless, debug_mode=debug_mode)
    scraper.max_per_page = max_per_page
    movements = asyncio.run(scraper.run())

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
    args = parser.parse_args()
    main(debug_mode=args.debug, headless=args.headless, max_per_page=args.limit)
