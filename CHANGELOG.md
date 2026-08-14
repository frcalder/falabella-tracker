# Changelog

## 2026-08-14

### Fix: modal de opt-in CMR Puntos bloqueaba el click en la tarjeta

**Síntoma:** El scraper falló en GitHub Actions los días 12, 13 y 14 de agosto de 2026 con `TimeoutError: Locator.click: Timeout 30000ms exceeded` sobre `a.div-product` (tarjeta CMR Mastercard). El call log de Playwright repetía: `<div class="backdrop"> ... subtree intercepts pointer events`. El screenshot de debug mostró un modal nuevo: _"¡Pronto podrás acumular más CMR puntos!"_.

**Causa raíz:** El banco agregó un modal de opt-in tras el login — componente Angular `app-popup-terms-conditions-optin#optin`, con `div.backdrop` (z-index 1000) a pantalla completa. El manejo de backdrop existente sólo cubría el modal de marketing antiguo (`#background-shadow.backdrop.visible` + `app-marketing button`), así que el backdrop nuevo nunca se cerraba e interceptaba todos los clicks.

**Fix (`scraper/bank_scraper.py`):**
- `_dismiss_blocking_modals()`: reemplaza el bloque específico de `#background-shadow`. Espera hasta 5s a que aparezca cualquier backdrop bloqueante (`JS_HAS_BLOCKING_BACKDROP` verifica visibilidad real de `div.backdrop, .cdk-overlay-backdrop`), lo cierra y **verifica que desapareció**, hasta 3 intentos. Si queda bloqueado, guarda screenshot `backdrop_bloqueado.png`.
- Selectores de cierre en orden: `#optin button.close-misdocumentos` → `app-popup-terms-conditions-optin button[class*='close']` → `app-marketing button` → fallback JS `JS_CLICK_PROMO_CLOSE` (busca botones de cierre por clase/aria/`svg.icon-close`) → Escape.
- **El modal se cierra siempre con la ×, nunca con "Acepto"**: ese botón inscribe al usuario en el programa CMR Puntos y autoriza comunicaciones comerciales. `JS_CLICK_PROMO_CLOSE` sólo matchea botones de cierre y excluye explícitamente "Cerrar sesión".
- Click en la tarjeta con reintento: si falla, vuelve a cerrar modales y reintenta; como último recurso `card_link.evaluate("el => el.click()")` (click directo en el DOM, ignora el overlay).

**Verificado:** `pipenv run python main.py --mode scraper --limit 1 --headless --debug` → `Modal bloqueante cerrado (#optin button.close-misdocumentos)`, tabla cargada, 5 páginas recorridas.

**Limpieza de datos:** no se requiere — los runs fallaron antes de escribir en la base.

### Doc: el banco volvió a servir `Código autorización`, `Pais` y `Origen de la compra`

`CLAUDE.md` decía que estos tres campos quedaron eliminados del modal en ~2026-03 y son "always NULL". Es falso: el bache fue **abril 2026** y el banco los volvió a servir **~2026-05-08**. Cobertura de `codigo_autorizacion` medida en una cuenta real, por mes de transacción: 04/2026 33/87, 05/2026 57/89, 06/2026 86/90, 07/2026 79/85, 08/2026 42/43 (~95% desde junio). El código nunca dejó de extraerlos (`JS_EXTRACT_FIELDS.LABELS` los mantiene), así que solo la documentación estaba desfasada.

- `CLAUDE.md`: corregidas las notas de las tres columnas; la sección **Installment uniqueness** ahora describe los dos caminos (auth code / `tx_hash`) como **estado permanente**, no como una migración en curso, e indica cuál aplica a cada fila (lo que el modal haya servido, pendientes incluidos: hoy la mayoría de los pendientes trae auth code y va por el camino de auth, con `tx_hash` NULL). Agregada la sección **Pendiente → confirmado key migration**, que documenta que `_save_movement` migra `clasificaciones` y `splits` de `tx_hash` a `codigo_autorizacion` en la misma transacción — relevante solo para pendientes guardados sin auth code.
- `CLAUDE.md`, **Single table**: la detección de pendiente estaba documentada como una sola regla (`modal_cuotas` ausente = pendiente). En realidad el scraper maneja tres estructuras y la vigente es la de auth code, donde el discriminador es la **ausencia de fecha en la tabla** y los pendientes **sí** traen cuotas (`01/01`) — verificado en la base: el pendiente actual tiene `fecha` NULL, `num_cuotas` `01/01` y auth code `156317`. También corregida la explicación de por qué los pendientes se re-procesan en cada run: no es por no tener cuotas, es porque `_load_existing_keys` solo carga filas `pendiente = FALSE`.
- `scraper/bank_scraper.py`: corregido el comentario y docstring de `_load_incomplete_keys`, que justificaban no reintentar por `codigo_autorizacion IS NULL` con una premisa ya falsa. El comportamiento **no cambia**: el motivo real es que las filas del bache abr–may nunca podrán obtener auth code y se re-procesarían en cada run.

**Limpieza de datos:** no se requiere para este cambio. Si vienes del bache abr–may 2026 puedes tener clasificaciones huérfanas (creadas por `tx_hash` o por un `codigo_autorizacion` que ya no existe en `movimientos`). Para revisarlas:

```sql
-- clasificaciones que no enlazan con ningún movimiento
SELECT * FROM clasificaciones c
 WHERE (c.tx_hash IS NOT NULL AND NOT EXISTS (
         SELECT 1 FROM movimientos m WHERE m.tx_hash = c.tx_hash))
    OR (c.codigo_autorizacion IS NOT NULL AND NOT EXISTS (
         SELECT 1 FROM movimientos m WHERE m.codigo_autorizacion = c.codigo_autorizacion));
```

Revisa el SELECT antes de borrar: una huérfana por `tx_hash` puede ser el duplicado benigno de una clasificación que ya migró a `codigo_autorizacion`, y borrarla no pierde información, pero las demás implican re-clasificar el movimiento a mano.

## 2026-05-25

### Fix: reintentos automáticos de login + escritura humana de credenciales

**Síntoma:** El scraper fallaba ~3 de cada 10 runs en GitHub Actions con "Login fallido". El patrón en los logs muestra el popup "En estos momentos no lo podemos atender" **dos veces**: una al cargar la página (normal, ya manejada) y otra 20 segundos después, coincidiendo exactamente con el timeout de `wait_for_selector("Hola", 20000)`.

**Causa raíz:** El banco detecta la automatización durante el envío de credenciales y muestra el popup de "servicio no disponible" a nivel de respuesta del backend. Descartar el popup no rescata la sesión ya rechazada. El bloqueo es intermitente (probabilístico) — algunos días bloquea todo el día (23/05), otros días funciona sin problemas.

**Fix (`scraper/bank_scraper.py`):**
- `_login_with_retry()`: wrapper que reintenta `login()` hasta 3 veces con 30s de espera entre intentos. Cada intento recarga la página desde cero (`page.goto()` al inicio de `login()`).
- `page.wait_for_timeout(1500)` tras `domcontentloaded`: pequeña espera para que Angular termine de renderizar el header antes del popup check.
- `rut_field.type(username, delay=80)` / `pass_field.type(password, delay=80)` en vez de `fill()`: genera key events por carácter (~80ms entre teclas) imitando escritura humana, reduciendo la señal de automatización.
- `run()` usa `_login_with_retry()` en vez de `login()` directamente; mensaje de error actualizado a "Login fallido tras 3 intentos".

**Limitación conocida:** Si el banco bloquea el IP de GitHub Actions durante todo el día (como ocurrió el 23/05), los 3 reintentos tampoco alcanzan. En ese caso la solución definitiva es correr desde un IP fijo o variar el horario del cron.

## 2026-05-14

### Fix: robustez del login en GitHub Actions — detección headless, popup Shadow DOM, exit code y diagnóstico

**Síntoma:** El scraper en GitHub Actions fallaba con `Login fallido` o timeout en `networkidle`. Localmente funcionaba en modo normal pero no en headless. Los runs fallidos aparecían como "success" en Actions.

**Causa raíz (múltiple):**
1. `wait_until="networkidle"` nunca disparaba: el SPA del banco mantiene conexiones persistentes (polling/WebSocket) que impiden que la red quede idle.
2. El banco detectaba Chromium en modo headless (via `navigator.webdriver` y la ausencia de UA real) y mostraba un popup de bloqueo: _"En estos momentos no lo podemos atender, por favor intenta más tarde"_ con botón "Entendido".
3. El popup está dentro del Shadow DOM del banco — `page.locator()` no puede alcanzarlo; requiere JS con traversal recursivo de shadow roots.
4. El click al botón de login se hacía sin esperar que el DOM de Angular terminara de renderizarlo.
5. Los screenshots de diagnóstico solo se guardaban con `--debug`, que no se usa en Actions.
6. El scraper terminaba con exit code 0 incluso en error → Actions marcaba el run como "success".

**Fix (`scraper/bank_scraper.py`):**
- `page.goto(..., wait_until="domcontentloaded")` en vez de `networkidle`.
- `p.chromium.launch(args=["--disable-blink-features=AutomationControlled"])` + user-agent de Chrome real + `add_init_script` para anular `navigator.webdriver`.
- `_dismiss_service_popup()`: usa `wait_for_function` + `page.evaluate()` con traversal recursivo de shadow roots para encontrar y clickear "Entendido". Se llama al cargar la página y tras enviar credenciales si "Hola" no aparece.
- `wait_for_selector` en el botón de login antes de clickearlo.
- `_screenshot(..., error=True)`: nuevo parámetro que guarda el screenshot siempre (sin `--debug`) en puntos de falla. Aplicado en todos los errores de login, carga de tabla y excepción inesperada en el run.
- `_finish_run` guarda el status en `self._run_status`; `main()` llama `sys.exit(1)` si es `"error"`.

**Fix (`.github/workflows/scraper.yml`):**
- Paso `Upload debug screenshots` con `actions/upload-artifact@v4` y `if: always()` — los screenshots quedan disponibles en cada run de Actions para diagnóstico.

## 2026-05-13

### Fix: soporte dual-estructura UI banco + logging silenciado + migración de clasificaciones

**Síntoma:** El banco Falabella revirtió su frontend a la estructura anterior (~2026-03), causando tres problemas simultáneos:
1. El scraper sólo procesaba la primera página (paginación rota).
2. Movimientos pendientes eran guardados como confirmados (`pendiente=FALSE`).
3. Las clasificaciones se perdían al pasar un movimiento de pendiente a confirmado.
4. Ningún log era visible en las ejecuciones de GitHub Actions (logging completamente silenciado).

**Causa raíz:**
- `JS_NEXT_PAGE_RECT` sólo buscaba `btn-move` (nueva estructura); la antigua usa `btn-pagination`.
- La detección de pendiente usaba únicamente `modal_cuotas`, que no existe en la estructura antigua. En estructura antigua, tanto confirmados como pendientes tienen `Código autorización` en el modal; el único discriminador confiable es la presencia de fecha en la fila de la tabla.
- Al confirmar una transacción, `clasificaciones` y `splits` no se migraban si cambiaba la llave (`tx_hash → codigo_autorizacion` o `tx_hash → tx_hash` distinto).
- `logging.getLogger("root")` retorna el logger raíz de Python (porque `root.name == 'root'`), por lo que `setLevel(CRITICAL)` silenciaba todo el output.

**Fix (`scraper/bank_scraper.py`):**
- `JS_NEXT_PAGE_RECT`: busca `btn-move` (nueva) y `btn-pagination` (antigua) en paralelo.
- `JS_EXTRACT_FIELDS`: restaura labels `Código autorización`, `Pais`/`País`, `Origen de la compra` como opcionales.
- `wait_for_function`: acepta `Comercio` OR `Código autorización` como señal de carga del modal.
- `_read_row`: restaura `DATE_RE` como fallback para detectar pendientes en estructura antigua (sin fecha).
- Detección de pendiente multi-señal en `extract_all_movements`:
  - Si modal tiene `Código autorización` → estructura antigua → usar DATE_RE (sin fecha = pendiente).
  - Si modal cargó sin auth → estructura nueva → usar `modal_cuotas`.
  - Si modal no cargó → DATE_RE como fallback.
- `_save_pending_hashes()`: guarda `{(desc_norm, monto_norm): tx_hash}` de todos los pendientes antes de `_reset_pending()`.
- `_upsert_to_db`: al confirmar, migra `clasificaciones` y `splits` del hash pendiente al nuevo identificador (ya sea nuevo `tx_hash` o `codigo_autorizacion`), en la misma transacción DB. `NOT EXISTS` previene sobrescribir clasificaciones existentes.
- Logging: reemplaza `getLogger("root").setLevel(CRITICAL)` por silenciar sólo loggers específicos (`urllib3`, `asyncio`, `playwright`).

## 2026-04-27

### Feat: rediseño UX de splits en Clasificación

**Problema:** El panel "✂ Dividir movimiento" usaba un `selectbox` que defaulteaba siempre al primer movimiento de la lista. Al abrir el expander sin revisar la selección era fácil guardar un split accidentalmente sobre la transacción equivocada.

**Solución (`dashboard/pages/01_Clasificacion.py`):**
- La tabla principal cambia de `st.data_editor` (con edición inline) a `st.dataframe` con `selection_mode="single-row"` y `on_select="rerun"`.
- Al hacer clic en una fila aparece un panel de acción debajo de la tabla con el contexto de esa transacción y dos tabs: **Clasificar** y **✂ Dividir**.
- La tab "Dividir" solo es accesible tras seleccionar explícitamente una fila, eliminando el riesgo de split accidental.
- La clasificación pasa de edición inline masiva a un selectbox por fila con botón "Guardar clasificación" / "Quitar clasificación".

## 2026-04-25

### Fix: adaptación a cambios de UI del Banco Falabella (~2026-03)

**Síntoma:** Movimientos confirmados quedaban sin `codigo_autorizacion`, `pais` y `origen`. Movimientos pendientes no se detectaban correctamente. El scraper dejó de navegar a la siguiente página.

**Causa raíz:** El banco realizó cambios en su frontend Angular:
1. Eliminó `Código autorización`, `País` y `Origen de la compra` del modal de detalle.
2. Unificó la tabla de movimientos: pendientes y confirmados aparecen en la misma tabla, todos con fecha. El ícono de reloj ya no es el señal de pendiente.
3. Cambió la clase CSS de los botones de paginación de `btn-pagination` a `btn-move`.

La detección de movimiento pendiente (antes: ausencia de fecha en la tabla) quedó rota — todos los rows parecían confirmados. Y `JS_NEXT_PAGE_RECT` no encontraba botones de paginación al buscar la clase antigua.

**Fix (`scraper/bank_scraper.py`):**
- `JS_EXTRACT_FIELDS`: elimina labels `Código autorización`/`País`/`Origen`; agrega `Cuotas → modal_cuotas`. La presencia de `Cuotas` en el modal indica movimiento confirmado; su ausencia indica pendiente.
- `wait_for_function`: espera `Comercio` (8 s) en lugar de `Código autorización` (15 s).
- `_read_row`: siempre retorna `pendiente=False` con fecha real; la señal definitiva viene del modal.
- `extract_all_movements`: skip check usa `num_cuotas` truthy (no `fecha`); pendiente detectado via `detail.pop("modal_cuotas")`.
- `_load_incomplete_keys`: ya no reintenta por `codigo_autorizacion IS NULL`; solo por `rubro`/`comercio` faltante.
- `JS_NEXT_PAGE_RECT`: busca clase `btn-move` en lugar de `btn-pagination`.
- Código muerto eliminado: `DATE_RE`, `num_cuotas_raw`.

**Fix (`analytics/schema.sql` + migración `005_tx_hash_cuotas_unique.sql`):**
El constraint `UNIQUE(tx_hash)` causaba que las cuotas de una misma compra se sobreescribieran (todas comparten el mismo `tx_hash = sha256(fecha_compra|descripcion|monto)`). Se reemplaza por un índice parcial `UNIQUE(tx_hash, periodo) WHERE tx_hash IS NOT NULL` — dentro de un período hay exactamente una cuota por compra.

```sql
-- Aplicar en Supabase SQL Editor (no funciona vía pooler por statement_timeout)
ALTER TABLE movimientos DROP CONSTRAINT IF EXISTS movimientos_tx_hash_key;

CREATE UNIQUE INDEX IF NOT EXISTS movimientos_tx_hash_periodo_idx
    ON movimientos (tx_hash, periodo)
    WHERE tx_hash IS NOT NULL;
```

**Impacto en clasificaciones existentes:** Movimientos que tenían `codigo_autorizacion` antes del cambio del banco (~2026-03) y estaban clasificados quedarán con su clasificación huérfana — el banco ya no sirve el auth code, por lo que el movimiento ahora tiene `tx_hash` como identificador y no hay forma de recuperar el vínculo automáticamente. Esos movimientos deben re-clasificarse manualmente.

```sql
-- Verificar clasificaciones huérfanas (no matchean ningún movimiento actual)
SELECT c.id, c.codigo_autorizacion, c.tx_hash, cat.nombre
FROM clasificaciones c
LEFT JOIN categorias cat ON cat.id = c.categoria_id
WHERE NOT EXISTS (
    SELECT 1 FROM movimientos m WHERE
        (c.codigo_autorizacion IS NOT NULL AND c.codigo_autorizacion = m.codigo_autorizacion)
        OR (c.tx_hash IS NOT NULL AND c.tx_hash = m.tx_hash)
);
```

## 2026-04-10

### Fix: duplicados en `movimientos` al re-procesar transacciones incompletas

**Síntoma:** Transacciones aparecían duplicadas en el dashboard (misma fecha, comercio y monto, dos filas).

**Causa raíz:** Cuando el modal de detalle no cargaba durante un scrape (timing/red), la transacción se guardaba sin `codigo_autorizacion`, usando `tx_hash` como identificador de fallback. En el siguiente run, la fila quedaba en `incomplete_keys` y se re-procesaba. Si en esa segunda pasada el modal sí cargaba y entregaba el auth code, `_upsert_to_db` forzaba `tx_hash = None` (comportamiento correcto para filas con auth code), pero el bloque de limpieza previo al INSERT solo corría `if tx_hash:`, que nunca era verdadero en ese branch. El `INSERT ON CONFLICT (codigo_autorizacion, num_cuotas)` no encontraba conflicto con la fila antigua (que tenía `codigo_autorizacion IS NULL`), por lo que creaba una segunda fila.

**Fix (`scraper/bank_scraper.py`):** Se separa el cálculo del hash en dos variables:
- `potential_hash` — calculado siempre para transacciones confirmadas, usado exclusivamente para el DELETE previo.
- `tx_hash` — el valor que se persiste en la DB (sigue siendo `None` cuando hay `codigo_autorizacion`).

El DELETE ahora usa `potential_hash` en lugar de `tx_hash`, eliminando correctamente la fila antigua antes de insertar la nueva con auth code.

**Limpieza de duplicados existentes:** Si ya hay duplicados en la DB, ejecutar en el SQL Editor de Supabase:

```sql
-- 1. Verificar duplicados
SELECT old.id, old.fecha, old.descripcion, old.monto, old.tx_hash,
       new.id AS id_con_auth, new.codigo_autorizacion
FROM movimientos old
JOIN movimientos new ON (
    old.fecha = new.fecha
    AND old.descripcion = new.descripcion
    AND old.monto = new.monto
    AND COALESCE(old.num_cuotas, '') = COALESCE(new.num_cuotas, '')
    AND old.codigo_autorizacion IS NULL
    AND old.tx_hash IS NOT NULL
    AND new.codigo_autorizacion IS NOT NULL
    AND old.pendiente = FALSE
    AND new.pendiente = FALSE
);

-- 2. Eliminar las filas antiguas (sin auth code)
DELETE FROM movimientos
WHERE id IN (
  SELECT old.id
  FROM movimientos old
  JOIN movimientos new ON (
      old.fecha = new.fecha
      AND old.descripcion = new.descripcion
      AND old.monto = new.monto
      AND COALESCE(old.num_cuotas, '') = COALESCE(new.num_cuotas, '')
      AND old.codigo_autorizacion IS NULL
      AND old.tx_hash IS NOT NULL
      AND new.codigo_autorizacion IS NOT NULL
      AND old.pendiente = FALSE
      AND new.pendiente = FALSE
  )
);
```
