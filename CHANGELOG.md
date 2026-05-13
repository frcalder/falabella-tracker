# Changelog

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
