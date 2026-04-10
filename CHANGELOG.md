# Changelog

## 2026-04-10

### Fix: duplicados en `movimientos` al re-procesar transacciones incompletas

**Síntoma:** Transacciones aparecían duplicadas en el dashboard (misma fecha, comercio y monto, dos filas).

**Causa raíz:** Cuando el modal de detalle no cargaba durante un scrape (timing/red), la transacción se guardaba sin `codigo_autorizacion`, usando `tx_hash` como identificador de fallback. En el siguiente run, la fila quedaba en `incomplete_keys` y se re-procesaba. Si en esa segunda pasada el modal sí cargaba y entregaba el auth code, `_upsert_to_db` forzaba `tx_hash = None` (comportamiento correcto para filas con auth code), pero el bloque de limpieza previo al INSERT solo corría `if tx_hash:`, que nunca era verdadero en ese branch. El `INSERT ON CONFLICT (codigo_autorizacion, num_cuotas)` no encontraba conflicto con la fila antigua (que tenía `codigo_autorizacion IS NULL`), por lo que creaba una segunda fila.

**Fix (`scraper/bank_scraper.py`):** Se separa el cálculo del hash en dos variables:
- `potential_hash` — calculado siempre para transacciones confirmadas, usado exclusivamente para el DELETE previo al INSERT.
- `tx_hash` — el valor que se persiste en la DB (sigue siendo `None` cuando hay `codigo_autorizacion`).

El DELETE ahora usa `potential_hash` en lugar de `tx_hash`, eliminando correctamente la fila antigua antes de insertar la nueva con auth code.

**¿Cómo saber si tienes duplicados?** Ejecuta esto en el SQL Editor de Supabase:

```sql
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
```

Si la query retorna filas, tienes duplicados. Para eliminarlos:

```sql
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

Después de limpiar la DB, el fix en el código previene que esto vuelva a ocurrir en futuros scrapes.
