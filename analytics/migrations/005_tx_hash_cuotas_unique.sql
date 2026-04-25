-- Reemplaza el constraint UNIQUE (tx_hash) por un partial unique index sobre
-- (tx_hash, periodo) WHERE tx_hash IS NOT NULL.
--
-- Motivo: con el banco ya sin codigo_autorizacion, los movimientos confirmados usan
-- tx_hash = sha256(fecha_compra|desc|monto) como identificador único. Para compras
-- en cuotas, todas las cuotas de la misma compra comparten el mismo tx_hash (mismo
-- fecha_compra, desc y monto). El constraint antiguo UNIQUE (tx_hash) hacía que cada
-- cuota nueva sobreescribiera la anterior — solo sobrevivía la última.
--
-- La solución: dentro de un mismo período siempre hay exactamente una cuota por compra,
-- así que (tx_hash, periodo) es único por definición. Se mantiene tx_hash = hash por
-- compra (sin num_cuotas) para que clasificaciones apliquen a todas las cuotas a la vez.

-- 1. Eliminar constraint antiguo
ALTER TABLE movimientos DROP CONSTRAINT IF EXISTS movimientos_tx_hash_key;

-- 2. Nuevo partial unique index: una fila por (compra, período)
CREATE UNIQUE INDEX movimientos_tx_hash_periodo_idx
    ON movimientos (tx_hash, periodo)
    WHERE tx_hash IS NOT NULL;
