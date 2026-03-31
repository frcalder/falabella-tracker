-- Migración: cambiar UNIQUE de codigo_autorizacion a (codigo_autorizacion, num_cuotas)
-- Permite tener una fila por cuota (cuota 01/03, 02/03, 03/03) en lugar de sobreescribir.
-- Ejecutar en Supabase > SQL Editor.

-- 1. Eliminar la constraint UNIQUE individual sobre codigo_autorizacion
ALTER TABLE movimientos
    DROP CONSTRAINT IF EXISTS movimientos_codigo_autorizacion_key;

-- 2. Agregar constraint compuesta (codigo_autorizacion, num_cuotas)
--    PostgreSQL permite múltiples NULLs en UNIQUE, así que las filas sin auth siguen usando tx_hash.
ALTER TABLE movimientos
    ADD CONSTRAINT movimientos_auth_cuota_unique
    UNIQUE (codigo_autorizacion, num_cuotas);
