-- Migración: crear tabla splits para distribución de movimientos en múltiples categorías
-- Ejecutar en Supabase > SQL Editor.

CREATE TABLE IF NOT EXISTS splits (
    id                  SERIAL PRIMARY KEY,
    codigo_autorizacion TEXT,
    num_cuotas          TEXT,
    tx_hash             TEXT,
    categoria_id        INTEGER NOT NULL REFERENCES categorias(id),
    monto               NUMERIC NOT NULL,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Si la tabla ya existe sin num_cuotas (migración incremental):
ALTER TABLE splits ADD COLUMN IF NOT EXISTS num_cuotas TEXT;

CREATE INDEX IF NOT EXISTS idx_splits_cod_aut  ON splits(codigo_autorizacion);
CREATE INDEX IF NOT EXISTS idx_splits_tx_hash  ON splits(tx_hash);
