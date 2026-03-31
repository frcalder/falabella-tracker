-- Schema para Supabase PostgreSQL
-- Ejecutar una vez en el SQL Editor de Supabase (Database > SQL Editor).
-- Las tablas se crean en orden correcto respetando las foreign keys.

CREATE TABLE IF NOT EXISTS categorias (
    id         SERIAL PRIMARY KEY,
    nombre     TEXT    NOT NULL UNIQUE,
    color      TEXT    NOT NULL DEFAULT '#9E9E9E',
    activa     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presupuestos (
    id           SERIAL PRIMARY KEY,
    categoria_id INTEGER NOT NULL REFERENCES categorias(id),
    periodo      TEXT    NOT NULL,
    monto        NUMERIC NOT NULL DEFAULT 0,
    UNIQUE (categoria_id, periodo)
);

CREATE TABLE IF NOT EXISTS clasificaciones (
    id                  SERIAL PRIMARY KEY,
    codigo_autorizacion TEXT    UNIQUE,
    tx_hash             TEXT    UNIQUE,
    categoria_id        INTEGER REFERENCES categorias(id),
    origen              TEXT    NOT NULL DEFAULT 'manual',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reglas_sugerencia (
    id           SERIAL PRIMARY KEY,
    comercio     TEXT    NOT NULL,
    categoria_id INTEGER NOT NULL REFERENCES categorias(id),
    frecuencia   INTEGER NOT NULL DEFAULT 1,
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (comercio, categoria_id)
);

CREATE TABLE IF NOT EXISTS movimientos (
    id                  SERIAL PRIMARY KEY,
    fecha               DATE,
    descripcion         TEXT,
    persona             TEXT,
    monto               NUMERIC,
    monto_periodo       NUMERIC,
    pendiente           BOOLEAN NOT NULL DEFAULT FALSE,
    rubro               TEXT,
    comercio            TEXT,
    codigo_autorizacion TEXT,
    fecha_compra        DATE,
    hora                TEXT,
    pais                TEXT,
    origen              TEXT,
    periodo_facturacion TEXT,
    periodo             TEXT,
    num_cuotas          TEXT,
    valor_cuota         NUMERIC,
    tx_hash             TEXT UNIQUE,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE (codigo_autorizacion, num_cuotas)
);

CREATE TABLE IF NOT EXISTS scraper_runs (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    status          TEXT NOT NULL DEFAULT 'running',
    headless        BOOLEAN NOT NULL DEFAULT FALSE,
    paginas         INTEGER DEFAULT 0,
    procesados      INTEGER DEFAULT 0,
    nuevos          INTEGER DEFAULT 0,
    actualizados    INTEGER DEFAULT 0,
    pendientes      INTEGER DEFAULT 0,
    error_message   TEXT,
    periodo         TEXT
);

CREATE TABLE IF NOT EXISTS splits (
    id                  SERIAL PRIMARY KEY,
    codigo_autorizacion TEXT,
    tx_hash             TEXT,
    categoria_id        INTEGER NOT NULL REFERENCES categorias(id),
    monto               NUMERIC NOT NULL,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Índices recomendados para queries frecuentes
CREATE INDEX IF NOT EXISTS idx_movimientos_periodo      ON movimientos(periodo);
CREATE INDEX IF NOT EXISTS idx_movimientos_pendiente    ON movimientos(pendiente);
CREATE INDEX IF NOT EXISTS idx_movimientos_fecha        ON movimientos(fecha);
CREATE INDEX IF NOT EXISTS idx_clasificaciones_cod_aut  ON clasificaciones(codigo_autorizacion);
CREATE INDEX IF NOT EXISTS idx_clasificaciones_tx_hash  ON clasificaciones(tx_hash);
CREATE INDEX IF NOT EXISTS idx_splits_cod_aut            ON splits(codigo_autorizacion);
CREATE INDEX IF NOT EXISTS idx_splits_tx_hash            ON splits(tx_hash);
