"""Inicialización y conexión a la base de datos PostgreSQL (Supabase)."""
import os
import psycopg2
import psycopg2.extras


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS categorias (
        id         SERIAL PRIMARY KEY,
        nombre     TEXT    NOT NULL UNIQUE,
        color      TEXT    NOT NULL DEFAULT '#9E9E9E',
        activa     BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS presupuestos (
        id           SERIAL PRIMARY KEY,
        categoria_id INTEGER NOT NULL REFERENCES categorias(id),
        periodo      TEXT    NOT NULL,
        monto        NUMERIC NOT NULL DEFAULT 0,
        UNIQUE (categoria_id, periodo)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS clasificaciones (
        id                  SERIAL PRIMARY KEY,
        codigo_autorizacion TEXT    UNIQUE,
        tx_hash             TEXT    UNIQUE,
        categoria_id        INTEGER REFERENCES categorias(id),
        origen              TEXT    NOT NULL DEFAULT 'manual',
        created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reglas_sugerencia (
        id           SERIAL PRIMARY KEY,
        comercio     TEXT    NOT NULL,
        categoria_id INTEGER NOT NULL REFERENCES categorias(id),
        frecuencia   INTEGER NOT NULL DEFAULT 1,
        updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
        UNIQUE (comercio, categoria_id)
    )
    """,
    """
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
        codigo_autorizacion TEXT UNIQUE,
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
        updated_at          TIMESTAMP DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS splits (
        id                  SERIAL PRIMARY KEY,
        codigo_autorizacion TEXT,
        tx_hash             TEXT,
        categoria_id        INTEGER NOT NULL REFERENCES categorias(id),
        monto               NUMERIC NOT NULL,
        created_at          TIMESTAMP DEFAULT NOW(),
        updated_at          TIMESTAMP DEFAULT NOW()
    )
    """,
    """
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
    )
    """,
]


def get_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    return conn


def init_db(conn: psycopg2.extensions.connection) -> None:
    """Crea tablas si no existen."""
    cur = conn.cursor()
    try:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
