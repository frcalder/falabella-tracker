"""Motor de sugerencias de clasificación basado en historial de comercios."""
from typing import Optional
import psycopg2.extensions

from .repository import upsert_clasificacion

# Mapeo rubro del banco → nombre de categoría (para seed inicial)
RUBRO_TO_CATEGORIA = {
    "SUPERMERCADOS": "Supermercado",
    "MINIMARKET O SUPERMERCADOS": "Supermercado",
    "CARNICERIA": "Supermercado",
    "PANADERIA": "Restaurantes",
    "COMIDA Y RESTAURANTE": "Restaurantes",
    "COMIDA RAPIDA": "Restaurantes",
    "ESTACIONES DE BENCINA O GAS": "Combustible",
    "SERVICIO DE TAXIS": "Transporte",
    "PEAJES": "Transporte",
    "ESTACIONAMIENTO": "Transporte",
    "FARMACIA": "Salud / Farmacia",
    "SERVICIOS MEDICOS": "Salud / Farmacia",
    "ROPA DE HOMBRES Y NINOS": "Ropa / Calzado",
    "ROPA DE DAMAS": "Ropa / Calzado",
    "ZAPATERIA": "Ropa / Calzado",
    "ELECTRONICA": "Tecnología",
    "COMPUTADORES": "Tecnología",
    "APLICACIONES, JUEGOS INTERNET": "Suscripciones",
    "PRODUCTO DIGITAL GRAN TAMAÑO": "Suscripciones",
    "SERVICIOS AGUA, LUZ Y GAS": "Servicios Básicos",
    "CINES Y TEATROS": "Entretenimiento",
    "HOTELES": "Viajes",
    "AEROLINEAS": "Viajes",
    "COLEGIOS Y UNIVERSIDADES": "Educación",
    "LIBROS Y REVISTAS": "Educación",
    "TIENDAS POR DEPARTAMENTO": "Otros",
    "DISTRIBUIDORES DE QUIOSCOS": "Otros",
    "SUMINISTROS DEL HOGAR": "Hogar",
}


def sugerir_categoria(
    comercio: str, conn: psycopg2.extensions.connection
) -> list[tuple[int, str, float]]:
    """
    Retorna lista de (categoria_id, nombre, confianza) ordenada por confianza desc.
    confianza = frecuencia / total para ese comercio.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT r.categoria_id, c.nombre,
                   r.frecuencia * 1.0 / SUM(r.frecuencia) OVER (PARTITION BY r.comercio) AS confianza
            FROM reglas_sugerencia r
            JOIN categorias c ON c.id = r.categoria_id
            WHERE r.comercio = %s
            ORDER BY confianza DESC
            LIMIT 3
            """,
            (comercio,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    return [(r["categoria_id"], r["nombre"], round(float(r["confianza"]), 2)) for r in rows]


def _actualizar_regla(conn: psycopg2.extensions.connection, comercio: str, categoria_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO reglas_sugerencia (comercio, categoria_id, frecuencia, updated_at)
            VALUES (%s, %s, 1, NOW())
            ON CONFLICT (comercio, categoria_id) DO UPDATE
            SET frecuencia = reglas_sugerencia.frecuencia + 1, updated_at = NOW()
            """,
            (comercio, categoria_id),
        )
    finally:
        cur.close()


def clasificar(
    conn: psycopg2.extensions.connection,
    codigo_autorizacion: Optional[str],
    tx_hash: Optional[str],
    categoria_id: int,
    comercio: Optional[str] = None,
    origen: str = "manual",
) -> None:
    """Guarda o actualiza clasificación y actualiza reglas de sugerencia."""
    upsert_clasificacion(conn, codigo_autorizacion, tx_hash, categoria_id, origen)
    if comercio:
        _actualizar_regla(conn, comercio, categoria_id)
    conn.commit()


def aplicar_seed_desde_rubro(conn: psycopg2.extensions.connection, df) -> int:
    """
    Clasifica automáticamente transacciones sin clasificar usando rubro del banco.
    Solo aplica origen='auto'. Retorna número de clasificaciones aplicadas.
    """
    # Obtener mapa nombre_categoria → id
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, nombre FROM categorias WHERE activa = TRUE")
        rows = cur.fetchall()
    finally:
        cur.close()
    cat_map = {r["nombre"]: r["id"] for r in rows}

    count = 0
    for _, row in df.iterrows():
        if row.get("categoria_id") is not None:
            continue  # ya clasificada
        rubro = str(row.get("rubro", "")).strip().upper()
        cat_nombre = RUBRO_TO_CATEGORIA.get(rubro)
        if not cat_nombre or cat_nombre not in cat_map:
            continue
        categoria_id = cat_map[cat_nombre]
        cod_aut = str(row["codigo_autorizacion"]) if row.get("codigo_autorizacion") else None
        tx_hash = row.get("tx_hash")
        comercio = str(row.get("comercio", "")).strip() or None
        clasificar(conn, cod_aut, tx_hash, categoria_id, comercio, origen="auto")
        count += 1
    return count
