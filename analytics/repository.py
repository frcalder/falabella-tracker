"""Operaciones CRUD sobre PostgreSQL."""
from typing import Optional
import pandas as pd
import psycopg2.extensions

from .models import Categoria, Presupuesto


# ── Categorías ────────────────────────────────────────────────────────────────

def get_categorias(conn: psycopg2.extensions.connection, solo_activas: bool = True) -> list[Categoria]:
    q = "SELECT id, nombre, color, activa FROM categorias"
    if solo_activas:
        q += " WHERE activa = TRUE"
    q += " ORDER BY nombre"
    cur = conn.cursor()
    try:
        cur.execute(q)
        rows = cur.fetchall()
    finally:
        cur.close()
    return [Categoria(id=r["id"], nombre=r["nombre"], color=r["color"], activa=bool(r["activa"])) for r in rows]


def create_categoria(conn: psycopg2.extensions.connection, nombre: str, color: str = "#9E9E9E") -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO categorias (nombre, color) VALUES (%s, %s) RETURNING id",
            (nombre, color),
        )
        new_id = cur.fetchone()["id"]
        conn.commit()
        return new_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def toggle_categoria(conn: psycopg2.extensions.connection, categoria_id: int, activa: bool) -> None:
    cur = conn.cursor()
    try:
        cur.execute("UPDATE categorias SET activa = %s WHERE id = %s", (activa, categoria_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def delete_categoria(conn: psycopg2.extensions.connection, categoria_id: int) -> tuple[bool, str]:
    """Elimina categoría. Retorna (ok, mensaje). Falla si tiene clasificaciones."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM clasificaciones WHERE categoria_id = %s", (categoria_id,)
        )
        n = cur.fetchone()["count"]
        if n > 0:
            return False, f"No se puede eliminar: tiene {n} transacciones clasificadas."
        cur.execute("DELETE FROM reglas_sugerencia WHERE categoria_id = %s", (categoria_id,))
        cur.execute("DELETE FROM presupuestos WHERE categoria_id = %s", (categoria_id,))
        cur.execute("DELETE FROM categorias WHERE id = %s", (categoria_id,))
        conn.commit()
        return True, "Categoría eliminada."
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ── Presupuestos ──────────────────────────────────────────────────────────────

def get_presupuesto(conn: psycopg2.extensions.connection, categoria_id: int, periodo: str) -> float:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT monto FROM presupuestos WHERE categoria_id = %s AND periodo = %s",
            (categoria_id, periodo),
        )
        row = cur.fetchone()
        return float(row["monto"]) if row else 0.0
    finally:
        cur.close()


def get_presupuestos_periodo(conn: psycopg2.extensions.connection, periodo: str) -> pd.DataFrame:
    """Retorna DataFrame con (categoria_id, nombre, color, monto_presupuesto)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT c.id AS categoria_id, c.nombre, c.color,
                   COALESCE(p.monto, 0) AS monto_presupuesto
            FROM categorias c
            LEFT JOIN presupuestos p ON p.categoria_id = c.id AND p.periodo = %s
            WHERE c.activa = TRUE
            ORDER BY c.nombre
            """,
            (periodo,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    df = pd.DataFrame([dict(r) for r in rows])
    df["monto_presupuesto"] = pd.to_numeric(df["monto_presupuesto"], errors="coerce").fillna(0.0)
    return df


def set_presupuesto(conn: psycopg2.extensions.connection, categoria_id: int, periodo: str, monto: float) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO presupuestos (categoria_id, periodo, monto) VALUES (%s, %s, %s)
            ON CONFLICT (categoria_id, periodo) DO UPDATE SET monto = EXCLUDED.monto
            """,
            (categoria_id, periodo, monto),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def copiar_presupuesto(conn: psycopg2.extensions.connection, periodo_origen: str, periodo_destino: str) -> int:
    """Copia presupuestos de un período a otro. Retorna filas copiadas."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO presupuestos (categoria_id, periodo, monto)
            SELECT categoria_id, %s, monto FROM presupuestos WHERE periodo = %s
            ON CONFLICT (categoria_id, periodo) DO UPDATE SET monto = EXCLUDED.monto
            """,
            (periodo_destino, periodo_origen),
        )
        rowcount = cur.rowcount
        conn.commit()
        return rowcount
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ── Clasificaciones ───────────────────────────────────────────────────────────

def get_clasificacion(conn: psycopg2.extensions.connection, codigo_autorizacion: Optional[str], tx_hash: Optional[str]) -> Optional[int]:
    """Retorna categoria_id o None."""
    cur = conn.cursor()
    try:
        if codigo_autorizacion:
            cur.execute(
                "SELECT categoria_id FROM clasificaciones WHERE codigo_autorizacion = %s",
                (codigo_autorizacion,),
            )
            row = cur.fetchone()
            if row:
                return row["categoria_id"]
        if tx_hash:
            cur.execute(
                "SELECT categoria_id FROM clasificaciones WHERE tx_hash = %s",
                (tx_hash,),
            )
            row = cur.fetchone()
            if row:
                return row["categoria_id"]
        return None
    finally:
        cur.close()


def delete_clasificacion(
    conn: psycopg2.extensions.connection,
    codigo_autorizacion: Optional[str],
    tx_hash: Optional[str],
) -> None:
    """Elimina la clasificación de una transacción."""
    cur = conn.cursor()
    try:
        if codigo_autorizacion:
            cur.execute(
                "DELETE FROM clasificaciones WHERE codigo_autorizacion = %s",
                (codigo_autorizacion,),
            )
        elif tx_hash:
            cur.execute(
                "DELETE FROM clasificaciones WHERE tx_hash = %s",
                (tx_hash,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def upsert_clasificacion(
    conn: psycopg2.extensions.connection,
    codigo_autorizacion: Optional[str],
    tx_hash: Optional[str],
    categoria_id: int,
    origen: str = "manual",
) -> None:
    cur = conn.cursor()
    try:
        if codigo_autorizacion:
            # Cuando hay codigo_autorizacion, no almacenamos tx_hash (evita colisiones UNIQUE)
            cur.execute(
                """
                INSERT INTO clasificaciones (codigo_autorizacion, categoria_id, origen, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (codigo_autorizacion) DO UPDATE
                SET categoria_id = EXCLUDED.categoria_id,
                    origen = EXCLUDED.origen,
                    updated_at = EXCLUDED.updated_at
                """,
                (codigo_autorizacion, categoria_id, origen),
            )
        elif tx_hash:
            cur.execute(
                """
                INSERT INTO clasificaciones (tx_hash, categoria_id, origen, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (tx_hash) DO UPDATE
                SET categoria_id = EXCLUDED.categoria_id,
                    origen = EXCLUDED.origen,
                    updated_at = EXCLUDED.updated_at
                """,
                (tx_hash, categoria_id, origen),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ── Splits ────────────────────────────────────────────────────────────────────

def get_splits(
    conn: psycopg2.extensions.connection,
    codigo_autorizacion: Optional[str] = None,
    tx_hash: Optional[str] = None,
) -> list[dict]:
    """Retorna lista de {categoria_id, categoria_nombre, color, monto} para una transacción."""
    cur = conn.cursor()
    try:
        if codigo_autorizacion:
            cur.execute(
                """
                SELECT s.categoria_id, c.nombre AS categoria_nombre, c.color, s.monto
                FROM splits s JOIN categorias c ON c.id = s.categoria_id
                WHERE s.codigo_autorizacion = %s
                ORDER BY s.id
                """,
                (codigo_autorizacion,),
            )
        elif tx_hash:
            cur.execute(
                """
                SELECT s.categoria_id, c.nombre AS categoria_nombre, c.color, s.monto
                FROM splits s JOIN categorias c ON c.id = s.categoria_id
                WHERE s.tx_hash = %s ORDER BY s.id
                """,
                (tx_hash,),
            )
        else:
            return []
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()


def upsert_splits(
    conn: psycopg2.extensions.connection,
    codigo_autorizacion: Optional[str],
    tx_hash: Optional[str],
    splits: list[dict],
) -> None:
    """Reemplaza todos los splits de una transacción. splits: lista de {categoria_id, monto}."""
    cur = conn.cursor()
    try:
        if codigo_autorizacion:
            cur.execute(
                "DELETE FROM splits WHERE codigo_autorizacion = %s",
                (codigo_autorizacion,),
            )
            for s in splits:
                cur.execute(
                    "INSERT INTO splits (codigo_autorizacion, categoria_id, monto) VALUES (%s, %s, %s)",
                    (codigo_autorizacion, s["categoria_id"], s["monto"]),
                )
        elif tx_hash:
            cur.execute("DELETE FROM splits WHERE tx_hash = %s", (tx_hash,))
            for s in splits:
                cur.execute(
                    "INSERT INTO splits (tx_hash, categoria_id, monto) VALUES (%s, %s, %s)",
                    (tx_hash, s["categoria_id"], s["monto"]),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def delete_splits(
    conn: psycopg2.extensions.connection,
    codigo_autorizacion: Optional[str] = None,
    tx_hash: Optional[str] = None,
) -> None:
    """Elimina todos los splits de una transacción."""
    cur = conn.cursor()
    try:
        if codigo_autorizacion:
            cur.execute(
                "DELETE FROM splits WHERE codigo_autorizacion = %s",
                (codigo_autorizacion,),
            )
        elif tx_hash:
            cur.execute("DELETE FROM splits WHERE tx_hash = %s", (tx_hash,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ── Resumen analítico ─────────────────────────────────────────────────────────

def get_resumen_vs_presupuesto(conn: psycopg2.extensions.connection, periodo: str, df_gastos: pd.DataFrame) -> pd.DataFrame:
    """
    Combina gasto real (calculado desde df_gastos) con presupuesto de la DB.
    df_gastos debe tener columnas: categoria_id, monto (solo cargos, excluye pagos y pendientes).
    Retorna DataFrame: categoria_id, nombre, color, monto_presupuesto, monto_gastado, diferencia, pct.
    """
    presupuestos_df = get_presupuestos_periodo(conn, periodo)

    if df_gastos.empty or "categoria_id" not in df_gastos.columns:
        presupuestos_df["monto_gastado"] = 0.0
    else:
        col = "monto_periodo" if "monto_periodo" in df_gastos.columns else "monto"
        gastos_agg = (
            df_gastos
            .groupby("categoria_id")[col]
            .sum()
            .reset_index()
            .rename(columns={col: "monto_gastado"})
        )
        presupuestos_df = presupuestos_df.merge(gastos_agg, on="categoria_id", how="left")
        presupuestos_df["monto_gastado"] = presupuestos_df["monto_gastado"].fillna(0.0)

    presupuestos_df["diferencia"] = presupuestos_df["monto_presupuesto"] - presupuestos_df["monto_gastado"]
    presupuestos_df["pct"] = presupuestos_df.apply(
        lambda r: (r["monto_gastado"] / r["monto_presupuesto"] * 100) if r["monto_presupuesto"] > 0 else None,
        axis=1,
    )
    return presupuestos_df
