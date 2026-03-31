"""Carga movimientos desde PostgreSQL y los enriquece con clasificaciones."""
from collections import defaultdict
from typing import Optional

import pandas as pd
import psycopg2.extensions


def load_transactions(conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Lee movimientos desde la tabla movimientos, parsea fechas y montos,
    y hace JOIN con clasificaciones de la DB.
    Retorna DataFrame listo para el dashboard.
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM movimientos")
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])

    # Los montos ya vienen como Decimal de PostgreSQL — convertir a float
    for col in ("monto", "monto_periodo", "valor_cuota"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # monto_periodo: usar directamente, con fallback a monto si None
    if "monto_periodo" in df.columns:
        df["monto_periodo"] = df["monto_periodo"].combine_first(df["monto"])
    else:
        df["monto_periodo"] = df["monto"]

    # Las fechas ya vienen como date objects de PostgreSQL — convertir a datetime para consistencia
    for col in ("fecha", "fecha_compra"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # periodo_label desde periodo_facturacion
    if "periodo_facturacion" in df.columns and df["periodo_facturacion"].notna().any():
        closing = pd.to_datetime(df["periodo_facturacion"], format="%d/%m/%Y", errors="coerce")

        def _periodo_label(d):
            if pd.isna(d):
                return ""
            return d.strftime("%d/%m/%Y")

        df["periodo_label"] = closing.apply(_periodo_label)

        # Si la columna periodo ya está en la tabla, usarla; sino calcular
        if "periodo" not in df.columns or df["periodo"].isna().all():
            df["periodo"] = closing.dt.strftime("%Y-%m")
    else:
        if "periodo" not in df.columns or df["periodo"].isna().all():
            df["periodo"] = df["fecha"].dt.strftime("%Y-%m")
        df["periodo_label"] = df.get("periodo", df["fecha"].dt.strftime("%Y-%m"))

    df["mes_label"] = df["fecha"].dt.strftime("%b %Y")

    # Asegurar pendiente es bool
    df["pendiente"] = df["pendiente"].astype(bool)

    # JOIN con clasificaciones y splits desde DB
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT codigo_autorizacion, tx_hash, categoria_id, origen FROM clasificaciones"
        )
        cls_rows = cur.fetchall()
        cur.execute("SELECT id, nombre, color FROM categorias")
        cat_rows = cur.fetchall()
        cur.execute(
            "SELECT DISTINCT codigo_autorizacion, tx_hash FROM splits"
        )
        split_rows = cur.fetchall()
    finally:
        cur.close()

    cls_by_cod = {r["codigo_autorizacion"]: r for r in cls_rows if r["codigo_autorizacion"]}
    cls_by_hash = {r["tx_hash"]: r for r in cls_rows if r["tx_hash"]}
    cat_map = {r["id"]: {"nombre": r["nombre"], "color": r["color"]} for r in cat_rows}
    # Split aplica a toda la compra (igual que clasificaciones), no por cuota individual
    split_key_set = {r["codigo_autorizacion"] for r in split_rows if r["codigo_autorizacion"]}
    split_hash_set = {r["tx_hash"] for r in split_rows if r["tx_hash"]}

    def _enrich(row):
        cod = row.get("codigo_autorizacion")
        th = row.get("tx_hash")
        # Splits tienen prioridad sobre clasificación directa
        if ((cod and cod in split_key_set) or (th and th in split_hash_set)):
            return pd.Series({
                "categoria_id": None,
                "categoria_nombre": "✂ DIVIDIDO",
                "categoria_color": "#9C27B0",
                "clasificacion_origen": "split",
                "is_split": True,
            })
        cls = cls_by_cod.get(cod) or cls_by_hash.get(th)
        if cls:
            cid = cls["categoria_id"]
            return pd.Series({
                "categoria_id": cid,
                "categoria_nombre": cat_map.get(cid, {}).get("nombre"),
                "categoria_color": cat_map.get(cid, {}).get("color"),
                "clasificacion_origen": cls["origen"],
                "is_split": False,
            })
        return pd.Series({
            "categoria_id": None,
            "categoria_nombre": None,
            "categoria_color": None,
            "clasificacion_origen": None,
            "is_split": False,
        })

    enriched = df.apply(_enrich, axis=1)
    df = pd.concat([df, enriched], axis=1)

    return df


def expand_splits(df: pd.DataFrame, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """
    Expande las filas con is_split=True en múltiples filas (una por parte del split),
    cada una con su monto y categoria_id propios. Para usar en analytics antes de agregar.
    """
    if "is_split" not in df.columns or not df["is_split"].any():
        return df

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT s.codigo_autorizacion, s.tx_hash, s.categoria_id, s.monto,
                   c.nombre AS categoria_nombre, c.color AS categoria_color
            FROM splits s
            JOIN categorias c ON c.id = s.categoria_id
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    if not rows:
        return df

    splits_by_cod: dict = defaultdict(list)   # codigo_autorizacion → parts
    splits_by_hash: dict = defaultdict(list)
    for r in rows:
        d = dict(r)
        if d.get("codigo_autorizacion"):
            splits_by_cod[d["codigo_autorizacion"]].append(d)
        elif d.get("tx_hash"):
            splits_by_hash[d["tx_hash"]].append(d)

    expanded = []
    for _, row in df.iterrows():
        cod = row.get("codigo_autorizacion")
        th = row.get("tx_hash")
        parts = splits_by_cod.get(cod) or splits_by_hash.get(th)
        if parts:
            for part in parts:
                new_row = row.copy()
                new_row["monto_periodo"] = float(part["monto"])
                new_row["categoria_id"] = part["categoria_id"]
                new_row["categoria_nombre"] = part["categoria_nombre"]
                new_row["categoria_color"] = part["categoria_color"]
                new_row["clasificacion_origen"] = "split"
                expanded.append(new_row)
        else:
            expanded.append(row)

    if not expanded:
        return pd.DataFrame(columns=df.columns)
    return pd.DataFrame(expanded, columns=df.columns).reset_index(drop=True)
