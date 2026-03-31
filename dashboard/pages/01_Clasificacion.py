"""Página de clasificación de transacciones."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import streamlit as st

from analytics.db import get_connection, init_db
from analytics.loader import load_transactions
from analytics.classifier import clasificar, sugerir_categoria, aplicar_seed_desde_rubro
from analytics.repository import get_categorias, delete_clasificacion, get_splits, upsert_splits, delete_splits


@st.cache_resource(ttl=3600)
def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


@st.cache_data(ttl=300)
def get_data(_conn):
    return load_transactions(_conn)


conn = get_db()

st.title("Clasificación de Transacciones")

# ── Controles superiores ──────────────────────────────────────────────────────
col_seed, col_refresh = st.columns([3, 1])

with col_seed:
    if st.button("Auto-clasificar con rubro del banco"):
        df_tmp = get_data(conn)
        n = aplicar_seed_desde_rubro(conn, df_tmp)
        get_data.clear()
        st.success(f"{n} transacciones clasificadas automáticamente.")
        st.rerun()

with col_refresh:
    if st.button("Actualizar"):
        get_data.clear()
        st.rerun()

df = get_data(conn)

if df.empty:
    st.error("No hay datos. Ejecuta el scraper primero.")
    st.stop()

# ── Filtros sidebar ───────────────────────────────────────────────────────────
st.sidebar.header("Filtros")

periodos = sorted(df["periodo"].dropna().unique(), reverse=True)
periodo_label_map = (
    df.drop_duplicates("periodo")
    .set_index("periodo")["periodo_label"]
    .to_dict()
)
periodo_labels = [periodo_label_map.get(p, p) for p in periodos]
periodo_label_sel = st.sidebar.selectbox("Período", periodo_labels)
periodo = periodos[periodo_labels.index(periodo_label_sel)]

mostrar = st.sidebar.radio("Mostrar", ["Sin clasificar", "Todas", "Solo pendientes de confirmación"])

# Aplicar filtros
mask = pd.Series([True] * len(df))
mask &= df["periodo"] == periodo
if mostrar == "Sin clasificar":
    mask &= df["categoria_id"].isna() & ~df["is_split"].fillna(False)
elif mostrar == "Solo pendientes de confirmación":
    mask &= df["pendiente"]

df_view = df[mask].copy()
st.sidebar.info(f"{len(df_view)} transacciones")

if df_view.empty:
    st.success("No hay transacciones para mostrar con los filtros actuales.")
    st.stop()

# ── Tabla editable ────────────────────────────────────────────────────────────
categorias = get_categorias(conn)
cat_nombres = ["(sin clasificar)"] + [c.nombre for c in categorias]
cat_id_map = {c.nombre: c.id for c in categorias}

# Preparar columnas para mostrar
cols_display = ["fecha_compra", "descripcion", "comercio", "rubro", "monto", "pendiente",
                "categoria_nombre", "codigo_autorizacion", "num_cuotas", "tx_hash", "is_split"]
cols_display = [c for c in cols_display if c in df_view.columns]

df_edit = df_view[cols_display].copy()
df_edit["fecha_compra"] = pd.to_datetime(df_edit["fecha_compra"], errors="coerce")
df_edit = df_edit.sort_values("fecha_compra", ascending=False)
df_edit["fecha_compra"] = df_edit["fecha_compra"].dt.strftime("%d/%m/%Y").fillna("—")
df_edit["categoria_nombre"] = df_edit["categoria_nombre"].fillna("(sin clasificar)")
df_edit["monto"] = df_edit["monto"].apply(lambda x: f"${x:,.0f}")
df_edit = df_edit.reset_index(drop=True)

# Columna editable de categoría
edited = st.data_editor(
    df_edit,
    column_config={
        "fecha_compra": st.column_config.TextColumn("Fecha", width="small"),
        "descripcion": st.column_config.TextColumn("Descripción", width="medium"),
        "comercio": st.column_config.TextColumn("Comercio", width="medium"),
        "rubro": st.column_config.TextColumn("Rubro", width="medium"),
        "monto": st.column_config.TextColumn("Monto", width="small"),
        "pendiente": st.column_config.CheckboxColumn("Pendiente", width="small"),
        "categoria_nombre": st.column_config.SelectboxColumn(
            "Categoría", options=cat_nombres, width="medium"
        ),
        "codigo_autorizacion": None,
        "num_cuotas": None,
        "tx_hash": None,
        "is_split": None,
    },
    disabled=["fecha_compra", "descripcion", "comercio", "rubro", "monto", "pendiente"],
    hide_index=True,
    num_rows="dynamic",
    width="stretch",
    key="tabla_clasificacion",
)

# ── Guardar cambios ───────────────────────────────────────────────────────────
if st.button("Guardar clasificaciones", type="primary"):
    cambios = 0
    eliminados = 0

    # Filas editadas (categoría cambiada)
    for idx, row in edited.iterrows():
        if idx not in df_edit.index:
            continue  # fila nueva agregada (ignorar)
        nueva_cat = row["categoria_nombre"]
        if nueva_cat in ("(sin clasificar)", "✂ DIVIDIDO"):
            continue
        if nueva_cat == df_edit.loc[idx, "categoria_nombre"]:
            continue
        categoria_id = cat_id_map.get(nueva_cat)
        if not categoria_id:
            continue
        cod_aut = df_edit.loc[idx, "codigo_autorizacion"]
        tx_hash = df_edit.loc[idx, "tx_hash"]
        comercio = str(df_edit.loc[idx, "comercio"]).strip() or None
        # Si era split y ahora se asigna categoría directa, eliminar splits primero
        if df_edit.loc[idx, "is_split"]:
            delete_splits(conn, cod_aut, tx_hash)
        clasificar(conn, cod_aut, tx_hash, categoria_id, comercio, origen="manual")
        cambios += 1

    # Filas eliminadas (quitar clasificación)
    deleted_indices = set(df_edit.index) - set(edited.index)
    for idx in deleted_indices:
        cod_aut = df_edit.loc[idx, "codigo_autorizacion"]
        tx_hash = df_edit.loc[idx, "tx_hash"]
        if not cod_aut and not tx_hash:
            continue
        delete_clasificacion(conn, cod_aut, tx_hash)
        eliminados += 1

    if cambios or eliminados:
        get_data.clear()
        partes = []
        if cambios:
            partes.append(f"{cambios} clasificaciones guardadas")
        if eliminados:
            partes.append(f"{eliminados} clasificaciones eliminadas")
        st.success(". ".join(partes) + ".")
        st.rerun()
    else:
        st.info("No hubo cambios para guardar.")

# ── Dividir movimiento ────────────────────────────────────────────────────────
st.divider()
with st.expander("✂ Dividir movimiento"):
    txs_conf = df_view[df_view["codigo_autorizacion"].notna()].reset_index(drop=True)
    if txs_conf.empty:
        st.info("No hay movimientos con código de autorización en este período.")
    else:
        labels = []
        for _, r in txs_conf.iterrows():
            prefix = "⏳ " if r.get("pendiente") else ""
            fecha_str = r["fecha_compra"].strftime("%d/%m") if pd.notna(r.get("fecha_compra")) else "—"
            labels.append(f"{prefix}{fecha_str} — {r['descripcion']} — ${r['monto_periodo']:,.0f}")

        sel_i = st.selectbox("Transacción a dividir", range(len(labels)),
                             format_func=lambda i: labels[i], key="split_tx_sel")
        tx_row = txs_conf.iloc[sel_i]
        monto_ref = float(tx_row.get("monto_periodo") or 0)
        cod_aut = tx_row.get("codigo_autorizacion") or None
        tx_hash_val = tx_row.get("tx_hash") or None

        splits_actuales = get_splits(conn, cod_aut, tx_hash_val)
        if splits_actuales:
            splits_df = pd.DataFrame(splits_actuales)[["categoria_nombre", "monto"]].reset_index(drop=True)
        else:
            splits_df = pd.DataFrame({"categoria_nombre": [None], "monto": [0.0]})

        splits_edited = st.data_editor(
            splits_df,
            column_config={
                "categoria_nombre": st.column_config.SelectboxColumn(
                    "Categoría", options=[c.nombre for c in categorias], width="medium"
                ),
                "monto": st.column_config.NumberColumn(
                    "Monto", format="$%,.0f", min_value=0, width="small"
                ),
            },
            num_rows="dynamic",
            hide_index=True,
            key="split_editor",
        )

        total_asignado = float(splits_edited["monto"].fillna(0).sum())
        diferencia = monto_ref - total_asignado
        st.caption(
            f"Referencia: **${monto_ref:,.0f}** | "
            f"Asignado: **${total_asignado:,.0f}** | "
            f"Diferencia: **${diferencia:,.0f}**"
        )

        col_save, col_del = st.columns([2, 1])
        with col_save:
            if st.button("Guardar split", type="primary", key="btn_guardar_split"):
                filas = [
                    {"categoria_id": cat_id_map[r["categoria_nombre"]], "monto": float(r["monto"])}
                    for _, r in splits_edited.iterrows()
                    if r.get("categoria_nombre") in cat_id_map and (r.get("monto") or 0) > 0
                ]
                if not filas:
                    st.warning("Agrega al menos una fila con categoría y monto.")
                else:
                    delete_clasificacion(conn, cod_aut, tx_hash_val)
                    upsert_splits(conn, cod_aut, tx_hash_val, filas)
                    get_data.clear()
                    st.success(f"Split guardado en {len(filas)} partes.")
                    st.rerun()
        with col_del:
            if st.button("Eliminar split", key="btn_eliminar_split"):
                delete_splits(conn, cod_aut, tx_hash_val)
                get_data.clear()
                st.success("Split eliminado.")
                st.rerun()

# ── Panel de sugerencias ──────────────────────────────────────────────────────
st.divider()
st.subheader("Sugerencias por comercio")
comercio_sel = st.selectbox(
    "Ver sugerencias para comercio",
    options=sorted(df_view["comercio"].dropna().unique()),
)
if comercio_sel:
    sugerencias = sugerir_categoria(comercio_sel, conn)
    if sugerencias:
        for cat_id, cat_nombre, confianza in sugerencias:
            st.write(f"- **{cat_nombre}** — {confianza*100:.0f}% confianza")
    else:
        st.info("Sin historial para este comercio todavía.")
