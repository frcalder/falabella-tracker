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

# ── Tabla ─────────────────────────────────────────────────────────────────────
categorias = get_categorias(conn)
cat_nombres = ["(sin clasificar)"] + [c.nombre for c in categorias]
cat_id_map = {c.nombre: c.id for c in categorias}

# df_sorted: todas las columnas, índice 0..N-1 (para lookup al seleccionar fila)
df_sorted = df_view.copy()
df_sorted["fecha_compra"] = pd.to_datetime(df_sorted["fecha_compra"], errors="coerce")
df_sorted = df_sorted.sort_values("fecha_compra", ascending=False).reset_index(drop=True)

# df_display: solo columnas visibles, valores formateados
cols_visible = ["fecha_compra", "descripcion", "comercio", "rubro", "monto", "pendiente", "categoria_nombre"]
cols_visible = [c for c in cols_visible if c in df_sorted.columns]

df_display = df_sorted[cols_visible].copy()
df_display["fecha_compra"] = df_sorted["fecha_compra"].dt.strftime("%d/%m/%Y").fillna("—")
df_display["categoria_nombre"] = df_display["categoria_nombre"].fillna("(sin clasificar)")
df_display["monto"] = df_sorted["monto"].apply(lambda x: f"${x:,.0f}")

st.caption("Haz clic en una fila para clasificar o dividir.")

event = st.dataframe(
    df_display,
    column_config={
        "fecha_compra": st.column_config.TextColumn("Fecha", width="small"),
        "descripcion": st.column_config.TextColumn("Descripción", width="medium"),
        "comercio": st.column_config.TextColumn("Comercio", width="medium"),
        "rubro": st.column_config.TextColumn("Rubro", width="medium"),
        "monto": st.column_config.TextColumn("Monto", width="small"),
        "pendiente": st.column_config.CheckboxColumn("Pendiente", width="small"),
        "categoria_nombre": st.column_config.TextColumn("Categoría", width="medium"),
    },
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    use_container_width=True,
    key="tabla_clasificacion",
)

# ── Panel de acción para fila seleccionada ────────────────────────────────────
sel_rows = event.selection.rows
if not sel_rows:
    st.info("Selecciona una fila para clasificar o dividir.")
else:
    sel_idx = sel_rows[0]
    row = df_sorted.iloc[sel_idx]

    cod_aut = row.get("codigo_autorizacion") or None
    tx_hash_val = row.get("tx_hash") or None
    is_split = bool(row.get("is_split"))
    monto_ref = float(row.get("monto_periodo") or row.get("monto") or 0)

    fecha_str = row["fecha_compra"].strftime("%d/%m/%Y") if pd.notna(row.get("fecha_compra")) else "—"
    prefix = "⏳ " if row.get("pendiente") else ""
    st.subheader(f"{prefix}{fecha_str} — {row['descripcion']} — ${monto_ref:,.0f}")

    tab_clasificar, tab_dividir = st.tabs(["Clasificar", "✂ Dividir"])

    with tab_clasificar:
        cat_actual = row.get("categoria_nombre") or "(sin clasificar)"
        if cat_actual not in cat_nombres:
            cat_actual = "(sin clasificar)"
        nueva_cat = st.selectbox(
            "Categoría",
            options=cat_nombres,
            index=cat_nombres.index(cat_actual),
            key="sel_cat_fila",
        )
        col_save, col_del = st.columns([2, 1])
        with col_save:
            if st.button("Guardar clasificación", type="primary", key="btn_guardar_cat"):
                if nueva_cat in ("(sin clasificar)", "✂ DIVIDIDO"):
                    st.warning("Selecciona una categoría válida.")
                else:
                    categoria_id = cat_id_map[nueva_cat]
                    comercio = str(row.get("comercio") or "").strip() or None
                    if is_split:
                        delete_splits(conn, cod_aut, tx_hash_val)
                    clasificar(conn, cod_aut, tx_hash_val, categoria_id, comercio, origen="manual")
                    get_data.clear()
                    st.success(f"Clasificado como {nueva_cat}.")
                    st.rerun()
        with col_del:
            if st.button("Quitar clasificación", key="btn_quitar_cat"):
                delete_clasificacion(conn, cod_aut, tx_hash_val)
                get_data.clear()
                st.success("Clasificación eliminada.")
                st.rerun()

    with tab_dividir:
        if not cod_aut and not tx_hash_val:
            st.warning("Este movimiento no tiene identificador — no se puede dividir.")
        else:
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

            col_save_split, col_del_split = st.columns([2, 1])
            with col_save_split:
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
            with col_del_split:
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
