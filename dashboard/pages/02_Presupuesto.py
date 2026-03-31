"""Página de gestión de presupuesto por categoría."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st

from analytics.db import get_connection, init_db
from analytics.loader import load_transactions
from analytics.repository import (
    get_categorias,
    get_presupuestos_periodo,
    set_presupuesto,
    copiar_presupuesto,
    create_categoria,
    toggle_categoria,
    delete_categoria,
)


@st.cache_resource
def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


@st.cache_data(ttl=300)
def get_data(_conn):
    return load_transactions(_conn)


conn = get_db()
df = get_data(conn)

st.title("Presupuesto")

# ── Selector de período ───────────────────────────────────────────────────────
if not df.empty and "periodo" in df.columns:
    periodos_disponibles = sorted(df["periodo"].dropna().unique(), reverse=True)
    periodo_label_map = (
        df.drop_duplicates("periodo")
        .set_index("periodo")["periodo_label"]
        .to_dict()
    )
    periodo_labels = [periodo_label_map.get(p, p) for p in periodos_disponibles]
    st.sidebar.header("Filtros")
    periodo_label_sel = st.sidebar.selectbox("Período", periodo_labels)
    periodo = periodos_disponibles[periodo_labels.index(periodo_label_sel)]
else:
    periodo = st.text_input("Período (YYYY-MM)", value="")

col_copiar, _ = st.columns([3, 2])
with col_copiar:
    year, month = int(periodo[:4]), int(periodo[5:7])
    periodo_anterior = f"{year-1}-12" if month == 1 else f"{year}-{month-1:02d}"
    label_anterior = periodo_label_map.get(periodo_anterior, periodo_anterior) if not df.empty else periodo_anterior
    if st.button(f"Copiar presupuesto desde {label_anterior}"):
        n = copiar_presupuesto(conn, periodo_anterior, periodo)
        st.success(f"Copiados {n} presupuestos desde {label_anterior}.")
        st.rerun()

st.divider()

# ── Tabla de presupuesto editable ─────────────────────────────────────────────
periodo_label_display = periodo_label_map.get(periodo, periodo) if not df.empty else periodo
st.subheader(f"Presupuesto para {periodo_label_display}")

df_ppto = get_presupuestos_periodo(conn, periodo)

if df_ppto.empty:
    st.info("No hay categorías. Agrega una abajo.")
else:
    edited = st.data_editor(
        df_ppto[["nombre", "monto_presupuesto"]].rename(
            columns={"nombre": "Categoría", "monto_presupuesto": "Presupuesto mensual ($)"}
        ),
        column_config={
            "Categoría": st.column_config.TextColumn(disabled=True),
            "Presupuesto mensual ($)": st.column_config.NumberColumn(min_value=0, step=1000),
        },
        hide_index=True,
        width="stretch",
        key="tabla_presupuesto",
    )

    if st.button("💾 Guardar presupuesto", type="primary"):
        cambios = 0
        for i, row in edited.iterrows():
            cat_id = int(df_ppto.loc[i, "categoria_id"])
            monto_nuevo = float(row["Presupuesto mensual ($)"] or 0)
            monto_actual = float(df_ppto.loc[i, "monto_presupuesto"])
            if monto_nuevo != monto_actual:
                set_presupuesto(conn, cat_id, periodo, monto_nuevo)
                cambios += 1
        if cambios:
            st.success(f"✓ {cambios} cambios guardados.")
            st.rerun()
        else:
            st.info("No hubo cambios.")

st.divider()

# ── Gestión de categorías ─────────────────────────────────────────────────────
st.subheader("Categorías")

with st.expander("Agregar nueva categoría"):
    with st.form("nueva_categoria"):
        nombre_nuevo = st.text_input("Nombre")
        color_nuevo = st.color_picker("Color", "#9E9E9E")
        submitted = st.form_submit_button("Crear")
        if submitted and nombre_nuevo.strip():
            try:
                create_categoria(conn, nombre_nuevo.strip(), color_nuevo)
                st.success(f"Categoría '{nombre_nuevo}' creada.")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

categorias_todas = get_categorias(conn, solo_activas=False)
with st.expander("Administrar categorías"):
    for cat in categorias_todas:
        col_n, col_toggle, col_del = st.columns([4, 1, 1])
        col_n.write(f"{'✅' if cat.activa else '⬜'} {cat.nombre}")
        label = "Desactivar" if cat.activa else "Activar"
        if col_toggle.button(label, key=f"toggle_{cat.id}"):
            toggle_categoria(conn, cat.id, not cat.activa)
            st.rerun()
        if col_del.button("Eliminar", key=f"del_{cat.id}", type="secondary"):
            ok, msg = delete_categoria(conn, cat.id)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
