"""Página de análisis de gastos vs presupuesto."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from analytics.db import get_connection, init_db
from analytics.loader import load_transactions, expand_splits
from analytics.repository import get_resumen_vs_presupuesto, get_categorias, delete_splits
from analytics.classifier import clasificar


@st.cache_resource(ttl=3600)
def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


@st.cache_data(ttl=300)
def get_data(_conn):
    return load_transactions(_conn)


def _prev_periodo(periodo: str) -> str:
    year, month = int(periodo[:4]), int(periodo[5:7])
    if month == 1:
        return f"{year-1}-12"
    return f"{year}-{month-1:02d}"


def _periodo_dates(periodo: str) -> tuple[date, date]:
    """Retorna (start, end) del período: abre el 20 del mes anterior, cierra el 19."""
    year, month = int(periodo[:4]), int(periodo[5:7])
    end_date = date(year, month, 19)
    start_date = date(year - 1, 12, 20) if month == 1 else date(year, month - 1, 20)
    return start_date, end_date


conn = get_db()
df_full = get_data(conn)

st.title("Análisis")

if df_full.empty:
    st.error("No hay datos. Ejecuta el scraper primero.")
    st.stop()

df_expanded = expand_splits(df_full, conn)
df_cargos = df_expanded[df_expanded["categoria_id"].notna()].copy()
periodos_disponibles = sorted(df_full["periodo"].dropna().unique(), reverse=True)

# Mapeo periodo (YYYY-MM) → label legible ("20/02 - 19/03/2026")
periodo_label_map = (
    df_full.drop_duplicates("periodo")
    .set_index("periodo")["periodo_label"]
    .to_dict()
)

st.sidebar.header("Filtros")
periodo_labels = [periodo_label_map.get(p, p) for p in periodos_disponibles]
periodo_label_sel = st.sidebar.selectbox("Período", periodo_labels)
periodo_sel = periodos_disponibles[periodo_labels.index(periodo_label_sel)]

df_periodo = df_cargos[df_cargos["periodo"] == periodo_sel]
resumen = get_resumen_vs_presupuesto(conn, periodo_sel, df_periodo)
resumen_datos = resumen[(resumen["monto_presupuesto"] > 0) | (resumen["monto_gastado"] > 0)].copy()

if resumen_datos.empty:
    st.info("Sin datos para este período. Configura el presupuesto en la pestaña Presupuesto.")
    st.stop()

resumen_datos = resumen_datos.sort_values("pct", ascending=False, na_position="last")

# ── Delta vs período anterior (ajustado al avance proporcional) ───────────────
prev = _prev_periodo(periodo_sel)
df_prev_periodo = df_cargos[df_cargos["periodo"] == prev]

con_ppto = resumen_datos[resumen_datos["monto_presupuesto"] > 0]
sin_ppto = resumen_datos[resumen_datos["monto_presupuesto"] == 0]

total_ppto = con_ppto["monto_presupuesto"].sum()
total_gasto = con_ppto["monto_gastado"].sum()
total_sin_ppto = sin_ppto["monto_gastado"].sum()
pct_total = (total_gasto / total_ppto * 100) if total_ppto > 0 else None

# Avance proporcional: si el período está en curso, filtrar el período anterior
# al equivalente proporcional de días transcurridos
curr_start, curr_end = _periodo_dates(periodo_sel)
today = date.today()
curr_total_days = (curr_end - curr_start).days + 1
curr_elapsed = max(1, min((today - curr_start).days + 1, curr_total_days))

if curr_elapsed < curr_total_days and not df_prev_periodo.empty:
    proportion = curr_elapsed / curr_total_days
    prev_start, _ = _periodo_dates(prev)
    prev_total_days = (_periodo_dates(prev)[1] - prev_start).days + 1
    prev_cutoff = prev_start + timedelta(days=max(0, round(proportion * prev_total_days) - 1))
    df_prev_comp = df_prev_periodo.copy()
    df_prev_comp["_fecha_dt"] = pd.to_datetime(df_prev_comp["fecha"], errors="coerce")
    df_prev_comp = df_prev_comp[df_prev_comp["_fecha_dt"] <= pd.Timestamp(prev_cutoff)]
    delta_label = f"vs día {curr_elapsed}/{curr_total_days} mes anterior"
else:
    df_prev_comp = df_prev_periodo
    delta_label = f"vs {prev}"

resumen_prev = get_resumen_vs_presupuesto(conn, prev, df_prev_comp)
prev_con_ppto = resumen_prev[resumen_prev["monto_presupuesto"] > 0]
prev_gasto = prev_con_ppto["monto_gastado"].sum() if not prev_con_ppto.empty else None
delta_gasto = (total_gasto - prev_gasto) if prev_gasto else None

# ── Cards ─────────────────────────────────────────────────────────────────────
total_cuota_periodo = df_periodo["monto_periodo"].sum() if not df_periodo.empty else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total presupuestado", f"${total_ppto:,.0f}")
c2.metric(
    "Total gastado",
    f"${total_gasto:,.0f}",
    delta=(f"-${abs(delta_gasto):,.0f} {delta_label}" if delta_gasto < 0 else f"+${delta_gasto:,.0f} {delta_label}") if delta_gasto is not None else None,
    delta_color="inverse",
)
c3.metric("% del presupuesto", f"{pct_total:.0f}%" if pct_total else "—")
c4.metric("Sin presupuesto asignado", f"${total_sin_ppto:,.0f}")
c5.metric("Total cuotas del período", f"${total_cuota_periodo:,.0f}")

st.divider()

# ── Barras de progreso + Tabla de detalle ──────────────────────────────────────
col_bars, col_table = st.columns([6, 4])

with col_bars:
    st.subheader("Progreso por categoría")
    rd = resumen_datos.copy()
    rd["pct_label"] = rd["pct"].apply(lambda x: f"{x:.0f}%" if x is not None else "—")
    rd["color_bar"] = rd["pct"].apply(
        lambda x: "#EF5350" if x is not None and x > 100
        else "#FFA726" if x is not None and x > 80
        else "#42A5F5"
    )
    max_x = max(rd["monto_presupuesto"].max(), rd["monto_gastado"].max()) * 1.35

    fig_prog = go.Figure()
    fig_prog.add_trace(go.Bar(
        name="Presupuesto",
        y=rd["nombre"],
        x=rd["monto_presupuesto"],
        orientation="h",
        marker_color="rgba(200,200,200,0.35)",
        showlegend=False,
    ))
    fig_prog.add_trace(go.Bar(
        name="Gastado",
        y=rd["nombre"],
        x=rd["monto_gastado"],
        orientation="h",
        marker_color=rd["color_bar"].tolist(),
        text=rd.apply(lambda r: f"  ${r['monto_gastado']:,.0f} ({r['pct_label']})", axis=1),
        textposition="outside",
        showlegend=False,
    ))
    fig_prog.update_layout(
        barmode="overlay",
        height=max(300, len(rd) * 48),
        xaxis=dict(range=[0, max_x], showgrid=True, tickprefix="$", tickformat=","),
        yaxis=dict(autorange="reversed"),
        margin=dict(l=0, r=130, t=10, b=10),
    )
    event = st.plotly_chart(fig_prog, on_select="rerun", key="prog_chart", width="stretch")

with col_table:
    selected_cat = None
    if event.selection and event.selection.points:
        selected_cat = event.selection.points[0].get("y")

    if not selected_cat:
        st.subheader("Movimientos")
        st.caption("← Haz click en una categoría para ver sus movimientos")
    else:
        st.subheader(selected_cat)
        cat_row = resumen_datos[resumen_datos["nombre"] == selected_cat]
        if cat_row.empty or cat_row.iloc[0]["categoria_id"] is None:
            st.info("Sin movimientos clasificados en esta categoría.")
        else:
            cat_id = int(cat_row.iloc[0]["categoria_id"])
            df_cat = df_periodo[df_periodo["categoria_id"] == cat_id].copy()

            if df_cat.empty:
                st.info("Sin movimientos para este período.")
            else:
                total_monto = df_cat["monto"].sum()
                total_cuota = df_cat["monto_periodo"].sum()
                st.caption(f"{len(df_cat)} movimientos")

                todas_cats = get_categorias(conn)
                cat_nombres = ["(sin cambio)"] + [c.nombre for c in todas_cats]
                cat_id_map = {c.nombre: c.id for c in todas_cats}

                cols = ["fecha_compra", "comercio", "num_cuotas", "monto_periodo",
                        "categoria_nombre", "codigo_autorizacion", "tx_hash"]
                cols = [c for c in cols if c in df_cat.columns]
                df_show = df_cat[cols].copy()
                df_show["fecha_compra"] = pd.to_datetime(df_show["fecha_compra"], errors="coerce")
                df_show = df_show.sort_values("fecha_compra", ascending=False)
                df_show["fecha_compra"] = df_show["fecha_compra"].dt.strftime("%d/%m/%Y").fillna("—")
                df_show["categoria_nombre"] = df_show["categoria_nombre"].fillna("(sin cambio)")
                if "clasificacion_origen" in df_show.columns:
                    df_show["comercio"] = df_show.apply(
                        lambda r: f"✂ {r['comercio']}" if r.get("clasificacion_origen") == "split" else r["comercio"],
                        axis=1,
                    )

                # Fila de totales (sin categoria para no confundir)
                totals = {c: "" for c in cols}
                totals["fecha_compra"] = "TOTAL"
                totals["monto_periodo"] = total_cuota
                df_show = pd.concat([df_show, pd.DataFrame([totals])], ignore_index=True)

                table_height = 38 + len(df_show) * 35

                edited = st.data_editor(
                    df_show,
                    column_config={
                        "fecha_compra": st.column_config.TextColumn("Fecha", width="small"),
                        "comercio": st.column_config.TextColumn("Comercio"),
                        "num_cuotas": st.column_config.TextColumn("Cuotas", width="small"),
                        "monto_periodo": st.column_config.NumberColumn("Valor cuota", format="$%,.0f", width="small"),
                        "categoria_nombre": st.column_config.SelectboxColumn(
                            "Categoría",
                            options=cat_nombres,
                            width="medium",
                            help="Selecciona una categoría para reclasificar este movimiento",
                        ),
                        "codigo_autorizacion": None,
                        "tx_hash": None,
                    },
                    disabled=["fecha_compra", "comercio", "num_cuotas", "monto_periodo"],
                    hide_index=True,
                    height=table_height,
                    width="stretch",
                    key=f"drill_{selected_cat}",
                )

                if st.button("Guardar reclasificaciones", type="primary", key="save_drill"):
                    cambios = 0
                    for idx, row in edited.iloc[:-1].iterrows():  # excluir fila TOTAL
                        nueva_cat = row.get("categoria_nombre", "(sin cambio)")
                        if nueva_cat in ("(sin cambio)", df_show.loc[idx, "categoria_nombre"]):
                            continue
                        categoria_id = cat_id_map.get(nueva_cat)
                        if not categoria_id:
                            continue
                        cod_aut = row.get("codigo_autorizacion")
                        tx_hash = row.get("tx_hash")
                        if row.get("clasificacion_origen") == "split":
                            delete_splits(conn, cod_aut, tx_hash)
                        clasificar(conn, cod_aut, tx_hash, categoria_id,
                                   str(row.get("comercio", "")).strip() or None, origen="manual")
                        cambios += 1
                    if cambios:
                        get_data.clear()
                        st.success(f"{cambios} movimientos reclasificados.")
                        st.rerun()
                    else:
                        st.info("No hubo cambios.")

st.divider()

# ── Tendencia multi-período ────────────────────────────────────────────────────
st.subheader("Tendencia por categoría")

periodos_trend = sorted(df_cargos["periodo"].dropna().unique())[-6:]
cats_ids_sel = resumen_datos["categoria_id"].dropna().tolist()

trend_df = (
    df_cargos[
        df_cargos["periodo"].isin(periodos_trend) &
        df_cargos["categoria_id"].isin(cats_ids_sel)
    ]
    .groupby(["periodo", "periodo_label", "categoria_id", "categoria_nombre"])["monto_periodo"]
    .sum()
    .reset_index()
)

if not trend_df.empty:
    color_map = dict(zip(resumen_datos["nombre"], resumen_datos["color"])) if "color" in resumen_datos.columns else {}

    fig_trend = go.Figure()
    for cat_nombre, group in trend_df.groupby("categoria_nombre"):
        group = group.sort_values("periodo")
        fig_trend.add_trace(go.Scatter(
            x=group["periodo_label"],
            y=group["monto_periodo"],
            name=str(cat_nombre),
            mode="lines+markers",
            line=dict(color=color_map.get(str(cat_nombre)), width=2),
            marker=dict(size=7),
            hovertemplate=f"<b>{cat_nombre}</b><br>%{{x}}<br>${{y:,.0f}}<extra></extra>",
        ))
    fig_trend.update_layout(
        height=350,
        xaxis_title="Período",
        yaxis=dict(tickprefix="$", tickformat=","),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=0, r=0, t=50, b=0),
    )
    st.plotly_chart(fig_trend, width="stretch")
else:
    st.info("Sin datos de tendencia para los últimos períodos.")
