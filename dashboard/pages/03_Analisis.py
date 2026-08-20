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
from analytics.repository import (get_resumen_vs_presupuesto, get_categorias,
                                  get_presupuestos_periodo, delete_splits)
from analytics.classifier import clasificar
from dashboard.theme import (CATEGORICAL_LIGHT, GHOST, GHOST_STRONG, INK_MUTED,
                             apply_layout, money, money_md, periodo_corto, status_of)


@st.cache_resource(ttl=3600)
def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


@st.cache_data(ttl=300)
def get_data(_conn):
    return load_transactions(_conn)


@st.cache_data(ttl=300)
def get_expanded(_conn, df):
    """expand_splits recorre fila por fila y la página se vuelve a ejecutar con cada clic, así
    que conviene cachearlo en vez de recalcularlo en cada interacción."""
    return expand_splits(df, _conn)


@st.cache_data(ttl=300)
def get_presupuestos_hist(_conn, periodos: tuple) -> dict:
    """{periodo: {categoria_id: monto}} de los presupuestos con monto asignado.

    El presupuesto se define por período, así que qué gasto era gestionable cambia mes a mes:
    una categoría puede tener tope en julio y no en agosto. El histórico respeta eso en vez de
    aplicar la lista del período seleccionado a todos.
    """
    salida = {}
    for p in periodos:
        df = get_presupuestos_periodo(_conn, p)
        df = df[df["monto_presupuesto"] > 0]
        salida[p] = dict(zip(df["categoria_id"].tolist(),
                             df["monto_presupuesto"].astype(float).tolist()))
    return salida


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


def _avance(periodo: str) -> tuple[bool, int, int]:
    """(en_curso, dias_transcurridos, dias_totales) de un período."""
    start, end = _periodo_dates(periodo)
    total = (end - start).days + 1
    elapsed = max(1, min((date.today() - start).days + 1, total))
    return (elapsed < total, elapsed, total)


conn = get_db()
df_full = get_data(conn)

st.title("Análisis")

if df_full.empty:
    st.error("No hay datos. Ejecuta el scraper primero.")
    st.stop()

df_expanded = get_expanded(conn, df_full)
df_cargos = df_expanded[df_expanded["categoria_id"].notna()].copy()
periodos_disponibles = sorted(df_full["periodo"].dropna().unique(), reverse=True)

# Mapeo periodo (YYYY-MM) → label legible (la fecha de cierre, "19/03/2026")
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

# ── Delta vs período anterior (ajustado al avance proporcional) ───────────────
prev = _prev_periodo(periodo_sel)
df_prev_periodo = df_cargos[df_cargos["periodo"] == prev]

con_ppto = resumen_datos[resumen_datos["monto_presupuesto"] > 0].sort_values(
    "pct", ascending=False, na_position="last")
sin_ppto = resumen_datos[resumen_datos["monto_presupuesto"] == 0].sort_values(
    "monto_gastado", ascending=False)

total_ppto = con_ppto["monto_presupuesto"].sum()
total_gasto = con_ppto["monto_gastado"].sum()
total_sin_ppto = sin_ppto["monto_gastado"].sum()
pct_total = (total_gasto / total_ppto * 100) if total_ppto > 0 else None

# Avance proporcional: si el período está en curso, filtrar el período anterior
# al equivalente proporcional de días transcurridos
en_curso, curr_elapsed, curr_total_days = _avance(periodo_sel)

if en_curso and not df_prev_periodo.empty:
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
# La jerarquía es deliberada: el gasto CON presupuesto es el protagonista, porque es el único
# gestionable. El gasto sin presupuesto es contexto — no se gestiona y parte puede volver como
# reembolso — así que no compite por atención con el resto.
total_cuota_periodo = df_periodo["monto_periodo"].sum() if not df_periodo.empty else 0
pendiente_periodo = (df_periodo[df_periodo["pendiente"]]["monto_periodo"].sum()
                     if "pendiente" in df_periodo.columns and not df_periodo.empty else 0)
saldo = total_ppto - total_gasto

if en_curso:
    st.caption(f"Período en curso · día {curr_elapsed} de {curr_total_days}")
else:
    st.caption(f"Período cerrado el {periodo_label_sel}")

c1, c2, c3 = st.columns([3, 2, 2])
c1.metric(
    "Gastado del presupuesto",
    money(total_gasto),
    delta=(f"{'-' if delta_gasto < 0 else '+'}{money(abs(delta_gasto))} {delta_label}"
           if delta_gasto is not None else None),
    delta_color="inverse",
    help="Solo las categorías con presupuesto asignado en este período: el gasto gestionable.",
)
c2.metric("Presupuestado", money(total_ppto))
c3.metric(
    "Disponible" if saldo >= 0 else "Excedido",
    money(abs(saldo)),
    help="Presupuesto menos gasto. Es lo que queda (o lo que se pasó) en las categorías "
         "que sí tienen tope asignado.",
)

# ── Medidor del presupuesto ───────────────────────────────────────────────────
# Un ratio contra un límite se lee como medidor, no como número suelto.
if pct_total is not None:
    _, color_estado, icono = status_of(pct_total)
    tope = max(110.0, pct_total * 1.06)
    fig_meter = go.Figure()
    fig_meter.add_trace(go.Bar(
        x=[tope], y=[""], orientation="h",
        marker_color=GHOST, hoverinfo="skip", showlegend=False,
    ))
    fig_meter.add_trace(go.Bar(
        x=[pct_total], y=[""], orientation="h",
        marker_color=color_estado,
        hovertemplate=f"{money(total_gasto)} de {money(total_ppto)}<extra></extra>",
        showlegend=False,
    ))
    apply_layout(fig_meter, height=86, barmode="overlay", bargap=0.55,
                 margin=dict(l=0, r=0, t=26, b=0))
    fig_meter.update_yaxes(showgrid=False, showticklabels=False)
    fig_meter.update_xaxes(range=[0, tope], showticklabels=False, ticks="")
    # marca del 100%: la referencia contra la que se lee todo el medidor
    fig_meter.add_shape(type="line", x0=100, x1=100, y0=-0.5, y1=0.5,
                        line=dict(color=INK_MUTED, width=1))
    fig_meter.add_annotation(x=100, y=0.52, yanchor="bottom", text="100%",
                             showarrow=False, font=dict(size=11, color=INK_MUTED))
    fig_meter.add_annotation(
        x=0, y=0.52, xanchor="left", yanchor="bottom", showarrow=False,
        text=f"<b>{icono} {pct_total:.0f}%</b> del presupuesto",
        font=dict(size=13),
    )
    st.plotly_chart(fig_meter, width="stretch", config={"displayModeBar": False})

contexto = [f"Total clasificado del período: {money_md(total_cuota_periodo)}"]
if total_sin_ppto:
    contexto.append(f"de los cuales {money_md(total_sin_ppto)} en categorías sin presupuesto")
if pendiente_periodo:
    contexto.append(f"⏳ {money_md(pendiente_periodo)} sin confirmar por el banco")
st.caption(" · ".join(contexto))

st.divider()

# ── Barras de progreso + Tabla de detalle ──────────────────────────────────────
col_bars, col_table = st.columns([6, 4])

with col_bars:
    st.subheader("Progreso por categoría")

    sel_bar = None
    if len(con_ppto):
        etiquetas, colores = [], []
        for _, r in con_ppto.iterrows():
            _, color, icono = status_of(r["pct"])
            etiquetas.append(f"  {icono} {r['pct']:.0f}% · {money(r['monto_gastado'])}")
            colores.append(color)

        # Escala propia: solo las categorías con presupuesto. Antes el eje lo fijaba la
        # categoría sin presupuesto más grande y aplastaba a todas las gestionables.
        max_x = max(con_ppto["monto_presupuesto"].max(),
                    con_ppto["monto_gastado"].max()) * 1.42

        fig_prog = go.Figure()
        fig_prog.add_trace(go.Bar(
            name="Gastado", y=con_ppto["nombre"], x=con_ppto["monto_gastado"], orientation="h",
            marker_color=colores,
            text=etiquetas, textposition="outside", cliponaxis=False,
            customdata=con_ppto["categoria_id"],
            hovertemplate="<b>%{y}</b><br>gastado %{x:$,.0f}<extra></extra>",
        ))
        # El presupuesto va como MARCA DE OBJETIVO, no como barra de fondo: una barra detrás
        # queda tapada justo cuando importa (cuando el gasto se pasó del tope).
        fig_prog.add_trace(go.Scatter(
            name="Presupuesto", y=con_ppto["nombre"], x=con_ppto["monto_presupuesto"],
            mode="markers",
            marker=dict(symbol="line-ns", size=26,
                        line=dict(color=INK_MUTED, width=2)),
            hovertemplate="<b>%{y}</b><br>presupuesto %{x:$,.0f}<extra></extra>",
        ))
        apply_layout(fig_prog, height=max(300, len(con_ppto) * 46),
                     showlegend=True, margin=dict(l=0, r=150, t=44, b=6),
                     # con el default, el primer click se lo come el modo zoom y hay que
                     # hacer clic dos veces para seleccionar una categoría
                     clickmode="event+select",
                     legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                 xanchor="left", x=0))
        fig_prog.update_xaxes(range=[0, max_x], showgrid=True, gridcolor=GHOST,
                              tickprefix="$", tickformat="~s")
        fig_prog.update_yaxes(autorange="reversed", showgrid=False)

        event = st.plotly_chart(fig_prog, on_select="rerun", key="prog_chart",
                                width="stretch", config={"displayModeBar": False})
        if event.selection and event.selection.points:
            punto = event.selection.points[0]
            cd = punto.get("customdata")
            if isinstance(cd, (list, tuple)):
                cd = cd[0] if cd else None
            if cd is not None and not pd.isna(cd):
                sel_bar = int(cd)
            elif punto.get("y"):
                fila = con_ppto[con_ppto["nombre"] == punto.get("y")]
                if not fila.empty:
                    sel_bar = int(fila.iloc[0]["categoria_id"])
    else:
        st.info("Ninguna categoría tiene presupuesto asignado en este período. "
                "Asígnalos en la pestaña Presupuesto.")

    # ── Gasto sin presupuesto: contexto, no protagonista ──────────────────────
    # Va aparte y plegado a propósito: no es gestionable y parte puede volver como reembolso,
    # así que no debe competir con el bloque de arriba ni compartir su escala.
    sel_row = None
    if len(sin_ppto):
        with st.expander(
            f"Sin presupuesto · {money_md(total_sin_ppto)} en {len(sin_ppto)} categorías",
            expanded=False,
        ):
            st.caption("Gasto no gestionable: sin tope asignado, y parte puede volver como "
                       "reembolso. No entra en el medidor ni en el delta de arriba. "
                       "Selecciona una fila para ver sus movimientos.")
            # Texto y no número: `column_config` no tiene locale, así que un NumberColumn
            # mostraría $1,914,756 en una página donde todo lo demás dice $1.914.756. La lista
            # ya viene ordenada por monto, así que no se pierde nada.
            tabla_sin = pd.DataFrame({
                "nombre": sin_ppto["nombre"].tolist(),
                "gastado": [money(v) for v in sin_ppto["monto_gastado"]],
            })
            ev_sin = st.dataframe(
                tabla_sin,
                column_config={
                    "nombre": st.column_config.TextColumn("Categoría"),
                    "gastado": st.column_config.TextColumn("Gastado", width="small"),
                },
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="sin_ppto_tabla",
                width="stretch",
            )
            if ev_sin.selection and ev_sin.selection.rows:
                sel_row = int(sin_ppto.iloc[ev_sin.selection.rows[0]]["categoria_id"])

    # Dos fuentes de selección: gana la que cambió en este rerun.
    prev_bar = st.session_state.get("_sel_bar_prev")
    prev_row = st.session_state.get("_sel_row_prev")
    if sel_bar is not None and sel_bar != prev_bar:
        selected_id = sel_bar
    elif sel_row is not None and sel_row != prev_row:
        selected_id = sel_row
    else:
        selected_id = sel_bar if sel_bar is not None else sel_row
    st.session_state["_sel_bar_prev"] = sel_bar
    st.session_state["_sel_row_prev"] = sel_row

with col_table:
    selected_cat = None
    if selected_id is not None:
        fila = resumen_datos[resumen_datos["categoria_id"] == selected_id]
        selected_cat = str(fila.iloc[0]["nombre"]) if not fila.empty else None

    if selected_id is None or selected_cat is None:
        st.subheader("Movimientos")
        st.caption("← Haz clic en una categoría para ver sus movimientos")
    else:
        st.subheader(selected_cat)
        df_cat = df_periodo[df_periodo["categoria_id"] == selected_id].copy()

        if df_cat.empty:
            st.info("Sin movimientos para este período.")
        else:
            total_cuota = df_cat["monto_periodo"].sum()
            pendiente_cat = (df_cat[df_cat["pendiente"]]["monto_periodo"].sum()
                             if "pendiente" in df_cat.columns else 0)

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

            # Marcas: ✂ para partes de un split, ⏳ para movimientos aún no confirmados
            if "clasificacion_origen" in df_cat.columns:
                es_split = df_cat.loc[df_show.index, "clasificacion_origen"] == "split"
                df_show.loc[es_split, "comercio"] = (
                    "✂ " + df_show.loc[es_split, "comercio"].astype(str))
            if "pendiente" in df_cat.columns:
                pend = df_cat.loc[df_show.index, "pendiente"].fillna(False).astype(bool)
                df_show.loc[pend, "comercio"] = "⏳ " + df_show.loc[pend, "comercio"].astype(str)

            st.caption(f"{len(df_show)} movimientos")

            edited = st.data_editor(
                df_show,
                column_config={
                    "fecha_compra": st.column_config.TextColumn("Fecha", width="small"),
                    "comercio": st.column_config.TextColumn("Comercio"),
                    "num_cuotas": st.column_config.TextColumn("Cuotas", width="small"),
                    "monto_periodo": st.column_config.NumberColumn(
                        "Valor cuota", format="$%,.0f", width="small"),
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
                # tope de alto: una categoría con 30 movimientos estiraba la página entera;
                # pasado el tope la tabla scrollea sola
                height=min(38 + len(df_show) * 35, 560),
                width="stretch",
                key=f"drill_{selected_id}",
            )

            # El total va como pie y no como una fila más de la tabla: además de leerse mejor,
            # antes el guardado la salteaba con `iloc[:-1]`, que deja de apuntar a la fila
            # TOTAL en cuanto se ordena la tabla por otra columna.
            pie1, pie2 = st.columns(2)
            pie1.metric("Total categoría", money(total_cuota))
            if pendiente_cat:
                pie2.metric("⏳ Pendiente", money(pendiente_cat),
                            help="Movimientos aún no confirmados por el banco. Ya están "
                                 "sumados en el total.")

            if st.button("Guardar reclasificaciones", type="primary", key="save_drill"):
                cambios = 0
                for idx, row in edited.iterrows():
                    nueva_cat = row.get("categoria_nombre", "(sin cambio)")
                    if nueva_cat in ("(sin cambio)", df_show.loc[idx, "categoria_nombre"]):
                        continue
                    categoria_id = cat_id_map.get(nueva_cat)
                    if not categoria_id:
                        continue
                    cod_aut = row.get("codigo_autorizacion")
                    tx_hash = row.get("tx_hash")
                    if df_cat.loc[idx].get("clasificacion_origen") == "split":
                        delete_splits(conn, cod_aut, tx_hash)
                    comercio = str(row.get("comercio", "")).lstrip("✂⏳ ").strip() or None
                    clasificar(conn, cod_aut, tx_hash, categoria_id, comercio, origen="manual")
                    cambios += 1
                if cambios:
                    get_data.clear()
                    get_expanded.clear()
                    st.success(f"{cambios} movimientos reclasificados.")
                    st.rerun()
                else:
                    st.info("No hubo cambios.")

st.divider()

# ── Gasto por período ──────────────────────────────────────────────────────────
# El protagonista es el gasto gestionable (con presupuesto), en color y desde cero. El gasto sin
# presupuesto se apila arriba en gris: se ve el total del mes, pero queda claro cuál de los dos
# es el que se administra. El presupuesto es una marca de objetivo, siempre visible.
periodos_hist = sorted(df_cargos["periodo"].dropna().unique())[-7:]

if selected_id is not None and selected_cat:
    st.subheader(f"Gasto por período · {selected_cat}")
else:
    st.subheader("Gasto por período")

if not periodos_hist:
    st.info("Sin datos históricos todavía.")
else:
    ppto_hist = get_presupuestos_hist(conn, tuple(periodos_hist))

    etiquetas_x, gestionable, no_gestionable, objetivo = [], [], [], []
    for p in periodos_hist:
        dfp = df_cargos[df_cargos["periodo"] == p]
        presupuestos_p = ppto_hist.get(p, {})
        con = float(dfp[dfp["categoria_id"].isin(presupuestos_p.keys())]["monto_periodo"].sum())
        en_curso_p, _, _ = _avance(p)
        etiquetas_x.append(periodo_corto(p) + (" · en curso" if en_curso_p else ""))
        gestionable.append(con)
        no_gestionable.append(float(dfp["monto_periodo"].sum()) - con)
        objetivo.append(sum(presupuestos_p.values()))

    ultimo_en_curso, ult_elapsed, ult_total = _avance(periodos_hist[-1])
    patrones = ["" for _ in periodos_hist]
    if ultimo_en_curso:
        patrones[-1] = "/"

    fig_hist = go.Figure()
    if selected_id is not None and selected_cat:
        # Escala propia para la categoría: contra el total del mes su barra sería una astilla y
        # no se vería su tendencia. El contexto del mes completo ya está en las tarjetas de
        # arriba; aquí lo que interesa es la categoría y su propio tope.
        serie = (df_cargos[(df_cargos["periodo"].isin(periodos_hist)) &
                           (df_cargos["categoria_id"] == selected_id)]
                 .groupby("periodo")["monto_periodo"].sum())
        vals = [float(serie.get(p, 0)) for p in periodos_hist]
        topes = [ppto_hist.get(p, {}).get(selected_id) for p in periodos_hist]
        colores_cat = [status_of((v / t * 100) if t else None)[1] for v, t in zip(vals, topes)]

        fig_hist.add_trace(go.Bar(
            name=str(selected_cat), x=etiquetas_x, y=vals,
            marker=dict(color=colores_cat,
                        pattern=dict(shape=patrones, solidity=0.45, size=5)),
            text=[money(v) for v in vals], textposition="outside", cliponaxis=False,
            hovertemplate="<b>%{x}</b><br>%{y:$,.0f}<extra></extra>",
        ))
        if any(t for t in topes):
            fig_hist.add_trace(go.Scatter(
                name="Su presupuesto", x=etiquetas_x,
                y=[t if t else None for t in topes], mode="markers",
                marker=dict(symbol="line-ew", size=40, line=dict(color=INK_MUTED, width=2)),
                hovertemplate="<b>%{x}</b><br>presupuesto %{y:$,.0f}<extra></extra>",
            ))
        apply_layout(fig_hist, height=360, showlegend=True,
                     margin=dict(l=0, r=0, t=44, b=0),
                     legend=dict(orientation="h", yanchor="bottom", y=1.04,
                                 xanchor="left", x=0))
    else:
        fig_hist.add_trace(go.Bar(
            name="Con presupuesto", x=etiquetas_x, y=gestionable,
            marker=dict(color=CATEGORICAL_LIGHT[0],
                        pattern=dict(shape=patrones, solidity=0.45, size=5)),
            text=[money(v) for v in gestionable], textposition="inside",
            insidetextanchor="middle", textfont=dict(color="#ffffff", size=11),
            hovertemplate="<b>%{x}</b><br>con presupuesto %{y:$,.0f}<extra></extra>",
        ))
        fig_hist.add_trace(go.Bar(
            name="Sin presupuesto", x=etiquetas_x, y=no_gestionable,
            marker=dict(color=GHOST_STRONG,
                        pattern=dict(shape=patrones, solidity=0.45, size=5)),
            hovertemplate="<b>%{x}</b><br>sin presupuesto %{y:$,.0f}<extra></extra>",
        ))
        fig_hist.add_trace(go.Scatter(
            name="Presupuesto", x=etiquetas_x, y=objetivo, mode="markers",
            marker=dict(symbol="line-ew", size=40, line=dict(color=INK_MUTED, width=2)),
            hovertemplate="<b>%{x}</b><br>presupuesto %{y:$,.0f}<extra></extra>",
        ))
        apply_layout(fig_hist, height=380, barmode="stack", showlegend=True,
                     margin=dict(l=0, r=0, t=44, b=0),
                     legend=dict(orientation="h", yanchor="bottom", y=1.04,
                                 xanchor="left", x=0))

    fig_hist.update_yaxes(tickprefix="$", tickformat="~s", rangemode="tozero")
    st.plotly_chart(fig_hist, width="stretch", config={"displayModeBar": False})

    if selected_id is not None and selected_cat:
        st.caption(f"Gasto de {selected_cat} mes a mes, en su propia escala, con el color del "
                   "estado contra su tope. La marca horizontal es su presupuesto del período. "
                   "Deselecciona la categoría arriba para volver a la vista general.")
    else:
        st.caption("En color, el gasto de las categorías con presupuesto — el gestionable. "
                   "En gris, el resto. La marca horizontal es el presupuesto total del período.")
