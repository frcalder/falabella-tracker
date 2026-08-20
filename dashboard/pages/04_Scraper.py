"""Página de control del scraper: historial de ejecuciones y disparo vía GitHub Actions."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import requests
import streamlit as st

from analytics.db import get_connection, init_db

GITHUB_REPO     = "frcalder/gastos-falabella"
GITHUB_WORKFLOW = "scraper.yml"


@st.cache_resource
def get_db():
    conn = get_connection()
    init_db(conn)
    return conn


def load_runs(conn) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, started_at, finished_at, status, headless,
                   paginas, procesados, nuevos, actualizados, pendientes,
                   periodo, backfill_periodo, error_message
            FROM scraper_runs
            ORDER BY started_at DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def trigger_github_action(token: str, inputs: dict | None = None) -> tuple[bool, str]:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW}/dispatches"
    payload: dict = {"ref": "main"}
    if inputs:
        # La API de dispatch espera los inputs como strings.
        payload["inputs"] = {k: str(v) for k, v in inputs.items() if v not in ("", None)}
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        json=payload,
        timeout=10,
    )
    if resp.status_code == 204:
        return True, "Scraper iniciado en GitHub Actions."
    return False, f"Error {resp.status_code}: {resp.text}"


conn = get_db()
st.title("Scraper")

# ── Trigger ───────────────────────────────────────────────────────────────────
st.subheader("Ejecutar scraper")

github_token = st.secrets.get("GITHUB_TOKEN", "") if hasattr(st, "secrets") else ""
if not github_token:
    github_token = st.text_input(
        "GitHub Personal Access Token",
        type="password",
        help="Necesita permiso `actions:write`. Solo se usa para disparar el workflow.",
    )

with st.expander("Opciones avanzadas — backfill de un período cerrado"):
    st.caption(
        "El scraper completa solo el período recién cerrado desde la pestaña *Movimientos "
        "facturados*, la primera vez que el banco publica el detalle del estado de cuenta. "
        "Estas opciones sirven para forzar un período puntual (por ejemplo para recuperar un "
        "mes viejo). El banco ofrece los últimos 12 estados de cuenta."
    )
    backfill_periodo = st.text_input("Período a completar (YYYY-MM)", value="",
                                    placeholder="2026-07")
    backfill_dry_run = st.checkbox("Dry-run: solo listar lo que falta, sin escribir en la DB")

if st.button("▶ Ejecutar ahora", type="primary", disabled=not github_token):
    inputs = {
        "backfill_periodo": backfill_periodo.strip(),
        "backfill_dry_run": "true" if backfill_dry_run else "",
    }
    with st.spinner("Disparando workflow..."):
        ok, msg = trigger_github_action(github_token, inputs)
    if ok:
        st.success(msg)
        st.caption("El scraper tardará ~5 min. Recarga la página para ver el nuevo run.")
    else:
        st.error(msg)

st.divider()

# ── Historial ─────────────────────────────────────────────────────────────────
st.subheader("Historial de ejecuciones")

col_refresh, _ = st.columns([1, 5])
with col_refresh:
    if st.button("Actualizar"):
        st.rerun()

df = load_runs(conn)

if df.empty:
    st.info("Sin ejecuciones registradas todavía.")
else:
    # Convertir UTC → América/Santiago y formatear
    tz = "America/Santiago"
    started  = pd.to_datetime(df["started_at"],  utc=True).dt.tz_convert(tz)
    finished = pd.to_datetime(df["finished_at"], utc=True).dt.tz_convert(tz)

    df["duracion"] = (finished - started).apply(
        lambda x: f"{int(x.total_seconds()//60)}m {int(x.total_seconds()%60)}s" if pd.notna(x) else "—"
    )
    df["started_at"]  = started.dt.strftime("%d/%m/%Y %H:%M")
    df["finished_at"] = finished.dt.strftime("%d/%m/%Y %H:%M").fillna("—")

    df["status"] = df["status"].map({
        "success": "✅ ok",
        "error":   "❌ error",
        "running": "⏳ en curso",
    }).fillna(df["status"])

    st.dataframe(
        df[[
            "started_at", "finished_at", "duracion", "status",
            "periodo", "backfill_periodo", "paginas", "nuevos", "actualizados", "pendientes",
            "error_message",
        ]].rename(columns={
            "started_at":   "Inicio",
            "finished_at":  "Fin",
            "duracion":     "Duración",
            "status":       "Estado",
            "periodo":      "Período",
            "backfill_periodo": "Backfill",
            "paginas":      "Págs",
            "nuevos":       "Nuevos",
            "actualizados": "Actualizados",
            "pendientes":   "Pendientes",
            "error_message":"Error",
        }),
        hide_index=True,
        width="stretch",
        column_config={
            "Error": st.column_config.TextColumn(width="medium"),
        },
    )
