"""Paleta, formato de moneda y layout de gráficos compartidos por el dashboard.

**La paleta está validada, no elegida a ojo.** Los 8 slots categóricos pasan los seis chequeos
—banda de luminosidad, piso de croma, separación bajo daltonismo, piso de visión normal y
contraste— en modo claro y oscuro. Dos consecuencias que hay que respetar al usarla:

1. **El orden de los slots ES el mecanismo de seguridad**, no es cosmético: la garantía de que dos
   series vecinas se distinguen bajo daltonismo depende de la secuencia. Reordenarlos la invalida.
   Asignar siempre en orden y por entidad (una categoría se queda con su color), nunca por ranking:
   si el color siguiera al ranking, filtrar una serie repintaría a las demás.
2. **Regla de relieve**: en modo claro, `aqua`, `amarillo` y `magenta` quedan bajo 3:1 de contraste
   contra la superficie. Todo gráfico que los use tiene que llevar etiquetas visibles o vista de
   tabla — el color no puede ser la única señal.

Nunca agregar un 9º color: bajo daltonismo es indistinguible de uno de los 8. Por eso el
dashboard **no codifica la identidad de la categoría con color**: hay 19 categorías y 8 slots, así
que el color se reserva para lo que sí tiene pocos valores y mucho significado — el estado contra
el presupuesto — y la identidad la cargan la posición, el nombre y el gráfico de una serie a la
vez. Es también lo que evita el problema que había antes, con nueve categorías compartiendo el
mismo gris.

Comando para re-validar si se cambia algún hex (la guía de visualización trae el script):
    node scripts/validate_palette.js "<hex,hex,…>" --mode light
"""
from typing import Optional

import plotly.graph_objects as go

# ── Paleta categórica: 8 slots, en orden ──────────────────────────────────────
CATEGORICAL_LIGHT = [
    "#2a78d6",  # 1 azul
    "#eb6834",  # 2 naranja
    "#1baf7a",  # 3 aqua
    "#eda100",  # 4 amarillo
    "#e87ba4",  # 5 magenta
    "#008300",  # 6 verde
    "#4a3aa7",  # 7 violeta
    "#e34948",  # 8 rojo
]
CATEGORICAL_DARK = [
    "#3987e5", "#d95926", "#199e70", "#c98500",
    "#d55181", "#008300", "#9085e9", "#e66767",
]

# ── Estado: fija, nunca temática, y jamás reusada como color de serie ─────────
# Un color de estado no carga significado solo: va siempre con ícono + texto.
STATUS = {
    "good":     ("#0ca30c", "✓"),
    "warning":  ("#fab219", "⚠"),
    "critical": ("#d03b3b", "⚑"),
}

# ── Chrome ────────────────────────────────────────────────────────────────────
# Grises con alpha para que funcionen sobre superficie clara y oscura por igual.
GHOST = "rgba(137,135,129,0.22)"   # barra fantasma (presupuesto / total de contexto)
GHOST_STRONG = "rgba(137,135,129,0.38)"
GRID = "rgba(137,135,129,0.25)"    # grilla de una línea, recesiva
INK_MUTED = "#898781"


def money(x: Optional[float], dash: str = "—") -> str:
    """Monto en formato chileno: $1.234.567. Devuelve `dash` si no hay valor."""
    if x is None:
        return dash
    try:
        v = float(x)
    except (TypeError, ValueError):
        return dash
    if v != v:  # NaN
        return dash
    signo = "-" if v < 0 else ""
    # Se formatea con el separador inglés y después se da vuelta: es la forma corta de
    # no depender de que el locale es_CL esté instalado en el contenedor.
    return f"{signo}${abs(v):,.0f}".replace(",", ".")


def money_md(x: Optional[float], dash: str = "—") -> str:
    """Igual que `money`, pero escapado para markdown.

    Streamlit interpreta el texto de `st.caption`/`st.markdown` como markdown, y ahí un par de
    `$` delimita una fórmula LaTeX: dos montos en la misma frase hacen que todo lo que está en
    medio se renderice como matemática. En metric() no hace falta, ahí el valor es texto plano.
    """
    return money(x, dash).replace("$", r"\$")


def money_short(x: Optional[float], dash: str = "—") -> str:
    """Monto abreviado para ejes y chips: $4,8M · $980k."""
    if x is None:
        return dash
    try:
        v = float(x)
    except (TypeError, ValueError):
        return dash
    if v != v:
        return dash
    signo = "-" if v < 0 else ""
    a = abs(v)
    if a >= 1_000_000:
        return f"{signo}${a / 1_000_000:.1f}M".replace(".", ",")
    if a >= 1_000:
        return f"{signo}${a / 1_000:.0f}k"
    return f"{signo}${a:.0f}"


def status_of(pct: Optional[float]) -> tuple:
    """(clave, color, ícono) según el % de presupuesto consumido.

    Tolera NaN: `get_resumen_vs_presupuesto` devuelve `None` desde un `apply`, y pandas lo
    convierte a NaN en una Series float64 — así que `pct is not None` es True para esos casos y
    no sirve como guarda.
    """
    if pct is None or pct != pct:
        return ("none", INK_MUTED, "")
    if pct > 100:
        return ("critical", STATUS["critical"][0], STATUS["critical"][1])
    if pct > 80:
        return ("warning", STATUS["warning"][0], STATUS["warning"][1])
    return ("good", STATUS["good"][0], STATUS["good"][1])


def apply_layout(fig: go.Figure, height: Optional[int] = None, **kwargs) -> go.Figure:
    """Layout común de todos los gráficos del dashboard.

    Deliberadamente **no** fija colores de fuente ni de fondo: `st.plotly_chart` aplica por
    defecto el template de Streamlit, que ya los deriva del tema activo. Fijarlos aquí rompería
    el modo oscuro. Aquí va solo lo que ese template no cubre.
    """
    layout = dict(
        # separators: primer carácter = decimal, segundo = miles. ",." da 1.234.567 (formato CL),
        # así que los hovertemplate con %{y:$,.0f} salen bien formateados sin post-proceso.
        separators=",.",
        margin=dict(l=0, r=0, t=10, b=0),
        hoverlabel=dict(font_size=13),
        showlegend=False,
        bargap=0.28,
    )
    layout.update(kwargs)  # los kwargs pisan los defaults, no se duplican
    fig.update_layout(**layout)
    if height is not None:
        fig.update_layout(height=height)
    fig.update_xaxes(showgrid=False, zeroline=False, ticks="outside", ticklen=4,
                     tickcolor="rgba(0,0,0,0)")
    fig.update_yaxes(showgrid=True, gridcolor=GRID, gridwidth=1, zeroline=False)
    return fig


def periodo_corto(periodo: str) -> str:
    """'2026-07' → 'jul 26'. Etiqueta de eje, más corta que la fecha de cierre completa."""
    MESES = ["ene", "feb", "mar", "abr", "may", "jun",
             "jul", "ago", "sep", "oct", "nov", "dic"]
    try:
        y, m = int(periodo[:4]), int(periodo[5:7])
        return f"{MESES[m - 1]} {str(y)[2:]}"
    except (ValueError, IndexError, TypeError):
        return str(periodo)
