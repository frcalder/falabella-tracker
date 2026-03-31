import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

st.set_page_config(page_title="Gastos Falabella", page_icon="💳", layout="wide")

pg = st.navigation([
    st.Page("pages/01_Clasificacion.py", title="Clasificación", icon="🏷️"),
    st.Page("pages/02_Presupuesto.py", title="Presupuesto", icon="📋"),
    st.Page("pages/03_Analisis.py", title="Análisis", icon="📊"),
    st.Page("pages/04_Scraper.py", title="Scraper", icon="🤖"),
])
pg.run()
