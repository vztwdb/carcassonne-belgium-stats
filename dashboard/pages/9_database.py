"""Database viewer pagina."""

import os
import sys
from pathlib import Path

import streamlit as st

if not os.environ.get("CARCASSONNE_ADMIN"):
    st.error("Deze pagina is niet beschikbaar.")
    st.stop()

import duckdb

sys.path.insert(0, str(Path(__file__).parents[2]))

DB_PATH = Path(__file__).parents[2] / "data" / "carcassonne.duckdb"

st.set_page_config(page_title="Database", page_icon="🗄️", layout="wide")
st.title("🗄️ Database Viewer")

if not DB_PATH.exists():
    st.warning("Geen database gevonden. Importeer eerst data via de Import pagina.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Tabel overzicht ───────────────────────────────────────────────────────────

tables = conn.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchall()
table_names = [t[0] for t in tables]

col1, col2 = st.columns([1, 3])

with col1:
    st.subheader("Tabellen")
    for name in table_names:
        count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        st.metric(name, f"{count:,} rijen")

with col2:
    st.subheader("Tabel inhoud")
    selected = st.selectbox("Selecteer tabel", table_names)
    if selected:
        limit = st.slider("Aantal rijen", 10, 500, 50)
        df = conn.execute(f"SELECT * FROM {selected} LIMIT {limit}").df()
        st.dataframe(df, width="stretch")

# ── Vrije query ───────────────────────────────────────────────────────────────

st.divider()
st.subheader("🔍 Vrije SQL query")

default_query = """SELECT p.name, COUNT(*) as spellen, ROUND(AVG(gp.score), 1) as gem_score
FROM game_players gp
JOIN players p ON p.id = gp.player_id
GROUP BY p.name
ORDER BY spellen DESC"""

query = st.text_area("SQL", value=default_query, height=120)

if st.button("▶ Uitvoeren", type="primary"):
    try:
        result = conn.execute(query).df()
        st.dataframe(result, width="stretch")
        st.caption(f"{len(result)} rijen")
    except Exception as e:
        st.error(str(e))

conn.close()
