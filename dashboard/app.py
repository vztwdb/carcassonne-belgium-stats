import os

import streamlit as st

# ── Page definitions ─────────────────────────────────────────────────────────

public_pages = [
    st.Page("pages/2_players.py", title="Spelers", icon="👤"),
    st.Page("pages/3_player_detail.py", title="Speler detail", icon="📊"),
    st.Page("pages/4_head_to_head.py", title="Head to Head", icon="⚔️"),
]

admin_pages = []
if os.environ.get("CARCASSONNE_ADMIN"):
    admin_pages = [
        st.Page("pages/1_import.py", title="Import", icon="📥"),
        st.Page("pages/9_database.py", title="Database", icon="🗄️"),
    ]

nav = {"": public_pages}
if admin_pages:
    nav["Admin"] = admin_pages

pg = st.navigation(nav)

# ── Global config ────────────────────────────────────────────────────────────

st.set_page_config(page_title="Carcassonne Belgium Stats", page_icon="🏰", layout="wide")

pg.run()
