"""
BGA Data Import pagina.
Haalt Carcassonne speldata op van BoardGameArena en importeert in DuckDB.
De volledige import draait in een achtergrondthread zodat Streamlit reruns
de import niet onderbreken.
"""

import asyncio
import os
import sys
import threading
from pathlib import Path

import streamlit as st

if not os.environ.get("CARCASSONNE_ADMIN"):
    st.error("Deze pagina is niet beschikbaar.")
    st.stop()

import duckdb

sys.path.insert(0, str(Path(__file__).parents[2]))

from src.importers.bga_fetcher import get_token_and_cookies, fetch_player_games
from src.importers.bga_importer import import_game, DB_PATH

ROOT    = Path(__file__).parents[2]
DB_FILE = str(ROOT / DB_PATH)

st.title("📥 BGA Data Importeren")
st.caption("Haal speldata op van BoardGameArena en importeer in de database")


# ── Initialiseer session state ────────────────────────────────────────────────

for key, default in [
    ("import_running", False),
    ("import_log",     []),
    ("import_done",    False),
    ("import_total",   0),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── Achtergrond import functie ────────────────────────────────────────────────

def run_import(email, password, player_ids):
    """Draait volledig in een achtergrondthread — niet onderbroken door reruns."""

    def log(msg):
        st.session_state["import_log"].append(msg)

    # Helpers voor async in thread
    def run_async(coro):
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    try:
        # Stap 1: token ophalen
        log("🔑 Inloggen op BGA ...")
        token, cookies = run_async(
            get_token_and_cookies(email, password, player_ids[0], headless=True)
        )
        log(f"✅ Token: {token[:8]}... verkregen")

        # Stap 2: database initialiseren
        conn = duckdb.connect(DB_FILE)
        for mig in ["migrations/001_initial_schema.sql", "migrations/002_bga_fields.sql", "migrations/003_game_extra_fields.sql"]:
            sql = (ROOT / mig).read_text(encoding="utf-8")
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    try:
                        conn.execute(stmt)
                    except Exception:
                        pass

        # Stap 3: per speler ophalen en importeren
        total_new = 0
        for pid in player_ids:
            log(f"👤 Speler {pid} ophalen ...")
            try:
                games = fetch_player_games(pid, token, cookies)
                log(f"   📊 {len(games)} spellen gevonden")
                new = sum(1 for g in games if import_game(conn, g))
                total_new += new
                log(f"   ✅ {new} nieuw geïmporteerd")
            except Exception as e:
                log(f"   ❌ Fout: {e}")

        conn.close()
        st.session_state["import_total"] = total_new
        log(f"🏁 Klaar — {total_new} nieuwe spellen toegevoegd")

    except Exception as e:
        log(f"❌ Fout: {e}")
    finally:
        st.session_state["import_running"] = False
        st.session_state["import_done"]    = True


# ── UI ────────────────────────────────────────────────────────────────────────

with st.expander("⚙️ BGA Inloggegevens", expanded=not st.session_state["import_running"]):
    col1, col2 = st.columns(2)
    with col1:
        email = st.text_input("BGA Email", value=st.session_state.get("bga_email", ""), key="input_email")
    with col2:
        password = st.text_input("BGA Wachtwoord", type="password", value=st.session_state.get("bga_password", ""), key="input_password")

st.subheader("Spelers")
st.caption("Vind een speler-ID via boardgamearena.com/player?id=XXXXXXXX")
ids_text = st.text_area(
    "BGA Player IDs (één per lijn)",
    value=st.session_state.get("player_ids_text", "93464744\n84635111\n65246746"),
    height=120,
    disabled=st.session_state["import_running"],
)

st.divider()

if not st.session_state["import_running"]:
    if st.button("▶ Start import", type="primary"):
        player_ids = [int(x.strip()) for x in ids_text.splitlines() if x.strip().isdigit()]
        if not player_ids:
            st.error("Geen geldige player IDs.")
        elif not email or not password:
            st.error("Vul email en wachtwoord in.")
        else:
            st.session_state["bga_email"]       = email
            st.session_state["bga_password"]    = password
            st.session_state["player_ids_text"] = ids_text
            st.session_state["import_running"]  = True
            st.session_state["import_done"]     = False
            st.session_state["import_log"]      = []
            st.session_state["import_total"]    = 0

            t = threading.Thread(
                target=run_import,
                args=(email, password, player_ids),
                daemon=True,
            )
            t.start()
            st.rerun()
else:
    st.info("⏳ Import bezig — pagina ververst automatisch ...")
    st.button("↻ Ververs status", on_click=lambda: None)

# Log weergeven
if st.session_state["import_log"]:
    st.code("\n".join(st.session_state["import_log"][-40:]), language=None)

# Resultaat
if st.session_state["import_done"] and not st.session_state["import_running"]:
    st.success(f"✅ Import voltooid — {st.session_state['import_total']} nieuwe spellen toegevoegd")

    conn2 = duckdb.connect(DB_FILE, read_only=True)
    try:
        df = conn2.execute("""
            SELECT p.name, COUNT(*) as spellen, ROUND(AVG(gp.score),1) as gem_score
            FROM game_players gp
            JOIN players p ON p.id = gp.player_id
            GROUP BY p.name ORDER BY spellen DESC
        """).df()
        st.dataframe(df, width="stretch")
    except Exception:
        pass
    conn2.close()
