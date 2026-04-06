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
    ("fix_arena_running", False),
    ("fix_arena_log",     []),
    ("fix_arena_done",    False),
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
            bga_pid_str = str(pid)
            # Zoek since uit import_tracking
            track_row = conn.execute("""
                SELECT last_ended_at FROM import_tracking
                WHERE bga_player_id = ? AND boardgame_id = 1
            """, [bga_pid_str]).fetchone()
            since = track_row[0] if track_row and track_row[0] else None

            if since:
                log(f"👤 Speler {pid} ophalen (sinds {since.date()}) ...")
            else:
                log(f"👤 Speler {pid} ophalen (volledige historie) ...")
            try:
                games = fetch_player_games(pid, token, cookies, since=since)
                log(f"   📊 {len(games)} spellen gevonden")
                new = sum(1 for g in games if import_game(conn, g))
                total_new += new
                log(f"   ✅ {new} nieuw geïmporteerd")

                # Update import_tracking
                max_ended = conn.execute("""
                    SELECT MAX(COALESCE(g.ended_at, g.played_at))
                    FROM games g
                    JOIN game_players gp ON gp.game_id = g.id
                    JOIN players p ON p.id = gp.player_id
                    WHERE p.bga_player_id = ?
                """, [bga_pid_str]).fetchone()
                new_last = max_ended[0] if max_ended and max_ended[0] else None
                if new_last:
                    conn.execute("""
                        INSERT INTO import_tracking (bga_player_id, boardgame_id, last_ended_at, imported_at)
                        VALUES (?, 1, ?, current_timestamp)
                        ON CONFLICT (bga_player_id, boardgame_id)
                        DO UPDATE SET last_ended_at = EXCLUDED.last_ended_at,
                                      imported_at = current_timestamp
                    """, [bga_pid_str, new_last])
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


# ── Arena correctie per speler ────────────────────────────────────────────────

def run_fix_arena(email, password, bga_player_id, player_name):
    """Corrigeer arena scores voor één speler."""

    def log(msg):
        st.session_state["fix_arena_log"].append(msg)

    def run_async(coro):
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    try:
        log(f"🔑 Inloggen op BGA ...")
        token, cookies = run_async(
            get_token_and_cookies(email, password, bga_player_id, headless=True)
        )
        log(f"✅ Token: {token[:8]}... verkregen")

        log(f"👤 Games ophalen voor {player_name} ({bga_player_id}) ...")
        games = fetch_player_games(bga_player_id, token, cookies)
        log(f"   📊 {len(games)} games opgehaald")

        conn = duckdb.connect(DB_FILE)
        updated = 0
        for game in games:
            table_id = str(game.get("table_id", ""))
            arena_after_raw = game.get("arena_after")
            arena_win_raw = game.get("arena_win")

            if not table_id or arena_after_raw is None:
                continue

            raw_str = str(arena_after_raw)
            arena_elo = None
            if "." in raw_str:
                try:
                    arena_elo = int(raw_str.split(".")[1])
                except (ValueError, IndexError):
                    pass
            else:
                try:
                    arena_elo = int(raw_str)
                except (ValueError, TypeError):
                    pass

            arena_win = None
            if arena_win_raw is not None:
                win_str = str(arena_win_raw)
                if "." in win_str:
                    try:
                        arena_win = int(win_str.split(".")[0]) > 0
                    except (ValueError, IndexError):
                        pass
                elif win_str.isdigit():
                    arena_win = bool(int(win_str))

            if arena_elo is not None:
                result = conn.execute("""
                    UPDATE game_players
                    SET arena_after = ?, arena_win = ?
                    WHERE game_id = (SELECT id FROM games WHERE bga_table_id = ?)
                      AND player_id = (SELECT id FROM players WHERE bga_player_id = ?)
                """, [arena_elo, arena_win, table_id, str(bga_player_id)])
                if result.fetchone()[0] > 0:
                    updated += 1

        conn.close()
        log(f"🏁 Klaar — {updated} arena scores bijgewerkt voor {player_name}")

    except Exception as e:
        log(f"❌ Fout: {e}")
    finally:
        st.session_state["fix_arena_running"] = False
        st.session_state["fix_arena_done"] = True


# ── UI ────────────────────────────────────────────────────────────────────────

with st.expander("⚙️ BGA Inloggegevens", expanded=not st.session_state["import_running"]):
    col1, col2 = st.columns(2)
    with col1:
        email = st.text_input("BGA Email", value=st.session_state.get("bga_email", ""), key="input_email")
    with col2:
        password = st.text_input("BGA Wachtwoord", type="password", value=st.session_state.get("bga_password", ""), key="input_password")

# ── Tabs ─────────────────────────────────────────────────────────────────────

tab_import, tab_fix_arena = st.tabs(["📥 Import", "🎯 Arena correctie"])

with tab_import:
    st.subheader("Spelers importeren")

    # Laad geïmporteerde spelers (= spelers met minstens één elo_after waarde)
    try:
        conn_imp = duckdb.connect(DB_FILE, read_only=True)
        imported_players = conn_imp.execute("""
            SELECT p.bga_player_id, p.name,
                   COUNT(gp.id) AS games,
                   it.last_ended_at,
                   it.imported_at
            FROM players p
            JOIN game_players gp ON gp.player_id = p.id
            LEFT JOIN import_tracking it
                ON it.bga_player_id = p.bga_player_id AND it.boardgame_id = 1
            WHERE p.bga_player_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM game_players gp2
                  WHERE gp2.player_id = p.id AND gp2.elo_after IS NOT NULL
              )
            GROUP BY p.bga_player_id, p.name, it.last_ended_at, it.imported_at
            ORDER BY p.name
        """).df()
        conn_imp.close()
    except Exception:
        imported_players = None

    if imported_players is not None and not imported_players.empty:
        display_df = imported_players[["name", "bga_player_id", "games", "last_ended_at", "imported_at"]].rename(columns={
            "name":           "Naam",
            "bga_player_id":  "BGA ID",
            "games":          "Spellen",
            "last_ended_at":  "Laatste spel",
            "imported_at":    "Laatste import",
        })

        event = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row",
            key="import_players_table",
        )

        selected_rows = event.selection.rows if event.selection else []
        selected_pids = [int(imported_players.iloc[r]["bga_player_id"]) for r in selected_rows]

        st.caption(f"{len(selected_pids)} speler(s) geselecteerd")

        # Nieuw speler-ID toevoegen
        new_pid = st.text_input("Nieuw BGA Player ID toevoegen", placeholder="bv. 93464744")

        if not st.session_state["import_running"]:
            if st.button("▶ Start import", type="primary"):
                player_ids = list(selected_pids)
                if new_pid and new_pid.strip().isdigit():
                    player_ids.append(int(new_pid.strip()))
                if not player_ids:
                    st.error("Selecteer minstens één speler of voeg een nieuw ID toe.")
                elif not email or not password:
                    st.error("Vul email en wachtwoord in.")
                else:
                    st.session_state["bga_email"]       = email
                    st.session_state["bga_password"]    = password
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
    else:
        # Geen bestaande spelers — toon invoerveld
        new_pid = st.text_input("BGA Player ID", placeholder="bv. 93464744")

        if not st.session_state["import_running"]:
            if st.button("▶ Start import", type="primary"):
                if not new_pid or not new_pid.strip().isdigit():
                    st.error("Voer een geldig BGA Player ID in.")
                elif not email or not password:
                    st.error("Vul email en wachtwoord in.")
                else:
                    st.session_state["bga_email"]       = email
                    st.session_state["bga_password"]    = password
                    st.session_state["import_running"]  = True
                    st.session_state["import_done"]     = False
                    st.session_state["import_log"]      = []
                    st.session_state["import_total"]    = 0

                    t = threading.Thread(
                        target=run_import,
                        args=(email, password, [int(new_pid.strip())]),
                        daemon=True,
                    )
                    t.start()
                    st.rerun()
        else:
            st.info("⏳ Import bezig — pagina ververst automatisch ...")
            st.button("↻ Ververs status", on_click=lambda: None)

    if st.session_state["import_log"]:
        st.code("\n".join(st.session_state["import_log"][-40:]), language=None)

    if st.session_state["import_done"] and not st.session_state["import_running"]:
        st.success(f"✅ Import voltooid — {st.session_state['import_total']} nieuwe spellen toegevoegd")

# ── Tab 2: Arena correctie per speler ────────────────────────────────────────

with tab_fix_arena:
    st.subheader("Arena scores corrigeren per speler")
    st.caption("Selecteer een speler om de arena scores opnieuw op te halen van BGA.")

    try:
        conn_players = duckdb.connect(DB_FILE, read_only=True)
        arena_players = conn_players.execute("""
            SELECT p.id, p.name, p.bga_player_id,
                   COUNT(gp.id) AS games,
                   SUM(CASE WHEN gp.arena_after IS NOT NULL THEN 1 ELSE 0 END) AS with_arena,
                   SUM(CASE WHEN gp.arena_after IS NULL THEN 1 ELSE 0 END) AS without_arena
            FROM players p
            JOIN game_players gp ON gp.player_id = p.id
            WHERE p.bga_player_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM game_players gp2
                  WHERE gp2.player_id = p.id AND gp2.elo_after IS NOT NULL
              )
            GROUP BY p.id, p.name, p.bga_player_id
            ORDER BY p.name
        """).fetchall()
        conn_players.close()
    except duckdb.ConnectionException:
        arena_players = []

    if arena_players:
        player_options = {
            f"{name} ({games} games, {with_arena} arena, {without_arena} zonder)": (bga_pid, name)
            for _id, name, bga_pid, games, with_arena, without_arena in arena_players
        }
        selected_player_label = st.selectbox("Speler", list(player_options.keys()))
        selected_bga_pid, selected_name = player_options[selected_player_label]

        if not st.session_state["fix_arena_running"]:
            if st.button("▶ Corrigeer arena", type="primary"):
                if not email or not password:
                    st.error("Vul eerst email en wachtwoord in (bovenaan).")
                else:
                    st.session_state["bga_email"] = email
                    st.session_state["bga_password"] = password
                    st.session_state["fix_arena_running"] = True
                    st.session_state["fix_arena_done"] = False
                    st.session_state["fix_arena_log"] = []

                    t = threading.Thread(
                        target=run_fix_arena,
                        args=(email, password, int(selected_bga_pid), selected_name),
                        daemon=True,
                    )
                    t.start()
                    st.rerun()
        else:
            st.info("⏳ Arena correctie bezig ...")
            st.button("↻ Ververs status", on_click=lambda: None, key="fix_arena_refresh")

        if st.session_state["fix_arena_log"]:
            st.code("\n".join(st.session_state["fix_arena_log"][-40:]), language=None)

        if st.session_state["fix_arena_done"] and not st.session_state["fix_arena_running"]:
            st.success("✅ Arena correctie voltooid")
    else:
        st.info("Geen spelers gevonden in de database.")

