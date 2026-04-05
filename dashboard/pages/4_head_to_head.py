"""Head-to-head overzicht — alle spellen tussen twee spelers."""
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[2]))

DB_PATH = Path(__file__).parents[2] / "data" / "carcassonne.duckdb"
ELO_BASE = 1300


# ── Read params ──────────────────────────────────────────────────────────────

if "h2h_player" not in st.session_state or "h2h_opponent" not in st.session_state:
    st.warning("Selecteer eerst een tegenstander via een speler detail pagina.")
    if st.button("← Terug naar spelers"):
        st.switch_page("pages/2_players.py")
    st.stop()

player_id = st.session_state["h2h_player"]
opponent_id = st.session_state["h2h_opponent"]
boardgame_id = st.session_state.get("h2h_bg", 1)

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Player names ─────────────────────────────────────────────────────────────

p1 = conn.execute("SELECT name FROM players WHERE id = ?", [player_id]).fetchone()
p2 = conn.execute("SELECT name FROM players WHERE id = ?", [opponent_id]).fetchone()

if not p1 or not p2:
    st.error("Speler(s) niet gevonden.")
    conn.close()
    st.stop()

p1_name = p1[0]
p2_name = p2[0]

st.title(f"⚔️ {p1_name} vs {p2_name}")

col_back1, col_back2 = st.columns([1, 1])
with col_back1:
    if st.button(f"← Terug naar {p1_name}"):
        st.session_state["player_detail_id"] = player_id
        st.session_state["player_detail_bg"] = boardgame_id
        st.switch_page("pages/3_player_detail.py")
with col_back2:
    if st.button(f"← Terug naar {p2_name}"):
        st.session_state["player_detail_id"] = opponent_id
        st.session_state["player_detail_bg"] = boardgame_id
        st.switch_page("pages/3_player_detail.py")

# ── Summary ──────────────────────────────────────────────────────────────────

summary = conn.execute("""
    WITH common_games AS (
        SELECT g.id
        FROM games g
        JOIN game_players gp1 ON gp1.game_id = g.id AND gp1.player_id = ?
        JOIN game_players gp2 ON gp2.game_id = g.id AND gp2.player_id = ?
        WHERE g.boardgame_id = ? AND g.unranked = false
    )
    SELECT
        COUNT(*)                                                             AS total,
        SUM(CASE WHEN me.score > opp.score THEN 1 ELSE 0 END)              AS wins_p1,
        SUM(CASE WHEN me.score < opp.score THEN 1 ELSE 0 END)              AS wins_p2,
        SUM(CASE WHEN me.score = opp.score THEN 1 ELSE 0 END)              AS draws
    FROM common_games cg
    JOIN game_players me  ON me.game_id = cg.id  AND me.player_id = ?
    JOIN game_players opp ON opp.game_id = cg.id AND opp.player_id = ?
""", [player_id, opponent_id, boardgame_id, player_id, opponent_id]).fetchone()

total, wins_p1, wins_p2, draws = summary

col1, col2, col3, col4 = st.columns(4)
col1.metric("Spellen", total)
col2.metric(f"Winst {p1_name}", wins_p1)
col3.metric(f"Winst {p2_name}", wins_p2)
col4.metric("Gelijk", draws)

# ── Games list ───────────────────────────────────────────────────────────────

games_df = conn.execute("""
    SELECT
        COALESCE(g.ended_at, g.played_at)   AS datum,
        me.score                             AS score_p1,
        opp.score                            AS score_p2,
        CASE
            WHEN me.score > opp.score THEN ?
            WHEN me.score < opp.score THEN ?
            ELSE 'Gelijk'
        END                                  AS winnaar,
        me.elo_after                         AS elo_p1,
        opp.elo_after                        AS elo_p2,
        g.bga_table_id
    FROM games g
    JOIN game_players me  ON me.game_id = g.id  AND me.player_id = ?
    JOIN game_players opp ON opp.game_id = g.id AND opp.player_id = ?
    WHERE g.boardgame_id = ? AND g.unranked = false
    ORDER BY datum DESC
""", [p1_name, p2_name, player_id, opponent_id, boardgame_id]).df()

conn.close()

if games_df.empty:
    st.info("Geen spellen gevonden.")
    st.stop()

# ELO correctie
for col in ("elo_p1", "elo_p2"):
    games_df[col] = games_df[col].where(games_df[col].isna(), games_df[col] - ELO_BASE)

# BGA link
games_df["BGA"] = games_df["bga_table_id"].apply(
    lambda x: f"https://boardgamearena.com/table?table={x}" if x else None,
)

display_df = games_df[["datum", "score_p1", "score_p2", "winnaar", "elo_p1", "elo_p2", "BGA"]].rename(columns={
    "datum":    "Datum",
    "score_p1": f"Score {p1_name}",
    "score_p2": f"Score {p2_name}",
    "winnaar":  "Winnaar",
    "elo_p1":   f"ELO {p1_name}",
    "elo_p2":   f"ELO {p2_name}",
})

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "BGA": st.column_config.LinkColumn("BGA", display_text="🎲 Bekijk"),
    },
)
