"""Speler detail pagina — ELO evolutie, tegenstanders, statistieken."""
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[2]))

DB_PATH = Path(__file__).parents[2] / "data" / "carcassonne.duckdb"
ELO_BASE = 1300


# ── Read player from session state ───────────────────────────────────────────

if "player_detail_id" not in st.session_state:
    st.warning("Selecteer eerst een speler via de Spelers pagina.")
    if st.button("← Terug naar spelers"):
        st.switch_page("pages/2_players.py")
    st.stop()

player_id = st.session_state["player_detail_id"]
boardgame_id = st.session_state.get("player_detail_bg", 1)

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Player info ──────────────────────────────────────────────────────────────

player = conn.execute("""
    SELECT p.id, p.name, p.country, p.bga_player_id, bg.name AS boardgame
    FROM players p, boardgames bg
    WHERE p.id = ? AND bg.id = ?
""", [player_id, boardgame_id]).fetchone()

if not player:
    st.error("Speler niet gevonden.")
    conn.close()
    st.stop()

p_id, p_name, p_country, p_bga_id, bg_name = player

# ── Header with back button and BGA link ─────────────────────────────────────

st.title(f"👤 {p_name}")

col_back, col_bga, _ = st.columns([1, 1, 4])
with col_back:
    if st.button("← Terug naar spelers"):
        st.switch_page("pages/2_players.py")
with col_bga:
    if p_bga_id:
        st.link_button("🎲 BGA profiel", f"https://boardgamearena.com/player?id={p_bga_id}")

# ── Summary stats ────────────────────────────────────────────────────────────

stats = conn.execute("""
    WITH game_info AS (
        SELECT
            g.id AS game_id,
            (SELECT COUNT(*) FROM game_players gp2 WHERE gp2.game_id = g.id) AS num_players,
            g.duration_min,
            (SELECT MAX(gp2.score) FROM game_players gp2 WHERE gp2.game_id = g.id) AS max_score
        FROM games g
        WHERE g.boardgame_id = ? AND g.unranked = false
    )
    SELECT
        COUNT(*)                                                           AS total_games,
        ROUND(100.0 * SUM(CASE WHEN gp.elo_delta > 0 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gp.elo_delta IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS win_pct,
        MAX(gp.elo_after)                                                  AS max_elo,
        MAX(gp.arena_after)                                                AS max_arena,
        ROUND(100.0 * SUM(CASE WHEN gi.num_players = 2 THEN 1 ELSE 0 END)
              / COUNT(*), 1)                                               AS pct_2p,
        ROUND(100.0 * SUM(CASE WHEN gi.duration_min IS NOT NULL AND gi.duration_min <= 60 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gi.duration_min IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS pct_rt,
        ROUND(100.0 * SUM(CASE WHEN gi.max_score < 160 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gi.max_score IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS pct_basis,
        MIN(COALESCE(g.ended_at, g.played_at))                            AS first_game,
        MAX(COALESCE(g.ended_at, g.played_at))                            AS last_game
    FROM game_players gp
    JOIN games g ON g.id = gp.game_id
    JOIN game_info gi ON gi.game_id = g.id
    WHERE gp.player_id = ? AND g.boardgame_id = ? AND g.unranked = false
""", [boardgame_id, player_id, boardgame_id]).fetchone()

total_games, win_pct, max_elo, max_arena, pct_2p, pct_rt, pct_basis, first_game, last_game = stats

col1, col2, col3, col4 = st.columns(4)
col1.metric("Land", p_country or "?")
col2.metric("Spellen", total_games)
col3.metric("Win%", f"{win_pct}%" if win_pct is not None else "–")
col4.metric("Max ELO", int(max_elo - ELO_BASE) if max_elo is not None else "–")

col5, col6, col7, col8 = st.columns(4)
col5.metric("Max Arena", int(max_arena) if max_arena is not None else "–")
col6.metric("% 2 spelers", f"{pct_2p}%" if pct_2p is not None else "–")
col7.metric("% Realtime", f"{pct_rt}%" if pct_rt is not None else "–")
col8.metric("% Basisspel", f"{pct_basis}%" if pct_basis is not None else "–")

if first_game and last_game:
    st.caption(f"{bg_name} — van {first_game:%Y-%m-%d} tot {last_game:%Y-%m-%d}")

# ── ELO Evolution ────────────────────────────────────────────────────────────

st.subheader("ELO evolutie")

elo_df = conn.execute("""
    SELECT
        COALESCE(g.ended_at, g.played_at) AS datum,
        gp.elo_after
    FROM game_players gp
    JOIN games g ON g.id = gp.game_id
    WHERE gp.player_id = ? AND g.boardgame_id = ? AND g.unranked = false
      AND gp.elo_after IS NOT NULL
      AND COALESCE(g.ended_at, g.played_at) IS NOT NULL
    ORDER BY datum
""", [player_id, boardgame_id]).df()

if elo_df.empty:
    st.info("Geen ELO data beschikbaar.")
else:
    elo_df["elo_after"] = elo_df["elo_after"] - ELO_BASE
    st.line_chart(elo_df.set_index("datum")["elo_after"], use_container_width=True)

# ── Opponents ────────────────────────────────────────────────────────────────

st.subheader("Tegenstanders")

opp_col1, opp_col2 = st.columns([2, 2])

with opp_col1:
    opp_years = conn.execute("""
        SELECT DISTINCT YEAR(COALESCE(g.ended_at, g.played_at)) AS yr
        FROM game_players gp
        JOIN games g ON g.id = gp.game_id
        WHERE gp.player_id = ? AND g.boardgame_id = ? AND g.unranked = false
          AND COALESCE(g.ended_at, g.played_at) IS NOT NULL
        ORDER BY yr DESC
    """, [player_id, boardgame_id]).fetchall()
    opp_year_options = ["Alle jaren"] + [str(r[0]) for r in opp_years]
    opp_selected_year = st.selectbox("Jaar", opp_year_options, key="opp_year")

with opp_col2:
    opp_search = st.text_input("Zoek tegenstander", placeholder="Naam...", key="opp_search")

year_filter = 0 if opp_selected_year == "Alle jaren" else int(opp_selected_year)

opponents_df = conn.execute("""
    WITH my_games AS (
        SELECT g.id
        FROM game_players gp
        JOIN games g ON g.id = gp.game_id
        WHERE gp.player_id = ? AND g.boardgame_id = ? AND g.unranked = false
          AND (? = 0 OR YEAR(COALESCE(g.ended_at, g.played_at)) = ?)
    ),
    opponent_results AS (
        SELECT
            opp_gp.player_id                                    AS opp_id,
            p.name                                              AS opp_name,
            p.country                                           AS opp_country,
            CASE WHEN me.elo_delta > 0 THEN 1 ELSE 0 END       AS i_won,
            CASE WHEN me.elo_delta IS NOT NULL THEN 1 ELSE 0 END AS ranked
        FROM my_games mg
        JOIN game_players opp_gp ON opp_gp.game_id = mg.id AND opp_gp.player_id != ?
        JOIN game_players me     ON me.game_id = mg.id     AND me.player_id = ?
        JOIN players p           ON p.id = opp_gp.player_id
    )
    SELECT
        opp_id,
        opp_name,
        opp_country,
        COUNT(*)                                                           AS spellen,
        ROUND(100.0 * SUM(i_won) / NULLIF(SUM(ranked), 0), 1)            AS win_pct
    FROM opponent_results
    GROUP BY opp_id, opp_name, opp_country
    ORDER BY spellen DESC
""", [player_id, boardgame_id, year_filter, year_filter, player_id, player_id]).df()

conn.close()

if opp_search:
    opponents_df = opponents_df[opponents_df["opp_name"].str.contains(opp_search, case=False, na=False)]

if opponents_df.empty:
    st.info("Geen tegenstanders gevonden.")
else:
    st.caption(f"{len(opponents_df)} tegenstander(s)")

    # Navigate to head-to-head on selection from previous rerun
    if "go_to_h2h_opp" in st.session_state:
        st.session_state["h2h_player"] = player_id
        st.session_state["h2h_opponent"] = st.session_state.pop("go_to_h2h_opp")
        st.session_state["h2h_bg"] = boardgame_id
        st.switch_page("pages/4_head_to_head.py")

    opp_display = opponents_df[["opp_name", "opp_country", "spellen", "win_pct"]].rename(columns={
        "opp_name":    "Naam",
        "opp_country": "Land",
        "spellen":     "Spellen",
        "win_pct":     "Win%",
    })

    event = st.dataframe(
        opp_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="opponents_table",
    )

    if event.selection and event.selection.rows:
        selected_opp_row = event.selection.rows[0]
        st.session_state["go_to_h2h_opp"] = int(opponents_df.iloc[selected_opp_row]["opp_id"])
        st.rerun()
