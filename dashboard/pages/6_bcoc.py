"""BCOC (Belgian Championship of Carcassonne Online) tournament overview."""
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parents[2] / "data"
DB_PATH = DATA_DIR / "carcassonne.duckdb"

BGA_PLAYER_URL = "https://boardgamearena.com/player?id="
BGA_TABLE_URL = "https://boardgamearena.com/table?table="

st.title("BCOC Tournaments")

if not DB_PATH.exists():
    st.warning("No database found.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Filters ──────────────────────────────────────────────────────────────────

col_year, col_stage = st.columns([1, 1])

with col_year:
    years = conn.execute("""
        SELECT DISTINCT t.year
        FROM tournaments t
        WHERE t.type = 'BCOC'
        ORDER BY t.year DESC
    """).fetchall()
    year_options = ["All years"] + [r[0] for r in years]
    if len(year_options) == 1:
        st.info("No BCOC tournaments found.")
        conn.close()
        st.stop()
    selected_year = st.selectbox("Year", year_options)

# Get all BCOC tournament ids (either single year or all)
if selected_year == "All years":
    bcoc_tournaments = conn.execute(
        "SELECT id, name, year FROM tournaments WHERE type = 'BCOC' ORDER BY year DESC"
    ).fetchall()
    tourn_ids = [r[0] for r in bcoc_tournaments]
    tournament_label = f"BCOC All years ({len(tourn_ids)})"
else:
    tournament = conn.execute(
        "SELECT id, name, date_start, date_end FROM tournaments WHERE type = 'BCOC' AND year = ?",
        [selected_year],
    ).fetchone()
    if not tournament:
        st.info(f"No BCOC tournament found for {selected_year}.")
        conn.close()
        st.stop()
    tourn_ids = [tournament[0]]
    tournament_label = tournament[1]

with col_stage:
    placeholders = ",".join(["?"] * len(tourn_ids))
    stages = conn.execute(f"""
        SELECT DISTINCT stage FROM tournament_matches
        WHERE tournament_id IN ({placeholders})
        ORDER BY stage
    """, tourn_ids).fetchall()
    stage_options = ["All stages"] + [r[0] for r in stages]
    selected_stage = st.selectbox("Stage", stage_options)

# ── Build filter ─────────────────────────────────────────────────────────────

tourn_placeholders = ",".join(["?"] * len(tourn_ids))
where_clauses = [f"tm.tournament_id IN ({tourn_placeholders})"]
params = list(tourn_ids)

if selected_stage != "All stages":
    where_clauses.append("tm.stage = ?")
    params.append(selected_stage)

where_sql = "WHERE " + " AND ".join(where_clauses)

# ── Summary metrics ──────────────────────────────────────────────────────────

summary = conn.execute(f"""
    SELECT
        COUNT(*) AS total_matches,
        COUNT(DISTINCT player_1_id) + COUNT(DISTINCT player_2_id) AS player_count,
        COUNT(DISTINCT stage) AS stage_count
    FROM tournament_matches tm
    {where_sql}
""", params).fetchone()

# Distinct player count (union of player_1 and player_2)
player_count = conn.execute(f"""
    SELECT COUNT(DISTINCT pid) FROM (
        SELECT player_1_id AS pid FROM tournament_matches tm {where_sql}
        UNION
        SELECT player_2_id AS pid FROM tournament_matches tm {where_sql}
    )
""", params + params).fetchone()[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Tournament", tournament_label)
c2.metric("Matches", summary[0])
c3.metric("Players", player_count)
c4.metric("Stages", summary[2])

# ── Final ranking (only when not filtered by stage) ──────────────────────────

if selected_stage == "All stages":
    st.subheader("Final ranking")

    if selected_year == "All years":
        # Per-year rank columns (one per BCOC year, descending)
        bcoc_years = [r[2] for r in bcoc_tournaments]  # already sorted DESC
        year_rank_cols = ",\n                ".join(
            f'MAX(CASE WHEN t.year = {y} THEN tp.final_rank END) AS "{y}"'
            for y in bcoc_years
        )

        # Aggregate stats across all BCOC tournaments
        df_ranking = conn.execute(f"""
            SELECT
                p.name AS "Player",
                COUNT(*) AS "Editions",
                SUM(CASE WHEN tp.final_rank = 1 THEN 1 ELSE 0 END) AS "Gold",
                SUM(CASE WHEN tp.final_rank = 2 THEN 1 ELSE 0 END) AS "Silver",
                SUM(CASE WHEN tp.final_rank = 3 THEN 1 ELSE 0 END) AS "Bronze",
                MIN(tp.final_rank) AS "Best",
                {year_rank_cols},
                SUM(tp.duels_played) AS "DP",
                SUM(tp.duels_won) AS "W",
                SUM(tp.duels_lost) AS "L",
                SUM(tp.games_won) AS "GW",
                SUM(tp.games_lost) AS "GL",
                SUM(tp.games_won) - SUM(tp.games_lost) AS "GDiff",
                ROUND(100.0 * SUM(tp.duels_won) / NULLIF(SUM(tp.duels_played), 0), 1) AS "Win%",
                ROUND(
                    CAST(SUM(tp.games_won) - SUM(tp.games_lost) AS DOUBLE)
                    / NULLIF(SUM(tp.duels_played), 0), 2
                ) AS "Resistance"
            FROM tournament_participants tp
            JOIN players p ON tp.player_id = p.id
            JOIN tournaments t ON tp.tournament_id = t.id
            WHERE tp.tournament_id IN ({tourn_placeholders})
            GROUP BY p.name
            ORDER BY "Gold" DESC, "Silver" DESC, "Bronze" DESC, "Best" ASC, "DP" DESC
        """, list(tourn_ids)).df()
    else:
        df_ranking = conn.execute("""
            SELECT
                tp.final_rank AS "Rank",
                p.name AS "Player",
                tp.duels_played AS "DP",
                tp.duels_won AS "W",
                tp.duels_lost AS "L",
                tp.games_won AS "GW",
                tp.games_lost AS "GL",
                tp.games_won - tp.games_lost AS "GDiff",
                ROUND(tp.win_pct * 100, 1) AS "Win%",
                ROUND(tp.resistance, 2) AS "Resistance"
            FROM tournament_participants tp
            JOIN players p ON tp.player_id = p.id
            WHERE tp.tournament_id = ?
            ORDER BY tp.final_rank
        """, [tourn_ids[0]]).df()

    if not df_ranking.empty:
        st.dataframe(df_ranking, use_container_width=True, hide_index=True)
    else:
        st.info("No ranking available for this tournament.")

# ── Player stats ─────────────────────────────────────────────────────────────

st.subheader("Player stats")

df_players = conn.execute(f"""
    SELECT
        pid AS player_id,
        p.name AS "Player",
        p.bga_player_id,
        COUNT(*) AS "Duels",
        SUM(CASE WHEN won THEN 1 ELSE 0 END) AS "W",
        SUM(CASE WHEN NOT won AND NOT draw THEN 1 ELSE 0 END) AS "L",
        SUM(CASE WHEN draw THEN 1 ELSE 0 END) AS "D",
        ROUND(100.0 * SUM(CASE WHEN won THEN 1 ELSE 0 END) / COUNT(*), 1) AS "Win%",
        SUM(games_won) AS "GW",
        SUM(games_lost) AS "GL",
        SUM(games_won) - SUM(games_lost) AS "GDiff"
    FROM (
        SELECT tm.player_1_id AS pid, tm.score_1 AS games_won, tm.score_2 AS games_lost,
               tm.result = '1' AS won, tm.result = 'D' AS draw, tm.stage
        FROM tournament_matches tm
        {where_sql}
          AND (tm.score_1 > 0 OR tm.score_2 > 0)
        UNION ALL
        SELECT tm.player_2_id AS pid, tm.score_2 AS games_won, tm.score_1 AS games_lost,
               tm.result = '2' AS won, tm.result = 'D' AS draw, tm.stage
        FROM tournament_matches tm
        {where_sql}
          AND (tm.score_1 > 0 OR tm.score_2 > 0)
    ) sub
    JOIN players p ON sub.pid = p.id
    GROUP BY pid, p.name, p.bga_player_id
    ORDER BY "W" DESC, "GDiff" DESC, "GW" DESC
""", params + params).df()

if df_players.empty:
    st.info("No matches found for selected filters.")
    conn.close()
    st.stop()

df_players["BGA"] = df_players["bga_player_id"].apply(
    lambda x: f"{BGA_PLAYER_URL}{x}" if x else None
)

display_players = df_players[["Player", "BGA", "Duels", "W", "L", "D", "Win%", "GW", "GL", "GDiff"]]

event = st.dataframe(
    display_players,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="bcoc_players_table",
    column_config={
        "BGA": st.column_config.LinkColumn("BGA", display_text="🎲"),
    },
)

# Handle player row click
if event.selection and event.selection.rows:
    selected_row = event.selection.rows[0]
    clicked_player_id = int(df_players.iloc[selected_row]["player_id"])
    clicked_player_name = df_players.iloc[selected_row]["Player"]
    st.session_state["bcoc_detail_player_id"] = clicked_player_id
    st.session_state["bcoc_detail_player_name"] = clicked_player_name

# ── Player detail (when a player is selected) ────────────────────────────────

detail_player_id = st.session_state.get("bcoc_detail_player_id")
if detail_player_id:
    detail_name = st.session_state.get("bcoc_detail_player_name", str(detail_player_id))

    st.subheader(f"All BCOC games — {detail_name}")

    if st.button("Clear player selection"):
        del st.session_state["bcoc_detail_player_id"]
        if "bcoc_detail_player_name" in st.session_state:
            del st.session_state["bcoc_detail_player_name"]
        st.rerun()

    detail_where = [f"tm.tournament_id IN ({tourn_placeholders})"]
    detail_params = list(tourn_ids)
    if selected_stage != "All stages":
        detail_where.append("tm.stage = ?")
        detail_params.append(selected_stage)

    # Player can be either player_1 or player_2
    detail_where.append("(tm.player_1_id = ? OR tm.player_2_id = ?)")
    detail_params.extend([detail_player_id, detail_player_id])

    detail_where_sql = "WHERE " + " AND ".join(detail_where)

    df_games = conn.execute(f"""
        SELECT
            t.year AS "Year",
            tm.stage AS "Stage",
            tm.match_number AS "Match#",
            CASE WHEN tm.player_1_id = ? THEN p1.name ELSE p2.name END AS "Player",
            CASE WHEN tm.player_1_id = ? THEN p2.name ELSE p1.name END AS "Opponent",
            CASE WHEN tm.player_1_id = ?
                THEN tm.score_1 ELSE tm.score_2 END AS "Won",
            CASE WHEN tm.player_1_id = ?
                THEN tm.score_2 ELSE tm.score_1 END AS "Lost",
            CASE
                WHEN (tm.player_1_id = ? AND tm.result = '1')
                  OR (tm.player_2_id = ? AND tm.result = '2') THEN 'W'
                WHEN tm.result = 'D' THEN 'D'
                ELSE 'L'
            END AS "Result",
            tm.notes AS "Notes",
            g1.bga_table_id AS bga_id_1,
            g2.bga_table_id AS bga_id_2,
            g3.bga_table_id AS bga_id_3,
            -- Scores from perspective of selected player
            CASE WHEN tm.player_1_id = ?
                THEN CAST(gp1_p1.score AS INTEGER) ELSE CAST(gp1_p2.score AS INTEGER) END AS score_me_1,
            CASE WHEN tm.player_1_id = ?
                THEN CAST(gp1_p2.score AS INTEGER) ELSE CAST(gp1_p1.score AS INTEGER) END AS score_opp_1,
            CASE WHEN tm.player_1_id = ?
                THEN CAST(gp2_p1.score AS INTEGER) ELSE CAST(gp2_p2.score AS INTEGER) END AS score_me_2,
            CASE WHEN tm.player_1_id = ?
                THEN CAST(gp2_p2.score AS INTEGER) ELSE CAST(gp2_p1.score AS INTEGER) END AS score_opp_2,
            CASE WHEN tm.player_1_id = ?
                THEN CAST(gp3_p1.score AS INTEGER) ELSE CAST(gp3_p2.score AS INTEGER) END AS score_me_3,
            CASE WHEN tm.player_1_id = ?
                THEN CAST(gp3_p2.score AS INTEGER) ELSE CAST(gp3_p1.score AS INTEGER) END AS score_opp_3
        FROM tournament_matches tm
        JOIN tournaments t ON tm.tournament_id = t.id
        JOIN players p1 ON tm.player_1_id = p1.id
        JOIN players p2 ON tm.player_2_id = p2.id
        LEFT JOIN games g1 ON tm.game_id_1 = g1.id
        LEFT JOIN games g2 ON tm.game_id_2 = g2.id
        LEFT JOIN games g3 ON tm.game_id_3 = g3.id
        LEFT JOIN game_players gp1_p1 ON gp1_p1.game_id = tm.game_id_1 AND gp1_p1.player_id = tm.player_1_id
        LEFT JOIN game_players gp1_p2 ON gp1_p2.game_id = tm.game_id_1 AND gp1_p2.player_id = tm.player_2_id
        LEFT JOIN game_players gp2_p1 ON gp2_p1.game_id = tm.game_id_2 AND gp2_p1.player_id = tm.player_1_id
        LEFT JOIN game_players gp2_p2 ON gp2_p2.game_id = tm.game_id_2 AND gp2_p2.player_id = tm.player_2_id
        LEFT JOIN game_players gp3_p1 ON gp3_p1.game_id = tm.game_id_3 AND gp3_p1.player_id = tm.player_1_id
        LEFT JOIN game_players gp3_p2 ON gp3_p2.game_id = tm.game_id_3 AND gp3_p2.player_id = tm.player_2_id
        {detail_where_sql}
        ORDER BY
            t.year DESC,
            CASE tm.stage
                WHEN 'Final' THEN 1
                WHEN '1/2 Finals' THEN 2
                WHEN '1/4 Finals' THEN 3
                WHEN 'Round of 16' THEN 4
                WHEN 'Best 3rd' THEN 5
                ELSE 6
            END,
            tm.match_number NULLS LAST
    """, [detail_player_id] * 12 + detail_params).df()

    if not df_games.empty:
        # Build score and BGA link columns
        for i in range(1, 4):
            bga_col = f"bga_id_{i}"
            link_col = f"Game {i}"
            score_col = f"Score {i}"
            df_games[link_col] = df_games[bga_col].apply(
                lambda x: f"{BGA_TABLE_URL}{x}" if pd.notna(x) and x else None
            )
            me_col = f"score_me_{i}"
            opp_col = f"score_opp_{i}"
            df_games[score_col] = df_games.apply(
                lambda row, m=me_col, o=opp_col: (
                    f"{int(row[m])} - {int(row[o])}"
                    if pd.notna(row[m]) and pd.notna(row[o]) else None
                ), axis=1,
            )

        display_cols = [
            "Year", "Stage", "Opponent", "Result", "Won", "Lost",
            "Score 1", "Game 1", "Score 2", "Game 2", "Score 3", "Game 3",
            "Notes",
        ]

        game_col_config = {}
        for i in range(1, 4):
            game_col_config[f"Game {i}"] = st.column_config.LinkColumn(
                f"Game {i}", display_text="🎲"
            )

        st.dataframe(
            df_games[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config=game_col_config,
        )
    else:
        st.info("No games found for this player with current filters.")

# ── All matches overview ─────────────────────────────────────────────────────

st.subheader("All matches")

df_all = conn.execute(f"""
    SELECT
        t.year AS "Year",
        tm.stage AS "Stage",
        tm.match_number AS "Match#",
        p1.name AS "Player 1",
        tm.score_1 AS "S1",
        tm.score_2 AS "S2",
        p2.name AS "Player 2",
        CASE tm.result
            WHEN '1' THEN p1.name
            WHEN '2' THEN p2.name
            ELSE 'Draw'
        END AS "Winner",
        tm.notes AS "Notes"
    FROM tournament_matches tm
    JOIN tournaments t ON tm.tournament_id = t.id
    JOIN players p1 ON tm.player_1_id = p1.id
    JOIN players p2 ON tm.player_2_id = p2.id
    {where_sql}
    ORDER BY
        t.year DESC,
        CASE tm.stage
            WHEN 'Final' THEN 1
            WHEN '1/2 Finals' THEN 2
            WHEN '1/4 Finals' THEN 3
            WHEN 'Round of 16' THEN 4
            WHEN 'Best 3rd' THEN 5
            ELSE 6
        END,
        tm.stage,
        tm.match_number NULLS LAST
""", params).df()

if not df_all.empty:
    st.dataframe(df_all, use_container_width=True, hide_index=True)

conn.close()