"""Nationale ploeg overzicht."""
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parents[2] / "data"
DB_PATH = DATA_DIR / "carcassonne.duckdb"

BGA_PLAYER_URL = "https://boardgamearena.com/player?id="
BGA_TABLE_URL = "https://boardgamearena.com/table?table="

st.title("National Team Belgium")

if not DB_PATH.exists():
    st.warning("Geen database gevonden.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Filters ──────────────────────────────────────────────────────────────────

col_a, col_b = st.columns([2, 2])

with col_a:
    tournaments = conn.execute("""
        SELECT id, name, type, year FROM tournaments
        WHERE national_team_competition = TRUE
        ORDER BY type, year
    """).fetchall()
    tourn_options = {"All tournaments": None}
    for tid, name, ttype, year in tournaments:
        tourn_options[name] = tid
    selected_tourn_name = st.selectbox("Tournament", list(tourn_options.keys()))
    selected_tourn_id = tourn_options[selected_tourn_name]

with col_b:
    year_rows = conn.execute("""
        SELECT DISTINCT YEAR(d.date_played) AS yr
        FROM nations_competition_duels d
        WHERE d.date_played IS NOT NULL
        ORDER BY yr DESC
    """).fetchall()
    year_options = ["All years"] + [str(r[0]) for r in year_rows]
    selected_year = st.selectbox("Year", year_options)

# ── Build filter clauses ─────────────────────────────────────────────────────

where_clauses = []
params = []

if selected_tourn_id:
    where_clauses.append("d.tournament_id = ?")
    params.append(selected_tourn_id)

if selected_year != "All years":
    where_clauses.append("YEAR(d.date_played) = ?")
    params.append(int(selected_year))

where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

# ── Player stats ─────────────────────────────────────────────────────────────

st.subheader("Player stats")

df_players = conn.execute(f"""
    SELECT
        p.id AS player_id,
        p.name AS "Player",
        p.bga_player_id,
        COUNT(*) AS "Matches",
        SUM(CASE WHEN nm.result = 'W' THEN 1 ELSE 0 END) AS "W",
        SUM(CASE WHEN nm.result = 'L' THEN 1 ELSE 0 END) AS "L",
        ROUND(100.0 * SUM(CASE WHEN nm.result = 'W' THEN 1 ELSE 0 END) / COUNT(*), 1) AS "Win%",
        SUM(nm.score_belgium) AS "Points for",
        SUM(nm.score_opponent) AS "Points against",
        SUM(nm.score_belgium) - SUM(nm.score_opponent) AS "Diff",
        COUNT(DISTINCT d.tournament_id) AS "Tournaments",
        MIN(d.date_played) AS "First",
        MAX(d.date_played) AS "Last"
    FROM nations_matches nm
    JOIN nations_competition_duels d ON nm.duel_id = d.id
    JOIN players p ON nm.player_id = p.id
    {where_sql}
    GROUP BY p.id, p.name, p.bga_player_id
    ORDER BY "Matches" DESC
""", params).df()

if df_players.empty:
    st.info("No matches found for selected filters.")
    conn.close()
    st.stop()

# Add BGA link
df_players["BGA"] = df_players["bga_player_id"].apply(
    lambda x: f"{BGA_PLAYER_URL}{x}" if x else None
)

# Navigate to player detail on click
if "nt_go_to_player" in st.session_state:
    pid = st.session_state.pop("nt_go_to_player")
    selected_player_id = pid
    # Set the player filter via query param workaround — just show their games below
    st.session_state["nt_detail_player_id"] = pid

display_players = df_players[["Player", "BGA", "Matches", "W", "L", "Win%",
                               "Points for", "Points against", "Diff",
                               "Tournaments", "First", "Last"]]

event = st.dataframe(
    display_players,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="nt_players_table",
    column_config={
        "BGA": st.column_config.LinkColumn("BGA", display_text="🎲"),
    },
)

# Handle player row click
if event.selection and event.selection.rows:
    selected_row = event.selection.rows[0]
    clicked_player_id = int(df_players.iloc[selected_row]["player_id"])
    clicked_player_name = df_players.iloc[selected_row]["Player"]
    st.session_state["nt_detail_player_id"] = clicked_player_id
    st.session_state["nt_detail_player_name"] = clicked_player_name

# ── Player game detail (when a player is selected) ──────────────────────────

detail_player_id = st.session_state.get("nt_detail_player_id")
if detail_player_id:
    detail_name = st.session_state.get(
        "nt_detail_player_name",
        df_players[df_players["player_id"] == detail_player_id].iloc[0]["Player"]
        if not df_players[df_players["player_id"] == detail_player_id].empty
        else str(detail_player_id),
    )

    st.subheader(f"All national team games — {detail_name}")

    if st.button("Clear player selection"):
        del st.session_state["nt_detail_player_id"]
        if "nt_detail_player_name" in st.session_state:
            del st.session_state["nt_detail_player_name"]
        st.rerun()

    # Build filter for this specific player's games
    detail_where = ["nm.player_id = ?"]
    detail_params = [detail_player_id]
    if selected_tourn_id:
        detail_where.append("d.tournament_id = ?")
        detail_params.append(selected_tourn_id)
    if selected_year != "All years":
        detail_where.append("YEAR(d.date_played) = ?")
        detail_params.append(int(selected_year))

    detail_where_sql = "WHERE " + " AND ".join(detail_where)

    df_player_games = conn.execute(f"""
        SELECT
            d.date_played AS "Date",
            t.name AS "Tournament",
            d.stage AS "Stage",
            d.opponent_country AS "vs Country",
            op.name AS "Opponent",
            nm.result AS "Result",
            nm.score_belgium AS "Score BE",
            nm.score_opponent AS "Score Opp",
            nm.notes AS "Notes",
            g1.bga_table_id AS bga_id_1,
            g2.bga_table_id AS bga_id_2,
            g3.bga_table_id AS bga_id_3,
            g4.bga_table_id AS bga_id_4,
            g5.bga_table_id AS bga_id_5,
            CAST(gp1_be.score AS INTEGER) AS score_be_1,
            CAST(gp1_opp.score AS INTEGER) AS score_opp_1,
            CAST(gp2_be.score AS INTEGER) AS score_be_2,
            CAST(gp2_opp.score AS INTEGER) AS score_opp_2,
            CAST(gp3_be.score AS INTEGER) AS score_be_3,
            CAST(gp3_opp.score AS INTEGER) AS score_opp_3,
            CAST(gp4_be.score AS INTEGER) AS score_be_4,
            CAST(gp4_opp.score AS INTEGER) AS score_opp_4,
            CAST(gp5_be.score AS INTEGER) AS score_be_5,
            CAST(gp5_opp.score AS INTEGER) AS score_opp_5
        FROM nations_matches nm
        JOIN nations_competition_duels d ON nm.duel_id = d.id
        JOIN tournaments t ON d.tournament_id = t.id
        JOIN players op ON nm.opponent_player_id = op.id
        LEFT JOIN games g1 ON nm.game_id_1 = g1.id
        LEFT JOIN games g2 ON nm.game_id_2 = g2.id
        LEFT JOIN games g3 ON nm.game_id_3 = g3.id
        LEFT JOIN games g4 ON nm.game_id_4 = g4.id
        LEFT JOIN games g5 ON nm.game_id_5 = g5.id
        LEFT JOIN game_players gp1_be ON gp1_be.game_id = nm.game_id_1 AND gp1_be.player_id = nm.player_id
        LEFT JOIN game_players gp1_opp ON gp1_opp.game_id = nm.game_id_1 AND gp1_opp.player_id = nm.opponent_player_id
        LEFT JOIN game_players gp2_be ON gp2_be.game_id = nm.game_id_2 AND gp2_be.player_id = nm.player_id
        LEFT JOIN game_players gp2_opp ON gp2_opp.game_id = nm.game_id_2 AND gp2_opp.player_id = nm.opponent_player_id
        LEFT JOIN game_players gp3_be ON gp3_be.game_id = nm.game_id_3 AND gp3_be.player_id = nm.player_id
        LEFT JOIN game_players gp3_opp ON gp3_opp.game_id = nm.game_id_3 AND gp3_opp.player_id = nm.opponent_player_id
        LEFT JOIN game_players gp4_be ON gp4_be.game_id = nm.game_id_4 AND gp4_be.player_id = nm.player_id
        LEFT JOIN game_players gp4_opp ON gp4_opp.game_id = nm.game_id_4 AND gp4_opp.player_id = nm.opponent_player_id
        LEFT JOIN game_players gp5_be ON gp5_be.game_id = nm.game_id_5 AND gp5_be.player_id = nm.player_id
        LEFT JOIN game_players gp5_opp ON gp5_opp.game_id = nm.game_id_5 AND gp5_opp.player_id = nm.opponent_player_id
        {detail_where_sql}
        ORDER BY d.date_played DESC NULLS LAST
    """, detail_params).df()

    if not df_player_games.empty:
        # Build BGA game links and score columns
        for i in range(1, 6):
            col_name = f"bga_id_{i}"
            link_col = f"Game {i}"
            score_col = f"Score {i}"
            df_player_games[link_col] = df_player_games[col_name].apply(
                lambda x: f"{BGA_TABLE_URL}{x}" if pd.notna(x) and x else None
            )
            be_col = f"score_be_{i}"
            opp_col = f"score_opp_{i}"
            df_player_games[score_col] = df_player_games.apply(
                lambda row, b=be_col, o=opp_col: (
                    f"{int(row[b])} - {int(row[o])}"
                    if pd.notna(row[b]) and pd.notna(row[o]) else None
                ), axis=1
            )

        display_cols = ["Date", "Tournament", "Stage", "vs Country", "Opponent",
                        "Result", "Score BE", "Score Opp",
                        "Score 1", "Game 1", "Score 2", "Game 2", "Score 3", "Game 3"]
        for i in range(4, 6):
            if df_player_games[f"Game {i}"].notna().any() or df_player_games[f"Score {i}"].notna().any():
                display_cols.extend([f"Score {i}", f"Game {i}"])
        display_cols.append("Notes")

        game_col_config = {}
        for i in range(1, 6):
            col = f"Game {i}"
            if col in display_cols:
                game_col_config[col] = st.column_config.LinkColumn(col, display_text="🎲")

        st.dataframe(
            df_player_games[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config=game_col_config,
        )
    else:
        st.info("No games found for this player with current filters.")

# ── Duel overview ────────────────────────────────────────────────────────────

st.subheader("Match overview")

df_duels = conn.execute(f"""
    SELECT
        d.date_played AS "Date",
        t.name AS "Tournament",
        d.stage AS "Stage",
        d.opponent_country AS "Opponent",
        SUM(CASE WHEN nm.result = 'W' THEN 1 ELSE 0 END) AS "W",
        SUM(CASE WHEN nm.result = 'L' THEN 1 ELSE 0 END) AS "L",
        SUM(nm.score_belgium) AS "Pts BE",
        SUM(nm.score_opponent) AS "Pts Opp",
        d.id AS duel_id
    FROM nations_competition_duels d
    JOIN tournaments t ON d.tournament_id = t.id
    JOIN nations_matches nm ON nm.duel_id = d.id
    {where_sql}
    GROUP BY d.id, d.date_played, t.name, d.stage, d.opponent_country
    ORDER BY d.date_played DESC NULLS LAST
""", params).df()

if not df_duels.empty:
    def duel_result(row):
        if row["W"] > row["L"]:
            return "W"
        elif row["L"] > row["W"]:
            return "L"
        return "D"

    df_duels["Result"] = df_duels.apply(duel_result, axis=1)
    df_duels["Score"] = df_duels["W"].astype(str) + "-" + df_duels["L"].astype(str)

    display_duels = df_duels[["Date", "Tournament", "Stage", "Opponent", "Score", "Result", "Pts BE", "Pts Opp"]]

    # Total row
    total_duels_w = (df_duels["Result"] == "W").sum()
    total_duels_l = (df_duels["Result"] == "L").sum()
    total_duels_d = (df_duels["Result"] == "D").sum()
    total_matches_w = df_duels["W"].sum()
    total_matches_l = df_duels["L"].sum()
    total_row = pd.DataFrame([{
        "Date": None,
        "Tournament": "TOTAL",
        "Stage": f"{len(df_duels)} duels ({total_duels_w}W-{total_duels_l}L-{total_duels_d}D)",
        "Opponent": "",
        "Score": f"{total_matches_w}-{total_matches_l}",
        "Result": "",
        "Pts BE": df_duels["Pts BE"].sum(),
        "Pts Opp": df_duels["Pts Opp"].sum(),
    }])
    display_duels = pd.concat([display_duels, total_row], ignore_index=True)

    st.dataframe(display_duels, use_container_width=True, hide_index=True)

    # ── Duel detail ──────────────────────────────────────────────────────────

    st.subheader("Match details")

    duel_labels = {}
    for _, row in df_duels.iterrows():
        duel_labels[row["duel_id"]] = (
            f"{row['Date']} — {row['Opponent']} ({row['Stage']})"
        )

    selected_duel = st.selectbox(
        "Select a match to view details",
        options=df_duels["duel_id"].tolist(),
        format_func=lambda did: duel_labels.get(did, str(did)),
        key="nt_selected_duel",
    )

    if selected_duel:
        df_detail = conn.execute("""
            SELECT
                p.name AS "Player BE",
                p.bga_player_id AS bga_be,
                op.name AS "Opponent",
                op.bga_player_id AS bga_opp,
                nm.result AS "Result",
                nm.score_belgium AS "Score BE",
                nm.score_opponent AS "Score Opp",
                nm.notes AS "Notes",
                g1.bga_table_id AS bga_id_1,
                g2.bga_table_id AS bga_id_2,
                g3.bga_table_id AS bga_id_3,
                g4.bga_table_id AS bga_id_4,
                g5.bga_table_id AS bga_id_5,
                CAST(gp1_be.score AS INTEGER) AS score_be_1,
                CAST(gp1_opp.score AS INTEGER) AS score_opp_1,
                CAST(gp2_be.score AS INTEGER) AS score_be_2,
                CAST(gp2_opp.score AS INTEGER) AS score_opp_2,
                CAST(gp3_be.score AS INTEGER) AS score_be_3,
                CAST(gp3_opp.score AS INTEGER) AS score_opp_3,
                CAST(gp4_be.score AS INTEGER) AS score_be_4,
                CAST(gp4_opp.score AS INTEGER) AS score_opp_4,
                CAST(gp5_be.score AS INTEGER) AS score_be_5,
                CAST(gp5_opp.score AS INTEGER) AS score_opp_5
            FROM nations_matches nm
            JOIN players p ON nm.player_id = p.id
            JOIN players op ON nm.opponent_player_id = op.id
            LEFT JOIN games g1 ON nm.game_id_1 = g1.id
            LEFT JOIN games g2 ON nm.game_id_2 = g2.id
            LEFT JOIN games g3 ON nm.game_id_3 = g3.id
            LEFT JOIN games g4 ON nm.game_id_4 = g4.id
            LEFT JOIN games g5 ON nm.game_id_5 = g5.id
            LEFT JOIN game_players gp1_be ON gp1_be.game_id = nm.game_id_1 AND gp1_be.player_id = nm.player_id
            LEFT JOIN game_players gp1_opp ON gp1_opp.game_id = nm.game_id_1 AND gp1_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp2_be ON gp2_be.game_id = nm.game_id_2 AND gp2_be.player_id = nm.player_id
            LEFT JOIN game_players gp2_opp ON gp2_opp.game_id = nm.game_id_2 AND gp2_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp3_be ON gp3_be.game_id = nm.game_id_3 AND gp3_be.player_id = nm.player_id
            LEFT JOIN game_players gp3_opp ON gp3_opp.game_id = nm.game_id_3 AND gp3_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp4_be ON gp4_be.game_id = nm.game_id_4 AND gp4_be.player_id = nm.player_id
            LEFT JOIN game_players gp4_opp ON gp4_opp.game_id = nm.game_id_4 AND gp4_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp5_be ON gp5_be.game_id = nm.game_id_5 AND gp5_be.player_id = nm.player_id
            LEFT JOIN game_players gp5_opp ON gp5_opp.game_id = nm.game_id_5 AND gp5_opp.player_id = nm.opponent_player_id
            WHERE nm.duel_id = ?
            ORDER BY nm.result DESC, nm.score_belgium - nm.score_opponent DESC
        """, [selected_duel]).df()

        # Add BGA links for players
        df_detail["Player BE BGA"] = df_detail["bga_be"].apply(
            lambda x: f"{BGA_PLAYER_URL}{x}" if x else None
        )
        df_detail["Opponent BGA"] = df_detail["bga_opp"].apply(
            lambda x: f"{BGA_PLAYER_URL}{x}" if x else None
        )

        # Add BGA game links and score columns
        for i in range(1, 6):
            col_name = f"bga_id_{i}"
            link_col = f"Game {i}"
            score_col = f"Score {i}"
            df_detail[link_col] = df_detail[col_name].apply(
                lambda x: f"{BGA_TABLE_URL}{x}" if pd.notna(x) and x else None
            )
            be_col = f"score_be_{i}"
            opp_col = f"score_opp_{i}"
            df_detail[score_col] = df_detail.apply(
                lambda row, b=be_col, o=opp_col: (
                    f"{int(row[b])} - {int(row[o])}"
                    if pd.notna(row[b]) and pd.notna(row[o]) else None
                ), axis=1
            )

        display_cols = ["Player BE", "Player BE BGA", "Opponent", "Opponent BGA",
                        "Result", "Score BE", "Score Opp",
                        "Score 1", "Game 1", "Score 2", "Game 2", "Score 3", "Game 3"]
        for i in range(4, 6):
            if df_detail[f"Game {i}"].notna().any() or df_detail[f"Score {i}"].notna().any():
                display_cols.extend([f"Score {i}", f"Game {i}"])
        display_cols.append("Notes")

        game_col_config = {
            "Player BE BGA": st.column_config.LinkColumn("BE BGA", display_text="🎲"),
            "Opponent BGA": st.column_config.LinkColumn("Opp BGA", display_text="🎲"),
        }
        for i in range(1, 6):
            col = f"Game {i}"
            if col in display_cols:
                game_col_config[col] = st.column_config.LinkColumn(col, display_text="🎲")

        st.dataframe(
            df_detail[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config=game_col_config,
        )

# ── Overall record ───────────────────────────────────────────────────────────

st.subheader("Stats per opponent country")

df_countries = conn.execute(f"""
    SELECT
        d.opponent_country AS "Country",
        COUNT(DISTINCT d.id) AS "Duels",
        COUNT(DISTINCT CASE WHEN sub.duel_w > sub.duel_l THEN d.id END) AS "Duels W",
        COUNT(DISTINCT CASE WHEN sub.duel_w = sub.duel_l THEN d.id END) AS "Duels D",
        COUNT(DISTINCT CASE WHEN sub.duel_w < sub.duel_l THEN d.id END) AS "Duels L",
        SUM(CASE WHEN nm.result = 'W' THEN 1 ELSE 0 END) AS "Matches W",
        SUM(CASE WHEN nm.result = 'L' THEN 1 ELSE 0 END) AS "Matches L",
        SUM(
            CASE WHEN nm.result = 'W' THEN
                CASE WHEN nm.game_id_4 IS NOT NULL OR nm.game_id_5 IS NOT NULL THEN 3 ELSE 2 END
            ELSE
                GREATEST(0,
                    (CASE WHEN nm.game_id_1 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_2 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_3 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_4 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_5 IS NOT NULL THEN 1 ELSE 0 END)
                  - CASE WHEN nm.game_id_4 IS NOT NULL OR nm.game_id_5 IS NOT NULL THEN 3 ELSE 2 END
                )
            END
        ) AS "Games W",
        SUM(
            CASE WHEN nm.result = 'L' THEN
                CASE WHEN nm.game_id_4 IS NOT NULL OR nm.game_id_5 IS NOT NULL THEN 3 ELSE 2 END
            ELSE
                GREATEST(0,
                    (CASE WHEN nm.game_id_1 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_2 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_3 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_4 IS NOT NULL THEN 1 ELSE 0 END
                   + CASE WHEN nm.game_id_5 IS NOT NULL THEN 1 ELSE 0 END)
                  - CASE WHEN nm.game_id_4 IS NOT NULL OR nm.game_id_5 IS NOT NULL THEN 3 ELSE 2 END
                )
            END
        ) AS "Games L",
        SUM(nm.score_belgium) - SUM(nm.score_opponent) AS "Pts Diff"
    FROM nations_competition_duels d
    JOIN (
        SELECT
            nm.duel_id,
            SUM(CASE WHEN nm.result = 'W' THEN 1 ELSE 0 END) AS duel_w,
            SUM(CASE WHEN nm.result = 'L' THEN 1 ELSE 0 END) AS duel_l
        FROM nations_matches nm
        GROUP BY nm.duel_id
    ) sub ON sub.duel_id = d.id
    JOIN nations_matches nm ON nm.duel_id = d.id
    {where_sql}
    GROUP BY d.opponent_country
    ORDER BY "Duels" DESC
""", params).df()

if not df_countries.empty:
    event_countries = st.dataframe(
        df_countries,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="nt_countries_table",
    )

    # Handle country row click
    selected_country = None
    if event_countries.selection and event_countries.selection.rows:
        selected_row_c = event_countries.selection.rows[0]
        selected_country = df_countries.iloc[selected_row_c]["Country"]

    if selected_country:
        st.subheader(f"All games vs {selected_country}")

        country_where = ["d.opponent_country = ?"]
        country_params = [selected_country]
        if selected_tourn_id:
            country_where.append("d.tournament_id = ?")
            country_params.append(selected_tourn_id)
        if selected_year != "All years":
            country_where.append("YEAR(d.date_played) = ?")
            country_params.append(int(selected_year))

        country_where_sql = "WHERE " + " AND ".join(country_where)

        df_country_games = conn.execute(f"""
            SELECT
                d.date_played AS "Date",
                t.name AS "Tournament",
                d.stage AS "Stage",
                p.name AS "Player BE",
                op.name AS "Opponent",
                nm.result AS "Result",
                nm.score_belgium AS "Score BE",
                nm.score_opponent AS "Score Opp",
                nm.notes AS "Notes",
                g1.bga_table_id AS bga_id_1,
                g2.bga_table_id AS bga_id_2,
                g3.bga_table_id AS bga_id_3,
                g4.bga_table_id AS bga_id_4,
                g5.bga_table_id AS bga_id_5,
                CAST(gp1_be.score AS INTEGER) AS score_be_1,
                CAST(gp1_opp.score AS INTEGER) AS score_opp_1,
                CAST(gp2_be.score AS INTEGER) AS score_be_2,
                CAST(gp2_opp.score AS INTEGER) AS score_opp_2,
                CAST(gp3_be.score AS INTEGER) AS score_be_3,
                CAST(gp3_opp.score AS INTEGER) AS score_opp_3,
                CAST(gp4_be.score AS INTEGER) AS score_be_4,
                CAST(gp4_opp.score AS INTEGER) AS score_opp_4,
                CAST(gp5_be.score AS INTEGER) AS score_be_5,
                CAST(gp5_opp.score AS INTEGER) AS score_opp_5
            FROM nations_matches nm
            JOIN nations_competition_duels d ON nm.duel_id = d.id
            JOIN tournaments t ON d.tournament_id = t.id
            JOIN players p ON nm.player_id = p.id
            JOIN players op ON nm.opponent_player_id = op.id
            LEFT JOIN games g1 ON nm.game_id_1 = g1.id
            LEFT JOIN games g2 ON nm.game_id_2 = g2.id
            LEFT JOIN games g3 ON nm.game_id_3 = g3.id
            LEFT JOIN games g4 ON nm.game_id_4 = g4.id
            LEFT JOIN games g5 ON nm.game_id_5 = g5.id
            LEFT JOIN game_players gp1_be ON gp1_be.game_id = nm.game_id_1 AND gp1_be.player_id = nm.player_id
            LEFT JOIN game_players gp1_opp ON gp1_opp.game_id = nm.game_id_1 AND gp1_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp2_be ON gp2_be.game_id = nm.game_id_2 AND gp2_be.player_id = nm.player_id
            LEFT JOIN game_players gp2_opp ON gp2_opp.game_id = nm.game_id_2 AND gp2_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp3_be ON gp3_be.game_id = nm.game_id_3 AND gp3_be.player_id = nm.player_id
            LEFT JOIN game_players gp3_opp ON gp3_opp.game_id = nm.game_id_3 AND gp3_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp4_be ON gp4_be.game_id = nm.game_id_4 AND gp4_be.player_id = nm.player_id
            LEFT JOIN game_players gp4_opp ON gp4_opp.game_id = nm.game_id_4 AND gp4_opp.player_id = nm.opponent_player_id
            LEFT JOIN game_players gp5_be ON gp5_be.game_id = nm.game_id_5 AND gp5_be.player_id = nm.player_id
            LEFT JOIN game_players gp5_opp ON gp5_opp.game_id = nm.game_id_5 AND gp5_opp.player_id = nm.opponent_player_id
            {country_where_sql}
            ORDER BY d.date_played DESC NULLS LAST, nm.result DESC
        """, country_params).df()

        if not df_country_games.empty:
            for i in range(1, 6):
                col_name = f"bga_id_{i}"
                link_col = f"Game {i}"
                score_col = f"Score {i}"
                df_country_games[link_col] = df_country_games[col_name].apply(
                    lambda x: f"{BGA_TABLE_URL}{x}" if pd.notna(x) and x else None
                )
                be_col = f"score_be_{i}"
                opp_col = f"score_opp_{i}"
                df_country_games[score_col] = df_country_games.apply(
                    lambda row, b=be_col, o=opp_col: (
                        f"{int(row[b])} - {int(row[o])}"
                        if pd.notna(row[b]) and pd.notna(row[o]) else None
                    ), axis=1
                )

            display_cols_c = ["Date", "Tournament", "Stage", "Player BE", "Opponent",
                              "Result", "Score BE", "Score Opp",
                              "Score 1", "Game 1", "Score 2", "Game 2", "Score 3", "Game 3"]
            for i in range(4, 6):
                if df_country_games[f"Game {i}"].notna().any() or df_country_games[f"Score {i}"].notna().any():
                    display_cols_c.extend([f"Score {i}", f"Game {i}"])
            display_cols_c.append("Notes")

            game_col_config_c = {}
            for i in range(1, 6):
                col = f"Game {i}"
                if col in display_cols_c:
                    game_col_config_c[col] = st.column_config.LinkColumn(col, display_text="🎲")

            st.dataframe(
                df_country_games[display_cols_c],
                use_container_width=True,
                hide_index=True,
                column_config=game_col_config_c,
            )
        else:
            st.info(f"No games found vs {selected_country} with current filters.")

conn.close()
