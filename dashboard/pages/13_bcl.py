"""BCL (Belgian Carcassonne League) — multi-tier league competition.

Each season has four leagues (ML/GL/SL/BL), played as best-of-3 duels.
The season champion is the winner of the Cross Final between the two
Master League group winners.
"""
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parents[2] / "data"
DB_PATH = DATA_DIR / "carcassonne.duckdb"

BGA_PLAYER_URL = "https://boardgamearena.com/player?id="
BGA_TABLE_URL = "https://boardgamearena.com/table?table="

TIER_ORDER = ["ML", "GL", "SL", "BL"]
TIER_LABELS = {
    "ML": "Master League",
    "GL": "Gold League",
    "SL": "Silver League",
    "BL": "Bronze League",
}

st.title("🏟️ Belgian Carcassonne League (BCL)")

if not DB_PATH.exists():
    st.warning("No database found.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Season selector ──────────────────────────────────────────────────────────

seasons = conn.execute(
    """
    SELECT REGEXP_REPLACE(name, ' (ML|GL|SL|BL)$', '') AS season,
           MIN(date_start) AS dstart
    FROM tournaments
    WHERE type = 'BCL'
    GROUP BY 1
    ORDER BY dstart DESC, season DESC
    """
).fetchall()

if not seasons:
    st.info("No BCL data yet.")
    conn.close()
    st.stop()

season_labels = [s[0] for s in seasons]
selected_season = st.selectbox("Season", season_labels)

season_rows = conn.execute(
    """
    SELECT id, name, edition, date_start, date_end,
           CASE
               WHEN name LIKE '% ML' THEN 'ML'
               WHEN name LIKE '% GL' THEN 'GL'
               WHEN name LIKE '% SL' THEN 'SL'
               WHEN name LIKE '% BL' THEN 'BL'
           END AS tier
    FROM tournaments
    WHERE type = 'BCL' AND name LIKE ? || ' %'
    """,
    [selected_season],
).fetchall()

tournaments = {row[5]: {"id": row[0], "name": row[1], "edition": row[2],
                          "date_start": row[3], "date_end": row[4]}
               for row in season_rows if row[5]}

if "ML" not in tournaments:
    st.warning(f"No Master League found for {selected_season}.")
    conn.close()
    st.stop()

ml_tid = tournaments["ML"]["id"]

# ── Season Champion banner ───────────────────────────────────────────────────

cross = conn.execute(
    """
    SELECT p1.name, p2.name, tm.score_1, tm.score_2, tm.result
    FROM tournament_matches tm
    JOIN players p1 ON p1.id = tm.player_1_id
    JOIN players p2 ON p2.id = tm.player_2_id
    WHERE tm.tournament_id = ? AND tm.stage = 'Cross Final'
    LIMIT 1
    """,
    [ml_tid],
).fetchone()

champs = conn.execute(
    """
    SELECT t.edition, p.name
    FROM tournament_participants tp
    JOIN tournaments t ON t.id = tp.tournament_id
    JOIN players p ON p.id = tp.player_id
    WHERE t.type = 'BCL' AND tp.final_rank = 1
      AND t.name LIKE ? || ' %'
    """,
    [selected_season],
).fetchall()
champ_by_tier = {row[0].split()[-1]: row[1] for row in champs}

champion = None
if cross:
    n1, n2, s1, s2, res = cross
    if res == "1":
        champion, runner_up = n1, n2
    elif res == "2":
        champion, runner_up = n2, n1
    else:
        runner_up = None
    if champion:
        st.success(
            f"### 🏆 Season Champion: **{champion}**  \n"
            f"Cross Final: **{n1} {int(s1)}–{int(s2)} {n2}**"
        )
    else:
        st.info(f"Cross Final in progress: {n1} vs {n2}")
else:
    st.info("Cross Final not yet played.")

# ── Tabs ─────────────────────────────────────────────────────────────────────

tabs = st.tabs(["Overview"] + [TIER_LABELS[t] for t in TIER_ORDER if t in tournaments])

with tabs[0]:
    tids = [t["id"] for t in tournaments.values()]
    placeholders = ",".join(["?"] * len(tids))

    total_matches = conn.execute(
        f"SELECT COUNT(*) FROM tournament_matches WHERE tournament_id IN ({placeholders})",
        tids,
    ).fetchone()[0]
    total_players = conn.execute(
        f"""
        SELECT COUNT(DISTINCT player_id)
        FROM tournament_participants
        WHERE tournament_id IN ({placeholders})
        """,
        tids,
    ).fetchone()[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Leagues", len(tournaments))
    c2.metric("Matches", total_matches)
    c3.metric("Players", total_players)
    c4.metric("Season Champion", champion or "—")

    st.subheader("Tier summary")
    summary_df = conn.execute(
        f"""
        SELECT
            CASE
                WHEN t.name LIKE '% ML' THEN 'Master'
                WHEN t.name LIKE '% GL' THEN 'Gold'
                WHEN t.name LIKE '% SL' THEN 'Silver'
                WHEN t.name LIKE '% BL' THEN 'Bronze'
            END AS "Tier",
            MAX(CASE WHEN tp.final_rank = 1 THEN p.name END) AS "Champion",
            MAX(CASE WHEN tp.final_rank = 2 THEN p.name END) AS "Runner-up",
            COUNT(DISTINCT tp.player_id) AS "Players",
            (SELECT COUNT(*) FROM tournament_matches tm WHERE tm.tournament_id = t.id) AS "Matches"
        FROM tournaments t
        LEFT JOIN tournament_participants tp ON tp.tournament_id = t.id
        LEFT JOIN players p ON p.id = tp.player_id
        WHERE t.id IN ({placeholders})
        GROUP BY t.id, t.name
        ORDER BY
            CASE
                WHEN t.name LIKE '% ML' THEN 1
                WHEN t.name LIKE '% GL' THEN 2
                WHEN t.name LIKE '% SL' THEN 3
                WHEN t.name LIKE '% BL' THEN 4
            END
        """,
        tids,
    ).fetchdf()
    st.dataframe(summary_df, hide_index=True, use_container_width=True)

    if cross:
        st.caption(
            f"Cross Final: {cross[0]} {int(cross[2])}–{int(cross[3])} {cross[1]}"
        )


def render_league(tab, tier: str):
    tid = tournaments[tier]["id"]
    with tab:
        n_matches = conn.execute(
            "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ?", [tid]
        ).fetchone()[0]
        n_players = conn.execute(
            "SELECT COUNT(*) FROM tournament_participants WHERE tournament_id = ?", [tid]
        ).fetchone()[0]
        champ_name = champ_by_tier.get(tier) or "—"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tier", TIER_LABELS[tier])
        c2.metric("Players", n_players)
        c3.metric("Matches", n_matches)
        c4.metric("Champion", champ_name)

        # Final ranking with row-click drill-down (mirrors the BCOC page).
        st.subheader("Final ranking")
        df_rank = conn.execute(
            """
            SELECT p.id                       AS player_id,
                   tp.final_rank              AS "Rank",
                   p.name                     AS "Player",
                   p.bga_player_id            AS bga_player_id,
                   tp.duels_played            AS "DP",
                   tp.duels_won               AS "W",
                   tp.duels_lost              AS "L",
                   tp.games_won               AS "GW",
                   tp.games_lost              AS "GL",
                   tp.games_won - tp.games_lost AS "GDiff",
                   ROUND(tp.win_pct * 100, 1) AS "Win%"
            FROM tournament_participants tp
            JOIN players p ON p.id = tp.player_id
            WHERE tp.tournament_id = ?
            ORDER BY tp.final_rank NULLS LAST
            """,
            [tid],
        ).fetchdf()
        df_rank["BGA"] = df_rank["bga_player_id"].apply(
            lambda x: f"{BGA_PLAYER_URL}{x}" if x else None
        )
        display_cols = ["Rank", "Player", "BGA", "DP", "W", "L",
                          "GW", "GL", "GDiff", "Win%"]

        ss_key = f"bcl_{tier}_players_table"
        detail_id_key = f"bcl_{tier}_detail_player_id"
        detail_name_key = f"bcl_{tier}_detail_player_name"

        event = st.dataframe(
            df_rank[display_cols],
            hide_index=True,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            key=ss_key,
            column_config={
                "BGA": st.column_config.LinkColumn("BGA", display_text="🎲"),
            },
        )

        if event.selection and event.selection.rows:
            sel_row = event.selection.rows[0]
            st.session_state[detail_id_key] = int(df_rank.iloc[sel_row]["player_id"])
            st.session_state[detail_name_key] = df_rank.iloc[sel_row]["Player"]

        # Player drill-down: all the player's BCL games in this tournament.
        detail_pid = st.session_state.get(detail_id_key)
        if detail_pid:
            detail_name = st.session_state.get(detail_name_key, str(detail_pid))
            st.subheader(f"All {TIER_LABELS[tier]} matches — {detail_name}")
            if st.button("Clear player selection", key=f"bcl_{tier}_clear"):
                st.session_state.pop(detail_id_key, None)
                st.session_state.pop(detail_name_key, None)
                st.rerun()
            render_player_matches(tid, detail_pid, detail_name)
        else:
            st.caption("Click a player row to see their match details and games.")

        # All matches
        st.subheader("All matches")
        df_matches = conn.execute(
            """
            SELECT
                tm.stage    AS "Stage",
                tm.notes    AS "Round",
                p1.name     AS "Player 1",
                tm.score_1  AS "S1",
                tm.score_2  AS "S2",
                p2.name     AS "Player 2",
                CASE tm.result
                    WHEN '1' THEN p1.name
                    WHEN '2' THEN p2.name
                    ELSE 'Draw'
                END AS "Winner"
            FROM tournament_matches tm
            JOIN players p1 ON p1.id = tm.player_1_id
            JOIN players p2 ON p2.id = tm.player_2_id
            WHERE tm.tournament_id = ?
            ORDER BY
                CASE tm.stage
                    WHEN 'Cross Final' THEN 0
                    WHEN 'Final'       THEN 1
                    WHEN '1/2 Finals'  THEN 2
                    WHEN '1/4 Finals'  THEN 3
                    WHEN 'Round of 16' THEN 4
                    ELSE 9
                END,
                tm.stage,
                tm.match_number NULLS LAST
            """,
            [tid],
        ).fetchdf()
        st.dataframe(df_matches, hide_index=True, use_container_width=True)


def render_player_matches(tid: int, pid: int, pname: str):
    """Render a player's BCL match list with per-game scores + BGA links."""
    df = conn.execute(
        """
        SELECT
            tm.stage AS "Stage",
            tm.notes AS "Round",
            CASE WHEN tm.player_1_id = ? THEN p2.name ELSE p1.name END AS "Opponent",
            CASE WHEN tm.player_1_id = ? THEN tm.score_1 ELSE tm.score_2 END AS "Won",
            CASE WHEN tm.player_1_id = ? THEN tm.score_2 ELSE tm.score_1 END AS "Lost",
            CASE
                WHEN (tm.player_1_id = ? AND tm.result = '1')
                  OR (tm.player_2_id = ? AND tm.result = '2') THEN 'W'
                WHEN tm.result = 'D' THEN 'D'
                ELSE 'L'
            END AS "Result",
            g1.bga_table_id AS bga_id_1,
            g2.bga_table_id AS bga_id_2,
            g3.bga_table_id AS bga_id_3,
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
        JOIN players p1 ON p1.id = tm.player_1_id
        JOIN players p2 ON p2.id = tm.player_2_id
        LEFT JOIN games g1 ON g1.id = tm.game_id_1
        LEFT JOIN games g2 ON g2.id = tm.game_id_2
        LEFT JOIN games g3 ON g3.id = tm.game_id_3
        LEFT JOIN game_players gp1_p1 ON gp1_p1.game_id = tm.game_id_1 AND gp1_p1.player_id = tm.player_1_id
        LEFT JOIN game_players gp1_p2 ON gp1_p2.game_id = tm.game_id_1 AND gp1_p2.player_id = tm.player_2_id
        LEFT JOIN game_players gp2_p1 ON gp2_p1.game_id = tm.game_id_2 AND gp2_p1.player_id = tm.player_1_id
        LEFT JOIN game_players gp2_p2 ON gp2_p2.game_id = tm.game_id_2 AND gp2_p2.player_id = tm.player_2_id
        LEFT JOIN game_players gp3_p1 ON gp3_p1.game_id = tm.game_id_3 AND gp3_p1.player_id = tm.player_1_id
        LEFT JOIN game_players gp3_p2 ON gp3_p2.game_id = tm.game_id_3 AND gp3_p2.player_id = tm.player_2_id
        WHERE tm.tournament_id = ?
          AND (tm.player_1_id = ? OR tm.player_2_id = ?)
        ORDER BY
            CASE tm.stage
                WHEN 'Cross Final' THEN 0
                WHEN 'Final'       THEN 1
                WHEN '1/2 Finals'  THEN 2
                WHEN '1/4 Finals'  THEN 3
                ELSE 9
            END,
            tm.stage,
            tm.match_number NULLS LAST
        """,
        [pid] * 11 + [tid, pid, pid],
    ).fetchdf()

    if df.empty:
        st.info("No matches found for this player.")
        return

    # Build score and BGA link columns per game.
    for i in range(1, 4):
        bga_col = f"bga_id_{i}"
        link_col = f"Game {i}"
        score_col = f"Score {i}"
        df[link_col] = df[bga_col].apply(
            lambda x: f"{BGA_TABLE_URL}{x}" if pd.notna(x) and x else None
        )
        me_col = f"score_me_{i}"
        opp_col = f"score_opp_{i}"
        df[score_col] = df.apply(
            lambda row, m=me_col, o=opp_col: (
                f"{int(row[m])} - {int(row[o])}"
                if pd.notna(row[m]) and pd.notna(row[o]) else None
            ), axis=1,
        )

    display_cols = [
        "Stage", "Round", "Opponent", "Result", "Won", "Lost",
        "Score 1", "Game 1", "Score 2", "Game 2", "Score 3", "Game 3",
    ]
    game_col_config = {
        f"Game {i}": st.column_config.LinkColumn(f"Game {i}", display_text="🎲")
        for i in range(1, 4)
    }
    st.dataframe(
        df[display_cols],
        hide_index=True,
        use_container_width=True,
        column_config=game_col_config,
    )


for tier, tab in zip([t for t in TIER_ORDER if t in tournaments], tabs[1:]):
    render_league(tab, tier)

conn.close()
