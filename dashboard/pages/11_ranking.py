"""Belgian Carcassonne ranking — composite score + head-to-head Elo."""
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[2]))

DB_PATH = Path(__file__).parents[2] / "data" / "carcassonne.duckdb"

st.title("🏅 Belgian Ranking")

if not DB_PATH.exists():
    st.warning("No database found. Import data first.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

has_ranking = conn.execute(
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'player_ranking'"
).fetchone()[0]
has_h2h = conn.execute(
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'player_head2head_elo'"
).fetchone()[0]

tab_composite, tab_h2h = st.tabs(["Composite score", "Head-to-head Elo"])

# ═════════════════════════════════════════════════════════════════════════════
# Composite score tab
# ═════════════════════════════════════════════════════════════════════════════

with tab_composite:
    st.caption(
        "Elo = 1500 + BGA base (peak 0-60 + current 0-60, current halves per year of inactivity) "
        "+ recency-decayed bonuses (0.85^years) for BK live, BK online, WCC and national-team matches. "
        "Only Belgian players (country = BE)."
    )

    if not has_ranking:
        st.warning("Ranking not yet computed. Run `python scripts/compute_ranking.py`.")
    else:
        computed_at = conn.execute("SELECT MAX(computed_at) FROM player_ranking").fetchone()[0]
        if computed_at:
            st.caption(f"Computed: {computed_at:%Y-%m-%d %H:%M}")

        name_filter = st.text_input("Search player", "", key="composite_search")

        df = conn.execute(
            """
            SELECT pr.rank,
                   p.name,
                   p.country,
                   pr.ranking_elo               AS elo,
                   ROUND(pr.total_score, 1)     AS total,
                   ROUND(pr.bga_base, 1)        AS bga_base,
                   ROUND(pr.bk_live_bonus, 1)   AS bk_live,
                   ROUND(pr.bk_online_bonus, 1) AS bk_online,
                   ROUND(pr.wcc_bonus, 1)       AS wcc,
                   ROUND(pr.nations_bonus, 1)   AS nations,
                   pr.bga_games                 AS bga_games,
                   ROUND(pr.bga_peak_elo, 0)    AS bga_peak,
                   ROUND(pr.bga_current_elo, 0) AS bga_current,
                   p.id                         AS player_id
            FROM player_ranking pr
            JOIN players p ON p.id = pr.player_id
            WHERE pr.rank <= 100
              AND (? = '' OR lower(p.name) LIKE '%' || lower(?) || '%')
            ORDER BY pr.rank
            """,
            [name_filter, name_filter],
        ).fetchdf()

        st.dataframe(df.drop(columns=["player_id"]), hide_index=True, use_container_width=True)

        st.markdown("---")
        st.subheader("Breakdown per player")

        if len(df) == 0:
            st.info("No players match the filter.")
        else:
            player_options = {f"{row['name']} (rank {row['rank']})": row["player_id"]
                              for _, row in df.iterrows()}
            selected = st.selectbox("Player", list(player_options.keys()), key="composite_player")
            sel_id = player_options[selected]

            summary = conn.execute(
                """
                SELECT pr.ranking_elo, pr.rank, pr.total_score, pr.bga_base, pr.bga_peak_elo,
                       pr.bk_live_bonus, pr.bk_online_bonus, pr.wcc_bonus, pr.nations_bonus
                FROM player_ranking pr
                WHERE pr.player_id = ?
                """,
                [sel_id],
            ).fetchone()

            if summary:
                elo, rank, total, bga_base, bga_peak, bk_live, bk_online, wcc, nations = summary
                cols = st.columns(7)
                cols[0].metric("Elo", elo, f"rank {rank}")
                cols[1].metric("Total score", f"{total:.1f}")
                cols[2].metric("BGA base", f"{bga_base:.1f}",
                               f"peak {int(bga_peak) if bga_peak else '—'}")
                cols[3].metric("BK live", f"{bk_live:.1f}")
                cols[4].metric("BK online", f"{bk_online:.1f}")
                cols[5].metric("WCC", f"{wcc:.1f}")
                cols[6].metric("Nations", f"{nations:.1f}")

            events = conn.execute(
                """
                SELECT event_year, source, description,
                       ROUND(raw_points, 1) AS raw,
                       ROUND(decay, 2)      AS decay,
                       ROUND(points, 2)     AS points
                FROM player_ranking_events
                WHERE player_id = ?
                ORDER BY event_year DESC, source
                """,
                [sel_id],
            ).fetchdf()

            if len(events) == 0:
                st.info("No competitive events — BGA-only contribution.")
            else:
                st.dataframe(events, hide_index=True, use_container_width=True)

# ═════════════════════════════════════════════════════════════════════════════
# Head-to-head Elo tab
# ═════════════════════════════════════════════════════════════════════════════

with tab_h2h:
    st.caption(
        "Classical Elo (start 1500) updated game-by-game, only when BOTH players are Belgian. "
        "K-factors: BGA 8 · BCLC Swiss 24 · BCLC playoff 32 · BCOC 28."
    )

    if not has_h2h:
        st.warning("Head-to-head Elo not yet computed. Run `python scripts/compute_head2head_elo.py`.")
    else:
        computed_at = conn.execute("SELECT MAX(computed_at) FROM player_head2head_elo").fetchone()[0]
        if computed_at:
            st.caption(f"Computed: {computed_at:%Y-%m-%d %H:%M}")

        col_search, col_min = st.columns([2, 1])
        with col_search:
            h2h_search = st.text_input("Search player", "", key="h2h_search")
        with col_min:
            min_games = st.number_input("Min. games", min_value=0, value=20, step=5, key="h2h_min")

        df_h2h = conn.execute(
            """
            SELECT ROW_NUMBER() OVER (ORDER BY ph.rating DESC) AS rank,
                   p.name,
                   ROUND(ph.rating, 0)      AS elo,
                   ROUND(ph.peak_rating, 0) AS peak,
                   ph.peak_date             AS peak_date,
                   ph.games                 AS games,
                   ph.wins                  AS W,
                   ph.losses                AS L,
                   ph.draws                 AS D,
                   ph.last_played           AS last,
                   p.id                     AS player_id
            FROM player_head2head_elo ph
            JOIN players p ON p.id = ph.player_id
            WHERE ph.games >= ?
              AND (? = '' OR lower(p.name) LIKE '%' || lower(?) || '%')
            ORDER BY ph.rating DESC
            LIMIT 100
            """,
            [min_games, h2h_search, h2h_search],
        ).fetchdf()

        st.dataframe(df_h2h.drop(columns=["player_id"]), hide_index=True, use_container_width=True)

        st.markdown("---")
        st.subheader("Rating history per player")

        if len(df_h2h) == 0:
            st.info("No players match the filter.")
        else:
            h2h_options = {f"{row['name']} ({int(row['elo'])})": row["player_id"]
                           for _, row in df_h2h.iterrows()}
            h2h_selected = st.selectbox("Player", list(h2h_options.keys()), key="h2h_player")
            h2h_id = h2h_options[h2h_selected]

            history = conn.execute(
                """
                SELECT he.event_date,
                       he.source,
                       p.name                AS opponent,
                       he.result,
                       he.k_factor,
                       ROUND(he.rating_before, 0) AS before,
                       ROUND(he.rating_after, 0)  AS after,
                       ROUND(he.rating_after - he.rating_before, 1) AS delta,
                       CASE WHEN g.bga_table_id IS NOT NULL
                            THEN 'https://boardgamearena.com/table?table=' || g.bga_table_id
                            ELSE NULL END AS bga_link
                FROM player_head2head_events he
                JOIN players p ON p.id = he.opponent_id
                LEFT JOIN games g ON g.id = he.game_id
                WHERE he.player_id = ?
                ORDER BY he.event_date DESC, he.id DESC
                LIMIT 500
                """,
                [h2h_id],
            ).fetchdf()

            chart_df = conn.execute(
                """
                SELECT event_date, rating_after AS rating
                FROM player_head2head_events
                WHERE player_id = ? AND event_date IS NOT NULL
                ORDER BY event_date, id
                """,
                [h2h_id],
            ).fetchdf()

            if len(chart_df) > 0:
                st.line_chart(chart_df, x="event_date", y="rating", height=220)

            if len(history) > 0:
                st.dataframe(
                    history,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "bga_link": st.column_config.LinkColumn(
                            "BGA", display_text="open"
                        ),
                    },
                )
            else:
                st.info("No head-to-head events for this player.")

conn.close()
