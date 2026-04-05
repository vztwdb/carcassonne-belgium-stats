"""Spelers overzicht pagina."""
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[2]))

DB_PATH = Path(__file__).parents[2] / "data" / "carcassonne.duckdb"

ELO_BASE = 1300

st.title("👤 Spelers")

if not DB_PATH.exists():
    st.warning("Geen database gevonden. Importeer eerst data via de Import pagina.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Filters ───────────────────────────────────────────────────────────────────

boardgames = conn.execute("SELECT id, name FROM boardgames ORDER BY name").fetchall()
if not boardgames:
    st.info("Nog geen bordspellen in de database.")
    conn.close()
    st.stop()

col_a, col_b, col_c, col_d = st.columns([2, 2, 2, 2])

with col_a:
    bg_options = {name: bg_id for bg_id, name in boardgames}
    selected_bg_name = st.selectbox("Bordspel", list(bg_options.keys()))
    selected_bg_id = bg_options[selected_bg_name]

with col_b:
    available_years = conn.execute("""
        SELECT DISTINCT YEAR(COALESCE(ended_at, played_at)) AS yr
        FROM games
        WHERE boardgame_id = ? AND unranked = false
          AND COALESCE(ended_at, played_at) IS NOT NULL
        ORDER BY yr DESC
    """, [selected_bg_id]).fetchall()
    year_options = ["Alle jaren"] + [str(r[0]) for r in available_years]
    selected_year = st.selectbox("Jaar", year_options)

with col_c:
    available_countries = conn.execute("""
        SELECT DISTINCT p.country
        FROM players p
        JOIN game_players gp ON gp.player_id = p.id
        JOIN games g ON g.id = gp.game_id
        WHERE g.boardgame_id = ? AND g.unranked = false
          AND p.country IS NOT NULL
        ORDER BY p.country
    """, [selected_bg_id]).fetchall()
    country_options = ["Alle landen"] + [r[0] for r in available_countries]
    selected_country = st.selectbox("Land", country_options)

with col_d:
    search = st.text_input("Zoek speler", placeholder="Naam...")

# ── Statistieken per speler ───────────────────────────────────────────────────

df = conn.execute("""
    WITH filtered_games AS (
        SELECT id FROM games
        WHERE boardgame_id = ? AND unranked = false
          AND (? = 0 OR YEAR(COALESCE(ended_at, played_at)) = ?)
    ),
    last_elo AS (
        SELECT player_id, elo_after
        FROM (
            SELECT
                gp.player_id,
                gp.elo_after,
                ROW_NUMBER() OVER (
                    PARTITION BY gp.player_id
                    ORDER BY COALESCE(g.ended_at, g.played_at) DESC
                ) AS rn
            FROM game_players gp
            JOIN games g ON g.id = gp.game_id
            WHERE gp.elo_after IS NOT NULL
              AND g.id IN (SELECT id FROM filtered_games)
        )
        WHERE rn = 1
    )
    SELECT
        p.id,
        p.name,
        p.country,
        p.bga_player_id,
        COUNT(gp.id)                                        AS spellen,
        ROUND(100.0 * SUM(CASE WHEN gp.elo_delta > 0 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gp.elo_delta IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS win_pct,
        ROUND(AVG(gp.score), 1)                             AS gem_score,
        MAX(gp.elo_after)                                   AS max_elo,
        le.elo_after                                        AS last_elo
    FROM players p
    JOIN game_players gp ON gp.player_id = p.id
                         AND gp.game_id IN (SELECT id FROM filtered_games)
    LEFT JOIN last_elo le ON le.player_id = p.id
    GROUP BY p.id, p.name, p.country, p.bga_player_id, le.elo_after
    ORDER BY spellen DESC NULLS LAST

""", [selected_bg_id, 0 if selected_year == "Alle jaren" else int(selected_year),
      0 if selected_year == "Alle jaren" else int(selected_year)]).df()

conn.close()

# ELO-correctie: opgeslagen waarden zijn relatief t.o.v. 1300
for col in ("max_elo", "last_elo"):
    df[col] = df[col].where(df[col].isna(), df[col] - ELO_BASE)

# ── Client-side filters ───────────────────────────────────────────────────────

if selected_country != "Alle landen":
    df = df[df["country"] == selected_country]

if search:
    df = df[df["name"].str.contains(search, case=False, na=False)]

if df.empty:
    st.info(f"Geen spelers gevonden voor {selected_bg_name}.")
    st.stop()

st.caption(f"{len(df)} speler(s)")

# ── Navigate on selection from previous rerun ────────────────────────────────

if "go_to_player" in st.session_state:
    st.session_state["player_detail_id"] = st.session_state.pop("go_to_player")
    st.session_state["player_detail_bg"] = st.session_state.pop("go_to_bg", selected_bg_id)
    st.switch_page("pages/3_player_detail.py")

# ── Tabel ─────────────────────────────────────────────────────────────────────

df["BGA"] = df["bga_player_id"].apply(
    lambda x: f"https://boardgamearena.com/player?id={x}" if x else None,
)

display_df = df[["name", "BGA", "country", "spellen", "win_pct",
                  "gem_score", "max_elo", "last_elo"]].rename(columns={
    "name":      "Naam",
    "country":   "Land",
    "spellen":   "Spellen",
    "win_pct":   "Win%",
    "gem_score": "Gem. score",
    "max_elo":   "Max ELO",
    "last_elo":  "Laatste ELO",
})

event = st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="players_table",
    column_config={
        "BGA": st.column_config.LinkColumn("BGA", display_text="🎲"),
    },
)

if event.selection and event.selection.rows:
    selected_row = event.selection.rows[0]
    sel_player_id = int(df.iloc[selected_row]["id"])
    st.session_state["go_to_player"] = sel_player_id
    st.session_state["go_to_bg"] = selected_bg_id
    st.rerun()
