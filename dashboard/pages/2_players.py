"""Spelers overzicht pagina."""
import json
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[2]))

DATA_DIR = Path(__file__).parents[2] / "data"
DB_PATH = DATA_DIR / "carcassonne.duckdb"
SEASONS_PATH = DATA_DIR / "arena_seasons.json"

ELO_BASE = 1300

with open(SEASONS_PATH) as f:
    ARENA_SEASONS = json.load(f)["seasons"]

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

col_a, col_b, col_c, col_d, col_e = st.columns([2, 2, 2, 2, 2])

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
    season_options = {
        "Alle seizoenen": None,
        **{
            f"S{s['season']}" + (f" – {s['label']}" if "label" in s else ""):
            s for s in reversed(ARENA_SEASONS)
        },
    }
    selected_season_name = st.selectbox("Arena seizoen", list(season_options.keys()))
    selected_season = season_options[selected_season_name]

with col_d:
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

with col_e:
    search = st.text_input("Zoek speler", placeholder="Naam...")

only_elo = st.checkbox("Spelers met ELO", value=True, key="only_elo")

# ── Statistieken per speler ───────────────────────────────────────────────────

season_start = selected_season["start_date"] if selected_season else None
season_end = selected_season["end_date"] if selected_season else None

df = conn.execute("""
    WITH filtered_games AS (
        SELECT id FROM games
        WHERE boardgame_id = ? AND unranked = false
          AND (? = 0 OR YEAR(COALESCE(ended_at, played_at)) = ?)
          AND (? IS NULL OR COALESCE(ended_at, played_at) >= CAST(? AS DATE))
          AND (? IS NULL OR COALESCE(ended_at, played_at) < CAST(? AS DATE))
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
    ),
    last_arena AS (
        SELECT player_id, arena_after
        FROM (
            SELECT
                gp.player_id,
                gp.arena_after,
                ROW_NUMBER() OVER (
                    PARTITION BY gp.player_id
                    ORDER BY COALESCE(g.ended_at, g.played_at) DESC
                ) AS rn
            FROM game_players gp
            JOIN games g ON g.id = gp.game_id
            WHERE gp.arena_after IS NOT NULL
              AND g.id IN (SELECT id FROM filtered_games)
        )
        WHERE rn = 1
    ),
    game_info AS (
        SELECT
            g.id AS game_id,
            (SELECT COUNT(*) FROM game_players gp2 WHERE gp2.game_id = g.id) AS num_players,
            g.duration_min,
            (SELECT MAX(gp2.score) FROM game_players gp2 WHERE gp2.game_id = g.id) AS max_score
        FROM games g
        WHERE g.id IN (SELECT id FROM filtered_games)
    )
    SELECT
        p.id,
        p.name,
        p.country,
        p.bga_player_id,
        COUNT(gp.id)                                        AS spellen,
        ROUND(100.0 * SUM(CASE WHEN gp.rank = 1 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gp.rank IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS win_pct,
        MAX(gp.elo_after)                                   AS max_elo,
        le.elo_after                                        AS last_elo,
        MAX(gp.arena_after)                                 AS max_arena,
        la.arena_after                                      AS last_arena,
        ROUND(100.0 * SUM(CASE WHEN gi.num_players = 2 THEN 1 ELSE 0 END)
              / COUNT(gp.id), 1)                            AS pct_2p,
        ROUND(100.0 * SUM(CASE WHEN gi.duration_min IS NOT NULL AND gi.duration_min <= 60 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gi.duration_min IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS pct_rt,
        ROUND(100.0 * SUM(CASE WHEN gi.max_score < 160 THEN 1 ELSE 0 END)
              / NULLIF(SUM(CASE WHEN gi.max_score IS NOT NULL THEN 1 ELSE 0 END), 0), 1) AS pct_basis
    FROM players p
    JOIN game_players gp ON gp.player_id = p.id
                         AND gp.game_id IN (SELECT id FROM filtered_games)
    JOIN game_info gi ON gi.game_id = gp.game_id
    LEFT JOIN last_elo le ON le.player_id = p.id
    LEFT JOIN last_arena la ON la.player_id = p.id
    WHERE (? = false OR EXISTS (
        SELECT 1 FROM game_players gp3
        JOIN games g3 ON g3.id = gp3.game_id
        WHERE gp3.player_id = p.id AND gp3.elo_after IS NOT NULL
          AND g3.boardgame_id = ?
    ))
    GROUP BY p.id, p.name, p.country, p.bga_player_id, le.elo_after, la.arena_after
    ORDER BY spellen DESC NULLS LAST

""", [selected_bg_id,
      0 if selected_year == "Alle jaren" else int(selected_year),
      0 if selected_year == "Alle jaren" else int(selected_year),
      season_start, season_start,
      season_end, season_end,
      only_elo, selected_bg_id]).df()

conn.close()

# ELO-correctie: opgeslagen waarden zijn relatief t.o.v. 1300
for col in ("max_elo", "last_elo"):
    df[col] = df[col].where(df[col].isna(), df[col] - ELO_BASE)

# ── Client-side filters ───────────────────────────────────────────────────────

if selected_season:
    df = df[df["last_arena"].notna() & (df["max_arena"] != 1500)]

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
                  "max_elo", "last_elo", "max_arena", "last_arena",
                  "pct_2p", "pct_rt", "pct_basis"]].rename(columns={
    "name":       "Naam",
    "country":    "Land",
    "spellen":    "Spellen",
    "win_pct":    "Win%",
    "max_elo":    "Max ELO",
    "last_elo":   "Laatste ELO",
    "max_arena":  "Max Arena",
    "last_arena": "Laatste Arena",
    "pct_2p":     "% 2P",
    "pct_rt":     "% Realtime",
    "pct_basis":  "% Basis",
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
