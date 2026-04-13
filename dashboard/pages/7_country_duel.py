"""Country duel — compare line-ups of two countries based on historical BGA games."""
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parents[2] / "data"
DB_PATH = DATA_DIR / "carcassonne.duckdb"

BGA_PLAYER_URL = "https://boardgamearena.com/player?id="
BGA_TABLE_URL = "https://boardgamearena.com/table?table="
ELO_BASE = 1300

COUNTRY_NAMES = {
    "AD": "Andorra", "AE": "VAE", "AF": "Afghanistan", "AG": "Antigua en Barbuda",
    "AI": "Anguilla", "AL": "Albanië", "AM": "Armenië", "AO": "Angola",
    "AQ": "Antarctica", "AR": "Argentinië", "AS": "Amerikaans-Samoa",
    "AT": "Oostenrijk", "AU": "Australië", "AW": "Aruba", "AX": "Åland",
    "AZ": "Azerbeidzjan", "BA": "Bosnië en Herzegovina", "BB": "Barbados",
    "BD": "Bangladesh", "BE": "België", "BF": "Burkina Faso", "BG": "Bulgarije",
    "BH": "Bahrein", "BI": "Burundi", "BJ": "Benin", "BL": "Saint-Barthélemy",
    "BM": "Bermuda", "BN": "Brunei", "BO": "Bolivia", "BQ": "Caribisch Nederland",
    "BR": "Brazilië", "BS": "Bahama's", "BT": "Bhutan", "BV": "Bouveteiland",
    "BW": "Botswana", "BY": "Wit-Rusland", "BZ": "Belize", "CA": "Canada",
    "CC": "Cocoseilanden", "CD": "Congo-Kinshasa", "CF": "Centraal-Afrikaanse Republiek",
    "CG": "Congo-Brazzaville", "CH": "Zwitserland", "CI": "Ivoorkust",
    "CK": "Cookeilanden", "CL": "Chili", "CM": "Kameroen", "CN": "China",
    "CO": "Colombia", "CR": "Costa Rica", "CU": "Cuba", "CV": "Kaapverdië",
    "CW": "Curaçao", "CX": "Christmaseiland", "CY": "Cyprus", "CZ": "Tsjechië",
    "DE": "Duitsland", "DJ": "Djibouti", "DK": "Denemarken", "DM": "Dominica",
    "DO": "Dominicaanse Republiek", "DZ": "Algerije", "EC": "Ecuador",
    "EE": "Estland", "EG": "Egypte", "EH": "Westelijke Sahara", "ER": "Eritrea",
    "ES": "Spanje", "ET": "Ethiopië", "FI": "Finland", "FJ": "Fiji",
    "FK": "Falklandeilanden", "FM": "Micronesië", "FO": "Faeröer",
    "FR": "Frankrijk", "GA": "Gabon", "GB": "Verenigd Koninkrijk",
    "GD": "Grenada", "GE": "Georgië", "GF": "Frans-Guyana", "GG": "Guernsey",
    "GH": "Ghana", "GI": "Gibraltar", "GL": "Groenland", "GM": "Gambia",
    "GN": "Guinee", "GP": "Guadeloupe", "GQ": "Equatoriaal-Guinea",
    "GR": "Griekenland", "GS": "Zuid-Georgia", "GT": "Guatemala", "GU": "Guam",
    "GW": "Guinee-Bissau", "GY": "Guyana", "HK": "Hongkong",
    "HM": "Heard en McDonaldeilanden", "HN": "Honduras", "HR": "Kroatië",
    "HT": "Haïti", "HU": "Hongarije", "ID": "Indonesië", "IE": "Ierland",
    "IL": "Israël", "IM": "Man", "IN": "India", "IO": "Brits Indische Oceaanterr.",
    "IQ": "Irak", "IR": "Iran", "IS": "IJsland", "IT": "Italië",
    "JE": "Jersey", "JM": "Jamaica", "JO": "Jordanië", "JP": "Japan",
    "KE": "Kenia", "KG": "Kirgizië", "KH": "Cambodja", "KI": "Kiribati",
    "KM": "Comoren", "KN": "Saint Kitts en Nevis", "KP": "Noord-Korea",
    "KR": "Zuid-Korea", "KW": "Koeweit", "KY": "Kaaimaneilanden", "KZ": "Kazachstan",
    "LA": "Laos", "LB": "Libanon", "LC": "Saint Lucia", "LI": "Liechtenstein",
    "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho", "LT": "Litouwen",
    "LU": "Luxemburg", "LV": "Letland", "LY": "Libië", "MA": "Marokko",
    "MC": "Monaco", "MD": "Moldavië", "ME": "Montenegro", "MF": "Saint-Martin",
    "MG": "Madagaskar", "MH": "Marshalleilanden", "MK": "Noord-Macedonië",
    "ML": "Mali", "MM": "Myanmar", "MN": "Mongolië", "MO": "Macau",
    "MP": "Noordelijke Marianen", "MQ": "Martinique", "MR": "Mauritanië",
    "MS": "Montserrat", "MT": "Malta", "MU": "Mauritius", "MV": "Maldiven",
    "MW": "Malawi", "MX": "Mexico", "MY": "Maleisië", "MZ": "Mozambique",
    "NA": "Namibië", "NC": "Nieuw-Caledonië", "NE": "Niger", "NF": "Norfolk",
    "NG": "Nigeria", "NI": "Nicaragua", "NL": "Nederland", "NO": "Noorwegen",
    "NP": "Nepal", "NR": "Nauru", "NU": "Niue", "NZ": "Nieuw-Zeeland",
    "OM": "Oman", "PA": "Panama", "PE": "Peru", "PF": "Frans-Polynesië",
    "PG": "Papoea-Nieuw-Guinea", "PH": "Filipijnen", "PK": "Pakistan",
    "PL": "Polen", "PM": "Saint-Pierre en Miquelon", "PN": "Pitcairneilanden",
    "PR": "Puerto Rico", "PS": "Palestina", "PT": "Portugal", "PW": "Palau",
    "PY": "Paraguay", "QA": "Qatar", "RE": "Réunion", "RO": "Roemenië",
    "RS": "Servië", "RU": "Rusland", "RW": "Rwanda", "SA": "Saoedi-Arabië",
    "SB": "Salomonseilanden", "SC": "Seychellen", "SD": "Soedan", "SE": "Zweden",
    "SG": "Singapore", "SH": "Sint-Helena", "SI": "Slovenië",
    "SJ": "Spitsbergen en Jan Mayen", "SK": "Slowakije", "SL": "Sierra Leone",
    "SM": "San Marino", "SN": "Senegal", "SO": "Somalië", "SR": "Suriname",
    "SS": "Zuid-Soedan", "ST": "Sao Tomé en Principe", "SV": "El Salvador",
    "SX": "Sint Maarten", "SY": "Syrië", "SZ": "Eswatini", "TC": "Turks- en Caicoseilanden",
    "TD": "Tsjaad", "TF": "Franse Zuidelijke Gebieden", "TG": "Togo",
    "TH": "Thailand", "TJ": "Tadzjikistan", "TK": "Tokelau", "TL": "Oost-Timor",
    "TM": "Turkmenistan", "TN": "Tunesië", "TO": "Tonga", "TR": "Turkije",
    "TT": "Trinidad en Tobago", "TV": "Tuvalu", "TW": "Taiwan", "TZ": "Tanzania",
    "UA": "Oekraïne", "UG": "Oeganda", "UK": "Verenigd Koninkrijk",
    "UM": "Amerikaanse kleine eilanden", "US": "Verenigde Staten", "UY": "Uruguay",
    "UZ": "Oezbekistan", "VA": "Vaticaanstad", "VC": "Saint Vincent en de Grenadines",
    "VE": "Venezuela", "VG": "Britse Maagdeneilanden", "VI": "Amerikaanse Maagdeneilanden",
    "VN": "Vietnam", "VU": "Vanuatu", "WF": "Wallis en Futuna", "WS": "Samoa",
    "XX": "Onbekend", "YE": "Jemen", "YT": "Mayotte", "ZA": "Zuid-Afrika",
    "ZM": "Zambia", "ZW": "Zimbabwe", "UNKNOWN": "Onbekend",
}


def country_label(code: str) -> str:
    name = COUNTRY_NAMES.get(code)
    return f"{code} ({name})" if name else code

st.title("🆚 Country Duel")
st.caption("Pick a line-up for each country and see all historical games between these players.")

if not DB_PATH.exists():
    st.warning("No database found.")
    st.stop()

conn = duckdb.connect(str(DB_PATH), read_only=True)

# ── Boardgame & country filters ──────────────────────────────────────────────

boardgames = conn.execute("SELECT id, name FROM boardgames ORDER BY name").fetchall()
bg_options = {name: bg_id for bg_id, name in boardgames}

countries = conn.execute("""
    SELECT DISTINCT p.country
    FROM players p
    JOIN game_players gp ON gp.player_id = p.id
    JOIN games g ON g.id = gp.game_id
    WHERE p.country IS NOT NULL AND g.unranked = false
    ORDER BY p.country
""").fetchall()
country_list = [r[0] for r in countries]

selected_bg_id = bg_options.get("Carcassonne", 1)

col_a, col_b = st.columns([2, 2])

with col_a:
    default_a = country_list.index("BE") if "BE" in country_list else 0
    country_a = st.selectbox("Country A", country_list, index=default_a, format_func=country_label)

with col_b:
    other_countries = [c for c in country_list if c != country_a]
    country_b = st.selectbox("Country B", other_countries, format_func=country_label)

col_nt_a, col_nt_b = st.columns(2)
with col_nt_a:
    only_nt_a = st.checkbox(
        f"National team only ({country_a})",
        value=(country_a == "BE"),
        key="cd_only_nt_a",
    )
with col_nt_b:
    only_nt_b = st.checkbox(
        f"National team only ({country_b})",
        value=(country_b == "BE"),
        key="cd_only_nt_b",
    )

# ── Player pickers per country ───────────────────────────────────────────────

def load_players(country: str, only_nt: bool) -> pd.DataFrame:
    nt_clause = "AND p.national_team = TRUE" if only_nt else ""
    return conn.execute(f"""
        SELECT
            p.id,
            p.name,
            COUNT(gp.id) AS games
        FROM players p
        JOIN game_players gp ON gp.player_id = p.id
        JOIN games g ON g.id = gp.game_id
        WHERE p.country = ?
          AND g.boardgame_id = ?
          AND g.unranked = false
          {nt_clause}
        GROUP BY p.id, p.name
        ORDER BY games DESC, p.name
    """, [country, selected_bg_id]).df()


players_a = load_players(country_a, only_nt_a)
players_b = load_players(country_b, only_nt_b)

label_a = {int(row["id"]): f"{row['name']} ({int(row['games'])})" for _, row in players_a.iterrows()}
label_b = {int(row["id"]): f"{row['name']} ({int(row['games'])})" for _, row in players_b.iterrows()}

col_pa, col_pb = st.columns(2)
with col_pa:
    st.markdown(f"### Line-up {country_a}")
    lineup_a = st.multiselect(
        f"Players {country_a}",
        options=list(label_a.keys()),
        format_func=lambda pid: label_a[pid],
        key="cd_lineup_a",
    )
with col_pb:
    st.markdown(f"### Line-up {country_b}")
    lineup_b = st.multiselect(
        f"Players {country_b}",
        options=list(label_b.keys()),
        format_func=lambda pid: label_b[pid],
        key="cd_lineup_b",
    )

if not lineup_a or not lineup_b:
    st.info("Select at least one player for each country.")
    conn.close()
    st.stop()

# ── Common games: games where at least one A-player and one B-player played ──

placeholders_a = ",".join(["?"] * len(lineup_a))
placeholders_b = ",".join(["?"] * len(lineup_b))

common_games_sql = f"""
    WITH games_with_a AS (
        SELECT DISTINCT gp.game_id, gp.player_id AS a_player_id
        FROM game_players gp
        JOIN games g ON g.id = gp.game_id
        WHERE gp.player_id IN ({placeholders_a})
          AND g.boardgame_id = ?
          AND g.unranked = false
    ),
    games_with_b AS (
        SELECT DISTINCT gp.game_id, gp.player_id AS b_player_id
        FROM game_players gp
        JOIN games g ON g.id = gp.game_id
        WHERE gp.player_id IN ({placeholders_b})
          AND g.boardgame_id = ?
          AND g.unranked = false
    )
    SELECT ga.game_id, ga.a_player_id, gb.b_player_id
    FROM games_with_a ga
    JOIN games_with_b gb ON gb.game_id = ga.game_id
"""

pairs_df = conn.execute(
    common_games_sql,
    [*lineup_a, selected_bg_id, *lineup_b, selected_bg_id],
).df()

if pairs_df.empty:
    st.warning("No common games found for these line-ups.")
    conn.close()
    st.stop()

unique_game_ids = pairs_df["game_id"].unique().tolist()

# ── Summary metrics ──────────────────────────────────────────────────────────

summary_placeholders_a = ",".join(["?"] * len(lineup_a))
summary_placeholders_b = ",".join(["?"] * len(lineup_b))
game_placeholders = ",".join(["?"] * len(unique_game_ids))

# Per A/B pair: compute wins based on score comparison across the pair rows
scores_df = conn.execute(f"""
    SELECT
        gp.game_id,
        gp.player_id,
        gp.score,
        gp.rank
    FROM game_players gp
    WHERE gp.game_id IN ({game_placeholders})
      AND gp.player_id IN ({summary_placeholders_a + ',' + summary_placeholders_b})
""", [*unique_game_ids, *lineup_a, *lineup_b]).df()

set_a = set(lineup_a)
set_b = set(lineup_b)

# For each pair row (one per (game, a_player, b_player)), compare scores
pair_rows = pairs_df.merge(
    scores_df.rename(columns={"player_id": "a_player_id", "score": "score_a", "rank": "rank_a"}),
    on=["game_id", "a_player_id"],
).merge(
    scores_df.rename(columns={"player_id": "b_player_id", "score": "score_b", "rank": "rank_b"}),
    on=["game_id", "b_player_id"],
)


def pair_outcome(row):
    if pd.isna(row["score_a"]) or pd.isna(row["score_b"]):
        return None
    if row["score_a"] > row["score_b"]:
        return "A"
    if row["score_a"] < row["score_b"]:
        return "B"
    return "D"


pair_rows["outcome"] = pair_rows.apply(pair_outcome, axis=1)

total_pairs = len(pair_rows)
wins_a = (pair_rows["outcome"] == "A").sum()
wins_b = (pair_rows["outcome"] == "B").sum()
draws = (pair_rows["outcome"] == "D").sum()

st.subheader("Overview")
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Games", len(unique_game_ids))
m2.metric("Pair encounters", total_pairs)
m3.metric(f"Wins {country_a}", int(wins_a))
m4.metric(f"Wins {country_b}", int(wins_b))
m5.metric("Draws", int(draws))

# ── Head-to-head matrix ──────────────────────────────────────────────────────

st.subheader("Head-to-head matrix")
st.caption("Each cell shows W-L from the perspective of the Country A player (row).")

name_a = {int(row["id"]): row["name"] for _, row in players_a.iterrows()}
name_b = {int(row["id"]): row["name"] for _, row in players_b.iterrows()}

matrix = {}
for a_id in lineup_a:
    row = {}
    for b_id in lineup_b:
        sub = pair_rows[(pair_rows["a_player_id"] == a_id) & (pair_rows["b_player_id"] == b_id)]
        w = (sub["outcome"] == "A").sum()
        l = (sub["outcome"] == "B").sum()
        row[name_b.get(b_id, str(b_id))] = f"{w}-{l}" if len(sub) else ""
    matrix[name_a.get(a_id, str(a_id))] = row

matrix_df = pd.DataFrame.from_dict(matrix, orient="index")
matrix_df.index.name = f"{country_a} \\ {country_b}"

# Use first column as the index-display column so it can be clicked
display_matrix = matrix_df.reset_index().rename(columns={"index": f"{country_a} \\ {country_b}"})

matrix_event = st.dataframe(
    display_matrix,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode=["single-row", "single-column"],
    key="cd_matrix",
)

a_row_names = list(matrix_df.index)
b_col_names = list(matrix_df.columns)

filter_a_ids = set(lineup_a)
filter_b_ids = set(lineup_b)
filter_label_parts = []

if matrix_event.selection and matrix_event.selection.rows:
    sel_row = matrix_event.selection.rows[0]
    sel_a_name = a_row_names[sel_row]
    filter_a_ids = {pid for pid, nm in name_a.items() if nm == sel_a_name and pid in set_a}
    filter_label_parts.append(f"{country_a}: {sel_a_name}")

if matrix_event.selection and matrix_event.selection.columns:
    sel_col = matrix_event.selection.columns[0]
    if sel_col in b_col_names:
        sel_b_name = sel_col
        filter_b_ids = {pid for pid, nm in name_b.items() if nm == sel_b_name and pid in set_b}
        filter_label_parts.append(f"{country_b}: {sel_b_name}")

# ── Games list ───────────────────────────────────────────────────────────────

if filter_label_parts:
    st.subheader("Games — " + " / ".join(filter_label_parts))
    st.caption("Clear the matrix selection to see all games again.")
else:
    st.subheader("Games")

games_detail = conn.execute(f"""
    SELECT
        g.id AS game_id,
        COALESCE(g.ended_at, g.played_at) AS datum,
        g.bga_table_id,
        gp.player_id,
        p.name AS player_name,
        p.country AS player_country,
        gp.score,
        gp.rank
    FROM games g
    JOIN game_players gp ON gp.game_id = g.id
    JOIN players p ON p.id = gp.player_id
    WHERE g.id IN ({game_placeholders})
    ORDER BY datum DESC, g.id, gp.rank NULLS LAST, gp.score DESC NULLS LAST
""", unique_game_ids).df()

# Restrict to games matching the matrix selection (if any)
matching_pairs = pair_rows[
    pair_rows["a_player_id"].isin(filter_a_ids)
    & pair_rows["b_player_id"].isin(filter_b_ids)
]
filtered_game_ids = set(matching_pairs["game_id"].unique().tolist())
games_detail = games_detail[games_detail["game_id"].isin(filtered_game_ids)]

# Build a compact per-game summary row
rows = []
for gid, grp in games_detail.groupby("game_id", sort=False):
    grp = grp.sort_values(["rank", "score"], ascending=[True, False], na_position="last")
    a_part = grp[grp["player_id"].isin(set_a)]
    b_part = grp[grp["player_id"].isin(set_b)]

    def fmt(sub):
        return ", ".join(
            f"{row['player_name']} ({int(row['score']) if pd.notna(row['score']) else '?'})"
            for _, row in sub.iterrows()
        )

    datum = grp["datum"].iloc[0]
    bga = grp["bga_table_id"].iloc[0]

    # Winner determination: highest score across all players in game
    max_score = grp["score"].max()
    winners = grp[grp["score"] == max_score]
    if len(winners) == 1:
        w_pid = winners.iloc[0]["player_id"]
        if w_pid in set_a:
            winner = country_a
        elif w_pid in set_b:
            winner = country_b
        else:
            winner = "Other"
    else:
        winner = "Draw"

    rows.append({
        "Date": datum,
        f"{country_a} line-up": fmt(a_part),
        f"{country_b} line-up": fmt(b_part),
        "Winner": winner,
        "BGA": f"{BGA_TABLE_URL}{bga}" if bga else None,
    })

games_df = pd.DataFrame(rows)

st.dataframe(
    games_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "BGA": st.column_config.LinkColumn("BGA", display_text="🎲"),
    },
)

conn.close()
