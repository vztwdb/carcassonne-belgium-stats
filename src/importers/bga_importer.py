"""
Importeer BGA Carcassonne speldata naar de lokale DuckDB database.

Gebruik:
    python -m src.importers.bga_importer \
        --email jouw@email.com \
        --password *** \
        --players 84216333 65246746 \
        --since 2022-01-01
"""

import logging
import re
from datetime import datetime
from typing import Optional

import duckdb


logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = "data/carcassonne.duckdb"


# ---------------------------------------------------------------------------
# Hulpfuncties
# ---------------------------------------------------------------------------

def parse_bga_date(value: str) -> Optional[datetime]:
    """
    Zet BGA datum-string om naar datetime.
    BGA gebruikt formaten zoals "25-03-2026 om 20:22" of Unix timestamp.
    """
    if not value:
        return None
    # Probeer Unix timestamp (integer als string)
    if str(value).isdigit():
        return datetime.utcfromtimestamp(int(value))
    # Formaat: "25-03-2026 om 20:22"
    for fmt in ("%d-%m-%Y om %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    logger.warning(f"Onbekend datumformaat: {value!r}")
    return None


def get_or_create_player(conn: duckdb.DuckDBPyConnection, bga_id: str, name: str) -> int:
    """Zoek speler op via BGA ID, maak aan als nieuw."""
    row = conn.execute(
        "SELECT id FROM players WHERE bga_player_id = ?", [bga_id]
    ).fetchone()
    if row:
        return row[0]

    # Nieuwe speler aanmaken
    conn.execute(
        "INSERT INTO players (name, bga_player_id, country) VALUES (?, ?, 'BE')",
        [name, bga_id],
    )
    new_id = conn.execute(
        "SELECT id FROM players WHERE bga_player_id = ?", [bga_id]
    ).fetchone()[0]
    logger.info(f"  Nieuwe speler aangemaakt: {name} (BGA {bga_id}, intern ID {new_id})")
    return new_id


def import_game(conn: duckdb.DuckDBPyConnection, game: dict) -> bool:
    """
    Importeer één BGA spel naar de database.
    Geeft True terug als het spel nieuw was, False als het al bestond.
    """
    table_id = str(game.get("table_id", ""))
    if not table_id:
        return False

    # Controleer of het spel al bestaat (idempotent)
    existing = conn.execute(
        "SELECT id FROM games WHERE bga_table_id = ?", [table_id]
    ).fetchone()
    if existing:
        return False

    # Parseer tijdstippen en speelduur
    played_at  = parse_bga_date(str(game.get("start", "")))
    ended_at   = parse_bga_date(str(game.get("end",   "")))
    duration_min = None
    if played_at and ended_at:
        secs = (ended_at - played_at).total_seconds()
        if secs > 0:
            duration_min = int(secs // 60)

    unranked          = str(game.get("unranked",         "0")) == "1"
    normal_end        = str(game.get("normalend",        "1")) == "1"
    ranking_disabled  = str(game.get("ranking_disabled", "0")) == "1"

    conn.execute(
        """
        INSERT INTO games
            (tournament_id, round, bga_table_id, played_at, ended_at,
             duration_min, unranked, normal_end, ranking_disabled, source)
        VALUES (NULL, NULL, ?, ?, ?, ?, ?, ?, ?, 'bga')
        """,
        [table_id, played_at, ended_at, duration_min, unranked, normal_end, ranking_disabled],
    )
    game_id = conn.execute(
        "SELECT id FROM games WHERE bga_table_id = ?", [table_id]
    ).fetchone()[0]

    # Parseer spelers, scores, ranks
    player_ids_raw = [p.strip() for p in str(game.get("players", "")).split(",") if p.strip()]
    player_names_raw = [n.strip() for n in str(game.get("player_names", "")).split(",") if n.strip()]
    scores_raw = [s.strip() for s in str(game.get("scores", "")).split(",")]
    ranks_raw = [r.strip() for r in str(game.get("ranks", "")).split(",")]

    concede = str(game.get("concede", "0")) == "1"

    for i, bga_pid in enumerate(player_ids_raw):
        name = player_names_raw[i] if i < len(player_names_raw) else f"player_{bga_pid}"
        score = float(scores_raw[i]) if i < len(scores_raw) and scores_raw[i] else None
        rank = int(ranks_raw[i]) if i < len(ranks_raw) and ranks_raw[i].isdigit() else None

        # ELO en arena info (enkel voor speler i==0 beschikbaar in de API)
        elo_delta   = None
        elo_after_val = None
        elo_penalty_val = None
        arena_win_val   = None
        arena_after_val = None

        if i == 0:
            elo_win_str     = str(game.get("elo_win",     "") or "")
            elo_after_raw   = game.get("elo_after",   None)
            elo_penalty_raw = game.get("elo_penalty",  None)
            arena_win_raw   = game.get("arena_win",    None)
            arena_after_raw = game.get("arena_after",  None)

            if elo_win_str.lstrip("-").isdigit():
                elo_delta = int(elo_win_str)

            # elo_after kan HTML bevatten: <span class="gamerank_value">558</span>
            if elo_after_raw is not None:
                raw_str = str(elo_after_raw)
                m = re.search(r'gamerank_value[^>]*>(\d+)', raw_str)
                if m:
                    elo_after_val = int(m.group(1))
                else:
                    try:
                        elo_after_val = int(raw_str)
                    except (ValueError, TypeError):
                        m2 = re.search(r"(-?\d{1,5})", raw_str)
                        elo_after_val = int(m2.group(1)) if m2 else None

            # elo_penalty: gewoon integer
            if elo_penalty_raw is not None:
                try:
                    elo_penalty_val = int(elo_penalty_raw)
                except (ValueError, TypeError):
                    m = re.search(r"(-?\d{1,5})", str(elo_penalty_raw))
                    elo_penalty_val = int(m.group(1)) if m else None

            # arena_after formaat: "201.1528" → geheel=progressie, decimaal=arena ELO
            if arena_after_raw is not None:
                raw_str = str(arena_after_raw)
                if "." in raw_str:
                    parts = raw_str.split(".")
                    try:
                        arena_after_val = int(parts[1])  # arena ELO (bv. 1528)
                    except (ValueError, IndexError):
                        arena_after_val = None
                else:
                    try:
                        arena_after_val = int(raw_str)
                    except (ValueError, TypeError):
                        arena_after_val = None

            # arena_win formaat: "92.0014" → geheel=arena punten gewonnen
            if arena_win_raw is not None:
                raw_str = str(arena_win_raw)
                if "." in raw_str:
                    try:
                        arena_win_val = int(raw_str.split(".")[0]) > 0
                    except (ValueError, IndexError):
                        arena_win_val = None
                elif raw_str.isdigit():
                    arena_win_val = bool(int(raw_str))
                else:
                    arena_win_val = None

        player_id = get_or_create_player(conn, bga_pid, name)
        conn.execute(
            """
            INSERT INTO game_players
                (game_id, player_id, score, rank, elo_delta, elo_after,
                 elo_penalty, arena_win, arena_after, conceded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [game_id, player_id, score, rank, elo_delta, elo_after_val,
             elo_penalty_val, arena_win_val, arena_after_val, concede and i != 0],
        )

    return True


