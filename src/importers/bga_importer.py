"""
Importeer BGA Carcassonne speldata naar de lokale DuckDB database.

Gebruik:
    python -m src.importers.bga_importer \
        --email jouw@email.com \
        --password *** \
        --players 84216333 65246746 \
        --since 2022-01-01
"""

import argparse
import logging
import re
from datetime import datetime
from typing import Optional

import duckdb

from src.importers.bga_fetcher import BGASession

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

    # Parseer tijdstip
    played_at = parse_bga_date(str(game.get("start", "")))

    # Maak spel record aan (tournament_id=NULL voor niet-tornooi spellen)
    conn.execute(
        """
        INSERT INTO games (tournament_id, round, bga_table_id, played_at, source)
        VALUES (NULL, NULL, ?, ?, 'bga')
        """,
        [table_id, played_at],
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

        # ELO info (enkel beschikbaar voor de hoofdspeler in de ruwe API)
        elo_delta = None
        elo_after_val = None
        if i == 0:
            elo_win_str = str(game.get("elo_win", "") or "")
            elo_after_html = str(game.get("elo_after", "") or "")
            if elo_win_str.lstrip("-").isdigit():
                elo_delta = int(elo_win_str)
            m = re.search(r"gamerank_value[^>]*>(\d+)", elo_after_html)
            if m:
                elo_after_val = int(m.group(1))

        player_id = get_or_create_player(conn, bga_pid, name)
        conn.execute(
            """
            INSERT INTO game_players
                (game_id, player_id, score, rank, elo_delta, elo_after, conceded)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [game_id, player_id, score, rank, elo_delta, elo_after_val, concede and i != 0],
        )

    return True


# ---------------------------------------------------------------------------
# Hoofd import functie
# ---------------------------------------------------------------------------

def run_import(
    email: str,
    password: str,
    bga_player_ids: list[int],
    since: datetime,
    db_path: str = DB_PATH,
    chunk_days: int = 90,
    delay: float = 2.0,
) -> None:
    conn = duckdb.connect(db_path)

    # Schema migrations uitvoeren als nog niet gedaan
    for migration in ["migrations/001_initial_schema.sql", "migrations/002_bga_fields.sql"]:
        try:
            with open(migration) as f:
                conn.executescript(f.read())
        except Exception as e:
            logger.warning(f"Migration {migration}: {e}")

    logger.info("Verbinding met BGA ...")
    bga = BGASession(email, password)

    total_new = 0
    for player_id in bga_player_ids:
        logger.info(f"\nSpeler BGA {player_id} ophalen ...")
        games = bga.fetch_all_games(
            player_id, since=since, chunk_days=chunk_days, delay=delay
        )
        logger.info(f"  {len(games)} spellen opgehaald.")

        new_count = 0
        for game in games:
            if import_game(conn, game):
                new_count += 1
        logger.info(f"  {new_count} nieuwe spellen geïmporteerd.")
        total_new += new_count

    conn.close()
    logger.info(f"\nKlaar. {total_new} nieuwe spellen totaal geïmporteerd.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BGA Carcassonne data importeren")
    parser.add_argument("--email", required=True, help="BGA email")
    parser.add_argument("--password", required=True, help="BGA wachtwoord")
    parser.add_argument(
        "--players",
        nargs="+",
        type=int,
        required=True,
        help="BGA player IDs (bijv. --players 84216333 65246746)",
    )
    parser.add_argument(
        "--since",
        default="2020-01-01",
        help="Startdatum (YYYY-MM-DD), standaard 2020-01-01",
    )
    parser.add_argument("--db", default=DB_PATH, help="Pad naar DuckDB database")
    parser.add_argument(
        "--chunk-days", type=int, default=90, help="Dagvenster per API aanroep"
    )
    parser.add_argument(
        "--delay", type=float, default=2.0, help="Vertraging tussen API aanroepen (seconden)"
    )
    args = parser.parse_args()

    run_import(
        email=args.email,
        password=args.password,
        bga_player_ids=args.players,
        since=datetime.strptime(args.since, "%Y-%m-%d"),
        db_path=args.db,
        chunk_days=args.chunk_days,
        delay=args.delay,
    )
