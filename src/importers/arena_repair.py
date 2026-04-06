"""
Herstel arena_after waarden in de database.

BGA API geeft arena_after als "201.1528" waarbij:
- geheel (201) = seizoensprogressie (level*100 + punten)
- decimaal (1528) = echte arena ELO

De originele import sloeg alleen het geheel deel op (0-501).
Dit script haalt de games opnieuw op en corrigeert arena_after naar de echte ELO.

Gebruik:
    python -m src.importers.arena_repair --email jouw@email.com --password ***
"""

import asyncio
import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from src.importers.bga_fetcher import get_token_and_cookies, fetch_player_games

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

DB_PATH = Path("data/carcassonne.duckdb")


def repair_arena(email: str, password: str):
    conn = duckdb.connect(str(DB_PATH))

    # Vind spelers met foute arena data (< 1000 = oude progressie waarde, niet ELO)
    players = conn.execute("""
        SELECT DISTINCT p.bga_player_id, p.name
        FROM game_players gp
        JOIN players p ON p.id = gp.player_id
        WHERE gp.arena_after IS NOT NULL AND gp.arena_after < 1000
          AND p.bga_player_id IS NOT NULL
    """).fetchall()
    logger.info(f"{len(players)} spelers met foute arena data (< 1000) gevonden")

    # Login
    logger.info("Token ophalen via browser ...")
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
    token, cookies = loop.run_until_complete(
        get_token_and_cookies(email, password, int(players[0][0]), headless=True)
    )
    loop.close()
    logger.info(f"Token: {token[:8]}...")

    total_updated = 0
    for idx, (bga_pid, name) in enumerate(players, 1):
        logger.info(f"[{idx}/{len(players)}] {name} ({bga_pid}) ...")
        try:
            games = fetch_player_games(int(bga_pid), token, cookies)
            updated = 0
            for game in games:
                table_id = str(game.get("table_id", ""))
                arena_after_raw = game.get("arena_after")

                if not table_id or arena_after_raw is None:
                    continue

                # Parse arena ELO uit "201.1528" → 1528
                raw_str = str(arena_after_raw)
                arena_elo = None
                if "." in raw_str:
                    parts = raw_str.split(".")
                    try:
                        arena_elo = int(parts[1])
                    except (ValueError, IndexError):
                        pass

                if arena_elo is None:
                    continue

                # Parse arena_win uit "92.0014"
                arena_win = None
                arena_win_raw = game.get("arena_win")
                if arena_win_raw is not None:
                    win_str = str(arena_win_raw)
                    if "." in win_str:
                        try:
                            arena_win = int(win_str.split(".")[0]) > 0
                        except (ValueError, IndexError):
                            pass

                result = conn.execute("""
                    UPDATE game_players
                    SET arena_after = ?, arena_win = ?
                    WHERE game_id = (SELECT id FROM games WHERE bga_table_id = ?)
                      AND player_id = (SELECT id FROM players WHERE bga_player_id = ?)
                """, [arena_elo, arena_win, table_id, bga_pid])
                if result.fetchone()[0] > 0:
                    updated += 1

            total_updated += updated
            logger.info(f"  ✅ {updated} games bijgewerkt ({len(games)} opgehaald)")
        except Exception as e:
            logger.error(f"  ❌ Fout: {e}")

    conn.close()
    logger.info(f"\nKlaar! {total_updated} arena scores gecorrigeerd")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Herstel arena ELO waarden")
    parser.add_argument("--email", default=os.environ.get("BGA_EMAIL"))
    parser.add_argument("--password", default=os.environ.get("BGA_PASSWORD"))
    args = parser.parse_args()

    if not args.email or not args.password:
        print("Gebruik: python -m src.importers.arena_repair --email X --password Y")
        sys.exit(1)

    repair_arena(args.email, args.password)
