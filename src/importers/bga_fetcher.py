"""
Fetch Carcassonne game data from BoardGameArena via Playwright (headless browser).

Playwright logt automatisch in op BGA en haalt speldata op via de interne API.
Geen manuele stappen nodig — volledig geautomatiseerd.

Gebruik:
    python -m src.importers.bga_fetcher

Of via environment variabelen:
    BGA_EMAIL=... BGA_PASSWORD=... python -m src.importers.bga_fetcher
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

BGA_URL = "https://en.boardgamearena.com"
CARCASSONNE_GAME_ID = 1
CHUNK_DAYS = 90
REQUEST_DELAY = 1.2  # seconden tussen API aanroepen
SESSION_PATH = Path("data/bga_session")


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

async def login(page: Page, email: str, password: str) -> None:
    """
    Log in op BGA. Tweestapslogin:
      Stap 1: email invullen in het login-formulier → klik de blauwe Next-link
      Stap 2: wachtwoord invullen → klik Login
    """
    logger.info("Navigeren naar BGA login ...")
    await page.goto(f"{BGA_URL}/account", wait_until="networkidle")

    # Cookie banner wegklikken indien aanwezig
    try:
        await page.click("button:has-text('Reject all')", timeout=4000)
        logger.info("  Cookie banner gesloten.")
    except Exception:
        pass

    # ----------------------------------------------------------------
    # Stap 1: email invullen
    # Het login-formulier bevat een <form class="space-y-2"> met de
    # "Next" knop als <a class="bga-button-inner ...">
    # We targetten de email-input BINNEN die form.
    # ----------------------------------------------------------------
    login_form = page.locator("form:has(a.bga-button-inner)").first
    await login_form.wait_for(state="visible", timeout=10000)

    email_input = login_form.locator("input[name='email']").first
    await email_input.fill(email)

    # Klik de Next link (de blauwe knop)
    await login_form.locator("a.bga-button-inner").click()
    logger.info("  Stap 1 (email) ingediend, wachten op wachtwoordveld ...")

    # ----------------------------------------------------------------
    # Stap 2: wachtwoord invullen
    # Na Next verschijnt een wachtwoordveld. BGA rendert het als
    # type="password" of soms als type="text" met autocomplete="password".
    # ----------------------------------------------------------------
    pwd_locator = page.locator(
        "input[type='password'], input[autocomplete='current-password'], "
        "input[autocomplete='password']"
    ).first
    try:
        await pwd_locator.wait_for(state="visible", timeout=10000)
    except Exception:
        await page.screenshot(path="data/bga_debug_step2.png")
        raise ValueError("Wachtwoordveld niet gevonden na Next. Screenshot: data/bga_debug_step2.png")

    await pwd_locator.fill(password)

    # Klik de blauwe "Login" knop (exacte tekst, blauwe klasse)
    await page.locator("a.bga-button--blue:has-text('Login')").click()
    logger.info("  Stap 2 (wachtwoord) ingediend ...")

    # ----------------------------------------------------------------
    # Wacht tot we weg zijn van de login stap, navigeer dan naar gamestats
    # ----------------------------------------------------------------
    await asyncio.sleep(2)  # korte wachttijd voor redirect/animatie

    # Als we nog op account pagina zitten: navigeer direct naar gamestats
    if "/account" in page.url:
        await page.goto("https://boardgamearena.com/gamestats", wait_until="networkidle", timeout=20000)

    logger.info(f"BGA login geslaagd. URL: {page.url}")


# ---------------------------------------------------------------------------
# Games ophalen via browser fetch (zelfde als console aanpak)
# ---------------------------------------------------------------------------

async def fetch_games_chunk(
    page: Page,
    player_id: int,
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """Roep de BGA gamestats API aan vanuit de browser context (al ingelogd)."""

    js = """
    async ([playerId, startTs, endTs, gameId]) => {
        const params = new URLSearchParams({
            player: playerId,
            opponent_id: 0,
            game_id: gameId,
            finished: 1,
            updateStats: 0,
            start_date: startTs,
            end_date: endTs,
            'dojo.preventCache': Date.now()
        });
        const url = `/gamestats/gamestats/getGames.html?${params}`;
        const r = await fetch(url, {credentials: 'include'});
        const data = await r.json();
        if (Array.isArray(data)) return data;
        if (data.data) return data.data;
        if (data.games) return data.games;
        if (data.status === '0') throw new Error(data.error || 'BGA fout: ' + JSON.stringify(data));
        return [];
    }
    """

    result = await page.evaluate(js, [player_id, start_ts, end_ts, CARCASSONNE_GAME_ID])
    return result or []


async def fetch_all_games(
    page: Page,
    player_id: int,
    since: datetime,
    until: Optional[datetime] = None,
) -> list[dict]:
    """Haal alle Carcassonne spellen op voor een speler via datum-chunks."""
    until = until or datetime.utcnow()
    all_games: list[dict] = []
    seen: set[str] = set()

    cursor = since
    while cursor < until:
        end = min(cursor + timedelta(days=CHUNK_DAYS), until)
        logger.info(f"  {cursor.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')} ...")

        chunk = await fetch_games_chunk(
            page,
            player_id,
            int(cursor.timestamp()),
            int(end.timestamp()),
        )

        new = 0
        for game in chunk:
            tid = str(game.get("table_id", ""))
            if tid and tid not in seen:
                seen.add(tid)
                all_games.append(game)
                new += 1

        logger.info(f"    {new} nieuw | totaal: {len(all_games)}")
        cursor = end
        await asyncio.sleep(REQUEST_DELAY)

    return all_games


# ---------------------------------------------------------------------------
# Hoofd functie
# ---------------------------------------------------------------------------

async def fetch_and_save(
    email: str,
    password: str,
    player_ids: list[int],
    since: datetime,
    output_path: Path = Path("data/raw/bga_games.json"),
    headless: bool = True,
) -> dict[str, list]:
    """
    Log in op BGA en haal alle Carcassonne spellen op voor de opgegeven spelers.
    Slaat het resultaat op als JSON.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    results: dict[str, list] = {}

    async with async_playwright() as pw:
        # Gebruik opgeslagen sessie indien beschikbaar, anders login via credentials
        if SESSION_PATH.exists():
            logger.info(f"Opgeslagen sessie laden uit {SESSION_PATH} ...")
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(SESSION_PATH),
                headless=headless,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            )
            page = await context.new_page()
            await page.goto("https://boardgamearena.com/gamestats", wait_until="networkidle", timeout=20000)
        else:
            if not email or not password:
                raise ValueError(
                    f"Geen opgeslagen sessie gevonden in {SESSION_PATH}.\n"
                    "Voer eerst uit: python scripts/bga_save_session.py"
                )
            logger.info("Geen opgeslagen sessie — inloggen met credentials ...")
            browser = await pw.chromium.launch(headless=headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()
            await login(page, email, password)
            if "en.boardgamearena.com" in page.url:
                await page.goto("https://boardgamearena.com/gamestats", wait_until="networkidle", timeout=20000)

        logger.info(f"Klaar voor API-calls op: {page.url}")

        for player_id in player_ids:
            logger.info(f"\nSpeler {player_id} ophalen ...")
            games = await fetch_all_games(page, player_id, since=since)
            results[str(player_id)] = games
            logger.info(f"✓ {len(games)} spellen voor speler {player_id}")

        await browser.close()

    # Opslaan
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"\nOpgeslagen in {output_path} ({sum(len(v) for v in results.values())} spellen totaal)")

    return results


# ---------------------------------------------------------------------------
# CLI / directe uitvoering
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    parser = argparse.ArgumentParser(description="BGA spellen ophalen via headless browser")
    parser.add_argument("--email",    default=os.environ.get("BGA_EMAIL"))
    parser.add_argument("--password", default=os.environ.get("BGA_PASSWORD"))
    parser.add_argument("--players",  nargs="+", type=int, default=[93464744, 84635111, 65246746])
    parser.add_argument("--since",    default="2020-01-01")
    parser.add_argument("--output",   default="data/raw/bga_games.json")
    parser.add_argument("--visible",  action="store_true", help="Browser zichtbaar tonen (voor debuggen)")
    args = parser.parse_args()

    if not SESSION_PATH.exists() and (not args.email or not args.password):
        print("Geen opgeslagen sessie gevonden. Voer eerst uit: python scripts/bga_save_session.py")
        print("Of geef --email en --password mee.")
        exit(1)

    asyncio.run(fetch_and_save(
        email=args.email,
        password=args.password,
        player_ids=args.players,
        since=datetime.strptime(args.since, "%Y-%m-%d"),
        output_path=Path(args.output),
        headless=not args.visible,
    ))
