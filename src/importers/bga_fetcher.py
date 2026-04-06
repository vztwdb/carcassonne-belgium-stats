"""
Fetch Carcassonne game data from BoardGameArena.

Aanpak:
1. Playwright logt in en laadt de gamestats pagina
2. Extraheer x-request-token + sessiecookies
3. requests doet de rest (sneller, geen browser overhead)
4. Paginering via page=1,2,3... (niet datumchunks)
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

BGA_BASE        = "https://boardgamearena.com"
CARCASSONNE_ID  = 1
REQUEST_DELAY   = 1.2
SESSION_PATH    = Path("data/bga_session")


# ── Stap 1: login + token ophalen via Playwright ──────────────────────────────

async def get_token_and_cookies(
    email: str,
    password: str,
    player_id: int,
    headless: bool = True,
) -> tuple[str, dict]:
    """
    Log in op BGA, laad de gamestats pagina van player_id,
    extraheer x-request-token en sessiecookies.
    Geeft (token, cookies_dict) terug.
    """
    async with async_playwright() as pw:
        if SESSION_PATH.exists():
            logger.info("Opgeslagen sessie laden ...")
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(SESSION_PATH),
                headless=headless,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            )
            page = await context.new_page()
        else:
            logger.info("Inloggen met credentials ...")
            browser = await pw.chromium.launch(headless=headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            )
            page = await context.new_page()
            await _login(page, email, password)

        # Laad de gamestats pagina — dit zet de token in de HTML
        stats_url = f"{BGA_BASE}/gamestats?player={player_id}&game_id={CARCASSONNE_ID}"
        logger.info(f"Laden: {stats_url}")
        await page.goto(stats_url, wait_until="networkidle", timeout=30000)

        # Controleer of we ingelogd zijn (redirect naar login = niet ingelogd)
        if "/account" in page.url:
            raise ValueError(
                "Niet ingelogd. Voer eerst uit: python scripts/bga_save_session.py\n"
                "Of geef --email en --password mee."
            )

        # Extraheer x-request-token uit de paginabron
        html = await page.content()
        token = _extract_token(html)
        if not token:
            raise ValueError("x-request-token niet gevonden in de pagina. Pagina gewijzigd?")
        logger.info(f"Token gevonden: {token[:8]}...")

        # Haal alle cookies op en zet ze klaar voor requests
        pw_cookies = await context.cookies()
        cookies = {c["name"]: c["value"] for c in pw_cookies if "boardgamearena" in c["domain"]}

        await context.close()
        return token, cookies


def _extract_token(html: str) -> Optional[str]:
    """Zoek x-request-token in BGA pagina HTML."""
    patterns = [
        r'requestToken\s*[=:]\s*["\']([a-zA-Z0-9_-]{8,})["\']',
        r'"token"\s*:\s*"([a-zA-Z0-9_-]{8,})"',
        r'x-request-token["\']?\s*[=:,]\s*["\']([a-zA-Z0-9_-]{8,})["\']',
        r'var\s+requestToken\s*=\s*["\']([a-zA-Z0-9_-]{8,})["\']',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


async def _login(page, email: str, password: str) -> None:
    """Tweestapslogin op BGA (email → Next → wachtwoord → Login)."""
    await page.goto(f"{BGA_BASE}/account", wait_until="networkidle")
    try:
        await page.click("button:has-text('Reject all')", timeout=4000)
    except Exception:
        pass

    login_form = page.locator("form:has(a.bga-button-inner)").first
    await login_form.wait_for(state="visible", timeout=10000)
    await login_form.locator("input[name='email']").first.fill(email)
    await login_form.locator("a.bga-button-inner").click()

    pwd = page.locator("input[type='password']").first
    await pwd.wait_for(state="visible", timeout=10000)
    await pwd.fill(password)
    await page.locator("a.bga-button--blue:has-text('Login')").click()

    await asyncio.sleep(3)
    # Klik Let's play! via JS (popup na login)
    await page.evaluate(
        "() => { const b = [...document.querySelectorAll('a')].find(e => e.innerText.includes(\"Let's play\")); if(b) b.click(); }"
    )
    await asyncio.sleep(2)
    logger.info(f"Ingelogd. URL: {page.url}")


# ── Stap 2: spellen ophalen via requests + token ──────────────────────────────

def fetch_player_games(
    player_id: int,
    token: str,
    cookies: dict,
    delay: float = REQUEST_DELAY,
    since: Optional[datetime] = None,
) -> list[dict]:
    """
    Haal Carcassonne spellen op voor een speler via paginering.
    Gebruikt x-request-token header + sessiecookies.
    Als since is opgegeven, stopt zodra alle spellen op een pagina ouder zijn.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "x-request-token": token,
        "Accept":          "application/json, text/javascript, */*",
        "Referer":         f"{BGA_BASE}/gamestats?player={player_id}&game_id={CARCASSONNE_ID}",
        "X-Requested-With": "XMLHttpRequest",
    })
    session.cookies.update(cookies)

    all_games: list[dict] = []
    seen: set[str] = set()
    page_num = 1

    while True:
        params = {
            "player":              player_id,
            "opponent_id":         0,
            "game_id":             CARCASSONNE_ID,
            "finished":            0,      # 0 = alle spellen
            "page":                page_num,
            "updateStats":         0,
            "dojo.preventCache":   int(time.time() * 1000),
        }
        logger.info(f"  Pagina {page_num} ...")
        time.sleep(delay)

        resp = session.get(f"{BGA_BASE}/gamestats/gamestats/getGames.html", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == 0 or data.get("status") == "0":
            raise ValueError(f"BGA fout: {data.get('error')}")

        # Structuur: {"status":1, "data": {"tables": [...], "stats": []}}
        inner = data.get("data", {})
        games = inner.get("tables", []) if isinstance(inner, dict) else inner

        if not games:
            logger.info(f"  Geen spellen meer op pagina {page_num}, klaar.")
            break

        # Log alle velden van het eerste spel (eenmalig)
        if page_num == 1 and games:
            logger.info(f"  === RAW API FIELDS (eerste spel) ===")
            for k, v in sorted(games[0].items()):
                logger.info(f"    {k}: {v!r}")
            logger.info(f"  === END RAW API FIELDS ===")

        new = 0
        stop_early = False
        for g in games:
            tid = str(g.get("table_id", ""))
            if tid and tid not in seen:
                # Check if game is older than since cutoff
                if since:
                    end_raw = str(g.get("end", "") or "")
                    start_raw = str(g.get("start", "") or "")
                    game_ts = None
                    for raw in (end_raw, start_raw):
                        if raw and raw.isdigit():
                            game_ts = datetime.utcfromtimestamp(int(raw))
                            break
                    if game_ts and game_ts < since:
                        stop_early = True
                        break

                seen.add(tid)
                all_games.append(g)
                new += 1

        logger.info(f"    {new} nieuw | totaal: {len(all_games)}")

        if stop_early:
            logger.info(f"  Spellen ouder dan {since.date()} bereikt, stoppen.")
            break

        page_num += 1

    return all_games


# ── Hoofd functie ──────────────────────────────────────────────────────────────

async def fetch_and_save(
    email: str,
    password: str,
    player_ids: list[int],
    output_path: Path = Path("data/raw/bga_games.json"),
    headless: bool = True,
    delay: float = REQUEST_DELAY,
) -> dict[str, list]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Gebruik de eerste speler om in te loggen en de token te halen
    logger.info("Token ophalen via browser ...")
    token, cookies = await get_token_and_cookies(
        email, password, player_ids[0], headless=headless
    )

    results: dict[str, list] = {}
    for player_id in player_ids:
        logger.info(f"\nSpeler {player_id} ophalen ...")
        try:
            games = fetch_player_games(player_id, token, cookies, delay=delay)
            results[str(player_id)] = games
            logger.info(f"✓ {len(games)} spellen voor speler {player_id}")
        except Exception as e:
            logger.error(f"Fout voor speler {player_id}: {e}")
            results[str(player_id)] = []

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    total = sum(len(v) for v in results.values())
    logger.info(f"\nKlaar. {total} spellen opgeslagen in {output_path}")
    return results


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    parser = argparse.ArgumentParser(description="BGA Carcassonne data ophalen")
    parser.add_argument("--email",    default=os.environ.get("BGA_EMAIL"))
    parser.add_argument("--password", default=os.environ.get("BGA_PASSWORD"))
    parser.add_argument("--players",  nargs="+", type=int, default=[93464744, 84635111, 65246746])
    parser.add_argument("--output",   default="data/raw/bga_games.json")
    parser.add_argument("--delay",    type=float, default=REQUEST_DELAY)
    parser.add_argument("--visible",  action="store_true")
    args = parser.parse_args()

    if not SESSION_PATH.exists() and (not args.email or not args.password):
        print("Geen sessie gevonden. Voer eerst: python scripts/bga_save_session.py")
        exit(1)

    asyncio.run(fetch_and_save(
        email=args.email or "",
        password=args.password or "",
        player_ids=args.players,
        output_path=Path(args.output),
        headless=not args.visible,
        delay=args.delay,
    ))
