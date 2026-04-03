"""
Fetch Carcassonne game data from BoardGameArena (BGA).

BGA has no official API. This module uses internal endpoints discovered
through reverse engineering. Use responsibly:
- Always add a delay between requests (default 1.5s)
- Only fetch data for your own community/players
- Do not run bulk imports during peak hours

BGA Carcassonne game_id = 1
"""

import time
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BGA_BASE = "https://boardgamearena.com"
CARCASSONNE_GAME_ID = 1


class BGASession:
    """Authenticated BGA session."""

    def __init__(self, email: str, password: str):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/javascript, */*",
            "Referer": BGA_BASE,
        })
        self._login(email, password)

    def _login(self, email: str, password: str) -> None:
        # Stap 1: haal de login pagina op om het CSRF token te bemachtigen
        resp = self.session.get(f"{BGA_BASE}/account")
        resp.raise_for_status()

        # requestToken zit als JS variabele in de HTML
        match = re.search(r'"requestToken"\s*:\s*"([^"]+)"', resp.text)
        if not match:
            # Alternatieve locatie
            match = re.search(r"requestToken['\"]?\s*[=:]\s*['\"]([^'\"]+)['\"]", resp.text)
        if not match:
            raise ValueError("Kon requestToken niet vinden op BGA login pagina.")

        csrf_token = match.group(1)

        # Stap 2: inloggen
        resp = self.session.post(
            f"{BGA_BASE}/account/account/login.html",
            data={
                "email": email,
                "password": password,
                "rememberme": "on",
                "request_token": csrf_token,
            },
        )
        resp.raise_for_status()

        try:
            data = resp.json()
        except Exception:
            raise ValueError("BGA login antwoord is geen JSON — mogelijk geblokkeerd.")

        if data.get("status") != 1:
            raise ValueError(f"BGA login mislukt: {data.get('error', data)}")

        logger.info("BGA login geslaagd.")

    # ------------------------------------------------------------------
    # Lage-niveau API aanroep
    # ------------------------------------------------------------------

    def _get_games_chunk(
        self,
        player_id: int,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        delay: float = 1.5,
    ) -> list[dict]:
        """Één API aanroep voor een specifiek tijdsvenster."""
        time.sleep(delay)

        params: dict = {
            "player": player_id,
            "opponent_id": 0,       # 0 = alle tegenstanders
            "game_id": CARCASSONNE_GAME_ID,
            "finished": 1,          # enkel afgewerkte spellen
            "updateStats": 0,
            "dojo.preventCache": int(time.time() * 1000),
        }
        if start_ts:
            params["start_date"] = start_ts
        if end_ts:
            params["end_date"] = end_ts

        resp = self.session.get(
            f"{BGA_BASE}/gamestats/gamestats/getGames.html",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()

        data = resp.json()

        # Response kan een lijst zijn of gewrapped in een dict
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Probeer bekende keys
            for key in ("data", "games", "items"):
                if key in data:
                    return data[key]
            # Als er geen bekende key is maar er wel game-data lijkt te zijn
            if "table_id" in data:
                return [data]  # enkel object
        return []

    # ------------------------------------------------------------------
    # Hoge-niveau: alle spellen ophalen via datum-chunks
    # ------------------------------------------------------------------

    def fetch_all_games(
        self,
        player_id: int,
        since: datetime,
        until: Optional[datetime] = None,
        chunk_days: int = 90,
        delay: float = 1.5,
    ) -> list[dict]:
        """
        Haal alle Carcassonne spellen op voor een speler.

        Gebruikt datum-chunks om paginatie te omzeilen — BGA limiteert
        het aantal resultaten per aanroep, maar bij een smal tijdsvenster
        komen alle spellen terug.

        Args:
            player_id:   BGA player ID
            since:       Startdatum (bijv. datetime(2020, 1, 1))
            until:       Einddatum (standaard: nu)
            chunk_days:  Grootte van elk tijdsvenster in dagen
            delay:       Wachttijd tussen aanroepen in seconden
        """
        until = until or datetime.utcnow()
        all_games: list[dict] = []
        seen_ids: set[str] = set()

        cursor = since
        while cursor < until:
            end = min(cursor + timedelta(days=chunk_days), until)
            logger.info(
                f"  Ophalen {cursor.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')} "
                f"voor speler {player_id} ..."
            )
            chunk = self._get_games_chunk(
                player_id,
                start_ts=int(cursor.timestamp()),
                end_ts=int(end.timestamp()),
                delay=delay,
            )
            new = 0
            for game in chunk:
                tid = str(game.get("table_id", ""))
                if tid and tid not in seen_ids:
                    seen_ids.add(tid)
                    all_games.append(game)
                    new += 1
            logger.info(f"    → {new} nieuwe spellen (totaal: {len(all_games)})")
            cursor = end

        return all_games

    def get_player_info(self, player_id: int, delay: float = 1.0) -> dict:
        """
        Haal spelerprofiel op van BGA.
        Parseert de HTML profielpagina omdat er geen JSON endpoint voor is.
        """
        time.sleep(delay)
        resp = self.session.get(f"{BGA_BASE}/player?id={player_id}", timeout=30)
        resp.raise_for_status()

        html = resp.text
        info: dict = {"bga_player_id": str(player_id)}

        # Naam
        m = re.search(r'<h1[^>]*class="[^"]*playername[^"]*"[^>]*>([^<]+)</h1>', html)
        if not m:
            m = re.search(r'"playername"\s*:\s*"([^"]+)"', html)
        if m:
            info["name"] = m.group(1).strip()

        # ELO voor Carcassonne (game_id=1)
        m = re.search(
            r'game_id["\s:=]+1[^}]{0,200}?"elo"\s*:\s*"?(\d+)"?', html
        )
        if m:
            info["elo"] = int(m.group(1))

        return info
