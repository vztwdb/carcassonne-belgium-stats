"""
Snel testscript: haal de 10 laatste Carcassonne spellen op voor één speler.
Vereist: BGA_EMAIL en BGA_PASSWORD als environment variabelen of CLI args.

Gebruik:
    python scripts/test_bga_fetch.py --player 84216333
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, ".")
from src.importers.bga_fetcher import BGASession

parser = argparse.ArgumentParser()
parser.add_argument("--player", type=int, required=True, help="BGA player ID")
parser.add_argument("--email", default=os.environ.get("BGA_EMAIL"))
parser.add_argument("--password", default=os.environ.get("BGA_PASSWORD"))
args = parser.parse_args()

if not args.email or not args.password:
    print("Stel BGA_EMAIL en BGA_PASSWORD in als env variabelen, of geef --email en --password mee.")
    sys.exit(1)

print("Verbinding maken met BGA ...")
bga = BGASession(args.email, args.password)

# Haal spellen op van de laatste 90 dagen
since = datetime.utcnow() - timedelta(days=90)
print(f"Spellen ophalen voor speler {args.player} (laatste 90 dagen) ...")
games = bga.fetch_all_games(args.player, since=since, chunk_days=90)

print(f"\n{len(games)} spellen gevonden. Eerste 3:\n")
for g in games[:3]:
    print(json.dumps(g, indent=2, ensure_ascii=False))
    print("---")
