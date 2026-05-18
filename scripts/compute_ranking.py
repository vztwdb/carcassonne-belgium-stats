"""Compute the hybrid Carcassonne ranking for Belgian players.

Strategy:
  Final score = BGA base (0-100, normalized all-time peak Elo)
              + BK live bonus       (BCLC placement points, recency-decayed)
              + BK online bonus     (BCOC placement points, recency-decayed)
              + BCL bonus           (tier-scaled BCL placement, 0.5 per season)
              + WCC bonus           (participation + top-4 finish, recency-decayed)
              + Nations bonus       (per-match points, recency-decayed)

Idempotent: wipes player_ranking + player_ranking_events and rewrites both.
Tunable weights live at the top of this file.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

# ── Tunables ────────────────────────────────────────────────────────────────

CURRENT_YEAR = datetime.now().year
DECAY_RATE = 0.85          # multiplier per year-ago (0.85^n)
DECAY_FLOOR = 0.10         # never decay below this

MIN_BGA_GAMES = 20         # threshold for BGA base normalization
BGA_PEAK_MAX = 60.0        # peak component of BGA base (0..BGA_PEAK_MAX)
BGA_CURRENT_MAX = 60.0     # current component of BGA base (0..BGA_CURRENT_MAX)
BGA_CURRENT_HALFLIFE_DAYS = 365   # current_base halves every N days since last game

BK_LIVE_WEIGHT = 1.0
BK_ONLINE_WEIGHT = 0.75  # slightly lower than live due to smaller player pool

WCC_PLACEMENT_WEIGHT = 0.7

# Belgian Carcassonne League: 0.5 per season (2 seasons/year → 1.0 yearly weight).
BCL_WEIGHT = 0.5
# Tier multipliers — a Bronze League champion ≠ Master League champion.
BCL_TIER_FACTOR = {"ML": 1.00, "GL": 0.60, "SL": 0.35, "BL": 0.20}

NATIONS_OFFICIAL_WIN = 4.0
NATIONS_OFFICIAL_LOSS = 1.0
NATIONS_FRIENDLY_WIN = 2.0
NATIONS_FRIENDLY_LOSS = 0.5

# Chess-style display scaling: elo = ELO_FLOOR + total_score * ELO_SCALE
ELO_FLOOR = 1500
ELO_SCALE = 1.0


def placement_points(rank: int | None) -> float:
    """Points awarded for a tournament finishing rank."""
    if rank is None:
        return 0.0
    table = {1: 100, 2: 70, 3: 50, 4: 35, 5: 25, 6: 20, 7: 17, 8: 14}
    if rank in table:
        return float(table[rank])
    if rank <= 16:
        return 8.0
    if rank <= 32:
        return 3.0
    return 1.0


def decay(year: int | None) -> float:
    if year is None:
        return DECAY_FLOOR
    years_ago = max(0, CURRENT_YEAR - year)
    return max(DECAY_FLOOR, DECAY_RATE ** years_ago)


# ── Computation ─────────────────────────────────────────────────────────────


def belgian_player_ids(c: duckdb.DuckDBPyConnection) -> set[int]:
    rows = c.execute(
        "SELECT id FROM players WHERE country = 'BE'"
    ).fetchall()
    return {r[0] for r in rows}


def compute_bga_base(c: duckdb.DuckDBPyConnection, be_ids: set[int]) -> dict[int, dict]:
    """Returns {player_id: {'peak', 'current', 'games', 'base'}}."""
    rows = c.execute(
        """
        WITH bga AS (
            SELECT gp.player_id,
                   gp.elo_after,
                   g.ended_at,
                   ROW_NUMBER() OVER (PARTITION BY gp.player_id ORDER BY g.ended_at DESC) AS rn_latest
            FROM game_players gp
            JOIN games g ON g.id = gp.game_id
            WHERE g.source = 'bga'
              AND gp.elo_after IS NOT NULL
              AND g.ended_at IS NOT NULL
        )
        SELECT player_id,
               MAX(elo_after)                          AS peak,
               MAX(CASE WHEN rn_latest = 1 THEN elo_after END) AS current_elo,
               MAX(ended_at)                           AS last_played,
               COUNT(*)                                AS games
        FROM bga
        GROUP BY player_id
        """
    ).fetchall()

    ELO_BASE_OFFSET = 1300   # raw elo_after stores rating + 1300 (project convention)
    today = date.today()
    stats = {}
    for pid, peak, cur, last_played, games in rows:
        if pid not in be_ids:
            continue
        last_date = last_played.date() if hasattr(last_played, "date") else last_played
        days_since = (today - last_date).days if last_date else None
        recency = (0.5 ** (days_since / BGA_CURRENT_HALFLIFE_DAYS)) if days_since is not None else 0.0
        stats[pid] = {
            "peak": float(peak) - ELO_BASE_OFFSET,
            "current": (float(cur) - ELO_BASE_OFFSET) if cur is not None else None,
            "games": int(games),
            "last_played": last_date,
            "days_since": days_since,
            "recency": recency,
            "peak_base": 0.0, "current_base": 0.0, "base": 0.0,
        }

    def minmax(values: list[float], scale: float) -> tuple[float, float, float]:
        lo, hi = min(values), max(values)
        span = hi - lo if hi > lo else 1.0
        return lo, span, scale

    eligible_peaks = [s["peak"] for s in stats.values() if s["games"] >= MIN_BGA_GAMES]
    eligible_currents = [s["current"] for s in stats.values()
                         if s["games"] >= MIN_BGA_GAMES and s["current"] is not None]

    if len(eligible_peaks) >= 2:
        lo_p, span_p, _ = minmax(eligible_peaks, BGA_PEAK_MAX)
        lo_c, span_c, _ = (minmax(eligible_currents, BGA_CURRENT_MAX)
                           if len(eligible_currents) >= 2 else (0.0, 1.0, BGA_CURRENT_MAX))
        for s in stats.values():
            if s["games"] >= MIN_BGA_GAMES:
                s["peak_base"] = BGA_PEAK_MAX * (s["peak"] - lo_p) / span_p
                if s["current"] is not None:
                    raw_current_base = BGA_CURRENT_MAX * (s["current"] - lo_c) / span_c
                    s["current_base"] = raw_current_base * s["recency"]
                s["base"] = s["peak_base"] + s["current_base"]
    return stats


def compute_tournament_bonus(
    c: duckdb.DuckDBPyConnection,
    be_ids: set[int],
    tournament_type: str,
    source_label: str,
    weight: float,
) -> list[tuple]:
    """Placement-based bonus using the fixed placement_points table."""
    rows = c.execute(
        """
        SELECT tp.player_id, t.id, t.year, t.date_start, t.name, tp.final_rank
        FROM tournament_participants tp
        JOIN tournaments t ON t.id = tp.tournament_id
        WHERE t.type = ?
          AND tp.final_rank IS NOT NULL
        """,
        [tournament_type],
    ).fetchall()

    events = []
    for pid, tid, year, date_start, name, rank in rows:
        if pid not in be_ids:
            continue
        raw = placement_points(rank)
        if raw == 0:
            continue
        d = decay(year)
        pts = raw * d * weight
        desc = f"{name} — rank {rank}"
        events.append((pid, source_label, date_start, year, tid, desc, raw, d, pts))
    return events


def compute_bcl_bonus(c: duckdb.DuckDBPyConnection, be_ids: set[int]) -> list[tuple]:
    """BCL placement bonus, tier-scaled at 0.5 per season."""
    rows = c.execute(
        """
        SELECT tp.player_id, t.id, t.year, t.date_start, t.name, t.edition, tp.final_rank
        FROM tournament_participants tp
        JOIN tournaments t ON t.id = tp.tournament_id
        WHERE t.type = 'BCL' AND tp.final_rank IS NOT NULL
        """
    ).fetchall()

    events: list[tuple] = []
    for pid, tid, year, date_start, name, edition, rank in rows:
        if pid not in be_ids:
            continue
        # Edition is like "2026 Spring ML"; tier is the trailing token.
        tier = (edition or "").split()[-1] if edition else "ML"
        tier_factor = BCL_TIER_FACTOR.get(tier, 0.0)
        if tier_factor == 0.0:
            continue
        raw = placement_points(rank) * tier_factor
        if raw == 0:
            continue
        d = decay(year)
        events.append((pid, "bcl", date_start, year, tid,
                       f"{name} — rank {rank} ({tier})",
                       raw, d, raw * d * BCL_WEIGHT))
    return events


def compute_wcc_bonus(c: duckdb.DuckDBPyConnection, be_ids: set[int]) -> list[tuple]:
    # Field size per WCC tournament (from participants_count, fallback to tournament_participants).
    field_sizes = dict(c.execute(
        """
        SELECT t.id,
               COALESCE(t.participants_count,
                        (SELECT COUNT(*) FROM tournament_participants x WHERE x.tournament_id = t.id))
        FROM tournaments t
        WHERE t.type = 'WCC'
        """
    ).fetchall())

    rows = c.execute(
        """
        SELECT tp.player_id, t.id, t.year, t.date_start, t.name, tp.final_rank
        FROM tournament_participants tp
        JOIN tournaments t ON t.id = tp.tournament_id
        WHERE t.type = 'WCC'
        """
    ).fetchall()

    events = []
    for pid, tid, year, date_start, name, rank in rows:
        if pid not in be_ids:
            continue
        d = decay(year)

        n = field_sizes.get(tid) or 0
        if rank is not None and n >= 2:
            placement_raw = 100.0 * max(0, n - rank) / (n - 1)
            if placement_raw > 0:
                pts = placement_raw * d * WCC_PLACEMENT_WEIGHT
                events.append((pid, "wcc", date_start, year, tid,
                               f"{name} — rank {rank}/{n}",
                               placement_raw, d, pts))
    return events


def compute_nations_bonus(c: duckdb.DuckDBPyConnection, be_ids: set[int]) -> list[tuple]:
    rows = c.execute(
        """
        SELECT nm.id,
               nm.player_id,
               nm.result,
               ncd.date_played,
               t.id,
               t.year,
               t.name,
               t.type
        FROM nations_matches nm
        JOIN nations_competition_duels ncd ON ncd.id = nm.duel_id
        JOIN tournaments t ON t.id = ncd.tournament_id
        """
    ).fetchall()

    events = []
    for match_id, pid, result, date_played, tid, year, name, ttype in rows:
        if pid not in be_ids:
            continue
        is_friendly = (ttype == "FRIENDLIES")
        if is_friendly:
            source = "nations_friendly"
            win_pts, loss_pts = NATIONS_FRIENDLY_WIN, NATIONS_FRIENDLY_LOSS
        else:
            source = "nations_official"
            win_pts, loss_pts = NATIONS_OFFICIAL_WIN, NATIONS_OFFICIAL_LOSS

        if result == "W":
            raw = win_pts
        elif result == "L":
            raw = loss_pts
        elif result == "D":
            raw = (win_pts + loss_pts) / 2
        else:
            continue

        match_year = year if year else (date_played.year if date_played else None)
        d = decay(match_year)
        desc = f"{name} — {result} vs opp"
        events.append((pid, source, date_played, match_year, tid, desc, raw, d, raw * d))
    return events


def main() -> None:
    c = duckdb.connect(str(DB_PATH))

    be_ids = belgian_player_ids(c)
    print(f"Belgian players in scope: {len(be_ids)}")

    bga = compute_bga_base(c, be_ids)
    print(f"BGA stats for {len(bga)} players "
          f"({sum(1 for s in bga.values() if s['games'] >= MIN_BGA_GAMES)} above threshold)")

    events: list[tuple] = []
    events += compute_tournament_bonus(c, be_ids, "BCLC", "bk_live", BK_LIVE_WEIGHT)
    events += compute_tournament_bonus(c, be_ids, "BCOC", "bk_online", BK_ONLINE_WEIGHT)
    events += compute_bcl_bonus(c, be_ids)
    events += compute_wcc_bonus(c, be_ids)
    events += compute_nations_bonus(c, be_ids)
    print(f"Generated {len(events)} ranking events")

    sums: dict[int, dict[str, float]] = {}
    for pid, source, *_rest, _raw, _d, points in events:
        bucket = sums.setdefault(pid, {"bk_live": 0.0, "bk_online": 0.0,
                                        "bcl": 0.0,
                                        "wcc": 0.0, "nations_official": 0.0,
                                        "nations_friendly": 0.0})
        bucket[source] += points

    all_pids = set(bga.keys()) | set(sums.keys())
    rows_out = []
    for pid in all_pids:
        b = bga.get(pid, {})
        s = sums.get(pid, {})
        bga_base = b.get("base", 0.0)
        bk_live = s.get("bk_live", 0.0)
        bk_online = s.get("bk_online", 0.0)
        bcl = s.get("bcl", 0.0)
        wcc = s.get("wcc", 0.0)
        nations = s.get("nations_official", 0.0) + s.get("nations_friendly", 0.0)
        total = bga_base + bk_live + bk_online + bcl + wcc + nations
        elo = int(round(ELO_FLOOR + total * ELO_SCALE))
        rows_out.append((
            pid,
            bga_base,
            b.get("peak"),
            b.get("current"),
            b.get("games", 0),
            bk_live,
            bk_online,
            bcl,
            wcc,
            nations,
            total,
            elo,
        ))

    rows_out.sort(key=lambda r: r[-2], reverse=True)
    ranked = [(*row, idx + 1) for idx, row in enumerate(rows_out)]

    c.execute("DELETE FROM player_ranking_events")
    c.execute("DELETE FROM player_ranking")

    c.executemany(
        """
        INSERT INTO player_ranking
            (player_id, bga_base, bga_peak_elo, bga_current_elo, bga_games,
             bk_live_bonus, bk_online_bonus, bcl_bonus, wcc_bonus, nations_bonus,
             total_score, ranking_elo, rank)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ranked,
    )

    c.executemany(
        """
        INSERT INTO player_ranking_events
            (player_id, source, event_date, event_year, tournament_id,
             description, raw_points, decay, points)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        events,
    )

    c.close()
    print(f"Wrote {len(ranked)} rankings and {len(events)} breakdown events.")


if __name__ == "__main__":
    main()
