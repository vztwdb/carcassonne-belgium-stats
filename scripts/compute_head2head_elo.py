"""Compute a classical head-to-head Elo rating for Belgian players.

Only games where BOTH participants are Belgian (country = 'BE') count.

Scope:
  - 2-player games only.
  - Carcassonne base game only: Framework (boardgame_id = 2) is excluded, and
    games where any player scored > 160 are assumed to use an expansion and
    skipped.
  - BGA games that are part of a BCOC match (linked via
    tournament_matches.game_id_*) are excluded here — the BCOC match-level
    event captures them once at K_BCOC.

Sources processed chronologically:
  - BGA 1v1 games         (source='bga', 2 players)       K = 8
  - BCLC Swiss-round games (source='swiss', BCLC)         K = 24
  - BCLC playoff games     (source='manual', BCLC)        K = 32
  - BCOC tournament_matches (game-share, e.g. 2-1 -> 0.667) K = 28

Idempotent: wipes player_head2head_elo + player_head2head_events and rewrites.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parents[1] / "data" / "carcassonne.duckdb"

START_RATING = 1500.0

K_BGA = 8
K_BCLC_SWISS = 24
K_BCLC_PLAYOFF = 32
K_BCOC = 28


def expected(ra: float, rb: float) -> float:
    return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))


def belgian_player_ids(c: duckdb.DuckDBPyConnection) -> set[int]:
    return {r[0] for r in c.execute(
        "SELECT id FROM players WHERE country = 'BE'"
    ).fetchall()}


def collect_events(c: duckdb.DuckDBPyConnection, be_ids: set[int]) -> list[tuple]:
    """Return a list of (sort_key, date, source, tournament_id, player_a, player_b, result_a, k).

    result_a is in [0, 1]: 1 = A fully wins, 0 = A fully loses, 0.5 = draw / 1-1, etc.
    """
    events: list[tuple] = []
    placeholders = ",".join(str(pid) for pid in be_ids) if be_ids else "-1"

    # Games that are already part of a BCOC match — they are counted
    # once at the match level below, never as standalone BGA events.
    bcoc_game_ids: set[int] = {
        row[0] for row in c.execute("""
            SELECT unnest([game_id_1, game_id_2, game_id_3, game_id_4, game_id_5])
            FROM tournament_matches tm
            JOIN tournaments t ON t.id = tm.tournament_id
            WHERE t.type = 'BCOC'
        """).fetchall()
        if row[0] is not None
    }

    # ── BGA + BCLC games (via games/game_players) ────────────────────────────
    # Exclude Framework (boardgame_id = 2) and expansion games (max score > 160).
    rows = c.execute(f"""
        WITH two_be AS (
            SELECT gp.game_id,
                   MIN(CASE WHEN gp.rank = 1 THEN gp.player_id END) AS winner_id,
                   MIN(CASE WHEN gp.rank = 2 THEN gp.player_id END) AS loser_id,
                   MAX(gp.score) AS max_score
            FROM game_players gp
            WHERE gp.player_id IN ({placeholders})
            GROUP BY gp.game_id
            HAVING COUNT(*) = 2
               AND SUM(CASE WHEN gp.player_id IN ({placeholders}) THEN 1 ELSE 0 END) = 2
               AND BOOL_OR(gp.rank IS NULL) = FALSE
        )
        SELECT g.id,
               g.source,
               g.tournament_id,
               COALESCE(g.ended_at, g.played_at) AS ts,
               t.date_start,
               t.type AS t_type,
               tb.winner_id,
               tb.loser_id
        FROM games g
        JOIN two_be tb ON tb.game_id = g.id
        LEFT JOIN tournaments t ON t.id = g.tournament_id
        WHERE g.source IN ('bga', 'swiss', 'manual')
          AND (g.unranked IS NULL OR g.unranked = FALSE)
          AND (g.boardgame_id IS NULL OR g.boardgame_id = 1)
          AND (tb.max_score IS NULL OR tb.max_score <= 160)
    """).fetchall()

    for gid, source, tid, ts, t_date_start, t_type, winner_id, loser_id in rows:
        if winner_id is None or loser_id is None:
            continue
        if gid in bcoc_game_ids:
            continue
        if source == "bga":
            k = K_BGA
            src_label = "bga"
        elif source == "swiss" and t_type == "BCLC":
            k = K_BCLC_SWISS
            src_label = "bclc_swiss"
        elif source == "manual" and t_type == "BCLC":
            k = K_BCLC_PLAYOFF
            src_label = "bclc_playoff"
        else:
            continue
        ev_date = (ts.date() if hasattr(ts, "date") else ts) or t_date_start
        sort_key = (ev_date or t_date_start, gid)
        # One event = one rating update. winner = A with result 1.
        events.append((sort_key, ev_date, src_label, tid, gid, winner_id, loser_id, 1.0, k))

    # ── BCOC matches (via tournament_matches) ────────────────────────────────
    rows = c.execute(f"""
        SELECT tm.id,
               tm.tournament_id,
               t.date_start,
               tm.player_1_id,
               tm.player_2_id,
               tm.score_1,
               tm.score_2,
               tm.result
        FROM tournament_matches tm
        JOIN tournaments t ON t.id = tm.tournament_id
        WHERE t.type = 'BCOC'
          AND tm.player_1_id IN ({placeholders})
          AND tm.player_2_id IN ({placeholders})
    """).fetchall()

    for mid, tid, d, p1, p2, s1, s2, res in rows:
        if p1 is None or p2 is None:
            continue
        # Match result weighted by game share: 2-0 -> 1.0, 2-1 -> 0.667,
        # 1-1 -> 0.5, etc. Counts as a single ELO event at K_BCOC.
        if s1 is not None and s2 is not None and (s1 + s2) > 0:
            result_a = s1 / (s1 + s2)
        elif res == "1":
            result_a = 1.0
        elif res == "2":
            result_a = 0.0
        elif res in ("D", "draw"):
            result_a = 0.5
        else:
            continue
        sort_key = (d, 10_000_000 + mid)  # BCOC after any same-date game records
        events.append((sort_key, d, "bcoc", tid, None, p1, p2, result_a, K_BCOC))

    from datetime import date as _date
    MIN_DATE = _date(1900, 1, 1)
    events.sort(key=lambda e: (e[0][0] or MIN_DATE, e[0][1]))
    return events


def main() -> None:
    c = duckdb.connect(str(DB_PATH))
    be_ids = belgian_player_ids(c)
    print(f"Belgian players in scope: {len(be_ids)}")

    events = collect_events(c, be_ids)
    print(f"Processing {len(events)} head-to-head events")

    ratings: dict[int, float] = {}
    peaks: dict[int, tuple[float, object]] = {}
    stats: dict[int, dict] = {}
    history_rows: list[tuple] = []

    def r(pid: int) -> float:
        return ratings.get(pid, START_RATING)

    for _sk, d, src, tid, gid, a, b, result_a, k in events:
        ra, rb = r(a), r(b)
        ea = expected(ra, rb)
        delta = k * (result_a - ea)
        ra_new = ra + delta
        rb_new = rb - delta

        ratings[a] = ra_new
        ratings[b] = rb_new

        for pid, rating_new in ((a, ra_new), (b, rb_new)):
            prev_peak, prev_date = peaks.get(pid, (START_RATING, None))
            if rating_new > prev_peak:
                peaks[pid] = (rating_new, d)

            s = stats.setdefault(pid, {"games": 0, "w": 0, "l": 0, "dr": 0, "last": None})
            s["games"] += 1
            if d is not None and (s["last"] is None or d > s["last"]):
                s["last"] = d

        if result_a == 1.0:
            stats[a]["w"] += 1
            stats[b]["l"] += 1
        elif result_a == 0.0:
            stats[a]["l"] += 1
            stats[b]["w"] += 1
        else:
            stats[a]["dr"] += 1
            stats[b]["dr"] += 1

        history_rows.append((a, d, src, tid, gid, b, result_a, k, ra, ra_new))
        history_rows.append((b, d, src, tid, gid, a, 1.0 - result_a, k, rb, rb_new))

    # Build final rows
    rows_out = []
    for pid, rating in ratings.items():
        peak_rating, peak_date = peaks.get(pid, (rating, None))
        s = stats.get(pid, {"games": 0, "w": 0, "l": 0, "dr": 0, "last": None})
        rows_out.append((
            pid,
            rating,
            peak_rating,
            peak_date,
            s["games"],
            s["w"],
            s["l"],
            s["dr"],
            s["last"],
        ))

    rows_out.sort(key=lambda r: r[1], reverse=True)
    ranked = [(*row, idx + 1) for idx, row in enumerate(rows_out)]

    c.execute("DELETE FROM player_head2head_events")
    c.execute("DELETE FROM player_head2head_elo")

    c.executemany(
        """
        INSERT INTO player_head2head_elo
            (player_id, rating, peak_rating, peak_date, games, wins, losses, draws, last_played, rank)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ranked,
    )

    c.executemany(
        """
        INSERT INTO player_head2head_events
            (player_id, event_date, source, tournament_id, game_id, opponent_id,
             result, k_factor, rating_before, rating_after)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        history_rows,
    )

    c.close()
    print(f"Wrote {len(ranked)} player ratings and {len(history_rows)} history rows.")


if __name__ == "__main__":
    main()
