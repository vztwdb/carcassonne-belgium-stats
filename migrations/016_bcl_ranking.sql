-- Migration 016: BCL bonus column on player_ranking.
-- DuckDB allows ADD COLUMN (no CHECK changes), no DB rebuild needed.
-- player_ranking_events.source is free-form VARCHAR; the new 'bcl' source
-- label requires no DDL.
-- DuckDB cannot ADD COLUMN with NOT NULL+DEFAULT; we add nullable then backfill.
ALTER TABLE player_ranking ADD COLUMN IF NOT EXISTS bcl_bonus REAL;
UPDATE player_ranking SET bcl_bonus = 0 WHERE bcl_bonus IS NULL;
