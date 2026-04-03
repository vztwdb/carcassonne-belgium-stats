-- BGA-specifieke velden toevoegen
-- COM-38/39: Online tornooien data via BoardGameArena

ALTER TABLE players ADD COLUMN IF NOT EXISTS bga_player_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_players_bga_id ON players(bga_player_id) WHERE bga_player_id IS NOT NULL;

ALTER TABLE games ADD COLUMN IF NOT EXISTS bga_table_id TEXT;
ALTER TABLE games ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'manual'; -- 'bga', 'manual', 'excel', 'pdf'
CREATE UNIQUE INDEX IF NOT EXISTS idx_games_bga_table ON games(bga_table_id) WHERE bga_table_id IS NOT NULL;

ALTER TABLE game_players ADD COLUMN IF NOT EXISTS elo_before INTEGER;
ALTER TABLE game_players ADD COLUMN IF NOT EXISTS elo_after  INTEGER;
ALTER TABLE game_players ADD COLUMN IF NOT EXISTS elo_delta  INTEGER;
ALTER TABLE game_players ADD COLUMN IF NOT EXISTS conceded   BOOLEAN DEFAULT FALSE;
