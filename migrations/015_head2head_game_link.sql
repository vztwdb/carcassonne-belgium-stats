-- Link each head-to-head event back to the underlying game so the dashboard
-- can show a BGA table URL.

ALTER TABLE player_head2head_events ADD COLUMN IF NOT EXISTS game_id INTEGER;
