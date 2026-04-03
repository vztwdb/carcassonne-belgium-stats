-- Carcassonne Belgium Stats — Initial Schema
-- COM-31

CREATE TABLE IF NOT EXISTS players (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    name_nl     TEXT,          -- Nederlandse naam variant
    wica_id     TEXT,          -- WICA player ID indien bekend
    country     TEXT DEFAULT 'BE',
    created_at  TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS player_aliases (
    alias       TEXT PRIMARY KEY,
    player_id   INTEGER NOT NULL REFERENCES players(id)
);

CREATE TABLE IF NOT EXISTS tournaments (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL CHECK (type IN ('BK', 'BCOC', 'BCL', 'NATIONS', 'OTHER')),
    year        INTEGER NOT NULL,
    edition     TEXT,          -- bijv. "BK 2023" of "BCOC Season 5"
    location    TEXT,          -- enkel voor live tornooien
    date_start  DATE,
    date_end    DATE,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS tournament_participants (
    id              INTEGER PRIMARY KEY,
    tournament_id   INTEGER NOT NULL REFERENCES tournaments(id),
    player_id       INTEGER NOT NULL REFERENCES players(id),
    final_rank      INTEGER,
    total_score     REAL,
    UNIQUE (tournament_id, player_id)
);

CREATE TABLE IF NOT EXISTS games (
    id              INTEGER PRIMARY KEY,
    tournament_id   INTEGER NOT NULL REFERENCES tournaments(id),
    round           INTEGER NOT NULL,
    table_number    INTEGER,
    played_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS game_players (
    id          INTEGER PRIMARY KEY,
    game_id     INTEGER NOT NULL REFERENCES games(id),
    player_id   INTEGER NOT NULL REFERENCES players(id),
    score       REAL,
    rank        INTEGER,       -- rang aan die tafel (1=gewonnen)
    UNIQUE (game_id, player_id)
);

-- Nationale ploeg competities
CREATE TABLE IF NOT EXISTS nations_competitions (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL, -- bijv. "Nations Cup 2023"
    year        INTEGER NOT NULL,
    host        TEXT,          -- gastland
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS nations_matches (
    id                  INTEGER PRIMARY KEY,
    competition_id      INTEGER NOT NULL REFERENCES nations_competitions(id),
    round               INTEGER,
    team_a              TEXT NOT NULL,  -- landcode bijv. 'BE'
    team_b              TEXT NOT NULL,
    team_a_score        REAL,
    team_b_score        REAL,
    result              TEXT CHECK (result IN ('W', 'D', 'L', NULL)) -- vanuit BE perspectief
);

CREATE TABLE IF NOT EXISTS nations_match_players (
    id          INTEGER PRIMARY KEY,
    match_id    INTEGER NOT NULL REFERENCES nations_matches(id),
    player_id   INTEGER NOT NULL REFERENCES players(id),
    team        TEXT NOT NULL,
    score       REAL,
    UNIQUE (match_id, player_id)
);
