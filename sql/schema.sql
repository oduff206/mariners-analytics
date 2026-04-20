-- Seattle Mariners Analytics — DuckDB schema
-- Historical window: 2015-present

CREATE TABLE IF NOT EXISTS players (
    mlbam_id        INTEGER PRIMARY KEY,
    name_full       VARCHAR NOT NULL,
    name_last       VARCHAR,
    name_first      VARCHAR,
    position        VARCHAR,   -- e.g. SP, RP, C, 1B, 2B, 3B, SS, LF, CF, RF, DH
    bats            CHAR(1),   -- R, L, S
    throws          CHAR(1),   -- R, L
    debut_date      DATE,
    birth_year      INTEGER
);

CREATE TABLE IF NOT EXISTS player_seasons (
    id              INTEGER PRIMARY KEY,
    mlbam_id        INTEGER NOT NULL REFERENCES players(mlbam_id),
    season          INTEGER NOT NULL,
    team            VARCHAR,
    age             INTEGER,
    -- Counting stats (batters)
    pa              INTEGER,
    ab              INTEGER,
    h               INTEGER,
    hr              INTEGER,
    rbi             INTEGER,
    sb              INTEGER,
    -- Rate stats (batters)
    avg             DOUBLE,
    obp             DOUBLE,
    slg             DOUBLE,
    woba            DOUBLE,
    wrc_plus        INTEGER,
    -- Pitching
    ip              DOUBLE,
    era             DOUBLE,
    fip             DOUBLE,
    k_per9          DOUBLE,
    bb_per9         DOUBLE,
    -- Value
    war             DOUBLE,
    -- Flags
    is_mariners     BOOLEAN DEFAULT FALSE,
    UNIQUE (mlbam_id, season, team)
);

CREATE TABLE IF NOT EXISTS contracts (
    id              INTEGER PRIMARY KEY,
    mlbam_id        INTEGER NOT NULL REFERENCES players(mlbam_id),
    season_start    INTEGER NOT NULL,
    season_end      INTEGER NOT NULL,
    years           INTEGER,
    total_value_m   DOUBLE,   -- total value in $M
    aav_m           DOUBLE,   -- average annual value in $M
    team            VARCHAR,
    contract_type   VARCHAR,  -- FA, extension, arbitration, pre-arb
    source          VARCHAR DEFAULT 'Spotrac',
    UNIQUE (mlbam_id, season_start)
);

CREATE TABLE IF NOT EXISTS team_seasons (
    id              INTEGER PRIMARY KEY,
    team            VARCHAR NOT NULL,
    season          INTEGER NOT NULL,
    wins            INTEGER,
    losses          INTEGER,
    run_diff        INTEGER,
    payroll_m       DOUBLE,
    payroll_rank    INTEGER,  -- 1 = highest payroll
    team_war        DOUBLE,
    UNIQUE (team, season)
);

CREATE TABLE IF NOT EXISTS draft_picks (
    id              INTEGER PRIMARY KEY,
    season          INTEGER NOT NULL,  -- draft year
    round           INTEGER,
    pick_overall    INTEGER,
    team            VARCHAR,
    mlbam_id        INTEGER REFERENCES players(mlbam_id),  -- NULL if never reached majors
    name_at_draft   VARCHAR,
    position        VARCHAR,
    school          VARCHAR,
    reached_mlb     BOOLEAN DEFAULT FALSE,
    mlb_debut_date  DATE
);

CREATE TABLE IF NOT EXISTS statcast_quality (
    id              INTEGER PRIMARY KEY,
    mlbam_id        INTEGER NOT NULL REFERENCES players(mlbam_id),
    season          INTEGER NOT NULL,
    -- Quality of contact (batters)
    exit_velo_avg   DOUBLE,
    launch_angle    DOUBLE,
    barrel_pct      DOUBLE,
    hard_hit_pct    DOUBLE,
    xba             DOUBLE,
    xwoba           DOUBLE,
    -- Pitching
    xera            DOUBLE,
    whiff_pct       DOUBLE,
    UNIQUE (mlbam_id, season)
);
