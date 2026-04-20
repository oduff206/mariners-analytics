"""
Load normalized DataFrames into DuckDB tables.
All functions are idempotent — safe to re-run.
"""

import re
import unicodedata

import duckdb
import pandas as pd


def load_players(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Upsert player stubs from a normalized batting DataFrame into the players table.

    Uses INSERT OR IGNORE so existing rows are never overwritten.
    Returns the number of new rows inserted.
    """
    required = {"mlbam_id", "name_full"}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame missing columns: {required - set(df.columns)}")

    players = (
        df[["mlbam_id", "name_full",
            *[c for c in ("position", "bats") if c in df.columns]]]
        .drop_duplicates(subset=["mlbam_id"])
        .dropna(subset=["mlbam_id"])
        .copy()
    )
    players["mlbam_id"] = players["mlbam_id"].astype(int)

    # Split full name into first / last
    players["name_last"] = players["name_full"].str.split().str[-1]
    players["name_first"] = players["name_full"].str.split().str[:-1].str.join(" ")

    # Ensure optional columns are present (fill with None if the source lacks them)
    for col in ("position", "bats"):
        if col not in players.columns:
            players[col] = None

    before = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    con.register("_players_staging", players)
    con.execute("""
        INSERT OR IGNORE INTO players (mlbam_id, name_full, name_last, name_first, position, bats)
        SELECT mlbam_id, name_full, name_last, name_first, position, bats
        FROM _players_staging
    """)
    after = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    return after - before


def load_player_seasons(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Upsert player-season rows into player_seasons.

    Skips rows with NULL mlbam_id. Replaces on (mlbam_id, season, team) conflict.
    Returns the number of rows upserted.
    """
    if "mlbam_id" not in df.columns:
        raise ValueError("DataFrame missing 'mlbam_id' column — run normalize_batting() first")

    schema_cols = [
        "mlbam_id", "season", "team", "age",
        "pa", "ab", "h", "hr", "rbi", "sb",
        "avg", "obp", "slg", "woba", "wrc_plus", "war",
        "is_mariners",
    ]
    keep = [c for c in schema_cols if c in df.columns]
    rows = df[keep].dropna(subset=["mlbam_id"]).copy()
    rows["mlbam_id"] = rows["mlbam_id"].astype(int)

    before = con.execute("SELECT COUNT(*) FROM player_seasons").fetchone()[0]
    con.register("_ps_staging", rows)
    cols = ", ".join(keep)
    # Build SET clause for every non-key column
    update_cols = [c for c in keep if c not in ("mlbam_id", "season", "team")]
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    con.execute(f"""
        INSERT INTO player_seasons (id, {cols})
        SELECT
            COALESCE((SELECT MAX(id) FROM player_seasons), 0)
                + row_number() OVER () AS id,
            {cols}
        FROM _ps_staging
        ON CONFLICT (mlbam_id, season, team) DO UPDATE SET {set_clause}
    """)
    after = con.execute("SELECT COUNT(*) FROM player_seasons").fetchone()[0]
    return after - before


def load_statcast(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Upsert Statcast batter metrics into statcast_quality.

    Only inserts rows for mlbam_ids already present in the players table.
    Returns the number of net new rows inserted.
    """
    required = {"mlbam_id", "season"}
    if not required.issubset(df.columns):
        raise ValueError(f"DataFrame missing columns: {required - set(df.columns)}")

    schema_cols = ["mlbam_id", "season", "exit_velo_avg", "barrel_pct",
                   "hard_hit_pct", "xba", "xwoba"]
    keep = [c for c in schema_cols if c in df.columns]
    rows = df[keep].dropna(subset=["mlbam_id", "season"]).copy()
    rows["mlbam_id"] = rows["mlbam_id"].astype(int)
    rows["season"]   = rows["season"].astype(int)

    known = set(con.execute("SELECT mlbam_id FROM players").fetchdf()["mlbam_id"])
    rows = rows[rows["mlbam_id"].isin(known)]

    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM statcast_quality").fetchone()[0]
    rows.insert(0, "id", range(max_id + 1, max_id + 1 + len(rows)))

    before = con.execute("SELECT COUNT(*) FROM statcast_quality").fetchone()[0]
    con.register("_sc_staging", rows)
    col_list   = ", ".join(keep)
    update_cols = [c for c in keep if c not in ("mlbam_id", "season")]
    set_clause  = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    con.execute(f"""
        INSERT INTO statcast_quality (id, {col_list})
        SELECT id, {col_list}
        FROM _sc_staging
        ON CONFLICT (mlbam_id, season) DO UPDATE SET {set_clause}
    """)
    after = con.execute("SELECT COUNT(*) FROM statcast_quality").fetchone()[0]
    return after - before


def load_draft_picks(
    con: duckdb.DuckDBPyConnection,
    raw_df: pd.DataFrame,
) -> int:
    """Normalize and insert draft picks into draft_picks.

    Matches player names to the players table to populate mlbam_id where
    possible. Clears existing rows for the seasons in raw_df before inserting
    (full reload per cohort).  Returns the number of rows inserted.
    """

    def _norm(name: object) -> str:
        if pd.isna(name):
            return ""
        s = unicodedata.normalize("NFD", str(name))
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        s = re.sub(r"[^a-z\s]", "", s.lower())
        return " ".join(s.split())

    df = raw_df.rename(columns={
        "OvPck":          "pick_overall",
        "Tm":             "team",
        "Name":           "name_at_draft",
        "Pos":            "position",
        "Drafted Out of": "school",
        "draft_season":   "season",
        "draft_round":    "round",
    }).copy()

    df["pick_overall"] = pd.to_numeric(df["pick_overall"], errors="coerce")

    g_bat  = pd.to_numeric(df.get("G",   pd.Series(dtype=float)), errors="coerce").fillna(0)
    g_pit  = pd.to_numeric(df.get("G.1", pd.Series(dtype=float)), errors="coerce").fillna(0)
    df["reached_mlb"] = (g_bat > 0) | (g_pit > 0)

    # Best-effort name match to players table for mlbam_id
    players = con.execute("SELECT mlbam_id, name_full FROM players").fetchdf()
    players["_norm"] = players["name_full"].apply(_norm)
    name_map = dict(zip(players["_norm"], players["mlbam_id"]))

    df["_norm"] = df["name_at_draft"].apply(_norm)
    df["mlbam_id"] = df["_norm"].map(name_map).where(df["_norm"] != "", other=pd.NA)
    df["mlb_debut_date"] = None

    out = df[["season", "round", "pick_overall", "team", "mlbam_id",
              "name_at_draft", "position", "school",
              "reached_mlb", "mlb_debut_date"]].copy()
    out = out.drop_duplicates(subset=["season", "round", "pick_overall"])
    out = out.dropna(subset=["season", "pick_overall"])

    # Full reload for these seasons
    seasons = out["season"].dropna().unique().tolist()
    if seasons:
        con.execute(f"DELETE FROM draft_picks WHERE season IN ({','.join(str(int(s)) for s in seasons)})")

    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM draft_picks").fetchone()[0]
    out.insert(0, "id", range(max_id + 1, max_id + 1 + len(out)))

    con.register("_draft_staging", out)
    con.execute("""
        INSERT OR IGNORE INTO draft_picks
            (id, season, round, pick_overall, team, mlbam_id,
             name_at_draft, position, school, reached_mlb, mlb_debut_date)
        SELECT id, season, round, pick_overall, team, mlbam_id,
               name_at_draft, position, school, reached_mlb, mlb_debut_date
        FROM _draft_staging
    """)
    return len(out)
