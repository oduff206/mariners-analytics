"""
Pull raw data from pybaseball and load into DuckDB.
Run directly: python -m src.ingest.pull_data
"""

import time
from contextlib import contextmanager

import duckdb
import pandas as pd
import pybaseball
from curl_cffi import requests as curl_requests
from pathlib import Path
from datetime import date

DB_PATH = Path(__file__).parents[2] / "data" / "mariners.duckdb"
RAW_PATH = Path(__file__).parents[2] / "data" / "raw"
START_SEASON = 2015

_FG_API = "https://www.fangraphs.com/api/leaders/major-league/data"

_TEAM_NAMES = {
    "ARI": "Diamondbacks", "ATL": "Braves",      "BAL": "Orioles",
    "BOS": "Red Sox",      "CHC": "Cubs",         "CHW": "White Sox",
    "CIN": "Reds",         "CLE": "Guardians",    "COL": "Rockies",
    "DET": "Tigers",       "HOU": "Astros",       "KCR": "Royals",
    "LAA": "Angels",       "LAD": "Dodgers",      "MIA": "Marlins",
    "MIL": "Brewers",      "MIN": "Twins",        "NYM": "Mets",
    "NYY": "Yankees",      "OAK": "Athletics",    "ATH": "Athletics",
    "PHI": "Phillies",     "PIT": "Pirates",      "SDP": "Padres",
    "SEA": "Mariners",     "SFG": "Giants",       "STL": "Cardinals",
    "TBR": "Rays",         "TEX": "Rangers",      "TOR": "Blue Jays",
    "WSN": "Nationals",
}


def _fetch_fg_batting_season(session: curl_requests.Session, season: int) -> pd.DataFrame:
    """Fetch player-level batting stats for one season from the FanGraphs JSON API."""
    params = {
        "age": 0,
        "pos": "all",
        "stats": "bat",
        "lg": "all",
        "qual": 1,          # minimum 1 PA — keeps bench/platoon players for accurate team totals
        "season": season,
        "season1": season,
        "ind": 0,
        "team": 0,
        "pageitems": 2000000000,
        "pagenum": 1,
        "type": 8,          # Dashboard stat set
        "sortstat": "WAR",
        "sortorder": "desc",
        "players": 0,
    }
    resp = session.get(
        _FG_API,
        params=params,
        timeout=30,
        headers={"Referer": "https://www.fangraphs.com/leaders/major-league"},
    )
    if resp.status_code == 403:
        raise PermissionError(
            f"FanGraphs returned 403 for season {season}. "
            "Your IP may be rate-limited — wait a few minutes and retry."
        )
    resp.raise_for_status()
    payload = resp.json()
    records = payload.get("data", payload) if isinstance(payload, dict) else payload
    df = pd.DataFrame(records)
    df["Season"] = season

    # TeamNameAbb is a clean abbreviation column the API always includes.
    # Use it directly instead of parsing HTML out of the Team anchor tag.
    if "TeamNameAbb" in df.columns:
        df["Team"] = df["TeamNameAbb"]
    elif "Team" in df.columns and df["Team"].astype(str).str.contains("<a ", na=False).any():
        df["Team"] = df["Team"].astype(str).str.extract(r">([A-Z]{2,3})<", expand=False)

    # Drop multi-team summary rows ("2 Tms", "3 Tms", "- - -") for traded players
    df = df[df["Team"].notna() & ~df["Team"].str.contains(r"Tms|- - -", na=False) & (df["Team"] != "")]

    return df


def get_db(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    if not read_only:
        schema_sql = Path(__file__).parents[2] / "sql" / "schema.sql"
        con.execute(schema_sql.read_text())
    return con


@contextmanager
def db_connection(read_only: bool = False):
    """Context manager that guarantees con.close() even on exception."""
    con = get_db(read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def pull_batting(
    start: int = START_SEASON,
    end: int = date.today().year,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Pull FanGraphs player-level batting leaderboard using the modern JSON API.

    When `con` is provided the normalized data is immediately upserted into
    the players and player_seasons tables and row counts are printed.
    Without `con` the function returns the raw DataFrame unchanged (same
    behaviour as before — used by pull_team_batting and notebook exploration).
    """
    from src.transform.clean import normalize_batting
    from src.transform.load import load_players, load_player_seasons

    RAW_PATH.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_PATH / f"batting_{start}_{end}_{date.today().isoformat()}.csv"
    if cache_path.exists():
        df = pd.read_csv(cache_path)
    else:
        session = curl_requests.Session(impersonate="chrome")

        seasons = []
        for season in range(start, end + 1):
            seasons.append(_fetch_fg_batting_season(session, season))
            if season < end:
                time.sleep(0.5)

        seasons = [s for s in seasons if not s.empty]
        if not seasons:
            return pd.DataFrame(columns=["IDfg", "Name", "Team", "Season", "Age",
                                          "PA", "AB", "H", "HR", "RBI", "SB",
                                          "AVG", "OBP", "SLG", "wOBA", "wRC+", "WAR"])
        df = pd.concat(seasons, ignore_index=True)
        df.columns = [c.strip() for c in df.columns]

        # Strip HTML from Name: <a href="...">Aaron Judge</a> → Aaron Judge
        if "Name" in df.columns and df["Name"].astype(str).str.contains("<a ", na=False).any():
            df["Name"] = df["Name"].astype(str).str.extract(r">([^<]+)</a>", expand=False)

        # normalize_batting() expects IDfg; the API returns playerid
        df = df.rename(columns={"playerid": "IDfg"})

        df.to_csv(cache_path, index=False)

    if con is not None:
        normalized = normalize_batting(df)
        # is_mariners is set inside normalize_batting via MARINERS_TEAM_CODES {"SEA","Mariners"}
        # but re-assert here defensively in case team names differ in the raw feed
        if "team" in normalized.columns:
            normalized["is_mariners"] = normalized["team"].isin({"SEA", "Mariners"})
        new_players  = load_players(con, normalized)
        new_seasons  = load_player_seasons(con, normalized)
        total_players = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        total_seasons = con.execute("SELECT COUNT(*) FROM player_seasons").fetchone()[0]
        print(f"  batting load: +{new_players} players, +{new_seasons} player-seasons")
        print(f"  table totals: {total_players} players | {total_seasons} player-seasons")

    return df


def pull_pitching(
    start: int = START_SEASON,
    end: int = date.today().year,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Pull FanGraphs player-level pitching leaderboard using the modern JSON API.

    When `con` is provided the normalized data is immediately upserted into
    the players table (so pitchers get mlbam_id rows) and row counts are printed.
    """
    from src.transform.clean import normalize_pitching
    from src.transform.load import load_players

    RAW_PATH.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_PATH / f"pitching_{start}_{end}_{date.today().isoformat()}.csv"
    if cache_path.exists():
        df = pd.read_csv(cache_path)
    else:
        session = curl_requests.Session(impersonate="chrome")

        seasons = []
        for season in range(start, end + 1):
            params = {
                "age": 0, "pos": "all", "stats": "pit", "lg": "all", "qual": 1,
                "season": season, "season1": season, "ind": 0, "team": 0,
                "pageitems": 2000000000, "pagenum": 1, "type": 8,
                "sortstat": "WAR", "sortorder": "desc", "players": 0,
            }
            resp = session.get(
                _FG_API, params=params, timeout=30,
                headers={"Referer": "https://www.fangraphs.com/leaders/major-league"},
            )
            if resp.status_code == 403:
                raise PermissionError(
                    f"FanGraphs returned 403 for season {season}. "
                    "Your IP may be rate-limited — wait a few minutes and retry."
                )
            resp.raise_for_status()
            payload = resp.json()
            records = payload.get("data", payload) if isinstance(payload, dict) else payload
            season_df = pd.DataFrame(records)
            season_df["Season"] = season
            if "TeamNameAbb" in season_df.columns:
                season_df["Team"] = season_df["TeamNameAbb"]
            season_df = season_df[
                season_df["Team"].notna()
                & ~season_df["Team"].astype(str).str.contains(r"Tms|- - -", na=False)
                & (season_df["Team"] != "")
            ]
            if "Name" in season_df.columns and season_df["Name"].astype(str).str.contains("<a ", na=False).any():
                season_df["Name"] = season_df["Name"].astype(str).str.extract(r">([^<]+)</a>", expand=False)
            seasons.append(season_df)
            if season < end:
                time.sleep(0.5)

        seasons = [s for s in seasons if not s.empty]
        if not seasons:
            return pd.DataFrame(columns=["IDfg", "Name", "Team", "Season", "Age",
                                          "IP", "ERA", "FIP", "K/9", "BB/9", "WAR"])
        df = pd.concat(seasons, ignore_index=True)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={"playerid": "IDfg"})
        df.to_csv(cache_path, index=False)

    if con is not None:
        normalized = normalize_pitching(df)
        new_players = load_players(con, normalized)
        total_players = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        print(f"  pitching load: +{new_players} players seeded from pitching data")
        print(f"  players table total: {total_players}")

    return df


def pull_statcast(
    start: int = 2015,
    end: int = 2025,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Pull Statcast exit-velo/barrel and expected-stats per season via pybaseball.

    Merges two Statcast leaderboards per year on mlbam_id:
      - statcast_batter_exitvelo_barrels → exit_velo_avg, barrel_pct, hard_hit_pct
      - statcast_batter_expected_stats   → xba, xwoba

    When `con` is provided the data is upserted into statcast_quality.
    """
    from src.transform.load import load_statcast

    RAW_PATH.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_PATH / f"statcast_{start}_{end}_{date.today().isoformat()}.csv"

    if cache_path.exists():
        df = pd.read_csv(cache_path)
    else:
        all_seasons: list[pd.DataFrame] = []
        for year in range(start, end + 1):
            try:
                ev = pybaseball.statcast_batter_exitvelo_barrels(year, minBBE=50)
                ev = (
                    ev.rename(columns={"player_id": "mlbam_id"})
                    [["mlbam_id", "avg_hit_speed", "brl_percent", "anglesweetspotpercent"]]
                    .rename(columns={
                        "avg_hit_speed":          "exit_velo_avg",
                        "brl_percent":             "barrel_pct",
                        "anglesweetspotpercent":   "hard_hit_pct",
                    })
                )
            except Exception as e:
                print(f"  {year} exitvelo error: {e}")
                ev = pd.DataFrame(columns=["mlbam_id", "exit_velo_avg", "barrel_pct", "hard_hit_pct"])

            try:
                xs = pybaseball.statcast_batter_expected_stats(year, minPA=50)
                xs = (
                    xs.rename(columns={"player_id": "mlbam_id"})
                    [["mlbam_id", "est_ba", "est_woba"]]
                    .rename(columns={"est_ba": "xba", "est_woba": "xwoba"})
                )
            except Exception as e:
                print(f"  {year} xstats error: {e}")
                xs = pd.DataFrame(columns=["mlbam_id", "xba", "xwoba"])

            season_df = ev.merge(xs, on="mlbam_id", how="outer")
            season_df["season"] = year
            all_seasons.append(season_df)
            time.sleep(0.5)
            print(f"  {year}: {len(season_df)} batter records")

        df = pd.concat(all_seasons, ignore_index=True) if all_seasons else pd.DataFrame()
        df.to_csv(cache_path, index=False)

    if con is not None:
        from src.transform.load import load_statcast as _load
        n = _load(con, df)
        total = con.execute("SELECT COUNT(*) FROM statcast_quality").fetchone()[0]
        print(f"  statcast load: {n} rows upserted  |  {total} total in table")

    return df


def pull_draft(
    start: int = 2013,
    end: int = 2025,
    max_round: int = 20,
    con: duckdb.DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Pull MLB amateur draft data from Baseball Reference via pybaseball.

    Fetches rounds 1–max_round for each year in [start, end], stopping early
    for a given year when pybaseball returns an empty DataFrame (e.g. the
    2020 COVID-shortened 5-round draft).  Results are cached to data/raw/.

    When `con` is provided the data is normalized and upserted into draft_picks.
    """
    from src.transform.load import load_draft_picks

    RAW_PATH.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_PATH / f"draft_{start}_{end}_r{max_round}_{date.today().isoformat()}.csv"

    if cache_path.exists():
        df = pd.read_csv(cache_path)
    else:
        all_picks: list[pd.DataFrame] = []
        for year in range(start, end + 1):
            year_picks: list[pd.DataFrame] = []
            for rnd in range(1, max_round + 1):
                try:
                    picks = pybaseball.amateur_draft(year, rnd, keep_stats=True)
                except Exception:
                    break
                if picks.empty:
                    break
                picks = picks.copy()
                picks["draft_season"] = year
                picks["draft_round"] = rnd
                year_picks.append(picks)
                time.sleep(0.3)
            all_picks.extend(year_picks)
            n_picks = sum(len(p) for p in year_picks)
            print(f"  {year}: {n_picks} picks across {len(year_picks)} rounds")

        if not all_picks:
            return pd.DataFrame()
        df = pd.concat(all_picks, ignore_index=True)
        df.to_csv(cache_path, index=False)

    if con is not None:
        n = load_draft_picks(con, df)
        total = con.execute("SELECT COUNT(*) FROM draft_picks").fetchone()[0]
        print(f"  draft load: {n} picks inserted  |  {total} total in table")

    return df


def pull_statcast_batter(mlbam_id: int, start: str, end: str) -> pd.DataFrame:
    """Pull Statcast data for a single batter. start/end: 'YYYY-MM-DD'."""
    return pybaseball.statcast_batter(start, end, player_id=mlbam_id)


def pull_team_batting(start: int = START_SEASON, end: int = date.today().year) -> pd.DataFrame:
    """Pull team-level batting aggregates — used for 01_exploration.ipynb.

    Bypasses pybaseball to call the FanGraphs JSON API directly, avoiding the
    legacy leaders-legacy.aspx endpoint that returns 403. Player rows are
    aggregated to team-season level; rate stats are recomputed from counting
    stats rather than averaged.
    """
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_PATH / f"team_batting_{start}_{end}_{date.today().isoformat()}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path)

    session = curl_requests.Session(impersonate="chrome")

    seasons = []
    for season in range(start, end + 1):
        seasons.append(_fetch_fg_batting_season(session, season))
        if season < end:
            time.sleep(0.5)

    seasons = [s for s in seasons if not s.empty]
    if not seasons:
        return pd.DataFrame(columns=["Season", "Team"])
    players = pd.concat(seasons, ignore_index=True)
    players.columns = [c.strip() for c in players.columns]

    if "Team" not in players.columns:
        raise KeyError(f"No 'Team' column after fetch. Got: {players.columns.tolist()}")

    if "1B" not in players.columns:
        players["1B"] = players["H"] - players["2B"] - players["3B"] - players["HR"]

    # Force numeric on every column that should be numeric; non-parseable → NaN
    numeric_cols = [
        "G", "AB", "PA", "H", "1B", "2B", "3B", "HR",
        "R", "RBI", "BB", "IBB", "SO", "HBP", "SF", "SH",
        "GDP", "SB", "CS", "WAR", "wOBA", "wRC+",
    ]
    for col in numeric_cols:
        if col in players.columns:
            players[col] = pd.to_numeric(players[col], errors="coerce")

    # Counting stats: sum per team-season
    counting = [
        "G", "AB", "PA", "H", "1B", "2B", "3B", "HR",
        "R", "RBI", "BB", "IBB", "SO", "HBP", "SF", "SH",
        "GDP", "SB", "CS", "WAR",
    ]
    existing_counting = [c for c in counting if c in players.columns]

    team_df = (
        players.groupby(["Season", "Team"])[existing_counting]
        .sum()
        .reset_index()
    )

    # Rate stats: PA-weighted mean per team-season
    # wRC+ and wOBA cannot be summed — weight each player's value by their PA share
    for rate_col in ("wOBA", "wRC+"):
        if rate_col in players.columns and "PA" in players.columns:
            weighted = (
                players.dropna(subset=[rate_col, "PA"])
                .assign(_w=lambda d: d[rate_col] * d["PA"])
                .groupby(["Season", "Team"])
                .apply(lambda g: g["_w"].sum() / g["PA"].sum(), include_groups=False)
                .rename(rate_col)
                .reset_index()
            )
            team_df = team_df.merge(weighted, on=["Season", "Team"], how="left")

    # Recompute AVG/OBP/SLG/OPS from counting stats (more accurate than averaging player rates)
    team_df["AVG"] = team_df["H"] / team_df["AB"]
    if {"BB", "HBP", "SF"}.issubset(team_df.columns):
        team_df["OBP"] = (team_df["H"] + team_df["BB"] + team_df["HBP"]) / (
            team_df["AB"] + team_df["BB"] + team_df["HBP"] + team_df["SF"]
        )
    if "1B" in team_df.columns:
        team_df["SLG"] = (
            team_df["1B"]
            + 2 * team_df["2B"]
            + 3 * team_df["3B"]
            + 4 * team_df["HR"]
        ) / team_df["AB"]
        team_df["OPS"] = team_df["OBP"] + team_df["SLG"]

    # Ensure all rate/value columns are float so .round() / .rank() never crash
    for col in ("WAR", "wOBA", "wRC+", "AVG", "OBP", "SLG", "OPS"):
        if col in team_df.columns:
            team_df[col] = pd.to_numeric(team_df[col], errors="coerce")

    # Map abbreviations to full names now that all joins on abbreviation keys are done
    team_df["Team"] = team_df["Team"].map(_TEAM_NAMES)
    team_df = team_df.dropna(subset=["Team"])

    team_df.to_csv(cache_path, index=False)
    return team_df


if __name__ == "__main__":
    with db_connection() as con:
        print("DB initialized at", DB_PATH)
        print("Pulling batting 2015–present...")
        pull_batting(con=con)
        print("Pulling pitching 2015–present...")
        pull_pitching(con=con)
        print("Pulling Statcast 2015–2025...")
        pull_statcast(con=con)
        print("Pulling draft data 2013–2025...")
        pull_draft(con=con)
        print("Done.")
