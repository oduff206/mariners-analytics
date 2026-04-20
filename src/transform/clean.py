"""
Transform raw pybaseball DataFrames into DuckDB-ready staging tables.
"""

import pandas as pd


MARINERS_TEAM_CODES = {"SEA", "Mariners"}


def normalize_batting(df: pd.DataFrame) -> pd.DataFrame:
    """Rename FanGraphs batting columns to schema names and add derived fields."""
    col_map = {
        "xMLBAMID": "mlbam_id",
        "IDfg": "fg_id",
        "Name": "name_full",
        "Team": "team",
        "Season": "season",
        "Age": "age",
        "Pos": "position",
        "Bats": "bats",
        "PA": "pa",
        "AB": "ab",
        "H": "h",
        "HR": "hr",
        "RBI": "rbi",
        "SB": "sb",
        "AVG": "avg",
        "OBP": "obp",
        "SLG": "slg",
        "wOBA": "woba",
        "wRC+": "wrc_plus",
        "WAR": "war",
    }
    out = df.rename(columns=col_map)
    out = out[[c for c in col_map.values() if c in out.columns]]
    out["is_mariners"] = out["team"].isin(MARINERS_TEAM_CODES)
    return out


def normalize_pitching(df: pd.DataFrame) -> pd.DataFrame:
    """Rename FanGraphs pitching columns to schema names."""
    col_map = {
        "xMLBAMID": "mlbam_id",
        "IDfg": "fg_id",
        "Name": "name_full",
        "Team": "team",
        "Season": "season",
        "Age": "age",
        "IP": "ip",
        "ERA": "era",
        "FIP": "fip",
        "K/9": "k_per9",
        "BB/9": "bb_per9",
        "WAR": "war",
    }
    out = df.rename(columns=col_map)
    out = out[[c for c in col_map.values() if c in out.columns]]
    out["is_mariners"] = out["team"].isin(MARINERS_TEAM_CODES)
    return out
