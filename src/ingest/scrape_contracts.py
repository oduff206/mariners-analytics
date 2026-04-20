"""
Scrape active Mariners contract data from Spotrac.
Uses curl_cffi (same TLS-impersonation approach as the FanGraphs fetcher)
so no headless browser is required.

Run directly:  python -m src.ingest.scrape_contracts
"""

import re
import time

import duckdb
import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from pathlib import Path
from datetime import date

DB_PATH = Path(__file__).parents[2] / "data" / "mariners.duckdb"
RAW_PATH = Path(__file__).parents[2] / "data" / "raw"

_SPOTRAC_URL = "https://www.spotrac.com/mlb/seattle-mariners/contracts/"

_CONTRACT_TYPE_MAP = {
    "extension": "extension",
    "free agent": "FA",
    "free-agent": "FA",
    "arbitration": "arbitration",
    "pre-arbitration": "pre-arb",
    "pre-arb": "pre-arb",
    "minor league": "minor-league",
}


def _parse_dollars(text: str) -> float | None:
    """'$108,000,000' → 108.0  (millions)"""
    cleaned = re.sub(r"[^\d.]", "", text)
    return round(float(cleaned) / 1_000_000, 4) if cleaned else None


def _normalize_contract_type(raw: str) -> str:
    raw_lower = raw.lower()
    for key, val in _CONTRACT_TYPE_MAP.items():
        if key in raw_lower:
            return val
    return raw.strip()


def _clean_name(raw: str) -> str:
    """'RodriguezJulio Rodriguez' → 'Julio Rodriguez'"""
    # Spotrac prepends the last name in title case before the full name
    # Split on the first capital letter that follows a lowercase letter
    # e.g. 'RodriguezJulio Rodriguez' → find where the full name starts
    match = re.search(r"[A-Z][a-z]+(?: [A-Z][a-z]+)+ [A-Z][a-z]+", raw)
    if match:
        return match.group(0)
    # Fallback: take everything after the first all-caps/title sequence
    parts = re.split(r"(?<=[a-z])(?=[A-Z])", raw, maxsplit=1)
    return parts[-1].strip()


def scrape_mariners_contracts() -> pd.DataFrame:
    """Fetch and parse the Mariners active contracts table from Spotrac.

    Returns one row per contract with columns:
        name_full, position, contract_type, season_start, season_end,
        years, total_value_m, aav_m
    """
    session = curl_requests.Session(impersonate="chrome")
    resp = session.get(
        _SPOTRAC_URL,
        timeout=20,
        headers={"Referer": "https://www.spotrac.com/mlb/"},
    )
    if resp.status_code == 403:
        raise PermissionError(
            "Spotrac returned 403. Try again in a few minutes."
        )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    if table is None:
        raise ValueError("No <table> found on Spotrac contracts page — page structure may have changed.")

    rows = []
    for tr in table.find_all("tr")[1:]:  # skip header row
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 10:
            continue
        rows.append({
            "name_full":       _clean_name(cells[0]),
            "position":        cells[1],
            "contract_type":   _normalize_contract_type(cells[3]),
            "season_start":    int(cells[5]) if cells[5].isdigit() else None,
            "season_end":      int(cells[6]) if cells[6].isdigit() else None,
            "years":           int(cells[7]) if cells[7].isdigit() else None,
            "total_value_m":   _parse_dollars(cells[8]),
            "aav_m":           _parse_dollars(cells[9]),
        })

    return pd.DataFrame(rows)


def match_mlbam_ids(con: duckdb.DuckDBPyConnection, contracts: pd.DataFrame) -> pd.DataFrame:
    """Join contracts to the players table by normalized name.

    Exact match only. Unmatched rows get mlbam_id = None and are printed
    so the user can add them manually.
    """
    players = con.execute("SELECT mlbam_id, name_full FROM players").fetchdf()
    # Normalize: lowercase, strip punctuation
    def norm(s: str) -> str:
        return re.sub(r"[^a-z ]", "", s.lower().strip())

    players["_key"] = players["name_full"].apply(norm)
    contracts["_key"] = contracts["name_full"].apply(norm)

    merged = contracts.merge(
        players[["mlbam_id", "_key"]],
        on="_key", how="left"
    ).drop(columns="_key")

    unmatched = merged[merged["mlbam_id"].isna()]["name_full"].tolist()
    if unmatched:
        print(f"⚠ Could not match {len(unmatched)} player(s) to mlbam_id:")
        for name in unmatched:
            print(f"  - {name}")
        print("  → Add them manually to the players table, then re-run.")

    return merged


def load_contracts(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Upsert contracts into DuckDB. Returns number of rows written."""
    rows = df.dropna(subset=["mlbam_id", "season_start"]).copy()
    rows["mlbam_id"] = rows["mlbam_id"].astype(int)
    rows["team"] = "Mariners"
    rows["source"] = "Spotrac"

    keep = ["mlbam_id", "season_start", "season_end", "years",
            "total_value_m", "aav_m", "team", "contract_type", "source"]
    rows = rows[[c for c in keep if c in rows.columns]]

    # Delete-then-insert: refreshes all Mariners contracts on each run
    con.execute("DELETE FROM contracts WHERE team = 'Mariners'")
    con.register("_contracts_staging", rows)
    cols = ", ".join(keep)
    con.execute(f"""
        INSERT INTO contracts (id, {cols})
        SELECT
            COALESCE((SELECT MAX(id) FROM contracts), 0)
                + row_number() OVER () AS id,
            {cols}
        FROM _contracts_staging
    """)
    return len(rows)


if __name__ == "__main__":
    from src.ingest.pull_data import db_connection
    RAW_PATH.mkdir(parents=True, exist_ok=True)

    with db_connection() as con:
        print("Scraping Spotrac contracts...")
        contracts = scrape_mariners_contracts()
        print(f"  → {len(contracts)} contracts scraped")

        contracts = match_mlbam_ids(con, contracts)
        inserted = load_contracts(con, contracts)
        print(f"  → {inserted} new/updated rows in contracts table")

        cache_path = RAW_PATH / f"contracts_mariners_{date.today().isoformat()}.csv"
        contracts.to_csv(cache_path, index=False)
        print(f"  → Saved to {cache_path}")
