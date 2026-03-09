"""
Script 01: Initialize Database + Import KenPom Data
====================================================
Run this script once to:
1. Create the SQLite database with full schema
2. Import all KenPom CSV files from data/raw/kenpom/
3. Validate the import and print a summary

Usage:
    python scripts/01_init_database.py

Requirements:
    - KenPom CSVs saved in data/raw/kenpom/
    - Naming convention: kenpom_YYYY_final.csv / kenpom_YYYY_pretourney.csv / kenpom_YYYY_current.csv
"""

import sqlite3
import pandas as pd
import os
import glob
import re
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent.parent   # project root
DB_PATH     = BASE_DIR / "data" / "basketball.db"
KENPOM_DIR  = BASE_DIR / "data" / "raw" / "kenpom"

# ── Create data directories if they don't exist ────────────────────────────────
os.makedirs(KENPOM_DIR, exist_ok=True)
os.makedirs(BASE_DIR / "data" / "processed", exist_ok=True)

print(f"Database path : {DB_PATH}")
print(f"KenPom folder : {KENPOM_DIR}")
print()


# ══════════════════════════════════════════════════════════════════════════════
# 1. INITIALIZE DATABASE SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

def init_database(conn):
    """Create all tables if they don't already exist."""
    c = conn.cursor()

    # -- KenPom ratings snapshots ------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS kenpom_ratings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            season          INTEGER NOT NULL,
            snapshot_type   TEXT    NOT NULL,   -- 'final', 'pretourney', 'current'
            team            TEXT    NOT NULL,
            adj_em          REAL,               -- Net efficiency (AdjEM)
            adj_em_rank     INTEGER,
            adj_o           REAL,               -- Offensive efficiency (AdjOE)
            adj_o_rank      INTEGER,
            adj_d           REAL,               -- Defensive efficiency (AdjDE)
            adj_d_rank      INTEGER,
            adj_t           REAL,               -- Adjusted tempo (AdjTempo)
            adj_t_rank      INTEGER,
            raw_o           REAL,               -- Raw offensive efficiency (OE)
            raw_o_rank      INTEGER,
            raw_d           REAL,               -- Raw defensive efficiency (DE)
            raw_d_rank      INTEGER,
            UNIQUE(season, snapshot_type, team)
        )
    """)

    # -- Games (results + metadata) ----------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            season          INTEGER NOT NULL,
            game_date       TEXT    NOT NULL,   -- YYYY-MM-DD
            home_team       TEXT    NOT NULL,
            away_team       TEXT    NOT NULL,
            home_score      INTEGER,
            away_score      INTEGER,
            neutral_site    INTEGER DEFAULT 0,  -- 1 = neutral court
            tournament      TEXT,               -- NULL, 'NCAA', 'NIT', conf name, etc.
            source          TEXT                -- 'sports_reference', etc.
        )
    """)

    # -- Odds (lines + totals) ---------------------------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS odds (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         INTEGER REFERENCES games(id),
            bookmaker       TEXT,
            market          TEXT,               -- 'spreads' or 'totals'
            open_line       REAL,               -- opening spread or total
            close_line      REAL,               -- closing spread or total
            open_odds_home  INTEGER,            -- American odds
            open_odds_away  INTEGER,
            close_odds_home INTEGER,
            close_odds_away INTEGER,
            retrieved_at    TEXT
        )
    """)

    # -- Pre-game features (built in script 03) ----------------------------
    c.execute("""
        CREATE TABLE IF NOT EXISTS game_features (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id             INTEGER REFERENCES games(id),
            home_adj_em         REAL,
            away_adj_em         REAL,
            em_gap              REAL,           -- home minus away
            home_adj_o          REAL,
            home_adj_d          REAL,
            away_adj_o          REAL,
            away_adj_d          REAL,
            off_matchup         REAL,           -- home_adj_o minus away_adj_d
            def_matchup         REAL,           -- away_adj_o minus home_adj_d
            tempo_avg           REAL,
            home_torvik_net     REAL,
            away_torvik_net     REAL,
            home_hasla_mom      REAL,
            away_hasla_mom      REAL,
            home_hasla_momd     REAL,
            away_hasla_momd     REAL,
            actual_margin       REAL,           -- home_score minus away_score
            covered_spread      INTEGER         -- 1 if home covered, 0 if not
        )
    """)

    conn.commit()
    print("✓ Database schema created (or already existed)")


# ══════════════════════════════════════════════════════════════════════════════
# 2. IMPORT KENPOM CSVs
# ══════════════════════════════════════════════════════════════════════════════

# Map KenPom export column names → our database column names
COLUMN_MAP = {
    "TeamName"      : "team",
    "AdjEM"         : "adj_em",
    "RankAdjEM"     : "adj_em_rank",
    "AdjOE"         : "adj_o",
    "RankAdjOE"     : "adj_o_rank",
    "AdjDE"         : "adj_d",
    "RankAdjDE"     : "adj_d_rank",
    "AdjTempo"      : "adj_t",
    "RankAdjTempo"  : "adj_t_rank",
    "OE"            : "raw_o",
    "RankOE"        : "raw_o_rank",
    "DE"            : "raw_d",
    "RankDE"        : "raw_d_rank",
    "Season"        : "season",
}


def get_snapshot_type(filename: str) -> str:
    """Determine snapshot type from filename."""
    name = filename.lower()
    if "pretourney" in name or "pre_tourney" in name or "pre-tourney" in name:
        return "pretourney"
    elif "final" in name:
        return "final"
    else:
        return "current"


def import_kenpom_csv(filepath: Path, conn):
    """Load one KenPom CSV into the kenpom_ratings table."""

    filename = filepath.stem
    snapshot_type = get_snapshot_type(filename)

    # Read CSV
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"  ⚠  Could not read {filename}: {e}")
        return 0, 0

    # Drop completely empty files
    if df.empty or len(df.columns) < 3:
        print(f"  ⚠  {filename} appears empty — skipping")
        return 0, 0

    # Show columns on first file to confirm mapping
    # (comment this out after first successful run)
    # print(f"     Columns: {df.columns.tolist()}")

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Drop rows where team name is missing
    if "team" in df.columns:
        df = df[df["team"].notna()]
        df = df[df["team"].str.strip() != ""]
    else:
        print(f"  ⚠  No 'TeamName' column found in {filename}")
        print(f"     Columns found: {df.columns.tolist()}")
        return 0, 0

    # If season not in data, parse from filename
    if "season" not in df.columns:
        year_matches = re.findall(r"\d{4}", filename)
        if year_matches:
            df["season"] = int(year_matches[0])
        else:
            print(f"  ⚠  Cannot determine season for {filename} — skipping")
            return 0, 0

    # Add snapshot type
    df["snapshot_type"] = snapshot_type

    # Columns to insert
    db_cols = [
        "season", "snapshot_type", "team",
        "adj_em", "adj_em_rank",
        "adj_o", "adj_o_rank",
        "adj_d", "adj_d_rank",
        "adj_t", "adj_t_rank",
        "raw_o", "raw_o_rank",
        "raw_d", "raw_d_rank",
    ]
    available = [c for c in db_cols if c in df.columns]
    df = df[available]

    # Insert into database
    inserted = 0
    skipped  = 0
    c = conn.cursor()

    for _, row in df.iterrows():
        try:
            c.execute(f"""
                INSERT OR IGNORE INTO kenpom_ratings
                    ({', '.join(available)})
                VALUES
                    ({', '.join(['?' for _ in available])})
            """, [row.get(col) for col in available])
            if c.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"    Row error: {e} — {row.get('team')}")

    conn.commit()
    return inserted, skipped


# ══════════════════════════════════════════════════════════════════════════════
# 3. MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    conn = sqlite3.connect(DB_PATH)

    # Drop and recreate tables for a clean reimport
    print("Dropping existing tables for clean reimport...")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS kenpom_ratings")
    c.execute("DROP TABLE IF EXISTS game_features")
    c.execute("DROP TABLE IF EXISTS odds")
    c.execute("DROP TABLE IF EXISTS games")
    conn.commit()

    init_database(conn)
    print()

    # Find all KenPom CSVs
    csv_files = sorted(glob.glob(str(KENPOM_DIR / "kenpom_*.csv")))

    if not csv_files:
        print(f"⚠  No KenPom CSV files found in {KENPOM_DIR}")
        print("   Make sure files are named like: kenpom_2023_final.csv")
        conn.close()
        return

    print(f"Found {len(csv_files)} KenPom CSV file(s) to import:\n")

    total_inserted = 0
    total_skipped  = 0

    for filepath in csv_files:
        name = Path(filepath).name
        result = import_kenpom_csv(Path(filepath), conn)
        if isinstance(result, tuple):
            ins, skp = result
            total_inserted += ins
            total_skipped  += skp
            status = "✓" if ins > 0 else "⚠"
            print(f"  {status}  {name:45s}  inserted: {ins:4d}  skipped: {skp:4d}")
        else:
            print(f"  ✗  {name} — skipped")

    print()
    print(f"── Import complete ───────────────────────────────────────")
    print(f"   Total rows inserted : {total_inserted}")
    print(f"   Total rows skipped  : {total_skipped} (duplicates)")
    print()

    # Validation summary
    c = conn.cursor()
    c.execute("""
        SELECT season, snapshot_type, COUNT(*) as teams
        FROM kenpom_ratings
        GROUP BY season, snapshot_type
        ORDER BY season, snapshot_type
    """)
    rows = c.fetchall()

    if rows:
        print("── Database contents (kenpom_ratings) ───────────────────")
        print(f"   {'Season':<10} {'Snapshot':<15} {'Teams':>6}")
        print(f"   {'-'*10} {'-'*15} {'-'*6}")
        for season, snap, count in rows:
            print(f"   {season:<10} {snap:<15} {count:>6}")
    else:
        print("⚠  No data in kenpom_ratings — check warnings above")

    print()
    print(f"Database saved to: {DB_PATH}")
    conn.close()


if __name__ == "__main__":
    main()
