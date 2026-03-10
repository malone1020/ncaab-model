"""
Script 02b: Import Kaggle CBB Season Stats
==========================================
Imports the cbb.csv file (team season summaries with four factors)
into a new table: team_season_stats

This complements KenPom data with four-factor metrics:
EFG_O, EFG_D, TOR, TORD, ORB, DRB, FTR, FTRD, BARTHAG

Usage:
    python scripts/02b_import_season_stats.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"
CSV_PATH = BASE_DIR / "data" / "raw" / "cbb_team_seasons.csv"

def init_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_season_stats (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            season      INTEGER NOT NULL,
            team        TEXT    NOT NULL,
            conf        TEXT,
            games       INTEGER,
            wins        INTEGER,
            adj_oe      REAL,       -- Adjusted offensive efficiency
            adj_de      REAL,       -- Adjusted defensive efficiency
            barthag     REAL,       -- Power rating (win prob vs avg D1)
            efg_o       REAL,       -- Effective FG% offense
            efg_d       REAL,       -- Effective FG% defense
            tor         REAL,       -- Turnover rate offense
            tord        REAL,       -- Turnover rate defense (forced)
            orb         REAL,       -- Offensive rebound rate
            drb         REAL,       -- Defensive rebound rate
            ftr         REAL,       -- Free throw rate offense
            ftrd        REAL,       -- Free throw rate defense
            two_p_o     REAL,       -- 2P% offense
            two_p_d     REAL,       -- 2P% defense
            three_p_o   REAL,       -- 3P% offense
            three_p_d   REAL,       -- 3P% defense
            adj_t       REAL,       -- Adjusted tempo
            wab         REAL,       -- Wins above bubble
            postseason  TEXT,       -- Tournament result
            seed        INTEGER,    -- NCAA seed if applicable
            UNIQUE(season, team)
        )
    """)
    conn.commit()
    print("✓ team_season_stats table ready")

def import_csv(conn):
    df = pd.read_csv(CSV_PATH)
    print(f"  Loaded {len(df)} rows from {CSV_PATH.name}")
    print(f"  Columns: {df.columns.tolist()}")
    print(f"  Years: {sorted(df['YEAR'].unique())}")

    # Rename columns to our schema
    df = df.rename(columns={
        'YEAR'      : 'season',
        'TEAM'      : 'team',
        'CONF'      : 'conf',
        'G'         : 'games',
        'W'         : 'wins',
        'ADJOE'     : 'adj_oe',
        'ADJDE'     : 'adj_de',
        'BARTHAG'   : 'barthag',
        'EFG_O'     : 'efg_o',
        'EFG_D'     : 'efg_d',
        'TOR'       : 'tor',
        'TORD'      : 'tord',
        'ORB'       : 'orb',
        'DRB'       : 'drb',
        'FTR'       : 'ftr',
        'FTRD'      : 'ftrd',
        '2P_O'      : 'two_p_o',
        '2P_D'      : 'two_p_d',
        '3P_O'      : 'three_p_o',
        '3P_D'      : 'three_p_d',
        'ADJ_T'     : 'adj_t',
        'WAB'       : 'wab',
        'POSTSEASON': 'postseason',
        'SEED'      : 'seed',
    })

    cols = [
        'season', 'team', 'conf', 'games', 'wins',
        'adj_oe', 'adj_de', 'barthag',
        'efg_o', 'efg_d', 'tor', 'tord', 'orb', 'drb',
        'ftr', 'ftrd', 'two_p_o', 'two_p_d',
        'three_p_o', 'three_p_d', 'adj_t', 'wab',
        'postseason', 'seed'
    ]
    available = [c for c in cols if c in df.columns]
    df = df[available]

    inserted = skipped = 0
    c = conn.cursor()
    for _, row in df.iterrows():
        try:
            c.execute(f"""
                INSERT OR IGNORE INTO team_season_stats ({', '.join(available)})
                VALUES ({', '.join(['?' for _ in available])})
            """, [row.get(col) for col in available])
            if c.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  Row error: {e}")
    conn.commit()
    return inserted, skipped

def main():
    if not CSV_PATH.exists():
        print(f"⚠  File not found: {CSV_PATH}")
        print("   Save the Kaggle cbb.csv as data/raw/cbb_team_seasons.csv")
        return

    conn = sqlite3.connect(DB_PATH)
    init_table(conn)

    ins, skp = import_csv(conn)
    print(f"\n  Inserted : {ins}")
    print(f"  Skipped  : {skp} (duplicates)")

    # Summary
    c = conn.cursor()
    c.execute("SELECT season, COUNT(*) FROM team_season_stats GROUP BY season ORDER BY season")
    print("\n── team_season_stats contents ────────────────")
    for row in c.fetchall():
        print(f"   {row[0]}: {row[1]} teams")

    conn.close()

if __name__ == "__main__":
    main()
