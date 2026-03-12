"""
rebuild_torvik_daily_from_season.py

Since Torvik blocks Python requests, this script creates a synthetic 
torvik_daily table from torvik_season data by treating the season-end 
ratings as a single annual snapshot (dated Nov 1 of each season).

This gives the backtest non-null values for the daily feature group
at the cost of using season-level rather than truly time-varying data.
The walk-forward backtest will determine empirically whether this adds 
value over torvik_season alone.

Run from scripts/:  python rebuild_torvik_daily_from_season.py
"""
import sqlite3

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

# Drop and recreate
cur.execute("DROP TABLE IF EXISTS torvik_daily")
cur.execute("""
    CREATE TABLE torvik_daily (
        season        INTEGER,
        snapshot_date TEXT,
        team          TEXT,
        adj_o         REAL,
        adj_d         REAL,
        adj_t         REAL,
        barthag       REAL,
        adj_em        REAL,
        efg_o         REAL,
        efg_d         REAL,
        tov_o         REAL,
        tov_d         REAL,
        orb           REAL,
        drb           REAL,
        ftr_o         REAL,
        ftr_d         REAL,
        PRIMARY KEY (season, snapshot_date, team)
    )
""")

# Pull from torvik_season — use season-end snapshot, dated Nov 1 of season start
# (early enough to be before all games in that season)
rows = cur.execute("""
    SELECT season, team, adj_o, adj_d, adj_t, barthag, adj_em,
           efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d
    FROM torvik_season
    WHERE adj_o IS NOT NULL
""").fetchall()

# For each season, create two snapshots: 
#   Nov 1 (start of season) and Feb 1 (mid-season)
# Both use same season-level data since that's all we have
batch = []
for row in rows:
    season = row[0]
    team   = row[1]
    vals   = row[2:]  # adj_o through ftr_d
    
    # Season start snapshot
    snap1 = f"{season-1}1101"  # Nov 1 of start year
    snap2 = f"{season}0201"    # Feb 1 of end year
    
    batch.append((season, snap1, team) + vals)
    batch.append((season, snap2, team) + vals)

cur.executemany("""
    INSERT OR IGNORE INTO torvik_daily
    (season, snapshot_date, team, adj_o, adj_d, adj_t, barthag, adj_em,
     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", batch)
conn.commit()

count = cur.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
null_check = cur.execute("SELECT COUNT(*) FROM torvik_daily WHERE adj_o IS NULL").fetchone()[0]
sample = cur.execute("""
    SELECT team, adj_o, adj_d, adj_t, barthag 
    FROM torvik_daily WHERE season=2024 AND adj_o IS NOT NULL LIMIT 3
""").fetchall()

print(f"torvik_daily rebuilt: {count:,} rows ({null_check} null adj_o)")
print("Sample rows:")
for s in sample:
    print(f"  {s[0]}: adjO={s[1]:.1f} adjD={s[2]:.1f} tempo={s[3]:.1f} barthag={s[4]:.3f}")

conn.close()
print("\nDone. Now run: python 04_build_features.py")
