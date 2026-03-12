"""
fix_torvik_season_v3.py
Fixes the pandas quoting issue with "Fun Rk, adjt" column in 2016-2022 CSVs.
Uses column positions instead of names for 2016-2022 where header parsing fails.
"""
import sqlite3, requests, time
import pandas as pd
from io import StringIO

DB = r"..\data\basketball.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def fv(x):
    try:
        if x in (None, '', 'nan', 'None', '---', 'N/A'): return None
        return float(str(x).strip().replace('%','').replace(',',''))
    except: return None

conn = sqlite3.connect(DB)
cur  = conn.cursor()

print("Rebuilding torvik_season from scratch...")
cur.execute("DROP TABLE IF EXISTS torvik_season")
cur.execute("""
    CREATE TABLE torvik_season (
        season INTEGER, team TEXT,
        adj_o REAL, adj_d REAL, adj_t REAL, barthag REAL, adj_em REAL,
        efg_o REAL, efg_d REAL, tov_o REAL, tov_d REAL,
        orb REAL, drb REAL, ftr_o REAL, ftr_d REAL,
        two_p_o REAL, two_p_d REAL, three_p_o REAL, three_p_d REAL,
        blk_pct REAL, ast_pct REAL, three_p_rate REAL,
        avg_hgt REAL, eff_hgt REAL, experience REAL,
        pake REAL, pase REAL, talent REAL,
        elite_sos REAL, ft_pct REAL, wab REAL,
        PRIMARY KEY (season, team)
    )
""")
conn.commit()

total = 0
for season in range(2016, 2026):
    url = f"https://barttorvik.com/{season}_team_results.csv"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
        if r.text.strip().startswith('<!'):
            print(f"  {season}: BOT BLOCKED")
            continue

        # Read raw text, parse header manually to detect column positions
        raw = r.text.strip()
        lines = raw.split('\n')
        
        # Parse header handling quoted fields with commas
        import csv
        header = next(csv.reader([lines[0]]))
        header = [h.strip().lower() for h in header]
        
        print(f"  {season}: {len(header)} columns. First 6: {header[:6]}")
        
        # Find column indices by name
        def ci(names):
            for n in names:
                if n in header: return header.index(n)
            return None
        
        idx_team    = ci(['team'])
        idx_adj_o   = ci(['adjoe', 'adj oe', 'adj. oe'])
        idx_adj_d   = ci(['adjde', 'adj de', 'adj. de'])
        idx_adj_t   = ci(['adjt', 'adj. t', 'tempo'])
        idx_barthag = ci(['barthag'])
        idx_wab     = ci(['wab'])
        idx_elite   = ci(['elite sos'])

        print(f"    team={idx_team} adj_o={idx_adj_o} adj_d={idx_adj_d} adj_t={idx_adj_t}")
        
        if idx_team is None or idx_adj_o is None:
            print(f"    ERROR: can't find team or adj_o column")
            continue

        inserted = 0
        reader = csv.reader(lines[1:])
        for row in reader:
            if len(row) <= max(idx_team, idx_adj_o): continue
            team  = row[idx_team].strip()
            adj_o = fv(row[idx_adj_o])
            adj_d = fv(row[idx_adj_d]) if idx_adj_d else None
            
            if not team or team in ('nan', 'None', ''): continue
            if adj_o is None or not (50 < adj_o < 150): continue

            adj_em = (adj_o - adj_d) if adj_o and adj_d else None

            def gv(idx): return fv(row[idx]) if idx is not None and idx < len(row) else None

            cur.execute("""
                INSERT OR REPLACE INTO torvik_season
                (season, team, adj_o, adj_d, adj_t, barthag, adj_em,
                 elite_sos, wab)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                season, team, adj_o, adj_d,
                gv(idx_adj_t), gv(idx_barthag), adj_em,
                gv(idx_elite), gv(idx_wab),
            ))
            inserted += 1

        conn.commit()
        sample = cur.execute(
            "SELECT team, adj_o, adj_d FROM torvik_season WHERE season=? LIMIT 1", (season,)
        ).fetchone()
        total += inserted
        print(f"    → {inserted} teams inserted | sample: {sample}")

    except Exception as e:
        print(f"  {season}: ERROR {e}")
    time.sleep(0.3)

print(f"\ntorvik_season total: {total}")

# Rebuild torvik_daily
print("\nRebuilding torvik_daily...")
cur.execute("DROP TABLE IF EXISTS torvik_daily")
cur.execute("""
    CREATE TABLE torvik_daily (
        season INTEGER, snapshot_date TEXT, team TEXT,
        adj_o REAL, adj_d REAL, adj_t REAL, barthag REAL, adj_em REAL,
        efg_o REAL, efg_d REAL, tov_o REAL, tov_d REAL,
        orb REAL, drb REAL, ftr_o REAL, ftr_d REAL,
        PRIMARY KEY (season, snapshot_date, team)
    )
""")

rows = cur.execute("""
    SELECT season, team, adj_o, adj_d, adj_t, barthag, adj_em,
           efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d
    FROM torvik_season WHERE adj_o IS NOT NULL
""").fetchall()

batch = []
for row in rows:
    season = row[0]; team = row[1]; vals = row[2:]
    batch.append((season, f"{season-1}1101", team) + vals)
    batch.append((season, f"{season}0201",   team) + vals)

cur.executemany("""
    INSERT OR IGNORE INTO torvik_daily
    (season, snapshot_date, team, adj_o, adj_d, adj_t, barthag, adj_em,
     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", batch)
conn.commit()

daily_ct = cur.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
print(f"torvik_daily: {daily_ct:,} rows")
print("\nPer-season:")
for s in range(2016, 2026):
    ct = cur.execute("SELECT COUNT(*) FROM torvik_daily WHERE season=?", (s,)).fetchone()[0]
    samp = cur.execute("SELECT team, adj_o FROM torvik_daily WHERE season=? AND adj_o IS NOT NULL LIMIT 1", (s,)).fetchone()
    print(f"  {s}: {ct} rows | {samp}")

conn.close()
print("\nDone. Run: python 04_build_features.py")
