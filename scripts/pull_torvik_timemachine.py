"""
pull_torvik_timemachine.py
==========================
Fetches real Torvik time-machine snapshots via the browser-accessible CSV endpoint.
Uses cumulative season stats as of each snapshot date.

Run from scripts/: python pull_torvik_timemachine.py
Requires: active internet, barttorvik.com accessible (no bot block for CSV endpoint)

Snapshots: Dec 1, Jan 1, Feb 1, Mar 1, Mar 15 per season (2016-2025)
= 50 fetches × ~362 teams = ~18,100 rows
"""

import sqlite3, requests, time, os
import pandas as pd
from io import StringIO

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://barttorvik.com/trank.php",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def fv(x):
    try:
        s = str(x).strip().replace('%','').replace(',','')
        if s in ('','nan','None','---','N/A'): return None
        return float(s)
    except: return None

# Season snapshots: (season, snapshot_label, begin_date, end_date)
# begin is always Nov 1 of prior year (season start), end is the snapshot date
def make_snapshots():
    snapshots = []
    for season in range(2016, 2026):
        season_start = f"{season-1}1101"
        for label, end in [
            ('dec01', f"{season-1}1201"),
            ('jan01', f"{season}0101"),
            ('feb01', f"{season}0201"),
            ('mar01', f"{season}0301"),
            ('mar15', f"{season}0315"),
        ]:
            snapshots.append((season, label, season_start, end))
    return snapshots

conn = sqlite3.connect(DB)
cur  = conn.cursor()

# Drop and recreate torvik_daily with proper schema
print("Rebuilding torvik_daily table...")
cur.execute("DROP TABLE IF EXISTS torvik_daily")
cur.execute("""
    CREATE TABLE torvik_daily (
        season        INTEGER,
        snapshot_date TEXT,
        snapshot_label TEXT,
        team          TEXT,
        adj_o  REAL, adj_d REAL, barthag REAL, adj_em REAL,
        wins   INTEGER, games INTEGER,
        efg_o  REAL, efg_d  REAL,
        tov_o  REAL, tov_d  REAL,
        orb    REAL, drb    REAL,
        ftr_o  REAL, ftr_d  REAL,
        two_p_o REAL, two_p_d REAL,
        three_p_o REAL, three_p_d REAL,
        blk_pct REAL, ast_pct REAL,
        three_p_rate REAL,
        ft_pct REAL,
        wab    REAL,
        PRIMARY KEY (season, snapshot_date, team)
    )
""")
conn.commit()

snapshots = make_snapshots()
print(f"Fetching {len(snapshots)} snapshots ({len(snapshots)} requests)...")
print()

total = 0
errors = 0
for season, label, begin, end in snapshots:
    url = (f"https://barttorvik.com/trank.php"
           f"?year={season}&begin={begin}&end={end}&top=0&conlimit=All&csv=1")
    try:
        r = requests.get(url, timeout=30, headers=HEADERS)
        r.raise_for_status()

        if r.text.strip().startswith('<!'):
            print(f"  {season} {label}: BOT BLOCKED — HTML response")
            errors += 1
            time.sleep(5)
            continue

        lines = [l.strip() for l in r.text.strip().split('\n') if l.strip()]
        if not lines:
            print(f"  {season} {label}: empty response")
            continue

        batch = []
        for line in lines:
            import csv as csv_mod
            parts = next(csv_mod.reader([line]))
            if len(parts) < 30: continue
            team = parts[0].strip().strip('"')
            if not team or team in ('nan','None',''): continue

            adj_o   = fv(parts[1])
            adj_d   = fv(parts[2])
            barthag = fv(parts[3])
            if adj_o is None or not (50 < adj_o < 160): continue  # skip bad rows

            adj_em  = (adj_o - adj_d) if adj_o and adj_d else None
            wins    = fv(parts[5])
            games   = fv(parts[6])

            # Four factors: [7]=efg_o [8]=efg_d [9]=tov_o [10]=tov_d
            # [11]=orb [12]=drb [13]=ftr_o [14]=ftr_d
            # [15]=2p_o [16]=2p_d [17]=3p_o [18]=3p_d -- need to verify
            # [19]=blk_pct [20]=ast_pct [22]=3p_rate [25]=ft_pct [34]=wab

            batch.append((
                season, end, label, team,
                adj_o, adj_d, barthag, adj_em,
                int(wins) if wins else None,
                int(games) if games else None,
                fv(parts[7]),  fv(parts[8]),   # efg_o, efg_d
                fv(parts[9]),  fv(parts[10]),  # tov_o, tov_d
                fv(parts[11]), fv(parts[12]),  # orb, drb
                fv(parts[13]), fv(parts[14]),  # ftr_o, ftr_d
                fv(parts[15]), fv(parts[16]),  # two_p_o, two_p_d
                fv(parts[17]), fv(parts[18]),  # three_p_o, three_p_d
                fv(parts[19]), fv(parts[20]),  # blk_pct, ast_pct
                fv(parts[22]),                  # three_p_rate
                fv(parts[25]) if len(parts) > 25 else None,  # ft_pct
                fv(parts[34]) if len(parts) > 34 else None,  # wab
            ))

        cur.executemany("""
            INSERT OR REPLACE INTO torvik_daily
            (season, snapshot_date, snapshot_label, team,
             adj_o, adj_d, barthag, adj_em, wins, games,
             efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d,
             two_p_o, two_p_d, three_p_o, three_p_d,
             blk_pct, ast_pct, three_p_rate, ft_pct, wab)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
        conn.commit()
        total += len(batch)

        # Sample check
        sample = batch[0] if batch else None
        sample_str = f"{sample[3]} adjO={sample[4]:.1f} adjD={sample[5]:.1f}" if sample else "no data"
        print(f"  {season} {label} ({end}): {len(batch)} teams | {sample_str}")

        time.sleep(1.0)  # polite rate limit

    except Exception as e:
        print(f"  {season} {label}: ERROR {e}")
        errors += 1
        time.sleep(2)

print(f"\nDone: {total} rows inserted, {errors} errors")

# Verify
ct = cur.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
null_ct = cur.execute("SELECT COUNT(*) FROM torvik_daily WHERE adj_o IS NULL").fetchone()[0]
print(f"torvik_daily: {ct:,} rows | {null_ct} null adj_o")

print("\nPer season/snapshot counts:")
for row in cur.execute("""
    SELECT season, snapshot_label, COUNT(*) as ct
    FROM torvik_daily
    GROUP BY season, snapshot_label
    ORDER BY season, snapshot_label
"""):
    print(f"  {row[0]} {row[1]}: {row[2]} teams")

conn.close()
print("\nNext: python 04_build_features.py")
