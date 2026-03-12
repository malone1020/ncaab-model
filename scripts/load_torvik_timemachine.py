"""
load_torvik_timemachine.py
Loads torvik_timemachine.csv into torvik_daily.
Place torvik_timemachine.csv in the data/ folder first.
Run from scripts/: python load_torvik_timemachine.py
"""
import sqlite3, os
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
CSV  = os.path.join(ROOT, 'data', 'torvik_timemachine.csv')

def fv(x):
    try:
        s = str(x).strip().replace('%','').replace(',','').strip('"')
        if s in ('','nan','None','---','N/A','inf','-inf'): return None
        f = float(s)
        return None if (f != f) else f  # catch NaN
    except: return None

if not os.path.exists(CSV):
    print(f"ERROR: {CSV} not found. Copy torvik_timemachine.csv to data/ folder.")
    exit(1)

conn = sqlite3.connect(DB)
cur  = conn.cursor()

print("Rebuilding torvik_daily...")
cur.execute("DROP TABLE IF EXISTS torvik_daily")
cur.execute("""
    CREATE TABLE torvik_daily (
        season         INTEGER,
        snapshot_label TEXT,
        snapshot_date  TEXT,
        team           TEXT,
        adj_o   REAL, adj_d   REAL, barthag REAL, adj_em REAL,
        wins    INTEGER, games INTEGER,
        efg_o   REAL, efg_d   REAL,
        tov_o   REAL, tov_d   REAL,
        orb     REAL, drb     REAL,
        ftr_o   REAL, ftr_d   REAL,
        two_p_o REAL, two_p_d REAL,
        three_p_o REAL, three_p_d REAL,
        blk_pct REAL, ast_pct REAL,
        three_p_rate REAL, ft_pct REAL, wab REAL,
        PRIMARY KEY (season, snapshot_date, team)
    )
""")
conn.commit()

# Read without header - data cols are offset from named header
# True layout: [0]=season [1]=label [2]=snapshot_date [3]=team
#              [4]=adj_o  [5]=adj_d  [6]=barthag [7]=record
#              [8]=wins   [9]=games
#              [10]=efg_o [11]=efg_d [12]=tov_o [13]=tov_d
#              [14]=orb   [15]=drb   [16]=ftr_o  [17]=ftr_d
#              [18]=two_p_o [19]=two_p_d [20]=three_p_o [21]=three_p_d
#              [22]=blk_pct [23]=ast_pct [24]=? [25]=three_p_rate
#              [26]=ft_pct  [27-29]=empty [30]=season_col [31-33]=empty
#              [34]=wab  [35]=? [36]=?
df = pd.read_csv(CSV, header=None, skiprows=1)
print(f"Loaded: {len(df)} rows, {len(df.columns)} columns")

batch = []
skipped = 0
for _, row in df.iterrows():
    season = row.iloc[0]
    label  = str(row.iloc[1]).strip()
    snap   = str(int(row.iloc[2])) if pd.notna(row.iloc[2]) else None
    team   = str(row.iloc[3]).strip().strip('"')
    adj_o  = fv(row.iloc[4])
    adj_d  = fv(row.iloc[5])

    if not team or not snap or adj_o is None or not (50 < adj_o < 160):
        skipped += 1
        continue

    adj_em = round(adj_o - adj_d, 4) if adj_d else None

    try: wins  = int(float(row.iloc[8]))
    except: wins = None
    try: games = int(float(row.iloc[9]))
    except: games = None

    batch.append((
        int(season), label, snap, team,
        adj_o, adj_d, fv(row.iloc[6]), adj_em,
        wins, games,
        fv(row.iloc[10]), fv(row.iloc[11]),  # efg_o, efg_d
        fv(row.iloc[12]), fv(row.iloc[13]),  # tov_o, tov_d
        fv(row.iloc[14]), fv(row.iloc[15]),  # orb, drb
        fv(row.iloc[16]), fv(row.iloc[17]),  # ftr_o, ftr_d
        fv(row.iloc[18]), fv(row.iloc[19]),  # two_p_o, two_p_d
        fv(row.iloc[20]), fv(row.iloc[21]),  # three_p_o, three_p_d
        fv(row.iloc[22]), fv(row.iloc[23]),  # blk_pct, ast_pct
        fv(row.iloc[25]),                    # three_p_rate
        fv(row.iloc[26]) if len(row) > 26 else None,  # ft_pct
        fv(row.iloc[34]) if len(row) > 34 else None,  # wab
    ))

cur.executemany("""
    INSERT OR REPLACE INTO torvik_daily
    (season, snapshot_label, snapshot_date, team,
     adj_o, adj_d, barthag, adj_em, wins, games,
     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d,
     two_p_o, two_p_d, three_p_o, three_p_d,
     blk_pct, ast_pct, three_p_rate, ft_pct, wab)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
""", batch)
conn.commit()

total = cur.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
print(f"Inserted: {len(batch):,} | Skipped: {skipped} | Total: {total:,}")

print("\nPer season/snapshot:")
for r in cur.execute("""
    SELECT season, snapshot_label, COUNT(*) 
    FROM torvik_daily GROUP BY season, snapshot_label 
    ORDER BY season, snapshot_label
"""):
    print(f"  {r[0]} {r[1]}: {r[2]} teams")

print("\nSample — 2024 Kansas feb01:")
for r in cur.execute("""
    SELECT team, adj_o, adj_d, adj_em, barthag, wins, games
    FROM torvik_daily WHERE season=2024 AND snapshot_label='feb01' AND team='Kansas'
"""):
    print(f"  {r}")

conn.close()
print("\nNext: python 04_build_features.py")
