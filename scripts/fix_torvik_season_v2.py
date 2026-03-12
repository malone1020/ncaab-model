"""
fix_torvik_season_v2.py
- Drops and recreates torvik_season cleanly
- Re-pulls all 10 seasons from barttorvik CSVs
- Rebuilds torvik_daily with 2 snapshots per team per season
Run from scripts/: python fix_torvik_season_v2.py
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

# ── Drop and recreate torvik_season cleanly ────────────────────
print("Rebuilding torvik_season from scratch...")
cur.execute("DROP TABLE IF EXISTS torvik_season")
cur.execute("""
    CREATE TABLE torvik_season (
        season   INTEGER,
        team     TEXT,
        adj_o    REAL, adj_d REAL, adj_t REAL,
        barthag  REAL, adj_em REAL,
        efg_o    REAL, efg_d REAL,
        tov_o    REAL, tov_d REAL,
        orb      REAL, drb   REAL,
        ftr_o    REAL, ftr_d REAL,
        two_p_o  REAL, two_p_d   REAL,
        three_p_o REAL, three_p_d REAL,
        blk_pct  REAL, ast_pct   REAL,
        three_p_rate REAL,
        avg_hgt  REAL, eff_hgt   REAL,
        experience REAL,
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
            print(f"  {season}: BOT BLOCKED — skipping")
            continue
        df = pd.read_csv(StringIO(r.text))
        df.columns = [str(c).strip().lower() for c in df.columns]
    except Exception as e:
        print(f"  {season}: FETCH ERROR {e}")
        continue

    # Normalize all known column name variants → standard names
    col_map = {
        'adjoe': 'adj_o', 'adj oe': 'adj_o', 'adj. oe': 'adj_o',
        'adjde': 'adj_d', 'adj de': 'adj_d', 'adj. de': 'adj_d',
        'adjt':  'adj_t', 'adj. t': 'adj_t', 'tempo': 'adj_t',
        'barthag': 'barthag',
        'efg%': 'efg_o',   'efg': 'efg_o',
        'efg d.': 'efg_d', 'efg% d': 'efg_d', 'efgd': 'efg_d',
        'tov%': 'tov_o',   'to%': 'tov_o',
        'tov% d': 'tov_d', 'to% d': 'tov_d',
        'o reb%': 'orb',   'oreb%': 'orb',
        'op oreb%': 'drb', 'dreb%': 'drb',
        'ft rate': 'ftr_o','ftr': 'ftr_o',
        'ft rate d': 'ftr_d',
        '2p %': 'two_p_o', '2p%': 'two_p_o',
        '2p % d.': 'two_p_d', '2p% d': 'two_p_d',
        '3p %': 'three_p_o', '3p%': 'three_p_o',
        '3p % d.': 'three_p_d', '3p% d': 'three_p_d',
        'blk %': 'blk_pct', 'blk%': 'blk_pct',
        'ast %': 'ast_pct', 'ast%': 'ast_pct',
        '3p rate': 'three_p_rate', '3pr': 'three_p_rate',
        'avg hgt.': 'avg_hgt', 'avg hgt': 'avg_hgt',
        'eff. hgt.': 'eff_hgt', 'eff hgt': 'eff_hgt',
        'exp.': 'experience',
        'elite sos': 'elite_sos',
        'ft%': 'ft_pct',
        'pake': 'pake', 'pase': 'pase',
        'talent': 'talent', 'wab': 'wab',
    }
    df = df.rename(columns={c: col_map[c] for c in df.columns if c in col_map})

    if 'team' not in df.columns or 'adj_o' not in df.columns:
        print(f"  {season}: Missing team or adj_o column. Cols: {list(df.columns[:8])}")
        continue

    # Filter out any conference-aggregate rows (no real team has adj_o > 150 or < 50)
    inserted = 0
    for _, row in df.iterrows():
        team = str(row.get('team', '')).strip()
        adj_o = fv(row.get('adj_o'))
        adj_d = fv(row.get('adj_d'))
        if not team or team in ('nan', 'None', ''): continue
        if adj_o is None or not (50 < adj_o < 150): continue  # Skip rank rows / bad data

        adj_em = (adj_o - adj_d) if adj_o and adj_d else None

        cur.execute("""
            INSERT OR REPLACE INTO torvik_season
            (season, team, adj_o, adj_d, adj_t, barthag, adj_em,
             efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d,
             two_p_o, two_p_d, three_p_o, three_p_d, blk_pct, ast_pct,
             three_p_rate, avg_hgt, eff_hgt, experience,
             pake, pase, talent, elite_sos, ft_pct, wab)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            season, team, adj_o, adj_d,
            fv(row.get('adj_t')), fv(row.get('barthag')), adj_em,
            fv(row.get('efg_o')), fv(row.get('efg_d')),
            fv(row.get('tov_o')), fv(row.get('tov_d')),
            fv(row.get('orb')),   fv(row.get('drb')),
            fv(row.get('ftr_o')), fv(row.get('ftr_d')),
            fv(row.get('two_p_o')), fv(row.get('two_p_d')),
            fv(row.get('three_p_o')), fv(row.get('three_p_d')),
            fv(row.get('blk_pct')), fv(row.get('ast_pct')),
            fv(row.get('three_p_rate')),
            fv(row.get('avg_hgt')), fv(row.get('eff_hgt')),
            fv(row.get('experience')),
            fv(row.get('pake')), fv(row.get('pase')), fv(row.get('talent')),
            fv(row.get('elite_sos')), fv(row.get('ft_pct')), fv(row.get('wab')),
        ))
        inserted += 1

    conn.commit()
    sample = cur.execute(
        "SELECT team, adj_o, adj_d FROM torvik_season WHERE season=? LIMIT 1", (season,)
    ).fetchone()
    total += inserted
    print(f"  {season}: {inserted} teams | sample: {sample}")
    time.sleep(0.3)

print(f"\ntorvik_season total: {total} rows")

# ── Rebuild torvik_daily ───────────────────────────────────────
print("\nRebuilding torvik_daily...")
cur.execute("DROP TABLE IF EXISTS torvik_daily")
cur.execute("""
    CREATE TABLE torvik_daily (
        season        INTEGER,
        snapshot_date TEXT,
        team          TEXT,
        adj_o REAL, adj_d REAL, adj_t REAL,
        barthag REAL, adj_em REAL,
        efg_o REAL, efg_d REAL,
        tov_o REAL, tov_d REAL,
        orb REAL, drb REAL,
        ftr_o REAL, ftr_d REAL,
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
null_ct  = cur.execute("SELECT COUNT(*) FROM torvik_daily WHERE adj_o IS NULL").fetchone()[0]
print(f"torvik_daily: {daily_ct:,} rows | {null_ct} null adj_o")

# Sanity check
print("\nPer-season daily counts:")
for season in range(2016, 2026):
    ct = cur.execute("SELECT COUNT(*) FROM torvik_daily WHERE season=?", (season,)).fetchone()[0]
    print(f"  {season}: {ct}")

conn.close()
print("\nDone. Run: python 04_build_features.py")
