"""
load_haslametrics_full.py
=========================
Loads all Haslametrics CSV files (TI + TD, all 5 sections, 2017-2025)
into haslametrics_full table in basketball.db.

Setup:
1. Create folder: data/hasla/
2. Copy all 18 hasla_YYYY_ti/td.csv files into data/hasla/
3. Run: python scripts/load_haslametrics_full.py
"""
import sqlite3, glob, re, os
import pandas as pd
import numpy as np
from collections import defaultdict

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB        = os.path.join(ROOT, 'data', 'basketball.db')
HASLA_DIR = os.path.join(ROOT, 'data', 'hasla')

def fv(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    s = str(x).strip().replace('---', '').replace('%', '').strip()
    if not s or s in ('nan', 'None'): return None
    try: return float(s)
    except: return None

def ts(x):
    if x is None: return None
    s = str(x).strip()
    return s if s and s not in ('nan', 'None', '') else None

def norm_team(name):
    return re.sub(r'\s*\(\d+[–\-]\d+\)\s*', '', str(name)).strip()

def is_valid_team(name):
    if re.search(r'\s+\d+$', name): return False
    if len(name) < 3: return False
    if re.match(r'^\d+$', name): return False
    return True

files = sorted(glob.glob(os.path.join(HASLA_DIR, 'hasla_*.csv')))
if not files:
    print(f"ERROR: No hasla_*.csv files found in {HASLA_DIR}")
    print("Copy all 18 CSV files into data/hasla/ first.")
    exit(1)

print(f"Loading {len(files)} Haslametrics files...")
data = defaultdict(dict)

for fpath in files:
    df = pd.read_csv(fpath, dtype=str, on_bad_lines='skip')
    df = df[df['section'].isin(['Offense', 'Defense', 'Fingerprint', 'Performance', 'Records'])]
    for _, row in df.iterrows():
        sec = row['section']
        try:
            season = int(row['season'])
        except Exception:
            continue
        variant = row['variant']
        team = norm_team(row.get('Team', ''))
        if not is_valid_team(team):
            continue
        key = (season, variant, team)
        v = row.values

        if sec == 'Offense':
            data[key].update({
                'o_eff':fv(v[5]),'o_ftar':fv(v[6]),'o_ft_pct':fv(v[7]),'o_fgar':fv(v[8]),
                'o_fg_pct':fv(v[9]),'o_3par':fv(v[10]),'o_3p_pct':fv(v[11]),
                'o_mrar':fv(v[12]),'o_mr_pct':fv(v[13]),'o_npar':fv(v[14]),'o_np_pct':fv(v[15]),
                'o_ppst':fv(v[16]),'o_ppsc':fv(v[17]),'o_scc_pct':fv(v[18]),
                'o_pct3pa':fv(v[19]),'o_pctmra':fv(v[20]),'o_pctnpa':fv(v[21]),'o_prox':fv(v[22]),
            })
        elif sec == 'Defense':
            data[key].update({
                'd_eff':fv(v[5]),'d_ftar':fv(v[6]),'d_ft_pct':fv(v[7]),'d_fgar':fv(v[8]),
                'd_fg_pct':fv(v[9]),'d_3par':fv(v[10]),'d_3p_pct':fv(v[11]),
                'd_mrar':fv(v[12]),'d_mr_pct':fv(v[13]),'d_npar':fv(v[14]),'d_np_pct':fv(v[15]),
                'd_ppst':fv(v[16]),'d_ppsc':fv(v[17]),'d_scc_pct':fv(v[18]),
                'd_pct3pa':fv(v[19]),'d_pctmra':fv(v[20]),'d_pctnpa':fv(v[21]),'d_prox':fv(v[22]),
            })
        elif sec == 'Fingerprint':
            data[key].update({
                'conf':ts(v[5]),'pace':fv(v[6]),'mom':fv(v[7]),'mom_o':fv(v[8]),'mom_d':fv(v[9]),
                'con':fv(v[10]),'con_r':fv(v[11]),'sos':fv(v[12]),'ptf':fv(v[13]),
                'rq':fv(v[14]),'afh':fv(v[15]),'asr':fv(v[16]),
                'rk1':fv(v[17]),'rk7':fv(v[18]),'rk30':fv(v[19]),'rkps':fv(v[20]),
            })
        elif sec == 'Performance':
            data[key].update({
                'last_perf':ts(v[7]),'best_pos_perf':ts(v[8]),'worst_neg_perf':ts(v[9]),
            })
        elif sec == 'Records':
            data[key].update({
                'v1_50':ts(v[5]),'v51_100':ts(v[6]),'v101_150':ts(v[7]),
                'v151_200':ts(v[8]),'v201_250':ts(v[9]),'v251_300':ts(v[10]),
                'v301plus':ts(v[11]),'hq1':ts(v[12]),'hq2':ts(v[13]),
                'hq3':ts(v[14]),'hq4':ts(v[15]),'home_rec':ts(v[16]),
                'away_rec':ts(v[17]),'neut_rec':ts(v[18]),
            })

print(f"  Parsed {len(data):,} unique (season, variant, team) entries")

conn = sqlite3.connect(DB)
conn.execute("DROP TABLE IF EXISTS haslametrics_full")
conn.execute("""
CREATE TABLE haslametrics_full (
    season INTEGER, variant TEXT, team TEXT,
    o_eff REAL, o_ftar REAL, o_ft_pct REAL, o_fgar REAL, o_fg_pct REAL,
    o_3par REAL, o_3p_pct REAL, o_mrar REAL, o_mr_pct REAL,
    o_npar REAL, o_np_pct REAL, o_ppst REAL, o_ppsc REAL, o_scc_pct REAL,
    o_pct3pa REAL, o_pctmra REAL, o_pctnpa REAL, o_prox REAL,
    d_eff REAL, d_ftar REAL, d_ft_pct REAL, d_fgar REAL, d_fg_pct REAL,
    d_3par REAL, d_3p_pct REAL, d_mrar REAL, d_mr_pct REAL,
    d_npar REAL, d_np_pct REAL, d_ppst REAL, d_ppsc REAL, d_scc_pct REAL,
    d_pct3pa REAL, d_pctmra REAL, d_pctnpa REAL, d_prox REAL,
    conf TEXT, pace REAL, mom REAL, mom_o REAL, mom_d REAL,
    con REAL, con_r REAL, sos REAL, ptf REAL, rq REAL,
    afh REAL, asr REAL, rk1 REAL, rk7 REAL, rk30 REAL, rkps REAL,
    last_perf TEXT, best_pos_perf TEXT, worst_neg_perf TEXT,
    v1_50 TEXT, v51_100 TEXT, v101_150 TEXT, v151_200 TEXT,
    v201_250 TEXT, v251_300 TEXT, v301plus TEXT,
    hq1 TEXT, hq2 TEXT, hq3 TEXT, hq4 TEXT,
    home_rec TEXT, away_rec TEXT, neut_rec TEXT,
    PRIMARY KEY (season, variant, team)
)
""")
conn.commit()

COLS = [
    'o_eff','o_ftar','o_ft_pct','o_fgar','o_fg_pct','o_3par','o_3p_pct','o_mrar','o_mr_pct',
    'o_npar','o_np_pct','o_ppst','o_ppsc','o_scc_pct','o_pct3pa','o_pctmra','o_pctnpa','o_prox',
    'd_eff','d_ftar','d_ft_pct','d_fgar','d_fg_pct','d_3par','d_3p_pct','d_mrar','d_mr_pct',
    'd_npar','d_np_pct','d_ppst','d_ppsc','d_scc_pct','d_pct3pa','d_pctmra','d_pctnpa','d_prox',
    'conf','pace','mom','mom_o','mom_d','con','con_r','sos','ptf','rq','afh','asr',
    'rk1','rk7','rk30','rkps','last_perf','best_pos_perf','worst_neg_perf',
    'v1_50','v51_100','v101_150','v151_200','v201_250','v251_300','v301plus',
    'hq1','hq2','hq3','hq4','home_rec','away_rec','neut_rec',
]
ph  = ','.join(['?'] * (3 + len(COLS)))
sql = f"INSERT OR REPLACE INTO haslametrics_full (season,variant,team,{','.join(COLS)}) VALUES ({ph})"
batch = [(s, var, t, *[d.get(c) for c in COLS]) for (s, var, t), d in data.items()]
conn.executemany(sql, batch)
conn.commit()

total = conn.execute("SELECT COUNT(*) FROM haslametrics_full").fetchone()[0]
print(f"  Inserted {total:,} rows")

print("\nCoverage by season/variant:")
for r in conn.execute("""
    SELECT season, variant, COUNT(*), ROUND(AVG(o_eff),1), ROUND(AVG(d_eff),1), ROUND(AVG(pace),1)
    FROM haslametrics_full GROUP BY season, variant ORDER BY season, variant
"""):
    print(f"  {r[0]} {r[1]}: {r[2]} teams | o_eff={r[3]} d_eff={r[4]} pace={r[5]}")

conn.close()
print("\nDone. Next: python scripts/04_build_features.py")
