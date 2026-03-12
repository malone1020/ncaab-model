"""
pull_haslametrics.py
====================
Loads Haslametrics data into the database.

TWO MODES:
1. From CSV files (if you downloaded them via browser JS — see below)
2. Direct load from uploaded Excel snapshots

The Haslametrics site requires a browser to render data (JavaScript-rendered tables).
Use the browser JS script below to export each year/variant/section to CSV,
then run this script to load them all into the DB.

BROWSER JS EXTRACTION SCRIPT
=============================
Open browser console on https://haslametrics.com/ratings.php?yr=YEAR&ti=1
(or without &ti=1 for time-dependent) and run:

--- PASTE THIS INTO BROWSER CONSOLE ---

(function() {
    const SECTIONS = ['Offense','Defense','Fingerprint','Performance','Records'];
    const HEADERS = {
        Offense:     ['Rk','Team','o_eff','o_ftar','o_ft_pct','o_fgar','o_fg_pct','o_3par','o_3p_pct','o_mrar','o_mr_pct','o_npar','o_np_pct','o_ppst','o_ppsc','o_scc_pct','o_pct3pa','o_pctmra','o_pctnpa','o_prox','ap_pct'],
        Defense:     ['Rk','Team','d_eff','d_ftar','d_ft_pct','d_fgar','d_fg_pct','d_3par','d_3p_pct','d_mrar','d_mr_pct','d_npar','d_np_pct','d_ppst','d_ppsc','d_scc_pct','d_pct3pa','d_pctmra','d_pctnpa','d_prox','ap_pct'],
        Fingerprint: ['Rk','Team','conf','pace','mom','mom_o','mom_d','con','con_r','sos','ptf','rq','afh','asr','rk1','rk7','rk30','rkps','ap_pct'],
        Performance: ['Rk','Team','lst5wl','lst5perf','last_perf','best_pos_perf','worst_neg_perf','ap_pct'],
        Records:     ['Rk','Team','v1_50','v51_100','v101_150','v151_200','v201_250','v251_300','v301plus','hq1','hq2','hq3','hq4','home','away','neut','ap_pct'],
    };
    
    const yr = new URLSearchParams(window.location.search).get('yr');
    const ti = new URLSearchParams(window.location.search).get('ti') === '1' ? 'ti' : 'td';
    
    const allCSVs = [];
    
    for (const section of SECTIONS) {
        document.getElementById('cboRatings').value = section;
        hideShowRatings();
        
        const tables = Array.from(document.querySelectorAll('table'));
        let rows = [];
        for (const t of tables) {
            if (t.style.display === 'none') continue;
            const tbody = t.querySelector('tbody');
            if (!tbody) continue;
            const dataRows = Array.from(tbody.querySelectorAll('tr')).filter(r =>
                r.cells[0]?.textContent.trim().match(/^\d+$/) && r.cells.length > 5);
            if (dataRows.length > 100) {
                rows = dataRows.map(r => Array.from(r.cells).map(c => c.textContent.trim().replace(/,/g,'').replace(/▲|▼/g,'')));
                break;
            }
        }
        
        if (!rows.length) { console.log('No data for', section); continue; }
        
        const headers = HEADERS[section];
        // Add season and variant prefix columns
        const csvRows = [['season','variant','section',...headers].join(',')];
        for (const row of rows) {
            csvRows.push([yr, ti, section, ...row].join(','));
        }
        allCSVs.push(csvRows.join('\n'));
        console.log(`Extracted ${rows.length} rows for ${section}`);
    }
    
    // Combine and download
    const combined = allCSVs.join('\n');
    const blob = new Blob([combined], {type: 'text/csv'});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `hasla_${yr}_${ti}.csv`;
    a.click();
    console.log('Download triggered: hasla_' + yr + '_' + ti + '.csv');
})();

--- END BROWSER CONSOLE SCRIPT ---

YEARS TO FETCH:
Time-independent (?ti=1): yr=2017 through yr=2025  (9 files)
Time-dependent  (no ti):  yr=2017 through yr=2025  (9 files)
Total: 18 CSV files → place all in data/hasla/ folder

Run: python pull_haslametrics.py
"""

import sqlite3, os, glob, re
import pandas as pd
import numpy as np

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
HASLA_DIR = os.path.join(ROOT, 'data', 'hasla')
os.makedirs(HASLA_DIR, exist_ok=True)

SECTION_COLS = {
    'Offense':     ['o_eff','o_ftar','o_ft_pct','o_fgar','o_fg_pct','o_3par','o_3p_pct',
                    'o_mrar','o_mr_pct','o_npar','o_np_pct','o_ppst','o_ppsc','o_scc_pct',
                    'o_pct3pa','o_pctmra','o_pctnpa','o_prox'],
    'Defense':     ['d_eff','d_ftar','d_ft_pct','d_fgar','d_fg_pct','d_3par','d_3p_pct',
                    'd_mrar','d_mr_pct','d_npar','d_np_pct','d_ppst','d_ppsc','d_scc_pct',
                    'd_pct3pa','d_pctmra','d_pctnpa','d_prox'],
    'Fingerprint': ['pace','mom','mom_o','mom_d','con','con_r','sos','ptf','rq','afh','asr',
                    'rk1','rk7','rk30','rkps'],
    'Performance': ['lst5wl','lst5perf','last_perf','best_pos_perf','worst_neg_perf'],
    'Records':     ['v1_50','v51_100','v101_150','v151_200','v201_250','v251_300','v301plus',
                    'hq1','hq2','hq3','hq4','home_rec','away_rec','neut_rec'],
}

def fv(x):
    if x is None: return None
    s = str(x).strip().replace('%','').replace(',','').replace('▲','').replace('▼','').strip()
    if s in ('','nan','None','---','N/A'): return None
    try: return float(s)
    except: return None

def norm_team(name):
    """Normalize team name: remove record suffix like '(35-4)'"""
    return re.sub(r'\s*\(\d+[–\-]\d+\)\s*', '', str(name)).strip()

def load_all_csvs(conn):
    csv_files = glob.glob(os.path.join(HASLA_DIR, 'hasla_*.csv'))
    if not csv_files:
        print(f"No CSV files found in {HASLA_DIR}")
        print("Run the browser console script to download data first.")
        return

    print(f"Found {len(csv_files)} CSV files")

    conn.execute("DROP TABLE IF EXISTS haslametrics_full")
    conn.execute(f"""
        CREATE TABLE haslametrics_full (
            season   INTEGER,
            variant  TEXT,    -- 'td' (time-dependent) or 'ti' (time-independent)
            team     TEXT,
            -- Offense
            o_eff REAL, o_ftar REAL, o_ft_pct REAL, o_fgar REAL, o_fg_pct REAL,
            o_3par REAL, o_3p_pct REAL, o_mrar REAL, o_mr_pct REAL,
            o_npar REAL, o_np_pct REAL, o_ppst REAL, o_ppsc REAL, o_scc_pct REAL,
            o_pct3pa REAL, o_pctmra REAL, o_pctnpa REAL, o_prox REAL,
            -- Defense
            d_eff REAL, d_ftar REAL, d_ft_pct REAL, d_fgar REAL, d_fg_pct REAL,
            d_3par REAL, d_3p_pct REAL, d_mrar REAL, d_mr_pct REAL,
            d_npar REAL, d_np_pct REAL, d_ppst REAL, d_ppsc REAL, d_scc_pct REAL,
            d_pct3pa REAL, d_pctmra REAL, d_pctnpa REAL, d_prox REAL,
            -- Fingerprint
            pace REAL, mom REAL, mom_o REAL, mom_d REAL,
            con REAL, con_r REAL, sos REAL, ptf REAL, rq REAL,
            afh REAL, asr REAL, rk1 REAL, rk7 REAL, rk30 REAL, rkps REAL,
            -- Performance
            lst5wl TEXT, lst5perf REAL, last_perf REAL,
            best_pos_perf REAL, worst_neg_perf REAL,
            -- Records
            v1_50 TEXT, v51_100 TEXT, v101_150 TEXT, v151_200 TEXT,
            v201_250 TEXT, v251_300 TEXT, v301plus TEXT,
            hq1 TEXT, hq2 TEXT, hq3 TEXT, hq4 TEXT,
            home_rec TEXT, away_rec TEXT, neut_rec TEXT,
            PRIMARY KEY (season, variant, team)
        )
    """)
    conn.commit()

    # Group files by (season, variant), merge sections
    from collections import defaultdict
    season_data = defaultdict(lambda: defaultdict(dict))  # [season][variant][team] = {col: val}

    for fpath in csv_files:
        df = pd.read_csv(fpath, dtype=str, on_bad_lines='skip')
        if 'season' not in df.columns:
            print(f"  Skipping {os.path.basename(fpath)}: no season column")
            continue

        for _, row in df.iterrows():
            season  = row.get('season', '').strip()
            variant = row.get('variant', 'ti').strip()
            section = row.get('section', '').strip()
            team    = norm_team(row.get('Team', row.get('team','')))
            if not season or not team: continue

            key = (int(season), variant)
            cols = SECTION_COLS.get(section, [])
            for col in cols:
                if col in row.index:
                    season_data[key][team][col] = fv(row[col])
            # Handle record columns (text like "3-1")
            if section == 'Records':
                for col in ['lst5wl','v1_50','v51_100','v101_150','v151_200',
                            'v201_250','v251_300','v301plus','hq1','hq2','hq3','hq4',
                            'home_rec','away_rec','neut_rec']:
                    if col in row.index:
                        season_data[key][team][col] = str(row[col]).strip() if pd.notna(row.get(col)) else None

    # Insert
    total = 0
    for (season, variant), teams in season_data.items():
        for team, cols in teams.items():
            conn.execute("""
                INSERT OR REPLACE INTO haslametrics_full
                (season, variant, team,
                 o_eff, o_ftar, o_ft_pct, o_fgar, o_fg_pct,
                 o_3par, o_3p_pct, o_mrar, o_mr_pct, o_npar, o_np_pct,
                 o_ppst, o_ppsc, o_scc_pct, o_pct3pa, o_pctmra, o_pctnpa, o_prox,
                 d_eff, d_ftar, d_ft_pct, d_fgar, d_fg_pct,
                 d_3par, d_3p_pct, d_mrar, d_mr_pct, d_npar, d_np_pct,
                 d_ppst, d_ppsc, d_scc_pct, d_pct3pa, d_pctmra, d_pctnpa, d_prox,
                 pace, mom, mom_o, mom_d, con, con_r, sos, ptf, rq, afh, asr,
                 rk1, rk7, rk30, rkps,
                 lst5wl, lst5perf, last_perf, best_pos_perf, worst_neg_perf,
                 v1_50, v51_100, v101_150, v151_200, v201_250, v251_300, v301plus,
                 hq1, hq2, hq3, hq4, home_rec, away_rec, neut_rec)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                season, variant, team,
                cols.get('o_eff'), cols.get('o_ftar'), cols.get('o_ft_pct'),
                cols.get('o_fgar'), cols.get('o_fg_pct'), cols.get('o_3par'),
                cols.get('o_3p_pct'), cols.get('o_mrar'), cols.get('o_mr_pct'),
                cols.get('o_npar'), cols.get('o_np_pct'), cols.get('o_ppst'),
                cols.get('o_ppsc'), cols.get('o_scc_pct'), cols.get('o_pct3pa'),
                cols.get('o_pctmra'), cols.get('o_pctnpa'), cols.get('o_prox'),
                cols.get('d_eff'), cols.get('d_ftar'), cols.get('d_ft_pct'),
                cols.get('d_fgar'), cols.get('d_fg_pct'), cols.get('d_3par'),
                cols.get('d_3p_pct'), cols.get('d_mrar'), cols.get('d_mr_pct'),
                cols.get('d_npar'), cols.get('d_np_pct'), cols.get('d_ppst'),
                cols.get('d_ppsc'), cols.get('d_scc_pct'), cols.get('d_pct3pa'),
                cols.get('d_pctmra'), cols.get('d_pctnpa'), cols.get('d_prox'),
                cols.get('pace'), cols.get('mom'), cols.get('mom_o'), cols.get('mom_d'),
                cols.get('con'), cols.get('con_r'), cols.get('sos'), cols.get('ptf'),
                cols.get('rq'), cols.get('afh'), cols.get('asr'),
                cols.get('rk1'), cols.get('rk7'), cols.get('rk30'), cols.get('rkps'),
                cols.get('lst5wl'), cols.get('lst5perf'), cols.get('last_perf'),
                cols.get('best_pos_perf'), cols.get('worst_neg_perf'),
                cols.get('v1_50'), cols.get('v51_100'), cols.get('v101_150'),
                cols.get('v151_200'), cols.get('v201_250'), cols.get('v251_300'),
                cols.get('v301plus'), cols.get('hq1'), cols.get('hq2'),
                cols.get('hq3'), cols.get('hq4'), cols.get('home_rec'),
                cols.get('away_rec'), cols.get('neut_rec'),
            ))
            total += 1
    conn.commit()
    print(f"Inserted {total:,} rows into haslametrics_full")

    print("\nBreakdown by season/variant:")
    for row in conn.execute("SELECT season, variant, COUNT(*) FROM haslametrics_full GROUP BY season, variant ORDER BY season, variant"):
        print(f"  {row[0]} {row[1]}: {row[2]} teams")

if __name__ == '__main__':
    conn = sqlite3.connect(DB)
    load_all_csvs(conn)
    conn.close()
    print("\nNext: update 04_build_features.py to use haslametrics_full")
