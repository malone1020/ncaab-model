"""
03_pull_all_sources.py  (v4 — final fix)
=========================================
All three root causes fixed:

1. torvik_game_preds: DROP + RECREATE table if missing actual_home/actual_away columns
   (old schema from v1 still in DB)

2. torvik_daily: already working (79,655 rows in DB) — skip gracefully

3. haslametrics: The HTML page is JS-rendered — requests.get() on the .php page
   gets an empty shell. The actual data comes from:
     https://haslametrics.com/ratings{YY}.xml  (e.g. ratings24.xml for 2024)
   This XML file contains ALL 131 fields per team (all 5 views) in a single request.
   Available for years 2016-2025 (2-digit year suffix).
   ~362 teams, ~1.6MB per file.

Run: python scripts/03_pull_all_sources.py
"""

import sqlite3, requests, time, os, re, json
import xml.etree.ElementTree as ET
import pandas as pd
from io import StringIO
from datetime import date, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'}


# ── Schema ───────────────────────────────────

SCHEMA_BASE = """
CREATE TABLE IF NOT EXISTS torvik_season (
    season INTEGER, team TEXT,
    adj_o REAL, adj_d REAL, adj_t REAL, barthag REAL, adj_em REAL,
    efg_o REAL, efg_d REAL, tov_o REAL, tov_d REAL,
    orb REAL, drb REAL, ftr_o REAL, ftr_d REAL,
    two_p_o REAL, two_p_d REAL, three_p_o REAL, three_p_d REAL,
    blk_pct REAL, ast_pct REAL, three_p_rate REAL,
    avg_hgt REAL, eff_hgt REAL, experience REAL,
    pake REAL, pase REAL, talent REAL, elite_sos REAL, ft_pct REAL, wab REAL,
    PRIMARY KEY(season, team)
);

CREATE TABLE IF NOT EXISTS torvik_daily (
    season INTEGER, snapshot_date TEXT, team TEXT,
    adj_o REAL, adj_d REAL, adj_t REAL, barthag REAL, adj_em REAL,
    efg_o REAL, efg_d REAL, tov_o REAL, tov_d REAL,
    orb REAL, drb REAL, ftr_o REAL, ftr_d REAL,
    PRIMARY KEY(season, snapshot_date, team)
);

-- haslametrics: all 5 views from ratingsYY.xml, one row per team per season
CREATE TABLE IF NOT EXISTS haslametrics (
    season INTEGER,
    team TEXT,
    abbr TEXT,
    conf TEXT,
    wins INTEGER,
    losses INTEGER,
    ap_pct REAL,
    -- Offense view
    o_eff REAL,          -- oe: offensive efficiency
    o_pace REAL,         -- ou: offensive pace/tempo
    o_ftar REAL,         -- oftar: ft attempt rate
    o_fgar REAL,         -- ofgar: fg attempt rate
    o_three_par REAL,    -- otpar: 3pt attempt rate
    o_npar REAL,         -- onpar: near-paint attempt rate
    o_fpar REAL,         -- ofpar: far mid-range attempt rate (unused zone)
    o_fgmr REAL,         -- ofgmr: fg% mid-range
    o_three_pmr REAL,    -- otpmr: 3pt%
    o_npm_r REAL,        -- onpmr: near-paint make rate
    o_fpmr REAL,         -- ofpmr: far mid-range make rate
    o_ft_pct REAL,       -- ftpct: ft%
    -- Defense view (opponent allowed)
    d_eff REAL,          -- de
    d_pace REAL,         -- du
    d_ftar REAL,         -- dftar
    d_fgar REAL,         -- dfgar
    d_three_par REAL,    -- dtpar
    d_npar REAL,         -- dnpar
    d_fpar REAL,         -- dfpar
    d_fgmr REAL,         -- dfgmr
    d_three_pmr REAL,    -- dtpmr
    d_npm_r REAL,        -- dnpmr
    d_fpmr REAL,         -- dfpmr
    d_ft_pct REAL,       -- dftpct
    -- Fingerprint view
    sos REAL,            -- sos: strength of schedule
    mom REAL,            -- mom: overall momentum
    mom_o REAL,          -- mmo: offensive momentum
    mom_d REAL,          -- mmd: defensive momentum
    consistency REAL,    -- inc: consistency score
    ptf REAL,            -- ptf: performance to forecast
    rq REAL,             -- rpi
    afh REAL,            -- afh: adjusted for home
    asr REAL,            -- asr: adjusted schedule rating
    rk_1 INTEGER,        -- vs1: rank change last 1 day
    rk_7 INTEGER,        -- vs7: rank change last 7 days
    rk_30 INTEGER,       -- vs30: rank change last 30 days
    -- Performance view
    last5_wl TEXT,       -- p5wl: last 5 game W/L string
    last5_ud TEXT,       -- p5ud: last 5 up/down trend
    vs_pre INTEGER,      -- vspre: vs preseason rank
    -- Records view
    r_1_50 TEXT,         -- r_1_50: record vs rank 1-50
    r_51_100 TEXT,
    r_101_150 TEXT,
    r_151_200 TEXT,
    r_201_250 TEXT,
    r_251_300 TEXT,
    r_301_up TEXT,
    r_q1 TEXT,           -- r_q1: record vs quad 1
    r_q2 TEXT,
    r_q3 TEXT,
    r_q4 TEXT,
    r_home TEXT,
    r_away TEXT,
    r_neut TEXT,
    PRIMARY KEY(season, team)
);
"""

# torvik_game_preds needs actual_home/actual_away — drop if old schema
GAME_PREDS_SCHEMA = """
CREATE TABLE IF NOT EXISTS torvik_game_preds (
    season INTEGER, game_date TEXT, home_team TEXT, away_team TEXT,
    torvik_home_pts REAL, torvik_away_pts REAL,
    torvik_margin REAL, torvik_win_prob REAL, actual_margin REAL,
    actual_home INTEGER, actual_away INTEGER,
    PRIMARY KEY(season, game_date, home_team, away_team)
);
"""


def db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def fv(v, default=None):
    if v is None: return default
    s = str(v).strip().replace('%', '').replace(',', '')
    if s in ('', '---', 'N/A', 'nan', 'None'): return default
    try:
        return float(s)
    except:
        return default


def migrate_tables(conn):
    """Drop and recreate tables with stale schemas."""
    # torvik_game_preds: old schema missing actual_home/actual_away
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(torvik_game_preds)")]
        if cols and 'actual_home' not in cols:
            print("  [migrate] Dropping old torvik_game_preds (missing actual_home)...")
            conn.execute("DROP TABLE torvik_game_preds")
            conn.commit()
    except Exception as e:
        print(f"  [migrate] torvik_game_preds: {e}")

    # haslametrics: old schema missing abbr column (created by v2/v3 script)
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(haslametrics)")]
        if cols and 'abbr' not in cols:
            print("  [migrate] Dropping old haslametrics (missing abbr — stale v2/v3 schema)...")
            conn.execute("DROP TABLE haslametrics")
            conn.commit()
    except Exception as e:
        print(f"  [migrate] haslametrics: {e}")

    # torvik_daily: add adj_t if missing
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(torvik_daily)")]
        if cols and 'adj_t' not in cols:
            print("  [migrate] Adding adj_t column to torvik_daily...")
            conn.execute("ALTER TABLE torvik_daily ADD COLUMN adj_t REAL")
            conn.commit()
    except Exception as e:
        print(f"  [migrate] torvik_daily adj_t: {e}")

    print("  [migrate] Done.")


# ─────────────────────────────────────────────
# 1. TORVIK SEASON FINALS
# ─────────────────────────────────────────────

def pull_torvik_season(seasons=range(2016, 2027)):
    conn = db()
    cur = conn.cursor()
    total = 0

    for season in seasons:
        existing = cur.execute(
            "SELECT COUNT(*) FROM torvik_season WHERE season=?", (season,)
        ).fetchone()[0]
        if existing > 100:
            print(f"  [torvik_season] {season}: already loaded ({existing} teams)")
            continue

        url = f"https://barttorvik.com/{season}_team_results.csv"
        try:
            r = requests.get(url, timeout=20, headers=HEADERS)
            r.raise_for_status()
            df = pd.read_csv(StringIO(r.text))
            df.columns = [str(c).strip().lower() for c in df.columns]

            col_map = {
                'team': 'team', 'adj oe': 'adj_o', 'adj de': 'adj_d',
                'adj. t': 'adj_t', 'barthag': 'barthag',
                'efg': 'efg_o', 'efg d.': 'efg_d',
                'tov%': 'tov_o', 'tov% d': 'tov_d',
                'o reb%': 'orb', 'op oreb%': 'drb',
                'ft rate': 'ftr_o', 'ft rate d': 'ftr_d',
                '2p %': 'two_p_o', '2p % d.': 'two_p_d',
                '3p %': 'three_p_o', '3p % d.': 'three_p_d',
                'blk %': 'blk_pct', 'ast %': 'ast_pct',
                '3p rate': 'three_p_rate', 'avg hgt.': 'avg_hgt',
                'eff. hgt.': 'eff_hgt', 'exp.': 'experience',
                'pake': 'pake', 'pase': 'pase', 'talent': 'talent',
                'elite sos': 'elite_sos', 'ft%': 'ft_pct', 'wab': 'wab',
            }

            inserted = 0
            for _, row in df.iterrows():
                team = str(row.get('team', '')).strip()
                if not team or team in ('nan', 'None', ''): continue
                vals = {'season': season, 'team': team}
                for csv_col, db_col in col_map.items():
                    if csv_col in df.columns and csv_col != 'team':
                        vals[db_col] = fv(row.get(csv_col))
                if vals.get('adj_o') and vals.get('adj_d'):
                    vals['adj_em'] = vals['adj_o'] - vals['adj_d']
                cols = ', '.join(vals.keys())
                ph = ', '.join(['?'] * len(vals))
                cur.execute(f"INSERT OR REPLACE INTO torvik_season ({cols}) VALUES ({ph})",
                            list(vals.values()))
                inserted += 1

            conn.commit()
            total += inserted
            print(f"  [torvik_season] {season}: {inserted} teams")
            time.sleep(0.4)
        except Exception as e:
            print(f"  [torvik_season] {season}: ERROR {e}")

    conn.close()
    print(f"[torvik_season] DONE: {total} new rows")


# ─────────────────────────────────────────────
# 2. TORVIK DAILY (already works — just report status)
# ─────────────────────────────────────────────

DAILY_IDX = {
    'adj_o': 2, 'adj_d': 3, 'barthag': 4,
    'efg_o': 8, 'efg_d': 9, 'ftr_o': 10, 'ftr_d': 11,
    'tov_o': 12, 'tov_d': 13, 'orb': 14, 'drb': 15, 'adj_t': 27,
}

def snapshot_dates(season):
    d = date(season - 1, 11, 5)
    end = date(season, 4, 10)
    out = []
    while d <= end:
        out.append(d.strftime('%Y%m%d'))
        d += timedelta(days=7)
    return out

def pull_torvik_daily(seasons=range(2016, 2027)):
    conn = db()
    cur = conn.cursor()
    total = 0

    for season in seasons:
        existing_dates = set(
            row[0] for row in cur.execute(
                "SELECT DISTINCT snapshot_date FROM torvik_daily WHERE season=?", (season,)
            )
        )
        season_new = 0
        for snap_date in snapshot_dates(season):
            if snap_date in existing_dates: continue
            url = f"https://barttorvik.com/timemachine/team_results/{snap_date}_team_results.json.gz"
            try:
                r = requests.get(url, timeout=15, headers=HEADERS)
                if r.status_code != 200: continue
                # Explicitly decompress gzip — requests.json() doesn't handle .gz files
                import gzip, json as _json
                try:
                    raw = gzip.decompress(r.content)
                    data = _json.loads(raw)
                except Exception:
                    data = r.json()  # fallback if not actually gzipped
                if not data: continue
                batch = []
                for td in data:
                    if not isinstance(td, (list, tuple)) or len(td) < 10: continue
                    team = str(td[1]).strip()
                    if not team or team == 'nan': continue
                    adj_o = fv(td[DAILY_IDX['adj_o']])
                    adj_d = fv(td[DAILY_IDX['adj_d']])
                    batch.append((
                        season, snap_date, team,
                        adj_o, adj_d,
                        fv(td[DAILY_IDX['adj_t']]) if len(td) > DAILY_IDX['adj_t'] else None,
                        fv(td[DAILY_IDX['barthag']]),
                        (adj_o - adj_d) if adj_o is not None and adj_d is not None else None,
                        fv(td[DAILY_IDX['efg_o']]), fv(td[DAILY_IDX['efg_d']]),
                        fv(td[DAILY_IDX['tov_o']]), fv(td[DAILY_IDX['tov_d']]),
                        fv(td[DAILY_IDX['orb']]), fv(td[DAILY_IDX['drb']]),
                        fv(td[DAILY_IDX['ftr_o']]), fv(td[DAILY_IDX['ftr_d']]),
                    ))
                cur.executemany("""
                    INSERT OR IGNORE INTO torvik_daily
                    (season, snapshot_date, team, adj_o, adj_d, adj_t, barthag, adj_em,
                     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, batch)
                conn.commit()
                season_new += len(batch)
                existing_dates.add(snap_date)
                time.sleep(0.2)
            except Exception as e:
                if season >= 2026:
                    print(f"    [torvik_daily] {snap_date}: ERROR {e}")

        total += season_new
        row_count = cur.execute(
            "SELECT COUNT(*) FROM torvik_daily WHERE season=?", (season,)
        ).fetchone()[0]
        if season_new > 0:
            print(f"  [torvik_daily] {season}: +{season_new} new rows ({row_count:,} total)")
        else:
            print(f"  [torvik_daily] {season}: already complete ({row_count:,} rows)")

    conn.close()
    print(f"[torvik_daily] DONE: {total} new rows")


# ─────────────────────────────────────────────
# 3. TORVIK GAME PREDICTIONS
# ─────────────────────────────────────────────

def pull_torvik_game_preds(seasons=range(2016, 2027)):
    conn = db()
    cur = conn.cursor()
    total = 0

    for season in seasons:
        existing = cur.execute(
            "SELECT COUNT(*) FROM torvik_game_preds WHERE season=?", (season,)
        ).fetchone()[0]
        if existing > 100:
            print(f"  [torvik_preds] {season}: already loaded ({existing} games)")
            continue

        url = f"https://barttorvik.com/getgamestats.php?year={season}"
        try:
            r = requests.get(url, timeout=45, headers=HEADERS)
            r.raise_for_status()
            data = r.json()

            # Field layout (verified from browser inspection of 2024 data):
            # [0]=date "12/21/23", [2]=team, [4]=opp, [5]=H/A/N
            # [27]=torvik_adj_margin, [28]=win_prob
            # [29]=box_json string: box[18]=away_pts, box[33]=home_pts,
            #                       box[36]=home_team_name, box[37]=away_team_name
            batch = []
            for game in data:
                if not isinstance(game, (list, tuple)) or len(game) < 30: continue
                try:
                    box = json.loads(game[29])
                    home_team = str(box[36]).strip()
                    away_team = str(box[37]).strip()
                    home_pts  = int(box[33])
                    away_pts  = int(box[18])
                    if not home_team or not away_team: continue

                    raw_date = str(game[0]).strip()
                    if '/' in raw_date:
                        parts = raw_date.split('/')
                        m, d_, y = parts[0], parts[1], parts[2]
                        if len(y) == 2: y = '20' + y
                        game_date = f"{y}-{m.zfill(2)}-{d_.zfill(2)}"
                    else:
                        game_date = raw_date

                    batch.append((
                        season, game_date, home_team, away_team,
                        None, None, fv(game[27]), fv(game[28]),
                        home_pts - away_pts, home_pts, away_pts,
                    ))
                except Exception:
                    continue

            seen, deduped = set(), []
            for row in batch:
                key = (row[1], row[2], row[3])
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)

            cur.executemany("""
                INSERT OR IGNORE INTO torvik_game_preds
                (season, game_date, home_team, away_team,
                 torvik_home_pts, torvik_away_pts, torvik_margin,
                 torvik_win_prob, actual_margin, actual_home, actual_away)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, deduped)
            conn.commit()
            total += len(deduped)
            print(f"  [torvik_preds] {season}: {len(deduped)} games (raw: {len(data)})")
            time.sleep(0.5)
        except Exception as e:
            print(f"  [torvik_preds] {season}: ERROR {e}")

    conn.close()
    print(f"[torvik_preds] DONE: {total} rows")


# ─────────────────────────────────────────────
# 4. HASLAMETRICS — XML-based (v4 fix)
#
# THE ROOT CAUSE: The .php page is JS-rendered. requests.get() on the HTML
# gets an empty DataTable shell. The actual data is in:
#   https://haslametrics.com/ratings{YY}.xml
# e.g. ratings24.xml for 2024, ratings16.xml for 2016, etc.
#
# The XML has one <mr> element per team with 131 attributes covering
# ALL 5 dropdown views. Parse with xml.etree.ElementTree.
#
# Available: ratings16.xml through ratings25.xml (all return 200).
# ratings26.xml returns 404 (current season not yet finalized).
# ─────────────────────────────────────────────

def season_to_xml_year(season):
    """2024 → '24', 2016 → '16'"""
    return str(season)[-2:]


def parse_rank_delta(val):
    """'▲ 1' or '-1' or '0' → integer delta"""
    if val is None: return None
    s = str(val).strip()
    # Remove arrows/spaces
    s = re.sub(r'[▲▼\s]', '', s)
    try:
        return int(s)
    except:
        return None


def pull_haslametrics(seasons=range(2016, 2027)):
    conn = db()
    cur = conn.cursor()
    total = 0

    for season in seasons:
        existing = cur.execute(
            "SELECT COUNT(*) FROM haslametrics WHERE season=?", (season,)
        ).fetchone()[0]
        if existing > 50:
            print(f"  [haslametrics] {season}: already loaded ({existing} teams)")
            continue

        yr = season_to_xml_year(season)
        url = f"https://haslametrics.com/ratings{yr}.xml"

        try:
            r = requests.get(url, timeout=30, headers=HEADERS)
            if r.status_code == 404:
                print(f"  [haslametrics] {season}: XML not available (404)")
                continue
            r.raise_for_status()

            root = ET.fromstring(r.content)
            batch = []

            for mr in root.findall('mr'):
                a = mr.attrib  # dict of all 131 attributes

                team = a.get('t', '').strip()
                if not team: continue

                batch.append((
                    season,
                    team,
                    a.get('abbr'),
                    a.get('c'),                        # conference
                    int(a.get('w', 0) or 0),           # wins
                    int(a.get('l', 0) or 0),           # losses
                    fv(a.get('ap')),                   # ap_pct
                    # Offense
                    fv(a.get('oe')),                   # o_eff
                    fv(a.get('ou')),                   # o_pace
                    fv(a.get('oftar')),                # o_ftar
                    fv(a.get('ofgar')),                # o_fgar
                    fv(a.get('otpar')),                # o_three_par
                    fv(a.get('onpar')),                # o_npar
                    fv(a.get('ofpar')),                # o_fpar
                    fv(a.get('ofgmr')),                # o_fgmr
                    fv(a.get('otpmr')),                # o_three_pmr
                    fv(a.get('onpmr')),                # o_npm_r
                    fv(a.get('ofpmr')),                # o_fpmr
                    fv(a.get('ftpct')),                # o_ft_pct
                    # Defense
                    fv(a.get('de')),                   # d_eff
                    fv(a.get('du')),                   # d_pace
                    fv(a.get('dftar')),                # d_ftar
                    fv(a.get('dfgar')),                # d_fgar
                    fv(a.get('dtpar')),                # d_three_par
                    fv(a.get('dnpar')),                # d_npar
                    fv(a.get('dfpar')),                # d_fpar
                    fv(a.get('dfgmr')),                # d_fgmr
                    fv(a.get('dtpmr')),                # d_three_pmr
                    fv(a.get('dnpmr')),                # d_npm_r
                    fv(a.get('dfpmr')),                # d_fpmr
                    fv(a.get('dftpct')),               # d_ft_pct
                    # Fingerprint
                    fv(a.get('sos')),                  # sos
                    fv(a.get('mom')),                  # mom
                    fv(a.get('mmo')),                  # mom_o
                    fv(a.get('mmd')),                  # mom_d
                    fv(a.get('inc')),                  # consistency
                    fv(a.get('ptf')),                  # ptf
                    fv(a.get('rpi')),                  # rq
                    fv(a.get('afh')),                  # afh
                    fv(a.get('asr')),                  # asr
                    parse_rank_delta(a.get('vs1')),    # rk_1
                    parse_rank_delta(a.get('vs7')),    # rk_7
                    parse_rank_delta(a.get('vs30')),   # rk_30
                    # Performance
                    a.get('p5wl'),                     # last5_wl
                    a.get('p5ud'),                     # last5_ud
                    int(a.get('vspre', 0) or 0),       # vs_pre
                    # Records
                    a.get('r_1_50'),
                    a.get('r_51_100'),
                    a.get('r_101_150'),
                    a.get('r_151_200'),
                    a.get('r_201_250'),
                    a.get('r_251_300'),
                    a.get('r_301_up'),
                    a.get('r_q1'),
                    a.get('r_q2'),
                    a.get('r_q3'),
                    a.get('r_q4'),
                    a.get('r_home'),
                    a.get('r_away'),
                    a.get('r_neut'),
                ))

            cur.executemany("""
                INSERT OR REPLACE INTO haslametrics
                (season, team, abbr, conf, wins, losses, ap_pct,
                 o_eff, o_pace, o_ftar, o_fgar, o_three_par, o_npar, o_fpar,
                 o_fgmr, o_three_pmr, o_npm_r, o_fpmr, o_ft_pct,
                 d_eff, d_pace, d_ftar, d_fgar, d_three_par, d_npar, d_fpar,
                 d_fgmr, d_three_pmr, d_npm_r, d_fpmr, d_ft_pct,
                 sos, mom, mom_o, mom_d, consistency, ptf, rq, afh, asr,
                 rk_1, rk_7, rk_30,
                 last5_wl, last5_ud, vs_pre,
                 r_1_50, r_51_100, r_101_150, r_151_200, r_201_250,
                 r_251_300, r_301_up, r_q1, r_q2, r_q3, r_q4,
                 r_home, r_away, r_neut)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()
            total += len(batch)
            print(f"  [haslametrics] {season}: {len(batch)} teams (from ratings{yr}.xml)")
            time.sleep(0.5)

        except ET.ParseError as e:
            print(f"  [haslametrics] {season}: XML parse error — {e}")
        except Exception as e:
            print(f"  [haslametrics] {season}: ERROR — {e}")

    conn.close()
    print(f"[haslametrics] DONE: {total} total rows")


# ─────────────────────────────────────────────
# 5. CBB.CSV SUPPLEMENT
# ─────────────────────────────────────────────

def load_cbb_csv():
    search = [
        os.path.join(ROOT, 'data', 'cbb.csv'),
        os.path.join(ROOT, 'cbb.csv'),
        'data/cbb.csv', 'cbb.csv',
    ]
    path = next((p for p in search if os.path.exists(p)), None)
    if not path:
        print("[cbb_csv] Not found — place in data/cbb.csv to use")
        return

    conn = db()
    cur = conn.cursor()
    df = pd.read_csv(path)
    ins = 0
    for _, row in df.iterrows():
        adj_o = fv(row.get('ADJOE'))
        adj_d = fv(row.get('ADJDE'))
        cur.execute("""
            INSERT OR IGNORE INTO torvik_season
            (season, team, adj_o, adj_d, adj_em, barthag, efg_o, efg_d,
             tov_o, tov_d, orb, drb, ftr_o, ftr_d,
             two_p_o, two_p_d, three_p_o, three_p_d, adj_t, wab)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            int(row['YEAR']), str(row['TEAM']).strip(),
            adj_o, adj_d,
            (adj_o - adj_d) if adj_o and adj_d else None,
            fv(row.get('BARTHAG')), fv(row.get('EFG_O')), fv(row.get('EFG_D')),
            fv(row.get('TOR')), fv(row.get('TORD')),
            fv(row.get('ORB')), fv(row.get('DRB')),
            fv(row.get('FTR')), fv(row.get('FTRD')),
            fv(row.get('2P_O')), fv(row.get('2P_D')),
            fv(row.get('3P_O')), fv(row.get('3P_D')),
            fv(row.get('ADJ_T')), fv(row.get('WAB')),
        ))
        ins += 1
    conn.commit()
    conn.close()
    print(f"[cbb_csv] Loaded {ins} rows from {path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 60)
    print("NCAAB Model — Pulling All Data Sources (v5)")
    print("=" * 60)

    # Setup schema + migration
    conn = db()
    migrate_tables(conn)  # drop stale schemas (torvik_game_preds, haslametrics)
    conn.executescript(SCHEMA_BASE)
    conn.executescript(GAME_PREDS_SCHEMA)
    conn.commit()
    conn.close()
    print("[SCHEMA] Tables created/verified.")

    print("\n[1/5] Torvik season finals...")
    pull_torvik_season(seasons=range(2016, 2027))

    print("\n[2/5] Torvik daily time machine...")
    pull_torvik_daily(seasons=range(2016, 2027))

    print("\n[3/5] Torvik game predictions...")
    pull_torvik_game_preds(seasons=range(2016, 2027))

    print("\n[4/5] Haslametrics (XML source: ratings{YY}.xml)...")
    pull_haslametrics(seasons=range(2016, 2027))

    print("\n[5/5] cbb.csv supplement...")
    load_cbb_csv()

    print("\n--- Data Summary ---")
    conn = db()
    for tbl in ['torvik_season', 'torvik_daily', 'torvik_game_preds', 'haslametrics']:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  {tbl}: {n:,} rows")
        except:
            print(f"  {tbl}: table missing")
    conn.close()
    print("\nNext: python scripts/04_build_features.py")
