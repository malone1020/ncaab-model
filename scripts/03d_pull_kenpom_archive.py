"""
03d_pull_kenpom_archive.py
==========================
Pulls KenPom archive (daily snapshot) ratings for every game date in the DB.
Stores in kenpom_daily table: one row per (season, snapshot_date, team).

Uses day-before-game snapshot to avoid leakage — same pattern as torvik_daily.

API docs: https://kenpom.com/api
Run: python scripts/03d_pull_kenpom_archive.py
Expected runtime: ~20-40 min depending on number of unique game dates.
"""
import sqlite3, requests, time, os, json
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

API_KEY  = 'b59dbfcaa5224d02cea409c73299040637a4d3445da5def3d1f320e313da1571'
BASE_URL = 'https://kenpom.com/api'
HEADERS  = {'Authorization': f'Bearer {API_KEY}'}

def db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kenpom_daily (
            season        INTEGER,
            snapshot_date TEXT,   -- YYYYMMDD
            team          TEXT,
            adj_em        REAL,
            adj_o         REAL,
            adj_d         REAL,
            adj_t         REAL,
            luck          REAL,
            sos_adj_em    REAL,
            opp_o         REAL,
            opp_d         REAL,
            ncon_sos      REAL,
            rank          INTEGER,
            PRIMARY KEY (season, snapshot_date, team)
        )
    """)
    conn.commit()

def season_for_date(d):
    return d.year + 1 if d.month >= 11 else d.year

def fetch_snapshot(snap_date_str, season):
    """
    Fetch KenPom archive ratings for a specific date.
    snap_date_str: YYYYMMDD
    Returns list of row dicts, or None on failure.
    """
    # Convert YYYYMMDD to YYYY-MM-DD for API
    d = datetime.strptime(snap_date_str, '%Y%m%d')
    date_param = d.strftime('%Y-%m-%d')

    url = f"{BASE_URL}/archive/ratings"
    params = {'date': date_param}

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        if r.status_code == 404:
            return None, 'not_found'
        if r.status_code == 401:
            return None, 'auth_error'
        if r.status_code == 429:
            return None, 'rate_limit'
        if r.status_code != 200:
            return None, f'http_{r.status_code}'

        data = r.json()

        # Probe response shape on first call
        if not data:
            return None, 'empty'

        return data, 'ok'

    except requests.exceptions.Timeout:
        return None, 'timeout'
    except Exception as e:
        return None, f'error:{e}'

def parse_and_insert(data, snap_date_str, season, cur):
    """
    Parse API response and insert rows.
    We'll probe the shape on first real call — KenPom API returns
    either a list of team dicts or a dict with a 'ratings' key.
    """
    rows = []

    # Handle both list and wrapped-list response shapes
    if isinstance(data, dict):
        # Try common wrapper keys
        for key in ('ratings', 'data', 'teams', 'results'):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            # Flat dict with team names as keys?
            if all(isinstance(v, dict) for v in data.values()):
                data = [{'team': k, **v} for k, v in data.items()]
            else:
                return 0, f'unexpected_shape: {list(data.keys())[:5]}'

    for item in data:
        if not isinstance(item, dict):
            continue

        # Team name — try multiple field names
        team = (item.get('team_name') or item.get('team') or
                item.get('TeamName') or item.get('name') or '')
        if not team:
            continue

        def fv(keys):
            """Try multiple key names, return float or None."""
            for k in (keys if isinstance(keys, list) else [keys]):
                v = item.get(k)
                if v is not None:
                    try: return float(v)
                    except: pass
            return None

        adj_em = fv(['adj_em', 'AdjEM', 'adj_efficiency_margin', 'em'])
        adj_o  = fv(['adj_o',  'AdjO',  'adj_offense',  'adjoe', 'adj_off'])
        adj_d  = fv(['adj_d',  'AdjD',  'adj_defense',  'adjde', 'adj_def'])
        adj_t  = fv(['adj_t',  'AdjT',  'adj_tempo',    'adjte', 'tempo'])
        luck   = fv(['luck',   'Luck'])
        sos    = fv(['sos_adj_em', 'sos', 'SOS', 'strength_of_schedule',
                     'sos_em', 'adj_em_sos'])
        opp_o  = fv(['opp_o', 'OppO', 'opp_adj_o', 'opp_offense'])
        opp_d  = fv(['opp_d', 'OppD', 'opp_adj_d', 'opp_defense'])
        ncon   = fv(['ncon_sos', 'ncon', 'non_conf_sos'])
        rank   = item.get('rank') or item.get('Rank') or item.get('rk')
        try: rank = int(rank) if rank is not None else None
        except: rank = None

        rows.append((season, snap_date_str, str(team).strip(),
                     adj_em, adj_o, adj_d, adj_t, luck,
                     sos, opp_o, opp_d, ncon, rank))

    if rows:
        cur.executemany("""
            INSERT OR IGNORE INTO kenpom_daily
            (season, snapshot_date, team, adj_em, adj_o, adj_d, adj_t,
             luck, sos_adj_em, opp_o, opp_d, ncon_sos, rank)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        cur.connection.commit()

    return len(rows), 'ok'

def probe_api():
    """Fetch one date to confirm API shape before bulk run."""
    print("Probing API with 2024-01-15...")
    data, status = fetch_snapshot('20240115', 2024)
    if status != 'ok':
        print(f"  ERROR: {status}")
        return False
    print(f"  Status: ok, response type: {type(data)}")
    if isinstance(data, list):
        print(f"  List length: {len(data)}")
        print(f"  First item keys: {list(data[0].keys()) if data else '(empty)'}")
        print(f"  First item sample: {data[0]}")
    elif isinstance(data, dict):
        print(f"  Dict keys: {list(data.keys())[:10]}")
    return True

def main():
    conn = db()
    cur  = conn.cursor()
    ensure_table(conn)

    # Probe API shape first
    if not probe_api():
        print("API probe failed — check key and endpoint. Aborting.")
        return

    print()

    # All unique game dates + seasons
    game_rows = cur.execute("""
        SELECT DISTINCT game_date, season FROM games
        WHERE home_score IS NOT NULL ORDER BY game_date
    """).fetchall()

    # Already-fetched snapshot dates
    existing = set(
        r[0] for r in cur.execute(
            "SELECT DISTINCT snapshot_date FROM kenpom_daily"
        )
    )
    print(f"Existing kenpom_daily snapshots: {len(existing)}")

    # Build fetch list: day before each game date
    seen = set()
    to_fetch = []
    for gd_str, season in game_rows:
        gd   = datetime.strptime(gd_str[:10], '%Y-%m-%d')
        snap = (gd - timedelta(days=1))
        snap_str = snap.strftime('%Y%m%d')
        s = season_for_date(snap.date())
        if snap_str not in existing and snap_str not in seen:
            seen.add(snap_str)
            to_fetch.append((snap_str, s))

    print(f"Dates to fetch: {len(to_fetch)}")
    print(f"Estimated time: {len(to_fetch)*1.2/60:.0f}-{len(to_fetch)*2/60:.0f} min\n")

    total_rows = errors = not_found = rate_limited = 0

    for i, (snap_str, season) in enumerate(to_fetch):
        data, status = fetch_snapshot(snap_str, season)

        if status == 'ok':
            n, parse_status = parse_and_insert(data, snap_str, season, cur)
            total_rows += n
            if (i+1) % 25 == 0 or i < 3:
                print(f"  [{i+1:>4}/{len(to_fetch)}] {snap_str} (s{season}): "
                      f"{n} teams | total: {total_rows:,}")
            if 'unexpected_shape' in parse_status:
                print(f"  [{i+1}] SHAPE WARNING: {parse_status}")
                print(f"  Raw data sample: {str(data)[:300]}")
                break  # Stop and fix parser before continuing
        elif status == 'not_found':
            not_found += 1
            if not_found <= 3:
                print(f"  [{i+1:>4}] {snap_str}: 404 (pre-season date?)")
        elif status == 'rate_limit':
            rate_limited += 1
            print(f"  [{i+1:>4}] {snap_str}: rate limited — sleeping 60s")
            time.sleep(60)
            # Retry once
            data, status = fetch_snapshot(snap_str, season)
            if status == 'ok':
                n, _ = parse_and_insert(data, snap_str, season, cur)
                total_rows += n
        elif status == 'auth_error':
            print("AUTH ERROR — check API key. Aborting.")
            break
        else:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1:>4}] {snap_str}: {status}")

        time.sleep(1.2)  # Respectful rate limit

    print(f"\n{'='*50}")
    print(f"Done.")
    print(f"  Fetched:    {len(to_fetch)} dates")
    print(f"  Rows added: {total_rows:,}")
    print(f"  Not found:  {not_found}")
    print(f"  Errors:     {errors}")

    total = cur.execute("SELECT COUNT(*) FROM kenpom_daily").fetchone()[0]
    udates = cur.execute("SELECT COUNT(DISTINCT snapshot_date) FROM kenpom_daily").fetchone()[0]
    print(f"\n  kenpom_daily: {total:,} rows, {udates} snapshot dates")
    print(f"\nNext: python scripts/04_build_features.py")
    conn.close()

if __name__ == '__main__':
    main()
