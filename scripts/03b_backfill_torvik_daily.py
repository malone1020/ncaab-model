"""
03b_backfill_torvik_daily.py
============================
Fetches a Torvik time-machine snapshot for every unique game date
in the games table that doesn't already have coverage.

The existing torvik_daily table only has 50 snapshots (monthly).
This script fills in all ~800 missing game dates (~350 teams each).

Torvik time machine URL format:
  https://barttorvik.com/timemachine/team_results/YYYYMMDD_team_results.json.gz

Run: python scripts/03b_backfill_torvik_daily.py
Expected runtime: ~15-25 minutes (rate limited to 1 req/sec)
"""
import sqlite3, requests, time, os, json
import pandas as pd
from datetime import date, timedelta, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'}

DAILY_IDX = {
    'adj_o': 2, 'adj_d': 3, 'barthag': 4,
    'efg_o': 8, 'efg_d': 9, 'ftr_o': 10, 'ftr_d': 11,
    'tov_o': 12, 'tov_d': 13, 'orb': 14, 'drb': 15, 'adj_t': 27,
}

def fv(v):
    if v is None: return None
    s = str(v).strip()
    if s in ('', '---', 'N/A', 'nan', 'None'): return None
    try: return float(s)
    except: return None

def db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def season_for_date(d):
    """Game date → season (season = year March finishes in)"""
    if d.month >= 11:
        return d.year + 1
    return d.year

def fetch_snapshot(snap_date_str, season, cur):
    """
    Fetch one date's snapshot from Torvik time machine.
    snap_date_str: 'YYYYMMDD' format
    Returns number of rows inserted.
    """
    url = f"https://barttorvik.com/timemachine/team_results/{snap_date_str}_team_results.json.gz"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code == 404:
            return 0, 'not_found'
        if r.status_code != 200:
            return 0, f'http_{r.status_code}'

        data = r.json()
        if not data or not isinstance(data, list):
            return 0, 'empty'

        batch = []
        for td in data:
            if not isinstance(td, (list, tuple)) or len(td) < 10:
                continue
            team = str(td[1]).strip()
            if not team or team in ('nan', 'None', ''):
                continue
            adj_o = fv(td[DAILY_IDX['adj_o']])
            adj_d = fv(td[DAILY_IDX['adj_d']])
            adj_t = fv(td[DAILY_IDX['adj_t']]) if len(td) > DAILY_IDX['adj_t'] else None
            batch.append((
                season, snap_date_str, team,
                adj_o, adj_d,
                fv(td[DAILY_IDX['barthag']]),
                (adj_o - adj_d) if adj_o is not None and adj_d is not None else None,
                fv(td[DAILY_IDX['efg_o']]), fv(td[DAILY_IDX['efg_d']]),
                fv(td[DAILY_IDX['tov_o']]), fv(td[DAILY_IDX['tov_d']]),
                fv(td[DAILY_IDX['orb']]),   fv(td[DAILY_IDX['drb']]),
                fv(td[DAILY_IDX['ftr_o']]), fv(td[DAILY_IDX['ftr_d']]),
            ))

        if batch:
            cur.executemany("""
                INSERT OR IGNORE INTO torvik_daily
                (season, snapshot_date, team, adj_o, adj_d, barthag, adj_em,
                 efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, batch)
            cur.connection.commit()
        return len(batch), 'ok'

    except requests.exceptions.Timeout:
        return 0, 'timeout'
    except Exception as e:
        return 0, f'error:{e}'

def main():
    conn = db()
    cur = conn.cursor()

    # ── Get all unique game dates from the games table ────────────────────────
    game_rows = cur.execute("""
        SELECT DISTINCT game_date, season FROM games
        WHERE home_score IS NOT NULL
        ORDER BY game_date
    """).fetchall()
    print(f"Total unique game dates in DB: {len(game_rows):,}")

    # ── Get already-covered snapshot dates ────────────────────────────────────
    existing = set(
        row[0] for row in cur.execute(
            "SELECT DISTINCT snapshot_date FROM torvik_daily"
        )
    )
    print(f"Already have snapshots for {len(existing)} dates")

    # ── Build list of dates to fetch ──────────────────────────────────────────
    # For each game date, we want a snapshot from the day BEFORE the game
    # (so it's genuinely pre-game data, no leakage)
    to_fetch = []
    for game_date_str, season in game_rows:
        gd = datetime.strptime(game_date_str[:10], '%Y-%m-%d').date()
        snap = gd - timedelta(days=1)  # day before game
        snap_str = snap.strftime('%Y%m%d')
        if snap_str not in existing:
            to_fetch.append((snap_str, season, game_date_str))

    # Deduplicate (multiple games same day → one snapshot)
    seen = set()
    unique_fetches = []
    for snap_str, season, gd in to_fetch:
        if snap_str not in seen:
            seen.add(snap_str)
            unique_fetches.append((snap_str, season))

    print(f"Dates to fetch: {len(unique_fetches):,}")
    print(f"Estimated time: {len(unique_fetches) * 1.2 / 60:.0f}-{len(unique_fetches) * 1.5 / 60:.0f} minutes\n")

    # ── Fetch with progress reporting ─────────────────────────────────────────
    total_rows = 0
    errors = 0
    not_found = 0

    for i, (snap_str, season) in enumerate(unique_fetches):
        n, status = fetch_snapshot(snap_str, season, cur)
        total_rows += n

        if status == 'ok':
            if (i + 1) % 25 == 0 or i < 5:
                print(f"  [{i+1:>4}/{len(unique_fetches)}] {snap_str} (season {season}): "
                      f"{n} teams | total rows: {total_rows:,}")
        elif status == 'not_found':
            not_found += 1
            if not_found <= 10:
                print(f"  [{i+1:>4}] {snap_str}: not found (404) — Torvik may not have this date")
        else:
            errors += 1
            if errors <= 10:
                print(f"  [{i+1:>4}] {snap_str}: {status}")

        time.sleep(1.0)  # respectful rate limit

    print(f"\n{'='*50}")
    print(f"DONE")
    print(f"  Fetched:    {len(unique_fetches):,} dates")
    print(f"  Rows added: {total_rows:,}")
    print(f"  Not found:  {not_found}")
    print(f"  Errors:     {errors}")

    # Final count
    total_in_db = cur.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
    unique_dates = cur.execute("SELECT COUNT(DISTINCT snapshot_date) FROM torvik_daily").fetchone()[0]
    print(f"\n  torvik_daily now: {total_in_db:,} rows across {unique_dates} snapshot dates")
    print(f"\nNext: python scripts/04_build_features.py")
    conn.close()

if __name__ == '__main__':
    main()
