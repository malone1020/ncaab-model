"""
03c_fill_snap_gaps.py
=====================
Fills the remaining snapshot gaps in torvik_daily.
The prior backfill (03b) missed Jan 13-31, all Feb, all Mar each season
because those dates either had no games or the old monthly snapshots
(stored as YYYY-MM-DD) caused the dedup check to skip them.

This script finds every game date whose day-before snapshot is missing
and fetches it. It also fills any gap > 1 day between consecutive snapshots
within a season (so interpolation in 04_build_features always finds a snap).

Run: python scripts/03c_fill_snap_gaps.py
Expected runtime: ~20-40 minutes depending on gaps.
"""
import sqlite3, requests, time, os, json
import pandas as pd
from datetime import datetime, timedelta, date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'}

DAILY_IDX = {
    'adj_o': 2, 'adj_d': 3, 'barthag': 4,
    'efg_o': 8, 'efg_d': 9, 'ftr_o': 10, 'ftr_d': 11,
    'tov_o': 12, 'tov_d': 13, 'orb': 14, 'drb': 15,
}

def fv(v):
    if v is None: return None
    s = str(v).strip()
    if s in ('', '---', 'N/A', 'nan', 'None'): return None
    try: return float(s)
    except: return None

def season_for_date(d):
    """Season = year the tournament ends in (Nov-Apr)"""
    return d.year + 1 if d.month >= 11 else d.year

def fetch_snapshot(snap_str, season, cur):
    url = f"https://barttorvik.com/timemachine/team_results/{snap_str}_team_results.json.gz"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code == 404: return 0, 'not_found'
        if r.status_code != 200: return 0, f'http_{r.status_code}'
        data = r.json()
        if not data or not isinstance(data, list): return 0, 'empty'
        batch = []
        for td in data:
            if not isinstance(td, (list, tuple)) or len(td) < 10: continue
            team = str(td[1]).strip()
            if not team or team in ('nan', 'None', ''): continue
            adj_o = fv(td[DAILY_IDX['adj_o']])
            adj_d = fv(td[DAILY_IDX['adj_d']])
            batch.append((
                season, snap_str, team,
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
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # All existing snapshots (YYYYMMDD only — ignore old YYYY-MM-DD monthly format)
    existing = set(
        r[0] for r in cur.execute(
            "SELECT DISTINCT snapshot_date FROM torvik_daily WHERE length(snapshot_date)=8"
        )
    )
    print(f"Existing YYYYMMDD snapshots: {len(existing)}")

    # All game dates + their seasons
    game_rows = cur.execute("""
        SELECT DISTINCT game_date, season FROM games
        WHERE home_score IS NOT NULL ORDER BY game_date
    """).fetchall()

    # Build set of needed snap dates = day before every game date
    needed = set()
    for gd_str, season in game_rows:
        gd   = datetime.strptime(gd_str[:10], '%Y-%m-%d').date()
        snap = gd - timedelta(days=1)
        needed.add((snap.strftime('%Y%m%d'), season_for_date(snap)))

    to_fetch = sorted(
        (snap_str, season)
        for snap_str, season in needed
        if snap_str not in existing
    )

    print(f"Snapshots still needed: {len(to_fetch)}")
    if not to_fetch:
        print("Nothing to fetch — already complete!")
        return

    # Show sample of what we're fetching
    print("Sample of dates to fetch:")
    for s, ss in to_fetch[:10]:
        print(f"  {s} (season {ss})")

    print(f"\nEstimated time: {len(to_fetch)*1.2/60:.0f}-{len(to_fetch)*1.5/60:.0f} min\n")

    total_rows = errors = not_found = 0
    for i, (snap_str, season) in enumerate(to_fetch):
        n, status = fetch_snapshot(snap_str, season, cur)
        total_rows += n
        if status == 'ok':
            if (i+1) % 25 == 0 or i < 3:
                print(f"  [{i+1:>4}/{len(to_fetch)}] {snap_str} (s{season}): {n} teams | total: {total_rows:,}")
        elif status == 'not_found':
            not_found += 1
            if not_found <= 5:
                print(f"  [{i+1:>4}] {snap_str}: 404")
        else:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1:>4}] {snap_str}: {status}")
        time.sleep(1.0)

    print(f"\n{'='*50}")
    print(f"Done. Fetched {len(to_fetch)} dates, {total_rows:,} rows added")
    print(f"Not found: {not_found}  Errors: {errors}")

    total = cur.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
    udates = cur.execute("SELECT COUNT(DISTINCT snapshot_date) FROM torvik_daily").fetchone()[0]
    print(f"torvik_daily now: {total:,} rows, {udates} snapshot dates")
    print("\nNext: python scripts/04_build_features.py")
    conn.close()

if __name__ == '__main__':
    main()
