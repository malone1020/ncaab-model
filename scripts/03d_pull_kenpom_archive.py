"""
03d_pull_kenpom_archive.py
==========================
Pulls two KenPom endpoints for every game date in the DB:

1. archive  — daily team ratings snapshot (AdjEM, AdjOE, AdjDE, AdjTempo, Luck, SOS)
              Uses day-before-game date to avoid leakage.
              Stored in: kenpom_daily

2. fanmatch — pre-game predictions (predicted scores, win prob, tempo)
              These are inherently leakage-free (pre-game projections).
              Stored in: kenpom_fanmatch

Base URL:  https://kenpom.com/api.php
Auth:      Authorization: Bearer <key>
Docs:      https://kenpom.com/api-documentation.php

Run: python scripts/03d_pull_kenpom_archive.py
"""
import sqlite3, requests, time, os
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

API_KEY  = 'b59dbfcaa5224d02cea409c73299040637a4d3445da5def3d1f320e313da1571'
BASE_URL = 'https://kenpom.com/api.php'
HEADERS  = {'Authorization': f'Bearer {API_KEY}'}

# ── DB setup ─────────────────────────────────────────────────────────────────

def db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def ensure_tables(conn):
    # NOTE: Do NOT drop kenpom_daily — use INSERT OR IGNORE to preserve existing data
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kenpom_daily (
            season        INTEGER,
            snapshot_date TEXT,    -- YYYYMMDD
            team          TEXT,
            adj_em        REAL,
            adj_o         REAL,
            adj_d         REAL,
            adj_tempo     REAL,
            luck          REAL,
            sos           REAL,
            sos_o         REAL,
            sos_d         REAL,
            ncsос         REAL,
            rank_adj_em   INTEGER,
            pythag        REAL,
            PRIMARY KEY (season, snapshot_date, team)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kenpom_fanmatch (
            season        INTEGER,
            game_date     TEXT,    -- YYYY-MM-DD
            game_id       INTEGER,
            home_team     TEXT,
            away_team     TEXT,
            home_rank     INTEGER,
            away_rank     INTEGER,
            home_pred     REAL,
            away_pred     REAL,
            home_wp       REAL,
            pred_tempo    REAL,
            thrill_score  REAL,
            PRIMARY KEY (season, game_date, game_id)
        )
    """)
    conn.commit()

# ── Helpers ───────────────────────────────────────────────────────────────────

def season_for_date(d):
    return d.year + 1 if d.month >= 11 else d.year

def fv(d, *keys):
    for k in keys:
        v = d.get(k)
        if v is not None:
            try: return float(v)
            except: pass
    return None

def iv(d, *keys):
    for k in keys:
        v = d.get(k)
        if v is not None:
            try: return int(v)
            except: pass
    return None

def api_get(endpoint, params, retries=2):
    p = {'endpoint': endpoint, **params}
    for attempt in range(retries + 1):
        try:
            r = requests.get(BASE_URL, headers=HEADERS, params=p, timeout=20)
            if r.status_code == 200:
                return r.json(), 'ok'
            if r.status_code == 404:
                return None, 'not_found'
            if r.status_code == 401:
                return None, 'auth_error'
            if r.status_code == 429:
                time.sleep(60)
                continue
            return None, f'http_{r.status_code}'
        except requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(5)
            else:
                return None, 'timeout'
        except Exception as e:
            return None, f'error:{e}'
    return None, 'max_retries'

# ── Archive (daily ratings) ───────────────────────────────────────────────────

def fetch_archive(snap_date_str, season, cur):
    """Fetch archive ratings for YYYYMMDD date, insert into kenpom_daily."""
    date_param = f"{snap_date_str[:4]}-{snap_date_str[4:6]}-{snap_date_str[6:8]}"
    data, status = api_get('archive', {'d': date_param})
    if status != 'ok' or not data:
        return 0, status

    rows = []
    for item in (data if isinstance(data, list) else []):
        team = item.get('TeamName', '').strip()
        if not team:
            continue
        rows.append((
            season, snap_date_str, team,
            fv(item, 'AdjEM'),
            fv(item, 'AdjOE'),
            fv(item, 'AdjDE'),
            fv(item, 'AdjTempo'),
            fv(item, 'Luck'),
            fv(item, 'SOS'),
            fv(item, 'SOSO'),
            fv(item, 'SOSD'),
            fv(item, 'NCSОС', 'NCSОС'),
            iv(item, 'RankAdjEM'),
            fv(item, 'Pythag'),
        ))

    if rows:
        cur.executemany("""
            INSERT OR IGNORE INTO kenpom_daily
            (season, snapshot_date, team, adj_em, adj_o, adj_d, adj_tempo,
             luck, sos, sos_o, sos_d, ncsос, rank_adj_em, pythag)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        cur.connection.commit()
    return len(rows), 'ok'

# ── Fanmatch (pre-game predictions) ──────────────────────────────────────────

def fetch_fanmatch(game_date_str, season, cur):
    """Fetch fanmatch predictions for a game date (YYYY-MM-DD)."""
    data, status = api_get('fanmatch', {'d': game_date_str})
    if status != 'ok' or not data:
        return 0, status

    rows = []
    for item in (data if isinstance(data, list) else []):
        gid = iv(item, 'GameID')
        home = item.get('Home', '').strip()
        away = (item.get('Visitor') or item.get('Visitors') or item.get('Away') or '').strip()
        if not home or not away:
            continue
        rows.append((
            season, game_date_str, gid,
            home, away,
            iv(item, 'HomeRank'),
            iv(item, 'VisitorRank'),
            fv(item, 'HomePred'),
            fv(item, 'VisitorPred'),
            fv(item, 'HomeWP'),
            fv(item, 'PredTempo'),
            fv(item, 'ThrillScore'),
        ))

    if rows:
        cur.executemany("""
            INSERT OR IGNORE INTO kenpom_fanmatch
            (season, game_date, game_id, home_team, away_team,
             home_rank, away_rank, home_pred, away_pred,
             home_wp, pred_tempo, thrill_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        cur.connection.commit()
    return len(rows), 'ok'

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn = db()
    cur  = conn.cursor()
    ensure_tables(conn)

    # ── Probe first ──────────────────────────────────────────────────────────
    print("Probing archive endpoint (2024-01-15)...")
    data, status = api_get('archive', {'d': '2024-01-15'})
    if status != 'ok':
        print(f"  FAILED: {status}")
        if status == 'auth_error':
            print("  Check API key.")
        return
    sample = data[0] if isinstance(data, list) and data else {}
    print(f"  OK — {len(data) if isinstance(data, list) else '?'} teams")
    print(f"  Sample keys: {list(sample.keys())[:12]}")
    print(f"  Sample TeamName: {sample.get('TeamName')}  AdjEM: {sample.get('AdjEM')}")

    print("\nProbing fanmatch endpoint (2024-01-15)...")
    data2, status2 = api_get('fanmatch', {'d': '2024-01-15'})
    if status2 == 'ok' and data2:
        s2 = data2[0] if isinstance(data2, list) else {}
        print(f"  OK — {len(data2)} games")
        print(f"  Sample: {s2.get('Home')} vs {s2.get('Visitors')}  HomeWP={s2.get('HomeWP')}")
    else:
        print(f"  Status: {status2}")

    print()

    # ── All game dates ────────────────────────────────────────────────────────
    game_rows = cur.execute("""
        SELECT DISTINCT game_date, season FROM games
        WHERE home_score IS NOT NULL ORDER BY game_date
    """).fetchall()

    existing_archive  = set(r[0] for r in cur.execute(
        "SELECT DISTINCT snapshot_date FROM kenpom_daily"))
    existing_fanmatch = set(r[0] for r in cur.execute(
        "SELECT DISTINCT game_date FROM kenpom_fanmatch"))

    # Build fetch lists
    seen_snaps = set()
    archive_todo  = []
    fanmatch_todo = []

    for gd_str, season in game_rows:
        gd = datetime.strptime(gd_str[:10], '%Y-%m-%d')
        s  = season_for_date(gd.date())

        # Archive: day before game
        snap = (gd - timedelta(days=1))
        snap_str = snap.strftime('%Y%m%d')
        if snap_str not in existing_archive and snap_str not in seen_snaps:
            seen_snaps.add(snap_str)
            archive_todo.append((snap_str, s))

        # Fanmatch: game date itself
        gd_fmt = gd.strftime('%Y-%m-%d')
        if gd_fmt not in existing_fanmatch:
            fanmatch_todo.append((gd_fmt, season))

    # Dedup fanmatch
    seen_fm = set()
    fanmatch_todo_dedup = []
    for gd_fmt, season in fanmatch_todo:
        if gd_fmt not in seen_fm:
            seen_fm.add(gd_fmt)
            fanmatch_todo_dedup.append((gd_fmt, season))

    print(f"Archive dates to fetch:  {len(archive_todo)}")
    print(f"Fanmatch dates to fetch: {len(fanmatch_todo_dedup)}")
    total_dates = len(archive_todo) + len(fanmatch_todo_dedup)
    print(f"Total requests: {total_dates}  (~{total_dates*1.3/60:.0f}-{total_dates*1.8/60:.0f} min)\n")

    # ── Fetch archive ─────────────────────────────────────────────────────────
    if archive_todo:
        print("── Fetching archive snapshots ──")
        total_rows = errors = not_found = 0
        for i, (snap_str, season) in enumerate(archive_todo):
            n, status = fetch_archive(snap_str, season, cur)
            if status == 'ok':
                total_rows += n
                if (i+1) % 50 == 0 or i < 3:
                    print(f"  [{i+1:>4}/{len(archive_todo)}] {snap_str}: {n} teams | total: {total_rows:,}")
            elif status == 'not_found':
                not_found += 1
                if not_found <= 5:
                    print(f"  [{i+1:>4}] {snap_str}: 404 (pre-season or unavailable)")
            elif status == 'auth_error':
                print("AUTH ERROR — aborting."); break
            else:
                errors += 1
                if errors <= 5:
                    print(f"  [{i+1:>4}] {snap_str}: {status}")
            time.sleep(1.3)

        print(f"  Archive done: {total_rows:,} rows, {not_found} not_found, {errors} errors\n")

    # ── Fetch fanmatch ────────────────────────────────────────────────────────
    if fanmatch_todo_dedup:
        print("── Fetching fanmatch predictions ──")
        total_fm = errors_fm = not_found_fm = 0
        for i, (gd_fmt, season) in enumerate(fanmatch_todo_dedup):
            n, status = fetch_fanmatch(gd_fmt, season, cur)
            if status == 'ok':
                total_fm += n
                if (i+1) % 50 == 0 or i < 3:
                    print(f"  [{i+1:>4}/{len(fanmatch_todo_dedup)}] {gd_fmt}: {n} games | total: {total_fm:,}")
            elif status == 'not_found':
                not_found_fm += 1
            elif status == 'auth_error':
                print("AUTH ERROR — aborting."); break
            else:
                errors_fm += 1
                if errors_fm <= 5:
                    print(f"  [{i+1:>4}] {gd_fmt}: {status}")
            time.sleep(1.3)

        print(f"  Fanmatch done: {total_fm:,} predictions, {not_found_fm} not_found, {errors_fm} errors\n")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("="*50)
    kd = cur.execute("SELECT COUNT(*), COUNT(DISTINCT snapshot_date) FROM kenpom_daily").fetchone()
    kf = cur.execute("SELECT COUNT(*), COUNT(DISTINCT game_date) FROM kenpom_fanmatch").fetchone()
    print(f"kenpom_daily:    {kd[0]:,} rows, {kd[1]} snapshot dates")
    print(f"kenpom_fanmatch: {kf[0]:,} rows, {kf[1]} game dates")
    print("\nNext: python scripts/04_build_features.py")
    conn.close()

if __name__ == '__main__':
    main()
