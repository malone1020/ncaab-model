"""
01_daily_update.py
==================
Daily data refresh — run each morning before generating bets.
Pulls only NEW data since last update. Runs in ~5-10 min.

Run: python scripts/01_daily_update.py
     python scripts/01_daily_update.py --dry-run
     python scripts/01_daily_update.py --since 2026-03-10  (specific lookback)
"""

import sqlite3, os, sys, time, argparse, requests, gzip, json as _json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except ImportError:
    pass

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
SCRIPTS  = os.path.join(ROOT, 'scripts')
PYTHON   = sys.executable
CBBD_KEY = os.getenv('CBBD_API_KEY', '')
CBBD_BASE = 'https://api.collegebasketballdata.com'
CBBD_HDR  = {'Authorization': f'Bearer {CBBD_KEY}', 'Accept': 'application/json'}
TVD_BASE  = 'https://barttorvik.com/timemachine/team_results'
HEADERS   = {'User-Agent': 'Mozilla/5.0'}

def db():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def season_for_date(d):
    if isinstance(d, str): d = date.fromisoformat(d[:10])
    return d.year + 1 if d.month >= 11 else d.year

def run(script, *args):
    import subprocess
    result = subprocess.run([PYTHON, os.path.join(SCRIPTS, script)] + list(args), cwd=ROOT)
    return result.returncode == 0


# ── Step 1: New games from CBBD ───────────────────────────────────────────────
def pull_new_games(since, dry_run):
    print(f"\n[1/7] New games from CBBD since {since}...")
    if not CBBD_KEY:
        print("  ⚠ CBBD_API_KEY not set — skipping"); return 0

    season = season_for_date(since)
    inserted = 0
    conn = db()

    for stype in ['regular', 'postseason']:
        try:
            r = requests.get(f"{CBBD_BASE}/games/teams", headers=CBBD_HDR,
                           params={'season': season, 'seasonType': stype}, timeout=30)
            if r.status_code != 200: continue
            games = [g for g in r.json() if (g.get('date','') or '')[:10] >= since]
            print(f"  {stype}: {len(games)} games since {since}")
            if dry_run: continue

            for g in games:
                gd = (g.get('date','') or '')[:10]
                if not gd: continue
                conn.execute("""
                    INSERT OR IGNORE INTO games
                    (season, game_date, home_team, away_team,
                     home_score, away_score, neutral_site, conf_game, season_type)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (season_for_date(gd), gd,
                      g.get('homeTeam',''), g.get('awayTeam',''),
                      g.get('homePoints'), g.get('awayPoints'),
                      int(bool(g.get('neutralSite'))), int(bool(g.get('conferenceGame'))),
                      stype))
                inserted += 1
            conn.commit()
        except Exception as e:
            print(f"  Error {stype}: {e}")

    conn.close()
    print(f"  Inserted {inserted} game records")
    return inserted


# ── Step 2: Torvik daily snapshot for today ───────────────────────────────────
def pull_torvik_today(dry_run):
    print(f"\n[2/7] Torvik daily snapshot...")
    today = date.today()
    season = season_for_date(today)
    snap = today.strftime('%Y%m%d')

    conn = db()
    exists = conn.execute(
        "SELECT COUNT(*) FROM torvik_daily WHERE snapshot_date=?", (snap,)
    ).fetchone()[0]

    if exists > 0:
        print(f"  Already have snapshot for {snap} ({exists} rows)")
        conn.close()
        return

    if dry_run:
        print(f"  DRY RUN — would fetch {snap}")
        conn.close()
        return

    url = f"{TVD_BASE}/{snap}_team_results.json.gz"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code != 200:
            print(f"  Torvik {snap}: HTTP {r.status_code}")
            conn.close()
            return

        try:
            data = gzip.decompress(r.content)
            rows = _json.loads(data)
        except Exception:
            rows = r.json()

        DAILY_IDX = {'adj_o':5,'adj_d':6,'barthag':8,'adj_t':27,
                     'efg_o':9,'efg_d':10,'tov_o':11,'tov_d':12,
                     'orb':13,'drb':14,'ftr_o':15,'ftr_d':16}

        def fv(v):
            try: return float(str(v).strip().replace('%','').replace(',',''))
            except: return None

        batch = []
        for td in rows:
            if not isinstance(td, (list,tuple)) or len(td) < 10: continue
            team = str(td[1]).strip()
            if not team or team == 'nan': continue
            adj_o = fv(td[DAILY_IDX['adj_o']])
            adj_d = fv(td[DAILY_IDX['adj_d']])
            batch.append((season, snap, team, adj_o, adj_d,
                         fv(td[DAILY_IDX['adj_t']]) if len(td) > DAILY_IDX['adj_t'] else None,
                         fv(td[DAILY_IDX['barthag']]),
                         (adj_o - adj_d) if adj_o and adj_d else None,
                         fv(td[DAILY_IDX['efg_o']]), fv(td[DAILY_IDX['efg_d']]),
                         fv(td[DAILY_IDX['tov_o']]), fv(td[DAILY_IDX['tov_d']]),
                         fv(td[DAILY_IDX['orb']]), fv(td[DAILY_IDX['drb']]),
                         fv(td[DAILY_IDX['ftr_o']]), fv(td[DAILY_IDX['ftr_d']])))

        conn.executemany("""
            INSERT OR IGNORE INTO torvik_daily
            (season, snapshot_date, team, adj_o, adj_d, adj_t, barthag, adj_em,
             efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)
        conn.commit()
        print(f"  Inserted {len(batch)} Torvik rows for {snap}")
    except Exception as e:
        print(f"  Torvik error: {e}")
    conn.close()


# ── Step 3: KenPom archive for new game dates ─────────────────────────────────
def pull_kenpom_new(since, dry_run):
    print(f"\n[3/7] KenPom snapshots for new game dates...")
    if dry_run:
        print("  DRY RUN — would run 03d_pull_kenpom_archive.py")
        return
    run('03d_pull_kenpom_archive.py')


# ── Step 4: Refs for recent games ─────────────────────────────────────────────
def pull_refs_recent(dry_run):
    print(f"\n[4/7] Referee assignments (today's games)...")
    if dry_run:
        print("  DRY RUN — would scrape today's refs via ESPN")
        return
    # Refs for current season with resume (only new games)
    season = season_for_date(date.today())
    run('03e_pull_referees.py', '--season', str(season), '--resume')


# ── Step 5: Recompute recency + trends ───────────────────────────────────────
def recompute_recency(dry_run):
    print(f"\n[5/7] Recomputing recency features + trend slopes...")
    if dry_run:
        print("  DRY RUN — would run 03g + 03h")
        return
    run('03g_recency_features.py')
    run('03h_compute_trends.py')


# ── Step 6: Rebuild features ──────────────────────────────────────────────────
def rebuild_features(dry_run):
    print(f"\n[6/7] Rebuilding game_features_v2...")
    if dry_run:
        print("  DRY RUN — would run 04_build_features.py")
        return
    run('04_build_features.py')


# ── Step 7: Freshness audit ───────────────────────────────────────────────────
def audit(dry_run):
    print(f"\n[7/7] Data freshness check...")
    conn = db()
    checks = [
        ("games",         "SELECT MAX(game_date) FROM games"),
        ("torvik_daily",  "SELECT MAX(snapshot_date) FROM torvik_daily WHERE season=2026"),
        ("kenpom_daily",  "SELECT MAX(snapshot_date) FROM kenpom_daily WHERE season=2026"),
        ("game_features", "SELECT MAX(game_date) FROM game_features_v2"),
        ("game_lines",    "SELECT MAX(game_date) FROM game_lines WHERE season=2026"),
    ]
    today = str(date.today())
    print(f"  {'Source':<18} {'Latest':>12}  Status")
    print(f"  {'─'*42}")
    for name, sql in checks:
        try:
            val = conn.execute(sql).fetchone()[0] or 'N/A'
            if val and len(str(val)) == 8:
                val = f"{val[:4]}-{val[4:6]}-{val[6:]}"
            if val == 'N/A':
                print(f"  {name:<18} {'N/A':>12}  ⚠ empty")
            else:
                days = (date.today() - date.fromisoformat(str(val)[:10])).days
                status = '✓ current' if days <= 1 else f'⚠ {days}d old'
                print(f"  {name:<18} {str(val)[:12]:>12}  {status}")
        except Exception as e:
            print(f"  {name:<18} ERROR: {e}")
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--since',        type=str, default=None,
                        help='Pull data since date (default: yesterday)')
    parser.add_argument('--dry-run',      action='store_true')
    parser.add_argument('--skip-features', action='store_true',
                        help='Skip feature rebuild (faster, use for data-only refresh)')
    parser.add_argument('--skip-refs',    action='store_true',
                        help='Skip ref scraping')
    args = parser.parse_args()

    since = args.since or str(date.today() - timedelta(days=1))

    print("=" * 55)
    print(f"NCAAB Daily Data Refresh — {date.today()}")
    print(f"Since: {since}" + (" [DRY RUN]" if args.dry_run else ""))
    print("=" * 55)

    t0 = time.time()
    pull_new_games(since, args.dry_run)
    pull_torvik_today(args.dry_run)
    pull_kenpom_new(since, args.dry_run)
    if not args.skip_refs:
        pull_refs_recent(args.dry_run)
    recompute_recency(args.dry_run)
    if not args.skip_features:
        rebuild_features(args.dry_run)
    audit(args.dry_run)

    print(f"\n{'='*55}")
    print(f"Done in {(time.time()-t0)/60:.1f} min")
    print(f"Next: python scripts/07_daily_bets.py")
    print(f"{'='*55}")
