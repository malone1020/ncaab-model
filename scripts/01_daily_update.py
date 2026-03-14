"""
01_daily_update.py
==================
Daily data refresh — run this each morning before generating bets.
Pulls all new data since the last update and keeps the DB current.

Run: python scripts/01_daily_update.py
     python scripts/01_daily_update.py --date 2026-03-13  (specific date)
     python scripts/01_daily_update.py --dry-run          (show what would run)

Steps:
  1. Pull new games from CBBD (yesterday's results into games table)
  2. Pull latest Torvik daily snapshot
  3. Pull KenPom archive snapshot for recent game dates
  4. Pull KenPom fanmatch for recent game dates
  5. Scrape referee assignments for recent games
  6. Recompute recency features for new games
  7. Recompute trend slopes
  8. Rebuild game_features_v2 for new/updated games

Total runtime: ~5-10 min
"""

import sqlite3, os, sys, time, argparse, requests
from datetime import date, datetime, timedelta, timezone

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except ImportError:
    pass

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
SCRIPTS = os.path.join(ROOT, 'scripts')
PYTHON  = sys.executable

CBBD_KEY = os.getenv('CBBD_API_KEY', '')
CBBD_BASE = 'https://api.collegebasketballdata.com'
HEADERS = {'Authorization': f'Bearer {CBBD_KEY}', 'Accept': 'application/json'}


def db():
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_last_game_date():
    """Find the most recent game date in the games table."""
    conn = db()
    r = conn.execute("SELECT MAX(game_date) FROM games").fetchone()[0]
    conn.close()
    return r


def season_for_date(d):
    """Return season year for a given date."""
    if isinstance(d, str):
        d = date.fromisoformat(d)
    return d.year + 1 if d.month >= 11 else d.year


def step1_pull_new_games(since_date, dry_run=False):
    """Pull new game results from CBBD since last update."""
    print(f"\n[1/8] Pulling new games from CBBD since {since_date}...")
    if not CBBD_KEY:
        print("  ⚠ CBBD_API_KEY not set — skipping")
        return 0

    conn = db()
    inserted = 0

    # Pull games for the current season
    season = season_for_date(since_date)
    try:
        r = requests.get(f"{CBBD_BASE}/games/teams",
                        headers=HEADERS,
                        params={'season': season, 'seasonType': 'regular'},
                        timeout=30)
        if r.status_code != 200:
            print(f"  API error: {r.status_code}")
            conn.close()
            return 0

        games = r.json()
        new_games = [g for g in games
                     if g.get('date', '') >= since_date]

        print(f"  Found {len(new_games)} new games since {since_date}")

        if dry_run:
            conn.close()
            return len(new_games)

        for g in new_games:
            game_date = g.get('date', '')[:10]
            home = g.get('homeTeam', '')
            away = g.get('awayTeam', '')
            home_score = g.get('homePoints')
            away_score = g.get('awayPoints')
            neutral = int(bool(g.get('neutralSite', False)))
            conf = int(bool(g.get('conferenceGame', False)))
            s = season_for_date(game_date)

            conn.execute("""
                INSERT OR IGNORE INTO games
                (season, game_date, home_team, away_team,
                 home_score, away_score, neutral_site, conf_game, season_type)
                VALUES (?,?,?,?,?,?,?,?,'regular')
            """, (s, game_date, home, away, home_score, away_score, neutral, conf))
            inserted += 1

        conn.commit()
        print(f"  Inserted {inserted} new game records")

    except Exception as e:
        print(f"  Error: {e}")

    conn.close()
    return inserted


def step2_pull_torvik_snapshot(dry_run=False):
    """Pull latest Torvik daily snapshot."""
    print(f"\n[2/8] Pulling latest Torvik daily snapshot...")

    import subprocess
    # Run the torvik daily section only for current season
    if dry_run:
        print("  DRY RUN — would fetch today's Torvik snapshot")
        return

    # Use the existing pull script but only for 2026
    result = subprocess.run(
        [PYTHON, os.path.join(SCRIPTS, '03_pull_all_sources.py')],
        cwd=ROOT, capture_output=True, text=True
    )
    # Count new rows from output
    for line in result.stdout.split('\n'):
        if 'torvik_daily' in line and '2026' in line:
            print(f"  {line.strip()}")


def step3_pull_kenpom(since_date, dry_run=False):
    """Pull KenPom archive snapshots for recent game dates."""
    print(f"\n[3/8] Pulling KenPom snapshots since {since_date}...")
    if dry_run:
        print("  DRY RUN — would fetch KenPom snapshots")
        return

    import subprocess
    result = subprocess.run(
        [PYTHON, os.path.join(SCRIPTS, '03d_pull_kenpom_archive.py')],
        cwd=ROOT
    )


def step4_pull_refs(since_date, dry_run=False):
    """Pull referee data for recent games."""
    print(f"\n[4/8] Pulling referee assignments since {since_date}...")
    if dry_run:
        print("  DRY RUN — would scrape refs for recent games")
        return

    season = season_for_date(since_date)
    import subprocess
    result = subprocess.run(
        [PYTHON, os.path.join(SCRIPTS, '03e_pull_referees.py'),
         '--season', str(season), '--resume'],
        cwd=ROOT
    )


def step5_recency(dry_run=False):
    """Recompute recency features."""
    print(f"\n[5/8] Recomputing recency features...")
    if dry_run:
        print("  DRY RUN — would recompute recency_eff")
        return

    import subprocess
    subprocess.run([PYTHON, os.path.join(SCRIPTS, '03g_recency_features.py')], cwd=ROOT)


def step6_trends(dry_run=False):
    """Recompute trend slopes."""
    print(f"\n[6/8] Recomputing trend slopes...")
    if dry_run:
        print("  DRY RUN — would recompute trends")
        return

    import subprocess
    subprocess.run([PYTHON, os.path.join(SCRIPTS, '03h_compute_trends.py')], cwd=ROOT)


def step7_features(dry_run=False):
    """Rebuild game_features_v2."""
    print(f"\n[7/8] Rebuilding game_features_v2...")
    if dry_run:
        print("  DRY RUN — would rebuild features")
        return

    import subprocess
    subprocess.run([PYTHON, os.path.join(SCRIPTS, '04_build_features.py')], cwd=ROOT)


def step8_audit(dry_run=False):
    """Quick audit of data freshness."""
    print(f"\n[8/8] Data freshness audit...")
    conn = db()

    checks = [
        ("games",           "SELECT MAX(game_date) FROM games"),
        ("game_lines",      "SELECT MAX(game_date) FROM game_lines WHERE spread IS NOT NULL"),
        ("torvik_daily",    "SELECT MAX(snapshot_date) FROM torvik_daily WHERE season=2026"),
        ("kenpom_daily",    "SELECT MAX(snapshot_date) FROM kenpom_daily WHERE season=2026"),
        ("recency_eff",     "SELECT MAX(game_date) FROM recency_eff"),
        ("game_features",   "SELECT MAX(game_date) FROM game_features_v2"),
    ]

    today = str(date.today())
    print(f"  {'Source':<20} {'Latest Date':<15} {'Status'}")
    print(f"  {'─'*50}")
    for name, sql in checks:
        try:
            val = conn.execute(sql).fetchone()[0] or 'N/A'
            # Normalize torvik date format
            if val and len(str(val)) == 8:
                val = f"{val[:4]}-{val[4:6]}-{val[6:]}"
            days_old = (date.today() - date.fromisoformat(str(val)[:10])).days if val != 'N/A' else 999
            status = '✓' if days_old <= 2 else f'⚠ {days_old}d old'
            print(f"  {name:<20} {str(val)[:15]:<15} {status}")
        except Exception as e:
            print(f"  {name:<20} ERROR: {e}")

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',    type=str, default=None,
                        help='Pull data since this date (default: yesterday)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would run without making changes')
    parser.add_argument('--skip-features', action='store_true',
                        help='Skip feature rebuild (faster, use if only checking data)')
    args = parser.parse_args()

    since = args.date or str(date.today() - timedelta(days=1))
    dry_run = args.dry_run

    print("=" * 60)
    print(f"NCAAB Daily Data Refresh — {date.today()}")
    print(f"Pulling data since: {since}")
    if dry_run:
        print("DRY RUN MODE — no changes will be made")
    print("=" * 60)

    start = time.time()

    step1_pull_new_games(since, dry_run)
    step2_pull_torvik_snapshot(dry_run)
    step3_pull_kenpom(since, dry_run)
    step4_pull_refs(since, dry_run)
    step5_recency(dry_run)
    step6_trends(dry_run)
    if not args.skip_features:
        step7_features(dry_run)
    step8_audit(dry_run)

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"Daily refresh complete in {elapsed/60:.1f} min")
    print(f"Next: python scripts/07_daily_bets.py")
    print(f"{'='*60}")
