"""
03d_fetch_espn_ids.py
=====================
Fetch ESPN game IDs for all games in our DB by querying ESPN's
scoreboard API by date, then matching on team names.

Adds an espn_id column to the games table.
Once populated, 03e_pull_referees.py uses espn_id instead of cbbd_id.

ESPN scoreboard: https://site.api.espn.com/apis/site/v2/sports/basketball/
                 mens-college-basketball/scoreboard?dates=YYYYMMDD&limit=100

Cost: free, ~1 req per game date (~400 dates = ~400 requests)
Runtime: ~10 minutes

Run: python scripts/03d_fetch_espn_ids.py
     python scripts/03d_fetch_espn_ids.py --season 2025
     python scripts/03d_fetch_espn_ids.py --resume
"""

import sqlite3, os, time, argparse, warnings
from datetime import datetime, timezone
import requests

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

ESPN_SCOREBOARD = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                   "mens-college-basketball/scoreboard?dates={date}&limit=100")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
}

REQUEST_DELAY = 0.5


def setup_db(conn):
    """Add espn_id column to games table if it doesn't exist."""
    cols = [c[1] for c in conn.execute('PRAGMA table_info(games)').fetchall()]
    if 'espn_id' not in cols:
        conn.execute("ALTER TABLE games ADD COLUMN espn_id TEXT")
        conn.commit()
        print("  Added espn_id column to games table")


def normalize(name):
    """Simple normalization for fuzzy team name matching."""
    name = str(name).lower().strip()
    # Common ESPN -> our DB differences
    replacements = {
        'ole miss': 'mississippi',
        'uconn': 'connecticut',
        'miami (oh)': 'miami (oh)',
        'miami': 'miami (fl)',
        'nc state': 'n.c. state',
        'usc': 'southern california',
        'lsu': 'lsu',
        'vcu': 'vcu',
        'byu': 'byu',
        'tcu': 'tcu',
        'ucf': 'central florida',
        'iowa state': 'iowa st.',
        'kansas state': 'kansas st.',
        'ohio state': 'ohio st.',
        'michigan state': 'michigan st.',
        'penn state': 'penn st.',
        'florida state': 'florida st.',
        'arizona state': 'arizona st.',
        'utah state': 'utah st.',
        'san diego state': 'san diego st.',
        'colorado state': 'colorado st.',
        'appalachian state': 'appalachian st.',
        'wichita state': 'wichita st.',
        'kent state': 'kent st.',
        'boise state': 'boise st.',
        'oregon state': 'oregon st.',
        'washington state': 'washington st.',
        'fresno state': 'fresno st.',
    }
    for k, v in replacements.items():
        if name == k:
            return v
    # Strip "state" -> "st." if ends with state
    if name.endswith(' state'):
        name = name[:-6].strip() + ' st.'
    return name


def fetch_espn_ids_for_date(date_str):
    """
    Fetch all ESPN game IDs for a given date (YYYYMMDD format).
    Returns list of {espn_id, home_team, away_team} dicts.
    """
    url = ESPN_SCOREBOARD.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
        games = []
        for event in data.get('events', []):
            espn_id = str(event.get('id', ''))
            comps = event.get('competitions', [])
            if not comps:
                continue
            comp = comps[0]
            competitors = comp.get('competitors', [])
            if len(competitors) < 2:
                continue
            home = next((c for c in competitors if c.get('homeAway') == 'home'), None)
            away = next((c for c in competitors if c.get('homeAway') == 'away'), None)
            if home and away:
                games.append({
                    'espn_id':   espn_id,
                    'home_team': home.get('team', {}).get('displayName', ''),
                    'away_team': away.get('team', {}).get('displayName', ''),
                })
        return games
    except Exception:
        return []


def load_dates(conn, season=None):
    """Get all unique game dates from DB, optionally filtered by season."""
    if season:
        rows = conn.execute("""
            SELECT DISTINCT game_date FROM games
            WHERE season = ? AND (espn_id IS NULL OR espn_id = '')
            ORDER BY game_date
        """, (season,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT DISTINCT game_date FROM games
            WHERE espn_id IS NULL OR espn_id = ''
            ORDER BY game_date
        """).fetchall()
    return [r[0] for r in rows]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--season', type=int, default=None)
    p.add_argument('--resume', action='store_true',
                   help='Skip dates where games already have espn_id')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    print("=" * 60)
    print("NCAAB -- Fetch ESPN Game IDs by Date")
    print("=" * 60)

    conn = sqlite3.connect(DB)
    setup_db(conn)

    dates = load_dates(conn, season=args.season)
    print(f"  Dates to process: {len(dates):,}")
    print(f"  Est. time: ~{len(dates)*REQUEST_DELAY/60:.1f} min")
    print()

    total_matched = 0
    total_unmatched = 0

    for i, game_date in enumerate(dates):
        # Convert YYYY-MM-DD to YYYYMMDD for ESPN
        date_espn = game_date.replace('-', '')
        espn_games = fetch_espn_ids_for_date(date_espn)
        time.sleep(REQUEST_DELAY)

        # Load our games for this date
        our_games = conn.execute("""
            SELECT id, home_team, away_team FROM games
            WHERE game_date = ? AND (espn_id IS NULL OR espn_id = '')
        """, (game_date,)).fetchall()

        matched = 0
        for our_id, our_home, our_away in our_games:
            our_h = normalize(our_home)
            our_a = normalize(our_away)

            best_match = None
            for eg in espn_games:
                esp_h = normalize(eg['home_team'])
                esp_a = normalize(eg['away_team'])
                # Exact match
                if esp_h == our_h and esp_a == our_a:
                    best_match = eg['espn_id']
                    break
                # Partial match — home team contains our name or vice versa
                if (our_h in esp_h or esp_h in our_h) and \
                   (our_a in esp_a or esp_a in our_a):
                    best_match = eg['espn_id']
                    break

            if best_match:
                conn.execute(
                    "UPDATE games SET espn_id = ? WHERE id = ?",
                    (best_match, our_id)
                )
                matched += 1
            else:
                total_unmatched += 1

        conn.commit()
        total_matched += matched

        if (i + 1) % 20 == 0 or i == 0:
            print(f"  [{i+1}/{len(dates)}] {game_date} | "
                  f"matched={matched}/{len(our_games)} | "
                  f"total={total_matched} | unmatched={total_unmatched}")

    conn.close()
    print()
    print("=" * 60)
    print(f"Done. ESPN IDs matched: {total_matched:,} | unmatched: {total_unmatched:,}")
    print()
    print("Next: python scripts/03e_pull_referees.py --season 2025")
