"""
10_scrape_historical_lines.py
==============================
Scrape historical opening + closing lines from OddsAPI for all NCAAB games
in our database. Stores spread_open, spread_close, total_open, total_close,
ml_home_open, ml_home_close, ml_away_open, ml_away_close, and derived
movement columns into a new `line_movement` table.

Cost: 30 credits per date (3 markets × 1 region × 10 per market)
      ~900 dates (6 seasons) × 30 = ~27,000 credits for opening snapshots
      Double for opening + closing  = ~54,000 credits total

Run: python scripts/10_scrape_historical_lines.py
     python scripts/10_scrape_historical_lines.py --season 2025
     python scripts/10_scrape_historical_lines.py --resume   (skip already-scraped dates)
"""

import sqlite3, os, json, time, argparse, warnings
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(_env_path)
except ImportError:
    pass

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
ODDS_KEY = os.getenv('ODDS_API_KEY', '')

# OddsAPI historical endpoint
HIST_URL = "https://api.the-odds-api.com/v4/historical/sports/basketball_ncaab/odds/"

# Cost: 10 credits per region per market
# We request 3 markets (spreads, totals, h2h) × 1 region = 30 credits per call
MARKETS  = 'spreads,totals,h2h'
REGION   = 'us'
BOOKMAKER = 'draftkings'

# How many hours before tipoff to capture "opening" line
# Games typically post lines 48-72hrs out; 24hrs before is a clean opening snapshot
OPEN_HOURS_BEFORE  = 24
# How many hours before tipoff to capture "closing" line
CLOSE_HOURS_BEFORE = 1

# Rate limiting — be conservative to avoid hammering the API
REQUEST_DELAY = 0.5   # seconds between requests

# OddsAPI nickname stripping (same as 07_daily_bets.py)
ODDS_NICKNAMES = {
    'Volunteers','Commodores','Boilermakers','Cyclones','Cavaliers',
    'Golden Flashes','Golden Hurricane','Mean Green','Blue Devils','Tigers',
    'Tar Heels','Jayhawks','Cougars','Rebels','Crimson Tide','Razorbacks',
    'Sooners','Longhorns','Aggies','Bulldogs','Huskies','Hoyas','Red Storm',
    'Pirates','Spartans','Bruins','Wolverines','Badgers','Cornhuskers',
    'Hawkeyes','Illini','Gophers','Wildcats','Nittany Lions','Terrapins',
    'Scarlet Knights','Bears','Horned Frogs','Red Raiders','Cowboys',
    'Mountaineers','Bearcats','Knights','Panthers','Seminoles','Hurricanes',
    'Yellow Jackets','Demon Deacons','Orange','Eagles','Lions','Owls','Rams',
    'Spiders','Flyers','Billikens','Dukes','Colonials','Retrievers',
    'Catamounts','Crimson','Quakers','Big Red','Aztecs','Lobos','Wolf Pack',
    'Falcons','Thunderbirds','Utes','Buffaloes','Sun Devils','Ducks',
    'Beavers','Trojans','Golden Bears','Cardinal','Gaels','Zags','Pilots',
    'Waves','Flames','Anteaters','Matadors','Titans','Lancers','Gauchos',
    'Tritons','Highlanders','Jaguars','Bison','Golden Eagles','Red Foxes',
    'Friars','Musketeers','Hilltoppers','Monarchs','Rattlers',
    'Rainbow Warriors','Broncos','Lumberjacks','Warhawks','Penguins','Zips',
    'Rockets','Chippewas','Cardinals','Redhawks','Ospreys','Hatters',
    'Mavericks','Roadrunners','Miners','Racers','Govs','Grizzlies',
    'Kangaroos','Jackrabbits','Coyotes','Lopes','Chargers','Ramblers',
    'Braves','Sycamores','Leathernecks','Redbirds','Salukis','Shockers',
    'Bluejays','Blue Jays','Privateers','Hokies','Penmen',
    'Seawolves','Fighting Hawks','Golden Grizzlies','Rebels','Minutemen',
    'Antelopes','Thunderwolves','Mean Green','Leathernecks',
}


# Full CBBD_TO_TORVIK mapping — same as 04_build_features.py
# Ensures OddsAPI names ("Iowa State") resolve to DB canonical ("Iowa St.")
CBBD_TO_TORVIK = {
    'A&M-Corpus Christi': 'Texas A&M-Corpus Christi', 'ALR': 'Little Rock',
    'AR-Pine Bluff': 'Arkansas Pine Bluff', 'ASU': 'Arizona St.',
    'Abil Chr': 'Abilene Christian', 'Abil Christian': 'Abilene Christian',
    'Abilene Christian University': 'Abilene Christian',
    'Alabama Birmingham': 'Alabama-Birmingham', 'Alabama St': 'Alabama St.',
    'Alabama State': 'Alabama St.', 'American Univ': 'American',
    'American University': 'American', 'App St': 'Appalachian St.',
    'App State': 'Appalachian St.', 'Appalachian St': 'Appalachian St.',
    'Appalachian State': 'Appalachian St.', 'Arizona St': 'Arizona St.',
    'Arizona State': 'Arizona St.', 'Ark.-Pine Bluff': 'Arkansas Pine Bluff',
    'Arkansas Little Rock': 'Little Rock', 'Arkansas State': 'Arkansas St.',
    'Arkansas-Pine Bluff': 'Arkansas Pine Bluff', 'Ball State': 'Ball St.',
    'Bethune-Cookman': 'Bethune Cookman', 'Boise St': 'Boise St.',
    'Boise State': 'Boise St.', 'BYU': 'BYU',
    'C Michigan': 'Central Michigan', 'Cal State Bakersfield': 'Cal St. Bakersfield',
    'Cal State Fullerton': 'Cal St. Fullerton', 'Cal State Northridge': 'Cal St. Northridge',
    'CalBaptist': 'Cal Baptist', 'California Baptist': 'Cal Baptist',
    'Central Conn.': 'Central Connecticut', 'Central Mich': 'Central Michigan',
    'Central Mich.': 'Central Michigan', 'Chicago St': 'Chicago St.',
    'Chicago State': 'Chicago St.', 'Cleveland St': 'Cleveland St.',
    'Cleveland State': 'Cleveland St.', 'Coast Carolina': 'Coastal Carolina',
    'Colorado State': 'Colorado St.', 'Coppin St': 'Coppin St.',
    'Coppin State': 'Coppin St.', 'Delaware St': 'Delaware St.',
    'Delaware State': 'Delaware St.', 'E Michigan': 'Eastern Michigan',
    'East Tennessee State': 'ETSU', 'East Texas A&M': 'Texas A&M-Commerce',
    'ETSU': 'East Tennessee St.', 'FAU': 'Florida Atlantic',
    'FGCU': 'Florida Gulf Coast', 'FIU': 'Florida International',
    'Florida International': 'FIU', 'Florida State': 'Florida St.',
    'Fresno St': 'Fresno St.', 'Fresno State': 'Fresno St.',
    'Gardner-Webb': 'Gardner Webb', 'Georgia State': 'Georgia St.',
    'Grambling': 'Grambling St.', "Hawai'i": 'Hawaii',
    'Houston Chr': 'Houston Christian', 'Idaho St': 'Idaho St.',
    'Idaho State': 'Idaho St.', 'Illinois St': 'Illinois St.',
    'Illinois State': 'Illinois St.', 'Indiana St': 'Indiana St.',
    'Indiana State': 'Indiana St.', 'Iowa State': 'Iowa St.',
    'IU Indy': 'IU Indianapolis', 'IUPUI': 'IU Indianapolis',
    'Jackson State': 'Jackson St.', 'Jacksonville': 'Jacksonville St.',
    'Jacksonville St': 'Jacksonville St.', 'Jacksonville State': 'Jacksonville St.',
    'Kansas State': 'Kansas St.', 'Kennesaw State': 'Kennesaw St.',
    'Kent': 'Kent St.', 'Kent St': 'Kent St.', 'Kent State': 'Kent St.',
    'LA Tech': 'Louisiana Tech', 'LIU Brooklyn': 'LIU',
    'LMU (CA)': 'Loyola Marymount', 'Long Beach State': 'Long Beach St.',
    'Long Island': 'LIU', 'Long Island University': 'LIU',
    'Louisiana': 'Louisiana-Lafayette', 'Louisiana-Monroe': 'Louisiana Monroe',
    'Loyola (IL)': 'Loyola Chicago', 'Loyola (MD)': 'Loyola MD',
    'Loyola Chicago': 'Loyola Chicago', 'Loyola Maryland': 'Loyola MD',
    'LSU': 'LSU', 'Maryland Eastern Shore': 'Maryland-Eastern Shore',
    'McNeese': 'McNeese St.', 'McNeese St': 'McNeese St.',
    'McNeese State': 'McNeese St.', 'Miami': 'Miami (FL)',
    'Miami (OH)': 'Miami (OH)', 'Miami FL': 'Miami (FL)',
    'Miami Ohio': 'Miami (OH)', 'Miami-FL': 'Miami (FL)',
    'Michigan St': 'Michigan St.', 'Michigan State': 'Michigan St.',
    'Mississippi': 'Mississippi', 'Mississippi Rebels': 'Mississippi',
    'Mississippi State': 'Mississippi St.', 'Missouri State': 'Missouri St.',
    'Montana State': 'Montana St.', 'Morehead State': 'Morehead St.',
    'Morgan St': 'Morgan St.', 'Morgan State': 'Morgan St.',
    'Mount St. Mary\'s': "Mount St. Mary's", 'Murray State': 'Murray St.',
    'N Dakota St': 'North Dakota St.', 'N.C. State': 'N.C. State',
    'N. Illinois': 'Northern Illinois', 'NC State': 'N.C. State',
    'New Mexico State': 'New Mexico St.', 'Nicholls': 'Nicholls St.',
    'Nicholls St': 'Nicholls St.', 'Nicholls State': 'Nicholls St.',
    'NIU': 'Northern Illinois', 'Norfolk St': 'Norfolk St.',
    'Norfolk State': 'Norfolk St.', 'North Dakota State': 'North Dakota St.',
    'Northwestern St': 'Northwestern St.', 'Northwestern State': 'Northwestern St.',
    'Ohio State': 'Ohio St.', 'Oklahoma State': 'Oklahoma St.',
    'Ole Miss': 'Mississippi', 'Omaha': 'Nebraska Omaha',
    'Oregon State': 'Oregon St.', 'Penn State': 'Penn St.',
    'Pennsylvania': 'Penn', 'Pitt': 'Pittsburgh',
    'Portland St': 'Portland St.', 'Portland State': 'Portland St.',
    'Prairie View A&M': 'Prairie View', 'PV A&M': 'Prairie View',
    "Saint John's": "St. John's (NY)", "Saint John's (NY)": "St. John's (NY)",
    'Saint Francis': 'St. Francis (PA)', 'Saint Francis (BKN)': 'St. Francis Brooklyn',
    'Saint Francis (PA)': 'St. Francis (PA)', 'Sam Houston': 'Sam Houston St.',
    'Sam Houston State': 'Sam Houston St.', 'San Diego State': 'San Diego St.',
    'S Carolina St': 'South Carolina St.', 'South Carolina State': 'South Carolina St.',
    'South Carolina St': 'South Carolina St.',
    'South Dakota St': 'South Dakota St.', 'South Dakota State': 'South Dakota St.',
    'Southeast Missouri State': 'Southeast Missouri St.',
    'Southeastern Louisiana': 'Southeastern La.', 'SE Louisiana': 'Southeastern La.',
    'SMU': 'Southern Methodist', 'Southern': 'Southern U.',
    'Southern (LA)': 'Southern U.', 'Southern Miss': 'Southern Mississippi',
    'Southern Univ': 'Southern U.', 'Southern University': 'Southern U.',
    "St John's": "St. John's (NY)", "St. Francis": 'St. Francis (PA)',
    'Stephen F Austin': 'Stephen F. Austin', 'SFA': 'Stephen F. Austin',
    'SIU Edwardsville': 'SIU-Edwardsville', 'SIUE': 'SIU-Edwardsville',
    'Tarleton': 'Tarleton St.', 'Tarleton St': 'Tarleton St.',
    'Tarleton State': 'Tarleton St.', 'TCU': 'TCU',
    'Tennessee Martin': 'Tennessee-Martin', 'Tennessee State': 'Tennessee St.',
    'Tex. A&M-Commerce': 'Texas A&M-Commerce',
    'Texas A&M Corpus Christi': 'Texas A&M-Corpus Christi',
    'Texas A&M-CC': 'Texas A&M-Corpus Christi', 'Texas Christian': 'TCU',
    'Texas So': 'Texas Southern', 'UAB': 'Alabama-Birmingham',
    'UCF': 'Central Florida', 'UConn': 'Connecticut',
    'UIW': 'Incarnate Word', 'UL Lafayette': 'Louisiana-Lafayette',
    'UL Monroe': 'Louisiana-Monroe', 'UMBC': 'Maryland-Baltimore County',
    'UMKC': 'Kansas City', 'UNCG': 'UNC Greensboro',
    'UNCW': 'UNC Wilmington', 'URI': 'Rhode Island',
    'USC': 'Southern California', 'UT Martin': 'Tennessee-Martin',
    'UTEP': 'UTEP', 'UTRGV': 'UT Rio Grande Valley', 'UTSA': 'UT San Antonio',
    'Utah St': 'Utah St.', 'Utah State': 'Utah St.',
    'VCU': 'VCU', 'VMI': 'VMI', 'W Carolina': 'Western Carolina',
    'W Michigan': 'Western Michigan', 'Weber State': 'Weber St.',
    'Wichita St': 'Wichita St.', 'Wichita State': 'Wichita St.',
    'Winston-Salem State': 'Winston-Salem St.', 'Youngstown State': 'Youngstown St.',
    'Nebraska-Omaha': 'Nebraska Omaha', 'Queens NC': 'Queens',
    'Queens University': 'Queens', 'Purdue Fort Wayne': 'Fort Wayne',
    'Mississippi Valley State': 'Mississippi Valley State',
    'Gardner Webb': 'Gardner Webb',
}


def norm_team(name):
    """
    Normalize an OddsAPI team name to the canonical DB name.
    Step 1: strip trailing nickname ("Iowa State Cyclones" -> "Iowa State")
    Step 2: run through CBBD_TO_TORVIK ("Iowa State" -> "Iowa St.")
    """
    if not name:
        return name
    s = str(name).strip()

    # Strip one-word nickname
    parts = s.rsplit(' ', 1)
    if len(parts) == 2 and parts[1] in ODDS_NICKNAMES:
        s = parts[0].strip()

    # Strip two-word nickname
    parts3 = s.rsplit(' ', 2)
    if len(parts3) == 3:
        two_word = parts3[1] + ' ' + parts3[2]
        if two_word in ODDS_NICKNAMES:
            s = parts3[0].strip()

    # Map to canonical DB name
    return CBBD_TO_TORVIK.get(s, s)


def setup_db(conn):
    """Create line_movement table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS line_movement (
            game_date       TEXT,
            home_team       TEXT,
            away_team       TEXT,

            -- Opening line (24hrs before tipoff)
            spread_open     REAL,
            total_open      REAL,
            ml_home_open    INTEGER,
            ml_away_open    INTEGER,
            open_timestamp  TEXT,

            -- Closing line (1hr before tipoff)
            spread_close    REAL,
            total_close     REAL,
            ml_home_close   INTEGER,
            ml_away_close   INTEGER,
            close_timestamp TEXT,

            -- Derived movement features
            spread_move     REAL,   -- close - open (positive = moved toward home)
            total_move      REAL,   -- close - open (positive = moved toward over)
            ml_home_move    INTEGER,

            scraped_at      TEXT,
            PRIMARY KEY (game_date, home_team, away_team)
        )
    """)
    conn.commit()


def get_already_scraped(conn):
    """
    Return set of (game_date, home_team, away_team) already attempted.
    Includes rows with NULL values — we don't want to re-attempt dates
    where OddsAPI returned nothing (saves credits).
    """
    rows = conn.execute("""
        SELECT game_date, home_team, away_team FROM line_movement
    """).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def get_already_scraped_dates(conn):
    """Return set of game_dates already attempted (to skip entire dates)."""
    rows = conn.execute("""
        SELECT DISTINCT game_date FROM line_movement
    """).fetchall()
    return {r[0] for r in rows}


def fetch_snapshot(target_dt_utc):
    """
    Fetch a single historical odds snapshot for a given UTC datetime.
    Returns list of game dicts with spread/total/ml or empty list on failure.
    Cost: 30 credits per call.
    """
    params = {
        'apiKey':     ODDS_KEY,
        'regions':    REGION,
        'markets':    MARKETS,
        'bookmakers': BOOKMAKER,
        'oddsFormat': 'american',
        'date':       target_dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    try:
        r = requests.get(HIST_URL, params=params, timeout=20)
        remaining = r.headers.get('X-Requests-Remaining', '?')
        if r.status_code == 200:
            data = r.json()
            games = data.get('data', data) if isinstance(data, dict) else data
            return games, remaining
        elif r.status_code == 422:
            # Date out of range or no data for that timestamp
            return [], remaining
        else:
            print(f"    HTTP {r.status_code}: {r.text[:200]}")
            return [], remaining
    except Exception as e:
        print(f"    Request error: {e}")
        return [], '?'


def parse_snapshot(games_data):
    """
    Parse a snapshot response into a dict keyed by (home_norm, away_norm).
    Returns {(home, away): {spread, total, ml_home, ml_away}}.
    """
    result = {}
    for game in games_data:
        home_raw = game.get('home_team', '')
        away_raw = game.get('away_team', '')
        home = norm_team(home_raw)
        away = norm_team(away_raw)

        spread = ml_home = ml_away = total = None
        for book in game.get('bookmakers', []):
            if book['key'] != BOOKMAKER:
                continue
            for mkt in book.get('markets', []):
                if mkt['key'] == 'spreads':
                    for oc in mkt.get('outcomes', []):
                        if oc['name'] == home_raw:
                            try: spread = float(oc['point'])
                            except: pass
                elif mkt['key'] == 'h2h':
                    for oc in mkt.get('outcomes', []):
                        try:
                            if oc['name'] == home_raw:   ml_home = int(oc['price'])
                            elif oc['name'] == away_raw: ml_away = int(oc['price'])
                        except: pass
                elif mkt['key'] == 'totals':
                    for oc in mkt.get('outcomes', []):
                        if oc.get('name') == 'Over':
                            try: total = float(oc['point'])
                            except: pass

        if spread is not None or total is not None:
            result[(home, away)] = {
                'spread': spread, 'total': total,
                'ml_home': ml_home, 'ml_away': ml_away,
            }
    return result


def get_game_dates(conn, season=None, start_season=2021):
    """
    Get all unique game dates from the games table, optionally filtered by season.
    Returns list of (game_date_str, list of (home_team, away_team)) per date.
    """
    if season:
        rows = conn.execute("""
            SELECT game_date, home_team, away_team
            FROM games WHERE season = ?
            ORDER BY game_date
        """, (season,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT game_date, home_team, away_team
            FROM games WHERE season >= ?
            ORDER BY game_date
        """, (start_season,)).fetchall()

    # Group by date
    from collections import defaultdict
    by_date = defaultdict(list)
    for game_date, home, away in rows:
        by_date[game_date].append((home, away))

    return sorted(by_date.items())


def scrape_date(conn, game_date_str, games_on_date, already_scraped, credits_used):
    """
    Scrape opening and closing lines for all games on a given date.
    Fetches 2 snapshots: 24hrs before first tipoff (open) and 1hr before (close).
    """
    # Parse the date — assume games tip around 7pm ET = midnight UTC
    # Use a fixed tipoff proxy of 23:59 UTC on the game date
    game_dt_utc = datetime.strptime(game_date_str, '%Y-%m-%d').replace(
        hour=23, minute=59, tzinfo=timezone.utc
    )

    open_dt  = game_dt_utc - timedelta(hours=OPEN_HOURS_BEFORE)
    close_dt = game_dt_utc - timedelta(hours=CLOSE_HOURS_BEFORE)

    # Skip if all games on this date are already scraped
    pending = [(h, a) for h, a in games_on_date
               if (game_date_str, h, a) not in already_scraped]
    if not pending:
        return 0, credits_used

    # Fetch opening snapshot
    open_games, remaining = fetch_snapshot(open_dt)
    credits_used += 30
    time.sleep(REQUEST_DELAY)
    open_parsed = parse_snapshot(open_games)

    # Fetch closing snapshot
    close_games, remaining = fetch_snapshot(close_dt)
    credits_used += 30
    time.sleep(REQUEST_DELAY)
    close_parsed = parse_snapshot(close_games)

    rows_inserted = 0
    now = datetime.now(timezone.utc).isoformat()

    for home_db, away_db in pending:
        # Try to find this game in the snapshots
        # OddsAPI team names may differ slightly from our DB — try direct match first
        open_line  = open_parsed.get((home_db, away_db)) or {}
        close_line = close_parsed.get((home_db, away_db)) or {}

        # If not found, try fuzzy: check if home_db is a substring of any key
        if not open_line and not close_line:
            for (oh, oa), v in open_parsed.items():
                if home_db.lower() in oh.lower() or oh.lower() in home_db.lower():
                    if away_db.lower() in oa.lower() or oa.lower() in away_db.lower():
                        open_line = v
                        break
            for (ch, ca), v in close_parsed.items():
                if home_db.lower() in ch.lower() or ch.lower() in home_db.lower():
                    if away_db.lower() in ca.lower() or ca.lower() in away_db.lower():
                        close_line = v
                        break

        if not open_line and not close_line:
            continue  # Game not found in OddsAPI (no DK line or too old)

        spread_open  = open_line.get('spread')
        total_open   = open_line.get('total')
        ml_home_open = open_line.get('ml_home')
        ml_away_open = open_line.get('ml_away')

        spread_close  = close_line.get('spread')
        total_close   = close_line.get('total')
        ml_home_close = close_line.get('ml_home')
        ml_away_close = close_line.get('ml_away')

        # Derived movement (close - open)
        spread_move  = (spread_close  - spread_open)  if spread_close  is not None and spread_open  is not None else None
        total_move   = (total_close   - total_open)   if total_close   is not None and total_open   is not None else None
        ml_home_move = (ml_home_close - ml_home_open) if ml_home_close is not None and ml_home_open is not None else None

        conn.execute("""
            INSERT OR REPLACE INTO line_movement
            (game_date, home_team, away_team,
             spread_open, total_open, ml_home_open, ml_away_open, open_timestamp,
             spread_close, total_close, ml_home_close, ml_away_close, close_timestamp,
             spread_move, total_move, ml_home_move, scraped_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            game_date_str, home_db, away_db,
            spread_open, total_open, ml_home_open, ml_away_open, open_dt.isoformat(),
            spread_close, total_close, ml_home_close, ml_away_close, close_dt.isoformat(),
            spread_move, total_move, ml_home_move, now,
        ))
        rows_inserted += 1

    conn.commit()
    return rows_inserted, credits_used


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--season', type=int, default=None,
                   help='Scrape only this season (e.g. 2025). Default: 2021+')
    p.add_argument('--start-season', type=int, default=2021,
                   help='First season to scrape (default: 2021)')
    p.add_argument('--resume', action='store_true',
                   help='Skip dates already attempted in line_movement table')
    p.add_argument('--wipe', action='store_true',
                   help='Drop and recreate line_movement table before scraping')
    p.add_argument('--dry-run', action='store_true',
                   help='Show what would be scraped without making API calls')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    if not ODDS_KEY:
        print("ERROR: ODDS_API_KEY not set in .env")
        exit(1)

    print("=" * 60)
    print("NCAAB — Historical Line Movement Scraper")
    print("=" * 60)
    print(f"  Cost: ~30 credits per date | Bookmaker: DraftKings")
    print(f"  Markets: spreads, totals, h2h | Start season: {args.start_season}")

    conn = sqlite3.connect(DB)

    if args.wipe:
        print("  Wiping line_movement table...")
        conn.execute("DROP TABLE IF EXISTS line_movement")
        conn.commit()

    setup_db(conn)

    already_scraped_dates = get_already_scraped_dates(conn) if args.resume else set()
    if args.resume:
        print(f"  Resume mode: {len(already_scraped_dates)} dates already attempted")

    dates = get_game_dates(conn, season=args.season, start_season=args.start_season)
    # Filter out already-scraped dates in resume mode
    if args.resume:
        dates = [(d, g) for d, g in dates if d not in already_scraped_dates]

    print(f"  Dates to process: {len(dates)}")
    print(f"  Estimated credits: ~{len(dates) * 60:,}")
    print()

    if args.dry_run:
        print("DRY RUN — no API calls made.")
        for d, games in dates[:5]:
            print(f"  {d}: {len(games)} games")
        print(f"  ... ({len(dates)} total dates)")
        conn.close()
        exit(0)

    total_inserted = 0
    credits_used   = 0
    already_scraped = get_already_scraped(conn)

    for i, (game_date_str, games_on_date) in enumerate(dates):
        inserted, credits_used = scrape_date(
            conn, game_date_str, games_on_date, already_scraped, credits_used
        )
        total_inserted += inserted

        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(dates)}] {game_date_str} | "
                  f"+{inserted} rows | total={total_inserted} | "
                  f"credits used={credits_used}")

    conn.close()

    print()
    print("=" * 60)
    print(f"Done. {total_inserted:,} games scraped.")
    print(f"Total credits used: ~{credits_used:,}")
    print()
    print("Next: python scripts/05b_backtest_totals_combos.py")


if __name__ == '__main__':
    args = parse_args()

    if not ODDS_KEY:
        print("ERROR: ODDS_API_KEY not set in .env")
        exit(1)

    print("=" * 60)
    print("NCAAB — Historical Line Movement Scraper")
    print("=" * 60)
    print(f"  Cost estimate: ~30 credits per date (open + close snapshots)")
    print(f"  Bookmaker: DraftKings | Markets: spreads, totals, h2h")

    conn = sqlite3.connect(DB)
    setup_db(conn)

    already_scraped = get_already_scraped(conn) if args.resume else set()
    print(f"  Already scraped: {len(already_scraped):,} games" if args.resume else "  Mode: full scrape (use --resume to skip existing)")

    dates = get_game_dates(conn, season=args.season)
    print(f"  Dates to process: {len(dates)}")
    estimated_credits = len(dates) * 60  # 2 snapshots × 30 credits
    print(f"  Estimated credits: ~{estimated_credits:,}")
    print()

    if args.dry_run:
        print("DRY RUN — no API calls made.")
        for d, games in dates[:5]:
            print(f"  {d}: {len(games)} games")
        print(f"  ... ({len(dates)} total dates)")
        conn.close()
        exit(0)

    total_inserted = 0
    credits_used   = 0

    for i, (game_date_str, games_on_date) in enumerate(dates):
        inserted, credits_used = scrape_date(
            conn, game_date_str, games_on_date, already_scraped, credits_used
        )
        total_inserted += inserted

        if (i + 1) % 10 == 0 or i == 0:
            print(f"  [{i+1}/{len(dates)}] {game_date_str} | "
                  f"+{inserted} rows | total={total_inserted} | "
                  f"credits used={credits_used}")

    conn.close()

    print()
    print("=" * 60)
    print(f"Done. {total_inserted:,} games scraped.")
    print(f"Total credits used: ~{credits_used:,}")
    print()
    print("Next: add LINE_MOVE feature group to 05b_backtest_totals_combos.py")
    print("      and rebuild game_features_v2 with line movement features.")
