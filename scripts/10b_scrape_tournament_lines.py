"""
10b_scrape_tournament_lines.py
==============================
Scrape DraftKings opening + closing lines for all conference tournament
and NCAA tournament games from OddsAPI historical endpoint.

Stores into game_lines table (spread, over_under, moneyline, home_covered, went_over).
This lets us measure how well DK prices tournament games and compute
the true residual model error for tournament pace adjustments.

Cost: ~30 credits per date × ~60 tournament dates = ~1,800 credits
Runtime: ~5 minutes

Run: python scripts/10b_scrape_tournament_lines.py
     python scripts/10b_scrape_tournament_lines.py --dry-run
     python scripts/10b_scrape_tournament_lines.py --resume
"""

import sqlite3, os, time, argparse, warnings
from datetime import datetime, timezone
import requests

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except ImportError:
    pass

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
ODDS_KEY = os.getenv('ODDS_API_KEY', '')

HIST_URL  = "https://api.the-odds-api.com/v4/historical/sports/basketball_ncaab/odds/"
MARKETS   = 'spreads,totals,h2h'
REGION    = 'us'
BOOKMAKER = 'draftkings'
DELAY     = 0.5  # seconds between requests — 30 credits/call, be deliberate

# OddsAPI nickname stripping
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
    'Bluejays','Blue Jays','Privateers','Hokies','Penmen','Seawolves',
    'Fighting Hawks','Golden Grizzlies','Minutemen','Antelopes',
    'Gamecocks','Terps','Hoosiers','Boilermakers','Buckeyes','Hawkeyes',
}

CBBD_TO_TORVIK = {
    'Iowa State': 'Iowa St.', 'Kansas State': 'Kansas St.',
    'Ohio State': 'Ohio St.', 'Michigan State': 'Michigan St.',
    'Penn State': 'Penn St.', 'Florida State': 'Florida St.',
    'Arizona State': 'Arizona St.', 'Utah State': 'Utah St.',
    'San Diego State': 'San Diego St.', 'Colorado State': 'Colorado St.',
    'Appalachian State': 'Appalachian St.', 'Wichita State': 'Wichita St.',
    'Kent State': 'Kent St.', 'Boise State': 'Boise St.',
    'Oregon State': 'Oregon St.', 'Fresno State': 'Fresno St.',
    'Mississippi State': 'Mississippi St.', 'Missouri State': 'Missouri St.',
    'Ole Miss': 'Mississippi', 'UConn': 'Connecticut',
    'UCF': 'Central Florida', 'USC': 'Southern California',
    'NC State': 'N.C. State', 'Miami': 'Miami (FL)',
    'Pitt': 'Pittsburgh', 'VCU': 'VCU', 'BYU': 'BYU', 'TCU': 'TCU',
    'LSU': 'LSU', 'SMU': 'Southern Methodist',
    'UAB': 'Alabama-Birmingham', 'FAU': 'Florida Atlantic',
    'FIU': 'Florida International', 'FGCU': 'Florida Gulf Coast',
    'ETSU': 'East Tennessee St.', 'SFA': 'Stephen F. Austin',
    'UTEP': 'UTEP', 'UTSA': 'UT San Antonio',
    'UTRGV': 'UT Rio Grande Valley', 'UMBC': 'Maryland-Baltimore County',
    'Murray State': 'Murray St.', 'Morehead State': 'Morehead St.',
    'Montana State': 'Montana St.', 'Jacksonville State': 'Jacksonville St.',
    'Kennesaw State': 'Kennesaw St.', 'Sam Houston State': 'Sam Houston St.',
    'McNeese State': 'McNeese St.', 'Nicholls State': 'Nicholls St.',
    "Hawai'i": 'Hawaii', 'Saint Francis (PA)': 'St. Francis (PA)',
    "Saint John's": "St. John's (NY)", 'Prairie View A&M': 'Prairie View',
    'Loyola Chicago': 'Loyola Chicago', 'Loyola Maryland': 'Loyola MD',
    'Long Island University': 'LIU', 'Gardner-Webb': 'Gardner Webb',
    'Bethune-Cookman': 'Bethune Cookman', 'UT Martin': 'Tennessee-Martin',
    'Purdue Fort Wayne': 'Fort Wayne', 'Louisiana': 'Louisiana-Lafayette',
    'UL Monroe': 'Louisiana-Monroe', 'Omaha': 'Nebraska Omaha',
    'SIU Edwardsville': 'SIU-Edwardsville',
    'SE Louisiana': 'Southeastern La.',
    'Southeastern Louisiana': 'Southeastern La.',
    'Southern': 'Southern U.', 'Grambling': 'Grambling St.',
}


def norm_team(name):
    if not name:
        return name
    s = str(name).strip()
    parts = s.rsplit(' ', 1)
    if len(parts) == 2 and parts[1] in ODDS_NICKNAMES:
        s = parts[0].strip()
    parts3 = s.rsplit(' ', 2)
    if len(parts3) == 3:
        two_word = parts3[1] + ' ' + parts3[2]
        if two_word in ODDS_NICKNAMES:
            s = parts3[0].strip()
    return CBBD_TO_TORVIK.get(s, s)


def get_tournament_dates(conn, resume=False, all_dates=False):
    """Get tournament game dates to process."""
    if all_dates:
        # Get ALL tournament dates regardless of existing lines
        rows = conn.execute("""
            SELECT DISTINCT game_date FROM games
            WHERE tournament IN ('conf_tournament', 'ncaa_tournament')
              AND season >= 2021
            ORDER BY game_date
        """).fetchall()
    elif resume:
        # Only dates where some tournament games still lack lines
        rows = conn.execute("""
            SELECT DISTINCT g.game_date
            FROM games g
            LEFT JOIN game_lines gl ON g.game_date=gl.game_date
                AND g.home_team=gl.home_team AND g.away_team=gl.away_team
            WHERE g.tournament IN ('conf_tournament', 'ncaa_tournament')
              AND g.season >= 2021
              AND gl.over_under IS NULL
            ORDER BY g.game_date
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT DISTINCT game_date FROM games
            WHERE tournament IN ('conf_tournament', 'ncaa_tournament')
              AND season >= 2021
            ORDER BY game_date
        """).fetchall()
    return [r[0] for r in rows]


def fetch_snapshot(date_str_db, hours_before_midnight=1):
    """
    Fetch OddsAPI historical snapshot for a date.
    Use 23:00 UTC (before midnight) as closing line proxy.
    Cost: 30 credits per call.
    """
    # Tournament games tip in the evening ET = ~midnight UTC
    # Use 23:00 UTC as a pre-game closing line snapshot
    snap_dt = f"{date_str_db}T23:00:00Z"
    params = {
        'apiKey':     ODDS_KEY,
        'regions':    REGION,
        'markets':    MARKETS,
        'bookmakers': BOOKMAKER,
        'oddsFormat': 'american',
        'date':       snap_dt,
    }
    try:
        r = requests.get(HIST_URL, params=params, timeout=20)
        remaining = r.headers.get('X-Requests-Remaining', '?')
        if r.status_code == 200:
            data = r.json()
            games = data.get('data', data) if isinstance(data, dict) else data
            return games, remaining
        return [], remaining
    except Exception:
        return [], '?'


def parse_snapshot(games_data):
    """Parse snapshot into {(home_norm, away_norm): {spread, total, ml_home, ml_away}}."""
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


def upsert_game_line(conn, game_date, home_team, away_team,
                     home_score, away_score, season,
                     spread, total, ml_home, ml_away):
    """Insert or update a game_lines row for a tournament game."""
    if spread is None and total is None:
        return False

    home_margin  = home_score - away_score if home_score and away_score else None
    home_covered = None
    went_over    = None
    if spread is not None and home_margin is not None:
        home_covered = 1 if (home_margin + spread) > 0 else 0
    if total is not None and home_score is not None and away_score is not None:
        actual = home_score + away_score
        went_over = 1 if actual > total else 0

    conn.execute("""
        INSERT OR REPLACE INTO game_lines
        (season, game_date, home_team, away_team,
         home_score, away_score,
         spread, over_under, home_moneyline, away_moneyline,
         home_margin, home_covered, went_over,
         provider)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'oddsapi_historical')
    """, (season, game_date, home_team, away_team,
          home_score, away_score,
          spread, total, ml_home, ml_away,
          home_margin, home_covered, went_over))
    return True


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--resume',  action='store_true',
                   help='Only scrape dates where some games still lack lines')
    p.add_argument('--all',     action='store_true',
                   help='Re-scrape all dates (re-match with improved name matching)')
    p.add_argument('--dry-run', action='store_true',
                   help='Show what would be scraped without API calls')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    if not ODDS_KEY:
        print("ERROR: ODDS_API_KEY not set in .env")
        exit(1)

    print("=" * 60)
    print("NCAAB — Tournament Lines Scraper (OddsAPI Historical)")
    print("=" * 60)
    print(f"  Markets: spreads, totals, h2h | Bookmaker: DraftKings")
    print(f"  Cost: ~30 credits per date")

    conn = sqlite3.connect(DB)
    dates = get_tournament_dates(conn, resume=args.resume, all_dates=args.all)

    print(f"  Tournament dates to scrape: {len(dates)}")
    print(f"  Estimated credits: ~{len(dates) * 30:,}")
    print()

    if args.dry_run:
        print("DRY RUN — dates that would be scraped:")
        for d in dates[:10]:
            n = conn.execute(
                "SELECT COUNT(*) FROM games WHERE game_date=? AND tournament IN ('conf_tournament','ncaa_tournament')",
                (d,)
            ).fetchone()[0]
            print(f"  {d}: {n} tournament games")
        if len(dates) > 10:
            print(f"  ... ({len(dates)} total dates)")
        conn.close()
        exit(0)

    total_inserted = 0
    total_matched  = 0
    credits_used   = 0

    for i, game_date in enumerate(dates):
        # Fetch closing line snapshot
        snap_games, remaining = fetch_snapshot(game_date)
        credits_used += 30
        time.sleep(DELAY)

        if not snap_games:
            if (i+1) % 10 == 0 or i == 0:
                print(f"  [{i+1}/{len(dates)}] {game_date} | no data | credits={credits_used} remaining={remaining}")
            continue

        parsed = parse_snapshot(snap_games)

        # Load tournament games for this date
        our_games = conn.execute("""
            SELECT g.home_team, g.away_team, g.home_score, g.away_score,
                   g.season, g.tournament
            FROM games g
            WHERE g.game_date = ?
              AND g.tournament IN ('conf_tournament', 'ncaa_tournament')
        """, (game_date,)).fetchall()

        matched = 0
        for home_db, away_db, home_score, away_score, season, tournament in our_games:
            line = None

            # Try 1: exact match (DB name == normalized ESPN name)
            line = parsed.get((home_db, away_db))

            # Try 2: flipped orientation
            if not line:
                flipped = parsed.get((away_db, home_db))
                if flipped:
                    # Swap ml_home/ml_away since orientation is flipped
                    line = {
                        'spread': -flipped['spread'] if flipped.get('spread') is not None else None,
                        'total':  flipped.get('total'),
                        'ml_home': flipped.get('ml_away'),
                        'ml_away': flipped.get('ml_home'),
                    }

            # Try 3: partial substring match (both orientations)
            if not line:
                h_low = home_db.lower()
                a_low = away_db.lower()
                for (ph, pa), v in parsed.items():
                    ph_low, pa_low = ph.lower(), pa.lower()
                    if ((h_low in ph_low or ph_low in h_low) and
                        (a_low in pa_low or pa_low in a_low)):
                        line = v
                        break
                    # flipped
                    if ((h_low in pa_low or pa_low in h_low) and
                        (a_low in ph_low or ph_low in a_low)):
                        line = {
                            'spread': -v['spread'] if v.get('spread') is not None else None,
                            'total':  v.get('total'),
                            'ml_home': v.get('ml_away'),
                            'ml_away': v.get('ml_home'),
                        }
                        break

            if line and (line.get('spread') is not None or line.get('total') is not None):
                inserted = upsert_game_line(
                    conn, game_date, home_db, away_db,
                    home_score, away_score, season,
                    line.get('spread'), line.get('total'),
                    line.get('ml_home'), line.get('ml_away')
                )
                if inserted:
                    matched += 1

        conn.commit()
        total_matched  += matched
        total_inserted += matched

        if (i+1) % 5 == 0 or i == 0:
            print(f"  [{i+1}/{len(dates)}] {game_date} | "
                  f"matched={matched}/{len(our_games)} | "
                  f"total={total_matched} | credits={credits_used} | remaining={remaining}")

    conn.close()

    print()
    print("=" * 60)
    print(f"Done. {total_inserted:,} tournament game lines inserted.")
    print(f"Credits used: ~{credits_used:,}")
    print()
    print("Next: python scripts/analyze_tournament_pace.py")
    print("      (will now show line accuracy for tournament games)")
