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
    'Montana State': 'Montana St.', 'Murray State': 'Murray St.',
    'Morehead State': 'Morehead St.', 'Idaho State': 'Idaho St.',
    'Indiana State': 'Indiana St.', 'Illinois State': 'Illinois St.',
    'Weber State': 'Weber St.', 'Sacramento State': 'Sacramento St.',
    'South Dakota State': 'South Dakota St.', 'North Dakota State': 'North Dakota St.',
    'Jacksonville State': 'Jacksonville St.', 'Kennesaw State': 'Kennesaw St.',
    'Sam Houston State': 'Sam Houston St.', 'McNeese State': 'McNeese St.',
    'Nicholls State': 'Nicholls St.', 'Northwestern State': 'Northwestern St.',
    'Youngstown State': 'Youngstown St.', 'Portland State': 'Portland St.',
    'Arkansas State': 'Arkansas St.', 'Georgia State': 'Georgia St.',
    'Long Beach State': 'Long Beach St.', 'New Mexico State': 'New Mexico St.',
    'Tennessee State': 'Tennessee St.', 'Jackson State': 'Jackson St.',
    'Norfolk State': 'Norfolk St.', 'Morgan State': 'Morgan St.',
    'Delaware State': 'Delaware St.', 'Coppin State': 'Coppin St.',
    'Chicago State': 'Chicago St.', 'Cleveland State': 'Cleveland St.',
    'Ball State': 'Ball St.', 'Tarleton State': 'Tarleton St.',
    'Oklahoma State': 'Oklahoma St.', 'Washington State': 'Washington St.',
    'Ole Miss': 'Mississippi', 'Mississippi': 'Mississippi',
    'UConn': 'Connecticut', 'UCF': 'Central Florida',
    'USC': 'Southern California', 'LSU': 'LSU',
    'VCU': 'VCU', 'BYU': 'BYU', 'TCU': 'TCU',
    'NC State': 'N.C. State', 'Miami': 'Miami (FL)',
    'Miami (OH)': 'Miami (OH)', 'IUPUI': 'IU Indianapolis',
    'IU Indianapolis': 'IU Indianapolis',
    'UT Arlington': 'UT Arlington',
    'Pitt': 'Pittsburgh', 'UConn': 'Connecticut',
    'ETSU': 'East Tennessee St.', 'FAU': 'Florida Atlantic',
    'FIU': 'Florida International', 'FGCU': 'Florida Gulf Coast',
    'SMU': 'Southern Methodist', 'UMBC': 'Maryland-Baltimore County',
    'UMKC': 'Kansas City', 'UNCG': 'UNC Greensboro',
    'UNCW': 'UNC Wilmington', 'UTEP': 'UTEP',
    'UTSA': 'UT San Antonio', 'UTRGV': 'UT Rio Grande Valley',
    'Loyola Chicago': 'Loyola Chicago', 'Loyola Maryland': 'Loyola MD',
    'Saint Francis (PA)': 'St. Francis (PA)',
    "Saint John's": "St. John's (NY)",
    'Prairie View A&M': 'Prairie View',
    'Alabama-Birmingham': 'Alabama-Birmingham', 'UAB': 'Alabama-Birmingham',
    'Omaha': 'Nebraska Omaha', 'Nebraska-Omaha': 'Nebraska Omaha',
    'SIU Edwardsville': 'SIU-Edwardsville',
    'SE Louisiana': 'Southeastern La.',
    'Southeastern Louisiana': 'Southeastern La.',
    'Southern': 'Southern U.', 'Southern University': 'Southern U.',
    "Hawai'i": 'Hawaii',
    'Grambling': 'Grambling St.',
    'Cal State Bakersfield': 'Cal St. Bakersfield',
    'Cal State Fullerton': 'Cal St. Fullerton',
    'Cal State Northridge': 'Cal St. Northridge',
    'Long Island University': 'LIU', 'Long Island': 'LIU',
    'Gardner-Webb': 'Gardner Webb',
    'Bethune-Cookman': 'Bethune Cookman',
    'Winston-Salem State': 'Winston-Salem St.',
    'Fort Valley State': 'Fort Valley St.',
    'Texas A&M-Corpus Christi': 'Texas A&M-Corpus Christi',
    'UT Martin': 'Tennessee-Martin',
    'Tennessee Martin': 'Tennessee-Martin',
    'Purdue Fort Wayne': 'Fort Wayne',
    'Middle Tennessee': 'Middle Tennessee',
    'Louisiana': 'Louisiana-Lafayette',
    'UL Monroe': 'Louisiana-Monroe',
    'Stephen F. Austin': 'Stephen F. Austin',
    'Incarnate Word': 'Incarnate Word',
}


def normalize(name):
    """
    Normalize an ESPN team name to match our DB canonical names.
    Step 1: strip nickname suffix ("Kansas Jayhawks" -> "Kansas")
    Step 2: map through CBBD_TO_TORVIK dict
    """
    if not name:
        return ''
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
    # Map to canonical name
    return CBBD_TO_TORVIK.get(s, s)


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
            our_h = normalize(our_home).lower()
            our_a = normalize(our_away).lower()

            best_match = None
            for eg in espn_games:
                esp_h = normalize(eg['home_team']).lower()
                esp_a = normalize(eg['away_team']).lower()
                # Exact match after normalization
                if esp_h == our_h and esp_a == our_a:
                    best_match = eg['espn_id']
                    break
                # Partial match
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
            if i == 0 and len(our_games) > 0 and espn_games:
                print(f"  DEBUG ESPN raw:  {[g['home_team'] for g in espn_games[:5]]}")
                print(f"  DEBUG ESPN norm: {[normalize(g['home_team']).lower() for g in espn_games[:5]]}")
                print(f"  DEBUG Our raw:   {[g[1] for g in our_games[:5]]}")
                print(f"  DEBUG Our norm:  {[normalize(g[1]).lower() for g in our_games[:5]]}")

    conn.close()
    print()
    print("=" * 60)
    print(f"Done. ESPN IDs matched: {total_matched:,} | unmatched: {total_unmatched:,}")
    print()
    print("Next: python scripts/03e_pull_referees.py --season 2025")
