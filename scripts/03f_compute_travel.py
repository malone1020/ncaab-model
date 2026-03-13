"""
03f_compute_travel.py
=====================
Computes travel distance and timezone cross features for every game.
Uses hardcoded arena lat/long (no API needed).
Stores in team_travel table.

Features produced:
  away_travel_miles   — straight-line distance from away team's arena to game venue
  tz_crossings        — number of timezone boundaries crossed (0-3)
  east_to_west        — 1 if away team travels east→west
  west_to_east        — 1 if away team travels west→east
  away_road_game_n    — how many consecutive road games this is (1, 2, 3...)
  days_since_last_road— days since away team last played a road game
  home_travel_miles   — home team travel (nonzero only for neutral site)

Run: python scripts/03f_compute_travel.py
"""

import sqlite3, os, math
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

# ── Arena coordinates (lat, long, timezone_offset_from_ET) ──────────────────
# tz_offset: hours relative to Eastern Time
#   ET=0, CT=1, MT=2, PT=3
ARENAS = {
    # ACC
    'Duke':                        (36.00,  -78.94, 0),
    'North Carolina':              (35.90,  -79.05, 0),
    'N.C. State':                  (35.77,  -78.68, 0),
    'Virginia':                    (38.03,  -78.51, 0),
    'Virginia Tech':               (37.23,  -80.42, 0),
    'Louisville':                  (38.22,  -85.76, 1),
    'Syracuse':                    (43.03,  -76.13, 0),
    'Pittsburgh':                  (40.44,  -79.99, 0),
    'Florida St.':                 (30.44,  -84.28, 0),
    'Miami (FL)':                  (25.80,  -80.19, 0),
    'Georgia Tech':                (33.77,  -84.40, 0),
    'Clemson':                     (34.68,  -82.84, 0),
    'Wake Forest':                 (36.13,  -80.27, 0),
    'Notre Dame':                  (41.70,  -86.24, 1),
    'Boston College':              (42.34,  -71.17, 0),
    'Stanford':                    (37.44, -122.16, 3),
    'California':                  (37.87, -122.30, 3),
    'SMU':                         (32.77,  -96.80, 1),
    # Big Ten
    'Michigan':                    (42.27,  -83.74, 0),
    'Michigan St.':                (42.73,  -84.48, 0),
    'Ohio St.':                    (40.00,  -83.02, 0),
    'Indiana':                     (39.18,  -86.52, 0),
    'Purdue':                      (40.43,  -86.91, 1),
    'Illinois':                    (40.09,  -88.23, 1),
    'Iowa':                        (41.66,  -91.54, 1),
    'Wisconsin':                   (43.07,  -89.41, 1),
    'Minnesota':                   (44.97,  -93.27, 1),
    'Northwestern':                (42.06,  -87.67, 1),
    'Penn St.':                    (40.79,  -77.86, 0),
    'Nebraska':                    (40.82,  -96.71, 1),
    'Maryland':                    (38.98,  -76.94, 0),
    'Rutgers':                     (40.52,  -74.43, 0),
    'UCLA':                        (34.02, -118.45, 3),
    'Southern California':         (34.02, -118.28, 3),
    'Washington':                  (47.65, -122.30, 3),
    'Oregon':                      (44.05, -123.07, 3),
    # Big 12
    'Kansas':                      (38.95,  -95.25, 1),
    'Kansas St.':                  (39.19,  -96.58, 1),
    'Baylor':                      (31.55,  -97.12, 1),
    'TCU':                         (32.71,  -97.37, 1),
    'Texas Tech':                  (33.59, -101.87, 1),
    'Oklahoma St.':                (36.12,  -97.07, 1),
    'Iowa St.':                    (42.02,  -93.64, 1),
    'West Virginia':               (39.65,  -79.98, 0),
    'BYU':                         (40.25, -111.65, 2),
    'UCF':                         (28.60,  -81.20, 0),
    'Houston':                     (29.76,  -95.37, 1),
    'Cincinnati':                  (39.13,  -84.52, 0),
    'Arizona':                     (32.23, -110.95, 2),
    'Arizona St.':                 (33.43, -111.93, 2),
    'Utah':                        (40.76, -111.84, 2),
    'Colorado':                    (40.01, -105.27, 2),
    # SEC
    'Kentucky':                    (38.21,  -84.50, 0),
    'Tennessee':                   (35.95,  -83.93, 0),
    'Auburn':                      (32.59,  -85.49, 1),
    'Alabama':                     (33.21,  -87.55, 1),
    'Florida':                     (29.65,  -82.35, 0),
    'Georgia':                     (33.96,  -83.37, 0),
    'South Carolina':              (33.99,  -81.03, 0),
    'LSU':                         (30.41,  -91.18, 1),
    'Mississippi':                 (34.36,  -89.53, 1),
    'Mississippi St.':             (33.46,  -88.79, 1),
    'Arkansas':                    (36.07,  -94.17, 1),
    'Missouri':                    (38.94,  -92.33, 1),
    'Vanderbilt':                  (36.14,  -86.80, 1),
    'Texas A&M':                   (30.61,  -96.34, 1),
    'Oklahoma':                    (35.21,  -97.44, 1),
    'Texas':                       (30.28,  -97.73, 1),
    # Big East
    'Connecticut':                 (41.81,  -72.25, 0),
    'Villanova':                   (40.04,  -75.34, 0),
    'Marquette':                   (43.04,  -87.93, 1),
    'Creighton':                   (41.27,  -96.01, 1),
    'Xavier':                      (39.15,  -84.47, 0),
    'Georgetown':                  (38.91,  -77.07, 0),
    "St. John's (NY)":             (40.72,  -73.79, 0),
    'Providence':                  (41.83,  -71.41, 0),
    'DePaul':                      (41.93,  -87.65, 1),
    'Seton Hall':                  (40.74,  -74.24, 0),
    'Butler':                      (39.84,  -86.17, 1),
    'Georgetown':                  (38.91,  -77.07, 0),
    # Mountain West
    'San Diego St.':               (32.75, -117.07, 3),
    'Utah St.':                    (41.75, -111.81, 2),
    'Boise St.':                   (43.60, -116.20, 3),
    'Nevada':                      (39.53, -119.82, 3),
    'New Mexico':                  (35.08, -106.62, 2),
    'Fresno St.':                  (36.81, -119.75, 3),
    'UNLV':                        (36.17, -115.14, 3),
    'Colorado St.':                (40.57, -105.08, 2),
    'Wyoming':                     (41.31, -105.59, 2),
    'Air Force':                   (38.99, -104.89, 2),
    # American
    'Wichita St.':                 (37.69,  -97.33, 1),
    'Memphis':                     (35.15,  -90.05, 1),
    'Tulsa':                       (36.16,  -95.93, 1),
    'Tulane':                      (29.95,  -90.12, 1),
    'East Carolina':               (35.61,  -77.37, 0),
    'South Florida':               (28.07,  -82.42, 0),
    'UTSA':                        (29.42,  -98.49, 1),
    'Temple':                      (39.98,  -75.16, 0),
    'UAB':                         (33.50,  -86.80, 1),
    'Charlotte':                   (35.23,  -80.83, 0),
    'North Texas':                 (33.21,  -97.13, 1),
    'Rice':                        (29.72,  -95.40, 1),
    'Florida Atlantic':            (26.38,  -80.10, 0),
    # A-10
    'Dayton':                      (39.76,  -84.19, 0),
    'Saint Louis':                 (38.64,  -90.24, 1),
    'VCU':                         (37.55,  -77.45, 0),
    'Rhode Island':                (41.83,  -71.43, 0),
    'George Mason':                (38.83,  -77.31, 0),
    'Richmond':                    (37.57,  -77.54, 0),
    'Fordham':                     (40.86,  -73.89, 0),
    'George Washington':           (38.90,  -77.05, 0),
    'Saint Joseph\'s':             (40.00,  -75.24, 0),
    'Massachusetts':               (42.39,  -72.53, 0),
    'La Salle':                    (40.04,  -75.19, 0),
    'Duquesne':                    (40.44,  -79.99, 0),
    # WCC
    'Gonzaga':                     (47.67, -117.40, 3),
    'Saint Mary\'s':               (37.90, -122.07, 3),
    'San Francisco':               (37.78, -122.45, 3),
    'Portland':                    (45.53, -122.68, 3),
    'Pepperdine':                  (34.03, -118.71, 3),
    'Loyola Marymount':            (33.97, -118.42, 3),
    'San Diego':                   (32.77, -117.07, 3),
    'Pacific':                     (37.98, -121.31, 3),
    'Santa Clara':                 (37.35, -121.95, 3),
    # Other notable
    'Wofford':                     (34.93,  -81.93, 0),
    'Murray St.':                  (36.61,  -88.31, 1),
    'Liberty':                     (37.35,  -79.18, 0),
    'Belmont':                     (36.13,  -86.78, 1),
    'Vermont':                     (44.47,  -73.21, 0),
    'Colgate':                     (42.82,  -75.53, 0),
    'Harvard':                     (42.37,  -71.12, 0),
    'Yale':                        (41.31,  -72.93, 0),
    'Princeton':                   (40.35,  -74.66, 0),
    'Penn':                        (39.95,  -75.19, 0),
    'Cornell':                     (42.45,  -76.48, 0),
    'Columbia':                    (40.81,  -73.96, 0),
    'Brown':                       (41.83,  -71.40, 0),
    'Dartmouth':                   (43.70,  -72.29, 0),
    'Drake':                       (41.60,  -93.65, 1),
    'Northern Iowa':               (42.51,  -92.46, 1),
    'Missouri St.':                (37.21,  -93.28, 1),
    'Bradley':                     (40.70,  -89.62, 1),
    'Indiana St.':                 (39.47,  -87.41, 1),
    'Evansville':                  (37.97,  -87.56, 1),
    'Loyola Chicago':              (41.99,  -87.66, 1),
    'Illinois St.':                (40.51,  -88.99, 1),
    'Southern Illinois':           (37.71,  -89.22, 1),
    'Valparaiso':                  (41.47,  -87.04, 1),
    'Xavier':                      (39.15,  -84.47, 0),
    'Denver':                      (39.74, -104.99, 2),
    'Utah Tech':                   (37.10, -113.58, 2),
    'Southern Utah':               (37.68, -113.07, 2),
    'Weber St.':                   (41.23, -111.97, 2),
    'Idaho':                       (46.73, -117.00, 3),
    'Idaho St.':                   (42.86, -112.44, 2),
    'Montana':                     (46.86, -113.99, 2),
    'Montana St.':                 (45.67, -111.05, 2),
    'Eastern Washington':          (47.66, -117.40, 3),
    'Portland St.':                (45.52, -122.68, 3),
    'Northern Arizona':            (35.18, -111.65, 2),
    'Sacramento St.':              (38.56, -121.42, 3),
    'UC Davis':                    (38.54, -121.75, 3),
    'Hawaii':                      (21.30, -157.82, 5),  # 5 = HST offset from ET
    'Long Beach St.':              (33.78, -118.19, 3),
    'Cal St. Northridge':          (34.24, -118.53, 3),
    'Cal St. Fullerton':           (33.88, -117.88, 3),
    'Cal St. Bakersfield':         (35.37, -119.01, 3),
    'UC Santa Barbara':            (34.41, -119.85, 3),
    'UC Irvine':                   (33.65, -117.84, 3),
    'UC Riverside':                (33.97, -117.33, 3),
    'UC San Diego':                (32.88, -117.23, 3),
    'Cal Poly':                    (35.30, -120.66, 3),
    'New Mexico St.':              (32.28, -106.75, 2),
    'UT Rio Grande Valley':        (26.30,  -98.17, 1),
    'Sam Houston St.':             (30.71,  -95.55, 1),
    'Stephen F. Austin':           (31.61,  -94.65, 1),
    'Abilene Christian':           (32.45,  -99.74, 1),
    'Lamar':                       (30.08,  -94.10, 1),
    'McNeese St.':                 (30.22,  -93.22, 1),
    'Southeastern La.':            (30.49,  -90.46, 1),
    'Nicholls St.':                (29.59,  -90.71, 1),
    'Northwestern St.':            (31.63,  -93.10, 1),
    'Grambling St.':               (32.53,  -92.72, 1),
    'Louisiana-Lafayette':         (30.22,  -92.02, 1),
    'Louisiana-Monroe':            (32.53,  -92.08, 1),
    'Arkansas St.':                (35.84,  -90.70, 1),
    'Little Rock':                 (34.75,  -92.27, 1),
    'South Dakota St.':            (44.32,  -96.79, 1),
    'North Dakota St.':            (46.89,  -96.80, 1),
    'North Dakota':                (47.92, -102.80, 1),
    'South Dakota':                (44.37,  -100.35, 1),
    'Oral Roberts':                (36.15,  -95.95, 1),
    'Omaha':                       (41.26,  -96.02, 1),
    'Nebraska Omaha':              (41.26,  -96.02, 1),
    'Denver':                      (39.74, -104.99, 2),
    'Western Illinois':            (40.46,  -90.68, 1),
    'IUPUI':                       (39.77,  -86.17, 0),
    'IU Indianapolis':             (39.77,  -86.17, 0),
    'Fort Wayne':                  (41.08,  -85.14, 0),
}

DEFAULT_LAT, DEFAULT_LON, DEFAULT_TZ = 37.0, -95.0, 1  # geographic center US


def haversine(lat1, lon1, lat2, lon2):
    """Distance in miles between two lat/long points."""
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def get_coords(team):
    return ARENAS.get(team, (DEFAULT_LAT, DEFAULT_LON, DEFAULT_TZ))


def build_travel(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_travel (
            game_date TEXT,
            home_team TEXT,
            away_team TEXT,
            away_travel_miles REAL,
            tz_crossings INTEGER,
            east_to_west INTEGER,
            west_to_east INTEGER,
            neutral_site_home_miles REAL,
            away_road_game_n INTEGER,
            PRIMARY KEY (game_date, home_team, away_team)
        )
    """)
    conn.commit()

    games = conn.execute("""
        SELECT game_date, home_team, away_team, neutral_site
        FROM games ORDER BY game_date
    """).fetchall()

    print(f"  Computing travel for {len(games):,} games...")

    # Track consecutive road games per team
    team_schedule = defaultdict(list)
    for gd, home, away, neutral in games:
        team_schedule[home].append((gd, 'home'))
        team_schedule[away].append((gd, 'away'))

    # Sort each team's schedule
    for t in team_schedule:
        team_schedule[t].sort()

    # Build consecutive road game index
    consecutive_road = {}
    for team, sched in team_schedule.items():
        streak = 0
        for gd, side in sched:
            if side == 'away':
                streak += 1
            else:
                streak = 0
            if side == 'away':
                consecutive_road[(gd, team)] = streak

    rows = []
    known = 0
    for gd, home, away, neutral in games:
        h_lat, h_lon, h_tz = get_coords(home)
        a_lat, a_lon, a_tz = get_coords(away)

        # Venue is home arena (or neutral site approximated as home)
        venue_lat, venue_lon, venue_tz = h_lat, h_lon, h_tz

        away_miles = haversine(a_lat, a_lon, venue_lat, venue_lon)
        tz_cross   = abs(a_tz - venue_tz)
        e2w        = 1 if a_tz < venue_tz else 0  # away in East, venue in West
        w2e        = 1 if a_tz > venue_tz else 0

        # Neutral site: home team also travels
        home_miles = 0.0
        if neutral:
            home_miles = haversine(h_lat, h_lon, venue_lat, venue_lon)

        road_n = consecutive_road.get((gd, away), 1)

        if away in ARENAS:
            known += 1

        rows.append((gd, home, away, away_miles, tz_cross, e2w, w2e, home_miles, road_n))

    conn.execute("DELETE FROM team_travel")
    conn.executemany("""
        INSERT OR REPLACE INTO team_travel VALUES (?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

    pct_known = known/len(games)*100 if games else 0
    print(f"  ✅ {len(rows):,} travel rows written")
    print(f"     Away team in ARENAS dict: {known:,}/{len(games):,} ({pct_known:.1f}%)")
    print(f"     (unknown teams use geographic center of US as fallback)")
    return len(rows)


if __name__ == '__main__':
    conn = sqlite3.connect(DB)
    print("Computing travel distance features...")
    n = build_travel(conn)
    conn.close()
    print(f"\n✅ team_travel: {n:,} rows")
    print("Next: add travel features to 04_build_features.py")
