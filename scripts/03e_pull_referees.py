"""
03e_pull_referees.py
====================
Pulls referee assignments from CollegeBasketballData.com (CBBD) API.
Builds referee profile features (foul rate, pace impact, home bias).

Tables created/updated:
  referee_game    — one row per game, with ref IDs and names
  referee_profiles — one row per ref per season, with computed stats

Run: python scripts/03e_pull_referees.py
"""

import sqlite3, os, time, requests
from collections import defaultdict

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
API_KEY = 'CKa1XBg0pAjKxElmF5YONntM9J/8tANHWnXN/MBXiX8pUJdaSBe2bYbOVkVtfZzM'  # CBBD key
BASE    = 'https://api.collegebasketballdata.com'

HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Accept': 'application/json'}

def init_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS referee_game (
            game_id     TEXT,
            game_date   TEXT,
            season      INTEGER,
            home_team   TEXT,
            away_team   TEXT,
            ref1_id     TEXT,
            ref2_id     TEXT,
            ref3_id     TEXT,
            ref1_name   TEXT,
            ref2_name   TEXT,
            ref3_name   TEXT,
            PRIMARY KEY (game_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS referee_profiles (
            ref_id          TEXT,
            ref_name        TEXT,
            season          INTEGER,
            games           INTEGER,
            avg_fouls_per_game   REAL,
            avg_pace_impact      REAL,   -- avg possessions vs expected
            home_foul_pct        REAL,   -- home fouls / total fouls
            home_foul_bias       REAL,   -- home_foul_pct - 0.5 (positive = home bias)
            ftr_home_avg         REAL,   -- avg home FT rate in their games
            ftr_away_avg         REAL,   -- avg away FT rate in their games
            PRIMARY KEY (ref_id, season)
        )
    """)
    conn.commit()


def pull_refs_for_season(conn, season):
    """Pull all referee assignments for a season."""
    url = f"{BASE}/games/referees"
    params = {'season': season, 'seasonType': 'regular'}

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 404:
            print(f"    {season}: no referee data (404)")
            return 0
        if r.status_code != 200:
            print(f"    {season}: HTTP {r.status_code}")
            return 0
        data = r.json()
    except Exception as e:
        print(f"    {season}: error — {e}")
        return 0

    if not data:
        return 0

    rows = []
    for g in data:
        refs = g.get('officials', []) or []
        # Pad to 3
        while len(refs) < 3:
            refs.append({})
        game_id   = str(g.get('id', ''))
        game_date = g.get('startDate', '')[:10]
        home      = g.get('homeTeam', '')
        away      = g.get('awayTeam', '')

        rows.append((
            game_id, game_date, season, home, away,
            str(refs[0].get('id', '')), str(refs[1].get('id', '')), str(refs[2].get('id', '')),
            refs[0].get('name', ''),  refs[1].get('name', ''),  refs[2].get('name', ''),
        ))

    conn.executemany("""
        INSERT OR REPLACE INTO referee_game VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    return len(rows)


def pull_tournament_refs(conn, season):
    """Also pull postseason referee data."""
    url = f"{BASE}/games/referees"
    params = {'season': season, 'seasonType': 'postseason'}
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code != 200:
            return 0
        data = r.json()
    except:
        return 0

    if not data:
        return 0

    rows = []
    for g in data:
        refs = g.get('officials', []) or []
        while len(refs) < 3:
            refs.append({})
        game_id   = str(g.get('id', ''))
        game_date = g.get('startDate', '')[:10]
        home      = g.get('homeTeam', '')
        away      = g.get('awayTeam', '')
        rows.append((
            game_id, game_date, season, home, away,
            str(refs[0].get('id', '')), str(refs[1].get('id', '')), str(refs[2].get('id', '')),
            refs[0].get('name', ''),  refs[1].get('name', ''),  refs[2].get('name', ''),
        ))

    conn.executemany("INSERT OR REPLACE INTO referee_game VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)


def build_ref_profiles(conn):
    """
    Build per-referee per-season profiles from referee_game + game_team_stats.
    Computes: avg fouls/game, home foul bias, FT rate differentials.
    """
    print("  Building referee profiles...")

    # Get all referee appearances with game stats
    rows = conn.execute("""
        SELECT rg.ref1_name, rg.ref1_id, rg.season, rg.game_date,
               rg.home_team, rg.away_team
        FROM referee_game rg
        WHERE rg.ref1_id != '' AND rg.ref1_id IS NOT NULL
        UNION ALL
        SELECT rg.ref2_name, rg.ref2_id, rg.season, rg.game_date,
               rg.home_team, rg.away_team
        FROM referee_game rg
        WHERE rg.ref2_id != '' AND rg.ref2_id IS NOT NULL
        UNION ALL
        SELECT rg.ref3_name, rg.ref3_id, rg.season, rg.game_date,
               rg.home_team, rg.away_team
        FROM referee_game rg
        WHERE rg.ref3_id != '' AND rg.ref3_id IS NOT NULL
    """).fetchall()

    # Get game team stats for foul/FTR data
    stats = conn.execute("""
        SELECT g.game_date, g.home_team, g.away_team,
               gts_h.fouls as home_fouls, gts_a.fouls as away_fouls,
               gts_h.ft_att as home_fta, gts_a.ft_att as away_fta,
               gts_h.fg_att as home_fga, gts_a.fg_att as away_fga
        FROM games g
        LEFT JOIN game_team_stats gts_h ON gts_h.game_id = g.id AND gts_h.team = g.home_team
        LEFT JOIN game_team_stats gts_a ON gts_a.game_id = g.id AND gts_a.team = g.away_team
    """).fetchall()

    stats_idx = {}
    for gd, ht, at, hf, af, hfta, afta, hfga, afga in stats:
        stats_idx[(gd, ht, at)] = {
            'home_fouls': hf or 0, 'away_fouls': af or 0,
            'home_fta': hfta or 0, 'away_fta': afta or 0,
            'home_fga': hfga or 0, 'away_fga': afga or 0,
        }

    # Aggregate by ref × season
    ref_data = defaultdict(lambda: {
        'name': '', 'games': 0, 'total_fouls': 0,
        'home_fouls': 0, 'away_fouls': 0,
        'home_fta': 0, 'away_fta': 0,
        'home_fga': 0, 'away_fga': 0,
    })

    for ref_name, ref_id, season, gd, ht, at in rows:
        key = (ref_id, season)
        s = stats_idx.get((gd, ht, at), {})
        d = ref_data[key]
        d['name'] = ref_name
        d['games'] += 1
        hf = s.get('home_fouls', 0) or 0
        af = s.get('away_fouls', 0) or 0
        d['total_fouls']  += hf + af
        d['home_fouls']   += hf
        d['away_fouls']   += af
        d['home_fta']     += s.get('home_fta', 0) or 0
        d['away_fta']     += s.get('away_fta', 0) or 0
        d['home_fga']     += s.get('home_fga', 0) or 0
        d['away_fga']     += s.get('away_fga', 0) or 0

    profile_rows = []
    for (ref_id, season), d in ref_data.items():
        if d['games'] < 5:
            continue
        total_f = d['total_fouls']
        home_foul_pct  = d['home_fouls'] / total_f if total_f > 0 else 0.5
        home_foul_bias = home_foul_pct - 0.5
        avg_fpg        = total_f / d['games']
        ftr_home = d['home_fta'] / d['home_fga'] if d['home_fga'] > 0 else None
        ftr_away = d['away_fta'] / d['away_fga'] if d['away_fga'] > 0 else None
        profile_rows.append((
            ref_id, d['name'], season, d['games'],
            avg_fpg, None, home_foul_pct, home_foul_bias,
            ftr_home, ftr_away,
        ))

    conn.execute("DELETE FROM referee_profiles")
    conn.executemany("INSERT OR REPLACE INTO referee_profiles VALUES (?,?,?,?,?,?,?,?,?,?)", profile_rows)
    conn.commit()
    print(f"  ✅ {len(profile_rows):,} referee profiles built")
    return len(profile_rows)


if __name__ == '__main__':
    conn = sqlite3.connect(DB)
    init_tables(conn)

    existing = conn.execute("SELECT COUNT(*) FROM referee_game").fetchone()[0]
    print(f"  Existing referee_game rows: {existing:,}")

    seasons = range(2016, 2026)
    total = 0
    print(f"\nPulling referee data for seasons {min(seasons)}-{max(seasons)}...")
    for s in seasons:
        n_reg  = pull_refs_for_season(conn, s)
        n_post = pull_tournament_refs(conn, s)
        n      = n_reg + n_post
        total += n
        print(f"  {s}: {n:,} games ({n_reg} regular + {n_post} postseason)")
        time.sleep(0.5)

    print(f"\n  Total referee_game rows: {total:,}")

    # Build profiles
    n_profiles = build_ref_profiles(conn)

    conn.close()
    print(f"\n✅ Done: {total:,} game-ref assignments, {n_profiles:,} ref profiles")
    print("Next: wire ref features into 04_build_features.py")
