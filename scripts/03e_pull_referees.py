"""
03e_pull_referees.py
====================
Scrape referee assignments + foul stats for NCAAB games using ESPN's JSON API.

Two-phase approach:
  Phase 1: For each game date, hit ESPN scoreboard to get ESPN game IDs
           matched to our home/away team names
  Phase 2: For each game, hit ESPN summary endpoint to get refs + box score

Adds espn_id column to games table for future use.

Run: python scripts/03e_pull_referees.py --season 2025
     python scripts/03e_pull_referees.py --resume
     python scripts/03e_pull_referees.py --profiles-only
"""

import sqlite3, os, time, argparse, warnings
from datetime import datetime, timezone
from collections import defaultdict
import requests
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

ESPN_SCOREBOARD = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                   "mens-college-basketball/scoreboard?dates={date}&limit=200")
ESPN_SUMMARY    = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                   "mens-college-basketball/summary?event={game_id}")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
}

REQUEST_DELAY = 0.5   # scoreboard calls are cheap
SUMMARY_DELAY = 1.0   # summary calls — be polite
BATCH_COMMIT  = 50


def setup_db(conn):
    # Add espn_id column to games if not present
    cols = [c[1] for c in conn.execute('PRAGMA table_info(games)').fetchall()]
    if 'espn_id' not in cols:
        conn.execute("ALTER TABLE games ADD COLUMN espn_id TEXT")
        conn.commit()
        print("  Added espn_id column to games table")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS referee_game (
            game_date   TEXT,
            home_team   TEXT,
            away_team   TEXT,
            ref_1       TEXT,
            ref_2       TEXT,
            ref_3       TEXT,
            home_fouls  REAL,
            away_fouls  REAL,
            home_fta    REAL,
            away_fta    REAL,
            home_fga    REAL,
            away_fga    REAL,
            scraped_at  TEXT,
            PRIMARY KEY (game_date, home_team, away_team)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS referee_profiles (
            ref_name            TEXT,
            season              INTEGER,
            games               INTEGER,
            avg_fouls_per_game  REAL,
            home_foul_bias      REAL,
            ftr_home_avg        REAL,
            ftr_away_avg        REAL,
            computed_at         TEXT,
            PRIMARY KEY (ref_name, season)
        )
    """)
    conn.commit()


def normalize(name):
    """Basic normalization for team name matching."""
    return name.lower().strip().replace('.', '').replace("'", '').replace('-', ' ')


def fetch_espn_ids_for_date(date_str):
    """
    Fetch ESPN game IDs for all games on a given date.
    date_str: YYYYMMDD format
    Returns dict: {(home_norm, away_norm): espn_id}
    """
    url = ESPN_SCOREBOARD.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
        result = {}
        for event in data.get('events', []):
            espn_id = str(event.get('id', ''))
            comps = event.get('competitions', [{}])
            if not comps:
                continue
            comp = comps[0]
            competitors = comp.get('competitors', [])
            home = away = None
            for c in competitors:
                team_name = c.get('team', {}).get('displayName', '')
                if c.get('homeAway') == 'home':
                    home = team_name
                else:
                    away = team_name
            if home and away and espn_id:
                result[(normalize(home), normalize(away))] = espn_id
                result[(normalize(away), normalize(home))] = espn_id  # both directions
        return result
    except Exception:
        return {}


def match_espn_id(home_team, away_team, espn_map):
    """Try to match our team names to ESPN's using normalization."""
    h = normalize(home_team)
    a = normalize(away_team)

    # Direct match
    eid = espn_map.get((h, a))
    if eid:
        return eid

    # Partial match — check if our name is contained in ESPN's or vice versa
    for (eh, ea), eid in espn_map.items():
        if (h in eh or eh in h) and (a in ea or ea in a):
            return eid
        if (h in ea or ea in h) and (a in eh or eh in a):
            return eid

    return None


def fetch_refs_and_fouls(espn_id):
    """
    Fetch refs + box score from ESPN summary endpoint.
    Returns dict or None.
    """
    url = ESPN_SUMMARY.format(game_id=espn_id)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()

        result = {
            'ref_1': None, 'ref_2': None, 'ref_3': None,
            'home_fouls': None, 'away_fouls': None,
            'home_fta': None,   'away_fta': None,
            'home_fga': None,   'away_fga': None,
        }

        # Refs
        officials = data.get('gameInfo', {}).get('officials', [])
        for i, o in enumerate(officials[:3]):
            name = o.get('fullName') or o.get('displayName', '')
            if name:
                result[f'ref_{i+1}'] = name.strip()

        # Box score stats
        for team_data in data.get('boxscore', {}).get('teams', []):
            side = 'home' if team_data.get('homeAway') == 'home' else 'away'
            for stat in team_data.get('statistics', []):
                name = stat.get('name', '').lower()
                try:
                    val = float(stat.get('displayValue', ''))
                except (ValueError, TypeError):
                    continue
                if 'foul' in name:
                    result[f'{side}_fouls'] = val
                elif name in ('freethrowsattempted', 'fta'):
                    result[f'{side}_fta'] = val
                elif name in ('fieldgoalsattempted', 'fga'):
                    result[f'{side}_fga'] = val

        return result
    except Exception:
        return None


def get_already_scraped(conn):
    rows = conn.execute(
        "SELECT game_date, home_team, away_team FROM referee_game"
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def load_games(conn, season=None):
    if season:
        return conn.execute("""
            SELECT game_date, home_team, away_team, season, espn_id
            FROM games WHERE season = ?
            ORDER BY game_date
        """, (season,)).fetchall()
    return conn.execute("""
        SELECT game_date, home_team, away_team, season, espn_id
        FROM games ORDER BY game_date
    """).fetchall()


def build_referee_profiles(conn):
    print("\nBuilding referee profiles...")
    rg = pd.read_sql("SELECT * FROM referee_game", conn)
    if rg.empty:
        print("  No data yet")
        return

    games_meta = pd.read_sql(
        "SELECT game_date, home_team, away_team, season FROM games", conn
    )
    rg = rg.merge(games_meta, on=['game_date', 'home_team', 'away_team'], how='left')
    rg = rg[rg['ref_1'].notna() | rg['ref_2'].notna() | rg['ref_3'].notna()]
    print(f"  Games with ref data: {len(rg):,}")

    rows = []
    for _, row in rg.iterrows():
        season = row.get('season')
        if pd.isna(season):
            continue
        for col in ['ref_1', 'ref_2', 'ref_3']:
            name = row.get(col)
            if name and pd.notna(name) and str(name).strip():
                rows.append({
                    'ref_name':   str(name).strip(),
                    'season':     int(season),
                    'home_fouls': row.get('home_fouls'),
                    'away_fouls': row.get('away_fouls'),
                    'home_fta':   row.get('home_fta'),
                    'away_fta':   row.get('away_fta'),
                    'home_fga':   row.get('home_fga'),
                    'away_fga':   row.get('away_fga'),
                })

    if not rows:
        print("  No assignments found")
        return

    df = pd.DataFrame(rows)
    profiles = []
    now = datetime.now(timezone.utc).isoformat()

    for (ref_name, season), grp in df.groupby(['ref_name', 'season']):
        hf = pd.to_numeric(grp['home_fouls'], errors='coerce')
        af = pd.to_numeric(grp['away_fouls'], errors='coerce')
        tf = hf.fillna(0) + af.fillna(0)
        ht = pd.to_numeric(grp['home_fta'],   errors='coerce')
        at = pd.to_numeric(grp['away_fta'],   errors='coerce')
        hg = pd.to_numeric(grp['home_fga'],   errors='coerce')
        ag = pd.to_numeric(grp['away_fga'],   errors='coerce')

        avg_fpg   = float(tf.mean()) if tf.sum() > 0 else None
        vb        = (hf + af).dropna()
        vb        = vb[vb > 0]
        home_bias = float((hf[vb.index] / vb).mean()) if len(vb) > 0 else None
        vh        = hg[hg > 0]
        ftr_h     = float((ht[vh.index] / vh).mean()) if len(vh) > 0 else None
        va        = ag[ag > 0]
        ftr_a     = float((at[va.index] / va).mean()) if len(va) > 0 else None

        profiles.append((ref_name, season, len(grp), avg_fpg, home_bias, ftr_h, ftr_a, now))

    conn.execute("DELETE FROM referee_profiles")
    conn.executemany("""
        INSERT OR REPLACE INTO referee_profiles
        (ref_name, season, games, avg_fouls_per_game,
         home_foul_bias, ftr_home_avg, ftr_away_avg, computed_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, profiles)
    conn.commit()

    n_fouls     = sum(1 for p in profiles if p[3] is not None)
    unique_refs = df['ref_name'].nunique()
    print(f"  {len(profiles):,} ref-season profiles | {unique_refs:,} unique refs")
    print(f"  Profiles with foul data: {n_fouls:,}/{len(profiles):,}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--season',        type=int,   default=None)
    p.add_argument('--resume',        action='store_true')
    p.add_argument('--wipe',          action='store_true')
    p.add_argument('--profiles-only', action='store_true')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    print("=" * 60)
    print("NCAAB -- Referee Scraper (ESPN JSON API, 2-phase)")
    print("=" * 60)

    conn = sqlite3.connect(DB)

    if args.wipe:
        print("  Wiping referee tables...")
        conn.execute("DROP TABLE IF EXISTS referee_game")
        conn.execute("DROP TABLE IF EXISTS referee_profiles")
        conn.commit()

    setup_db(conn)

    if args.profiles_only:
        build_referee_profiles(conn)
        conn.close()
        exit(0)

    all_games = load_games(conn, season=args.season)
    already   = get_already_scraped(conn) if args.resume else set()
    pending   = [(gd, ht, at, s, eid) for gd, ht, at, s, eid in all_games
                 if (gd, ht, at) not in already]

    print(f"  Games to process: {len(pending):,}")

    # Phase 1: build ESPN ID map by date
    print("\nPhase 1: Fetching ESPN game IDs by date...")
    dates = sorted(set(gd for gd, *_ in pending))
    espn_id_cache = {}   # date_str -> {(home_norm, away_norm): espn_id}
    id_hits = 0

    for i, game_date in enumerate(dates):
        date_nodash = game_date.replace('-', '')
        espn_map = fetch_espn_ids_for_date(date_nodash)
        espn_id_cache[game_date] = espn_map
        id_hits += len(espn_map) // 2  # divide by 2 since we store both directions
        time.sleep(REQUEST_DELAY)
        if (i+1) % 20 == 0 or i == 0:
            print(f"  [{i+1}/{len(dates)}] {game_date} | {len(espn_map)//2} games found | total={id_hits}")

    print(f"  Phase 1 done. ESPN IDs found for ~{id_hits} games across {len(dates)} dates")

    # Save ESPN IDs back to games table
    print("\n  Saving ESPN IDs to games table...")
    saved_ids = 0
    for game_date, home_team, away_team, season, existing_eid in pending:
        if existing_eid:
            continue  # already have it
        espn_map = espn_id_cache.get(game_date, {})
        eid = match_espn_id(home_team, away_team, espn_map)
        if eid:
            conn.execute("""
                UPDATE games SET espn_id = ?
                WHERE game_date = ? AND home_team = ? AND away_team = ?
            """, (eid, game_date, home_team, away_team))
            saved_ids += 1
    conn.commit()
    print(f"  Saved {saved_ids:,} ESPN IDs to games table")

    # Phase 2: fetch refs + fouls for each game
    print(f"\nPhase 2: Fetching refs + box scores ({len(pending):,} games)...")
    print(f"  Est. time: ~{len(pending)*SUMMARY_DELAY/3600:.1f} hrs at {SUMMARY_DELAY}s/req\n")

    found_refs = found_fouls = failed = no_id = 0
    now = datetime.now(timezone.utc).isoformat()

    for i, (game_date, home_team, away_team, season, existing_eid) in enumerate(pending):
        # Get ESPN ID — use stored or look up from cache
        espn_id = existing_eid
        if not espn_id:
            espn_map = espn_id_cache.get(game_date, {})
            espn_id  = match_espn_id(home_team, away_team, espn_map)

        if not espn_id:
            no_id += 1
            conn.execute("""
                INSERT OR IGNORE INTO referee_game
                (game_date, home_team, away_team, scraped_at) VALUES (?,?,?,?)
            """, (game_date, home_team, away_team, now))
        else:
            result = fetch_refs_and_fouls(espn_id)
            time.sleep(SUMMARY_DELAY)

            if result is None:
                failed += 1
                conn.execute("""
                    INSERT OR IGNORE INTO referee_game
                    (game_date, home_team, away_team, scraped_at) VALUES (?,?,?,?)
                """, (game_date, home_team, away_team, now))
            else:
                if result['ref_1']:      found_refs  += 1
                if result['home_fouls']: found_fouls += 1
                conn.execute("""
                    INSERT OR REPLACE INTO referee_game
                    (game_date, home_team, away_team,
                     ref_1, ref_2, ref_3,
                     home_fouls, away_fouls,
                     home_fta, away_fta, home_fga, away_fga,
                     scraped_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    game_date, home_team, away_team,
                    result['ref_1'], result['ref_2'], result['ref_3'],
                    result['home_fouls'], result['away_fouls'],
                    result['home_fta'],   result['away_fta'],
                    result['home_fga'],   result['away_fga'],
                    now,
                ))

        if (i+1) % BATCH_COMMIT == 0:
            conn.commit()
        if (i+1) % 10 == 0 or i == 0:
            pct = (i+1) / len(pending) * 100
            print(f"  [{i+1}/{len(pending)}] ({pct:.0f}%) {game_date} | "
                  f"refs={found_refs} fouls={found_fouls} no_id={no_id} failed={failed}")

    conn.commit()
    print()
    print("=" * 60)
    print(f"Done. refs={found_refs} | fouls={found_fouls} | no_id={no_id} | failed={failed}")

    build_referee_profiles(conn)
    conn.close()

    print()
    print("Next: python scripts/04_build_features.py")
    print("      python scripts/08_train_totals_model.py "
          "--combo \"CONTEXT+TVD+KPD+RECENCY+REFS+TRAVEL\"")
