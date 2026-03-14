"""
03e_pull_referees.py
====================
Scrape referee assignments + foul stats for all NCAAB games using
ESPN's public JSON summary API (more reliable than HTML scraping).

Endpoint: https://site.api.espn.com/apis/site/v2/sports/basketball/
          mens-college-basketball/summary?event={cbbd_id}

Returns refs (officials) + full box score including team fouls and FTA.

Populates:
  referee_game     -- ref names + foul counts per game
  referee_profiles -- per-ref season foul tendency profiles

Run: python scripts/03e_pull_referees.py --season 2025
     python scripts/03e_pull_referees.py --resume
     python scripts/03e_pull_referees.py --profiles-only
"""

import sqlite3, os, time, argparse, warnings
from datetime import datetime, timezone
import requests
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

ESPN_URL = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
            "mens-college-basketball/summary?event={game_id}")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://www.espn.com/',
}

REQUEST_DELAY = 1.0
BATCH_COMMIT  = 50


def setup_db(conn):
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


def get_already_scraped(conn):
    rows = conn.execute(
        "SELECT game_date, home_team, away_team FROM referee_game"
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def load_games(conn, season=None):
    if season:
        return conn.execute("""
            SELECT game_date, home_team, away_team, cbbd_id, season
            FROM games WHERE season = ? AND cbbd_id IS NOT NULL
            ORDER BY game_date
        """, (season,)).fetchall()
    return conn.execute("""
        SELECT game_date, home_team, away_team, cbbd_id, season
        FROM games WHERE cbbd_id IS NOT NULL
        ORDER BY game_date
    """).fetchall()


def scrape_game(cbbd_id):
    """
    Fetch game summary from ESPN JSON API.
    Returns dict with ref names + foul/FTA/FGA stats, or None on failure.
    """
    url = ESPN_URL.format(game_id=cbbd_id)
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

        # Extract referee names from gameInfo.officials
        officials = data.get('gameInfo', {}).get('officials', [])
        for i, o in enumerate(officials[:3]):
            name = o.get('fullName') or o.get('displayName') or o.get('name', '')
            if name:
                result[f'ref_{i+1}'] = name.strip()

        # Extract team foul/FTA/FGA from box score
        # ESPN structure: boxscore.teams[0/1].statistics array
        boxscore = data.get('boxscore', {})
        teams_bs = boxscore.get('teams', [])

        for team_data in teams_bs:
            home_away = team_data.get('homeAway', '')
            side = 'home' if home_away == 'home' else 'away'
            stats = team_data.get('statistics', [])

            for stat in stats:
                name = stat.get('name', '').lower()
                val  = stat.get('displayValue', '')
                try:
                    fval = float(val)
                except (ValueError, TypeError):
                    continue

                if name == 'foulscommitted' or name == 'teamfouls' or 'foul' in name:
                    result[f'{side}_fouls'] = fval
                elif name == 'freethrowsattempted' or name == 'fta':
                    result[f'{side}_fta'] = fval
                elif name == 'fieldgoalsattempted' or name == 'fga':
                    result[f'{side}_fga'] = fval

        return result

    except Exception:
        return None


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

        avg_fpg = float(tf.mean()) if tf.sum() > 0 else None

        valid_bias = (hf + af).dropna()
        valid_bias = valid_bias[valid_bias > 0]
        home_bias  = float((hf[valid_bias.index] / valid_bias).mean()) if len(valid_bias) > 0 else None

        vh    = hg[hg > 0]
        ftr_h = float((ht[vh.index] / vh).mean()) if len(vh) > 0 else None
        va    = ag[ag > 0]
        ftr_a = float((at[va.index] / va).mean()) if len(va) > 0 else None

        profiles.append((ref_name, season, len(grp), avg_fpg, home_bias, ftr_h, ftr_a, now))

    conn.execute("DELETE FROM referee_profiles")
    conn.executemany("""
        INSERT OR REPLACE INTO referee_profiles
        (ref_name, season, games, avg_fouls_per_game,
         home_foul_bias, ftr_home_avg, ftr_away_avg, computed_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, profiles)
    conn.commit()

    n_fouls    = sum(1 for p in profiles if p[3] is not None)
    unique_refs = df['ref_name'].nunique()
    print(f"  {len(profiles):,} ref-season profiles | {unique_refs:,} unique refs")
    print(f"  Profiles with foul data: {n_fouls:,}/{len(profiles):,}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--season',        type=int,   default=None)
    p.add_argument('--resume',        action='store_true')
    p.add_argument('--wipe',          action='store_true')
    p.add_argument('--profiles-only', action='store_true')
    p.add_argument('--delay',         type=float, default=REQUEST_DELAY)
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    print("=" * 60)
    print("NCAAB -- Referee Scraper (ESPN JSON API)")
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
    pending   = [(gd, ht, at, gid, s) for gd, ht, at, gid, s in all_games
                 if (gd, ht, at) not in already]

    print(f"  Games to scrape: {len(pending):,}")
    print(f"  Est. time: ~{len(pending)*args.delay/3600:.1f} hrs at {args.delay}s/req")
    print()

    found_refs = found_fouls = failed = 0
    now = datetime.now(timezone.utc).isoformat()

    for i, (game_date, home_team, away_team, cbbd_id, season) in enumerate(pending):
        result = scrape_game(cbbd_id)
        time.sleep(args.delay)

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
                  f"refs={found_refs} fouls={found_fouls} failed={failed}")

    conn.commit()
    print()
    print("=" * 60)
    print(f"Done. refs={found_refs} | fouls={found_fouls} | failed={failed}")

    build_referee_profiles(conn)
    conn.close()

    print()
    print("Next: python scripts/04_build_features.py")
    print("      python scripts/08_train_totals_model.py "
          "--combo \"CONTEXT+TVD+KPD+RECENCY+REFS+TRAVEL\"")
