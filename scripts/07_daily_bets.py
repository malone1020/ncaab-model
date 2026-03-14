"""
07_daily_bets.py
================
Generate today's bet card using the production calibrated model.
Uses the same feature pipeline as 04_build_features.py.
EV formula matches the backtest exactly: P(home covers) * 1.909 - 1.

Run: python scripts/07_daily_bets.py
     python scripts/07_daily_bets.py --date 2025-03-15
     python scripts/07_daily_bets.py --demo
     python scripts/07_daily_bets.py --bankroll 5000
"""

import sqlite3, os, json, sys, argparse, pickle, warnings
from datetime import date, datetime, timezone
import pandas as pd
import numpy as np
import requests

try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(_env_path)
except ImportError:
    pass

warnings.filterwarnings('ignore')

ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB        = os.path.join(ROOT, 'data', 'basketball.db')
MODEL_PKL = os.path.join(ROOT, 'models', 'production_model.pkl')
IMPUTER_F = os.path.join(ROOT, 'models', 'imputer.pkl')
FEAT_F    = os.path.join(ROOT, 'models', 'feature_cols.json')
OUT_DIR   = os.path.join(ROOT, 'outputs')
os.makedirs(OUT_DIR, exist_ok=True)

ODDS_KEY    = os.getenv('ODDS_API_KEY', '')
EV_MIN      = 0.03
PAYOUT      = 100/110
SPREAD_LO   = 0.5
SPREAD_HI   = 9.0
KELLY_FRAC  = 0.25
MAX_BET_PCT = 0.02
BANKROLL    = 10000


def load_model():
    if not os.path.exists(MODEL_PKL):
        print(f"ERROR: {MODEL_PKL} not found. Run 06_train_final_model.py first.")
        return None, None, None, None, None, None
    with open(MODEL_PKL, 'rb') as f:
        model = pickle.load(f)
    with open(IMPUTER_F, 'rb') as f:
        imputer = pickle.load(f)
    with open(FEAT_F) as f:
        meta = json.load(f)
    features = meta['features'] if isinstance(meta, dict) else meta
    combo = meta.get('combo', 'unknown') if isinstance(meta, dict) else 'unknown'
    print(f"  Spread model: {combo} | {len(features)} features")

    # Load totals model if available
    totals_model = totals_imputer = totals_features = None
    totals_pkl  = os.path.join(ROOT, 'models', 'totals_model.pkl')
    totals_imp  = os.path.join(ROOT, 'models', 'totals_imputer.pkl')
    totals_feat = os.path.join(ROOT, 'models', 'totals_feature_cols.json')
    if os.path.exists(totals_pkl):
        with open(totals_pkl, 'rb') as f:  totals_model   = pickle.load(f)
        with open(totals_imp, 'rb') as f:  totals_imputer = pickle.load(f)
        with open(totals_feat) as f:
            tm = json.load(f)
            totals_features = tm['features'] if isinstance(tm, dict) else tm
        print(f"  Totals model: {len(totals_features)} features")
    else:
        print(f"  Totals model: not found (run 08_train_totals_model.py)")

    # Load ML params if available
    ml_params = None
    ml_path = os.path.join(ROOT, 'models', 'ml_params.json')
    if os.path.exists(ml_path):
        with open(ml_path) as f: ml_params = json.load(f)
        print(f"  ML params: sigma={ml_params['margin_std']:.2f}")
    else:
        print(f"  ML params: not found (run 09_train_ml_model.py)")

    return model, imputer, features, totals_model, totals_imputer, totals_features, ml_params


# OddsAPI uses full names like "Tennessee Volunteers" — strip nicknames to match Torvik
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
    'Bluejays','Blue Jays','Privateers','Hokies','Retrievers','Penmen',
    'Seawolves','Runnin Utes','Fighting Hawks','Golden Grizzlies',
}

def get_norm_func():
    """Load norm() from 04_build_features.py, wrapped to strip OddsAPI nicknames."""
    import importlib.util
    path = os.path.join(ROOT, 'scripts', '04_build_features.py')
    spec = importlib.util.spec_from_file_location("bfm", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    base_norm = mod.norm

    def odds_norm(name):
        # Strip trailing nickname: "Tennessee Volunteers" -> "Tennessee"
        parts = name.rsplit(' ', 1)
        if len(parts) == 2 and parts[1] in ODDS_NICKNAMES:
            name = parts[0]
        # Also handle two-word nicknames: "Golden Flashes", "Mean Green", etc.
        parts3 = name.rsplit(' ', 2)
        if len(parts3) == 3:
            two_word = parts3[1] + ' ' + parts3[2]
            if two_word in ODDS_NICKNAMES:
                name = parts3[0]
        return base_norm(name)

    return odds_norm


def fetch_todays_lines(target_date):
    if not ODDS_KEY:
        print("  WARNING: ODDS_API_KEY not set. Use --demo to test.")
        return []
    url = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/"
    params = {'apiKey': ODDS_KEY, 'regions': 'us', 'markets': 'spreads,totals,h2h',
              'bookmakers': 'draftkings', 'oddsFormat': 'american'}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            print(f"  OddsAPI: {len(data)} events | remaining: {r.headers.get('X-Requests-Remaining','?')}")
            return data
        print(f"  OddsAPI HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  OddsAPI error: {e}")
        return []


def parse_lines(odds_data, target_date):
    games = []
    for game in odds_data:
        gdt = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
        if gdt.date() != target_date:
            continue
        home  = game.get('home_team', '')
        away  = game.get('away_team', '')
        spread = ml_home = ml_away = total = None
        for book in game.get('bookmakers', []):
            if book['key'] != 'draftkings': continue
            for mkt in book.get('markets', []):
                if mkt['key'] == 'spreads':
                    for oc in mkt.get('outcomes', []):
                        if oc['name'] == home:
                            try: spread = float(oc['point'])
                            except: pass
                elif mkt['key'] == 'h2h':
                    for oc in mkt.get('outcomes', []):
                        try:
                            if oc['name'] == home:   ml_home = int(oc['price'])
                            elif oc['name'] == away: ml_away = int(oc['price'])
                        except: pass
                elif mkt['key'] == 'totals':
                    for oc in mkt.get('outcomes', []):
                        if oc.get('name') == 'Over':
                            try: total = float(oc['point'])
                            except: pass
        if spread is not None:
            games.append({
                'home_team': home, 'away_team': away,
                'spread': spread, 'total': total,
                'ml_home': ml_home, 'ml_away': ml_away,
                'game_time': gdt,
            })
    return games



def store_line_movement(games, target_date, conn, norm_fn):
    """
    Store opening and closing lines in line_movement table.
    - First call of the day (no existing row): stores as open AND close
    - Subsequent calls: updates close only (preserves opening line)
    This builds a clean open/close history for future LINE_MOVE feature use.
    """
    now = datetime.now(timezone.utc).isoformat()
    stored = 0
    for g in games:
        home_norm = norm_fn(g['home_team'])
        away_norm = norm_fn(g['away_team'])
        date_str  = str(target_date)

        spread  = g.get('spread')
        total   = g.get('total')
        ml_home = g.get('ml_home')
        ml_away = g.get('ml_away')

        # Check if opening line already stored today
        existing = conn.execute("""
            SELECT spread_open, total_open, spread_close, total_close
            FROM line_movement
            WHERE game_date = ? AND home_team = ? AND away_team = ?
        """, (date_str, home_norm, away_norm)).fetchone()

        if existing is None:
            # First pull — store as both open and close (movement = 0 for now)
            conn.execute("""
                INSERT OR REPLACE INTO line_movement
                (game_date, home_team, away_team,
                 spread_open, total_open, ml_home_open, ml_away_open,
                 open_timestamp,
                 spread_close, total_close, ml_home_close, ml_away_close,
                 close_timestamp,
                 spread_move, total_move, ml_home_move,
                 scraped_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                date_str, home_norm, away_norm,
                spread, total, ml_home, ml_away, now,
                spread, total, ml_home, ml_away, now,
                0.0, 0.0, 0,
                now,
            ))
        else:
            # Subsequent pull — update close and recompute movement
            open_spread, open_total = existing[0], existing[1]
            spread_move = (spread - open_spread) if (spread is not None and open_spread is not None) else None
            total_move  = (total  - open_total)  if (total  is not None and open_total  is not None) else None
            conn.execute("""
                UPDATE line_movement
                SET spread_close    = ?,
                    total_close     = ?,
                    ml_home_close   = ?,
                    ml_away_close   = ?,
                    close_timestamp = ?,
                    spread_move     = ?,
                    total_move      = ?,
                    scraped_at      = ?
                WHERE game_date = ? AND home_team = ? AND away_team = ?
            """, (
                spread, total, ml_home, ml_away, now,
                spread_move, total_move,
                now,
                date_str, home_norm, away_norm,
            ))
        stored += 1

    conn.commit()
    return stored


def fetch_torvik_schedule(target_date):
    """
    Get conf_game and neutral_site flags for today's games.
    Primary: DB lookup from games table (reliable, no scraping needed).
    Fallback: Torvik schedule page (JS-rendered, often returns nothing).
    Returns dict keyed by frozenset of normalized team names.
    """
    result = {}

    # Primary: look up from games table — already has conf_game + neutral_site
    try:
        import importlib.util
        path = os.path.join(ROOT, 'scripts', '04_build_features.py')
        spec = importlib.util.spec_from_file_location("bfm", path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        norm_db = mod.norm

        db_conn = sqlite3.connect(DB)
        rows = db_conn.execute("""
            SELECT home_team, away_team, conf_game, neutral_site, tournament
            FROM games WHERE game_date = ?
        """, (str(target_date),)).fetchall()
        db_conn.close()

        for home, away, conf_game, neutral_site, tournament in rows:
            key = frozenset([norm_db(home), norm_db(away)])
            conf_code = 'CONF-T' if tournament == 'conf_tournament' else ''
            result[key] = {
                'conf_game':    int(conf_game or 0),
                'neutral_site': int(neutral_site or 0),
                'conf_code':    conf_code,
            }
        if result:
            return result
    except Exception:
        pass

    # Fallback: Torvik schedule page (JS-rendered, usually returns nothing)
    import re
    url = f"https://barttorvik.com/schedule.php?date={target_date.strftime('%Y%m%d')}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return result
        from html.parser import HTMLParser
        class ScheduleParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.games = {}
                self._in_td = False
                self._cells = []
                self._row = []
            def handle_starttag(self, tag, attrs):
                if tag == 'tr': self._row = []
                if tag == 'td': self._in_td = True; self._cells = []
            def handle_endtag(self, tag):
                if tag == 'td':
                    self._in_td = False
                    self._row.append(''.join(self._cells).strip())
                if tag == 'tr' and self._row:
                    for cell in self._row:
                        if ' vs ' not in cell:
                            continue
                        parts = cell.split(' vs ', 1)
                        t1 = re.sub(r'^\d+\s+', '', parts[0]).strip()
                        right_no_rank = re.sub(r'^\d+\s+', '', parts[1])
                        conf_match = re.search(r'([A-Z][A-Z0-9]*-[A-Z])', right_no_rank)
                        conf_code = conf_match.group(1) if conf_match else ''
                        t2 = right_no_rank[:right_no_rank.index(conf_code)].strip() if conf_code else re.sub(r'\s+[A-Z0-9]+\s*$', '', right_no_rank).strip()
                        is_conf    = 1 if conf_code and 'Non' not in conf_code else 0
                        is_neutral = 1 if conf_code.endswith('-T') or conf_code.endswith('-N') else 0
                        if t1 and t2:
                            self.games[frozenset([t1, t2])] = {
                                'conf_game': is_conf, 'neutral_site': is_neutral, 'conf_code': conf_code
                            }
                        break
            def handle_data(self, data):
                if self._in_td: self._cells.append(data)
        parser = ScheduleParser()
        parser.feed(r.text)
        return parser.games if parser.games else result
    except Exception as e:
        print(f"  Warning: could not fetch Torvik schedule: {e}")
        return result


def fetch_todays_refs(games, target_date, norm_fn):
    """
    Fetch referee assignments for today's games from ESPN's summary API.
    Gets ESPN event IDs from scoreboard, then hits each summary for officials.
    Stores in referee_game and updates referee_profiles incrementally.
    """
    ESPN_SCOREBOARD = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                       "mens-college-basketball/scoreboard")
    ESPN_SUMMARY    = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                       "mens-college-basketball/summary?event={eid}")
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }

    def strip_nick(name):
        parts = name.rsplit(' ', 1)
        if len(parts) == 2 and parts[1] in ODDS_NICKNAMES:
            name = parts[0]
        parts3 = name.rsplit(' ', 2)
        if len(parts3) == 3:
            two = parts3[1] + ' ' + parts3[2]
            if two in ODDS_NICKNAMES:
                name = parts3[0]
        return name.strip()

    try:
        # Step 1: get today's ESPN event IDs
        date_str = target_date.strftime('%Y%m%d')
        r = requests.get(ESPN_SCOREBOARD,
                         params={'dates': date_str, 'limit': 300},
                         headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return 0

        espn_map = {}
        for event in r.json().get('events', []):
            eid  = str(event.get('id', ''))
            comp = (event.get('competitions') or [{}])[0]
            competitors = comp.get('competitors', [])
            home_obj = next((c for c in competitors if c.get('homeAway') == 'home'), None)
            away_obj = next((c for c in competitors if c.get('homeAway') == 'away'), None)
            if home_obj and away_obj and eid:
                h = norm_fn(strip_nick(home_obj.get('team', {}).get('displayName', '')))
                a = norm_fn(strip_nick(away_obj.get('team', {}).get('displayName', '')))
                espn_map[(h, a)] = eid
                espn_map[(a, h)] = eid

        conn  = sqlite3.connect(DB)
        now   = datetime.now(timezone.utc).isoformat()
        date_db = str(target_date)
        scraped = 0

        for g in games:
            h_norm = norm_fn(g['home_team'])
            a_norm = norm_fn(g['away_team'])
            eid = espn_map.get((h_norm, a_norm))
            if not eid:
                continue
            try:
                rs = requests.get(ESPN_SUMMARY.format(eid=eid),
                                  headers=HEADERS, timeout=15)
                if rs.status_code != 200:
                    continue
                data = rs.json()

                # Officials
                officials = data.get('gameInfo', {}).get('officials', [])
                refs = [(o.get('fullName') or o.get('displayName', '')).strip()
                        for o in officials[:3]]
                while len(refs) < 3:
                    refs.append(None)
                ref_1, ref_2, ref_3 = [r if r else None for r in refs]

                # Box score fouls
                hf = af = ht = at = hg = ag = None
                for td in data.get('boxscore', {}).get('teams', []):
                    side = 'home' if td.get('homeAway') == 'home' else 'away'
                    for stat in td.get('statistics', []):
                        sn = stat.get('name', '').lower()
                        try: val = float(stat.get('displayValue', ''))
                        except: continue
                        if 'foul' in sn:
                            if side == 'home': hf = val
                            else:              af = val
                        elif sn in ('freethrowsattempted', 'fta'):
                            if side == 'home': ht = val
                            else:              at = val
                        elif sn in ('fieldgoalsattempted', 'fga'):
                            if side == 'home': hg = val
                            else:              ag = val

                conn.execute("""
                    INSERT OR REPLACE INTO referee_game
                    (game_date, home_team, away_team,
                     ref_1, ref_2, ref_3,
                     home_fouls, away_fouls,
                     home_fta, away_fta, home_fga, away_fga,
                     scraped_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (date_db, h_norm, a_norm,
                      ref_1, ref_2, ref_3,
                      hf, af, ht, at, hg, ag, now))
                scraped += 1
                time.sleep(0.3)
            except Exception:
                continue

        conn.commit()
        conn.close()
        return scraped

    except Exception as e:
        return 0


def fetch_injury_flags(games, norm_fn):
    """
    Fetch injury reports for today's teams from ESPN.
    Returns dict keyed by norm_fn(team_name) ->
        {'has_injury': bool, 'players': [{'name', 'status', 'position'}]}
    Only flags meaningful injuries: Out or Doubtful status.
    """
    ESPN_INJURIES = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                     "mens-college-basketball/teams/{team_id}/injuries")
    ESPN_TEAMS    = ("https://site.api.espn.com/apis/site/v2/sports/basketball/"
                     "mens-college-basketball/teams")
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }
    FLAGGED_STATUSES = {'out', 'doubtful', 'questionable'}

    # Get all teams playing today
    today_teams = set()
    for g in games:
        today_teams.add(norm_fn(g['home_team']))
        today_teams.add(norm_fn(g['away_team']))

    injury_map = {}

    try:
        # Step 1: get ESPN team IDs for today's teams
        r = requests.get(ESPN_TEAMS, params={'limit': 500}, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return {}

        team_id_map = {}  # norm_name -> espn_team_id
        for team in r.json().get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', []):
            t = team.get('team', {})
            raw_name = t.get('displayName', '')
            # Strip nickname
            parts = raw_name.rsplit(' ', 1)
            if len(parts) == 2 and parts[1] in ODDS_NICKNAMES:
                raw_name = parts[0]
            parts3 = raw_name.rsplit(' ', 2)
            if len(parts3) == 3:
                two = parts3[1] + ' ' + parts3[2]
                if two in ODDS_NICKNAMES:
                    raw_name = parts3[0]
            norm_name = norm_fn(raw_name.strip())
            if norm_name in today_teams:
                team_id_map[norm_name] = t.get('id')

        # Step 2: fetch injuries for each matched team
        for norm_name, team_id in team_id_map.items():
            if not team_id:
                continue
            try:
                ri = requests.get(
                    ESPN_INJURIES.format(team_id=team_id),
                    headers=HEADERS, timeout=10
                )
                if ri.status_code != 200:
                    continue
                data = ri.json()
                injured = []
                for item in data.get('injuries', []):
                    status = item.get('status', '').lower()
                    if any(s in status for s in FLAGGED_STATUSES):
                        athlete = item.get('athlete', {})
                        injured.append({
                            'name':     athlete.get('displayName', '?'),
                            'position': athlete.get('position', {}).get('abbreviation', '?'),
                            'status':   item.get('status', '?'),
                        })
                if injured:
                    injury_map[norm_name] = injured
                time.sleep(0.2)
            except Exception:
                continue

    except Exception:
        pass

    return injury_map
    home = norm_fn(home_raw)
    away = norm_fn(away_raw)
    gd_str = str(target_date)

    row = {c: None for c in feature_cols}
    # Look up conf_game and neutral_site from Torvik schedule
    sched_key  = frozenset([home, away])
    sched_data = (schedule_info or {}).get(sched_key, {})
    conf_game_val    = sched_data.get('conf_game', 0)
    neutral_site_val = sched_data.get('neutral_site', 0)
    conf_code        = sched_data.get('conf_code', '')
    # Tournament flags — conf tournaments end with -T in Torvik conf code
    # NCAA tournament games are neutral site, non-conf, in March/April
    is_conf_tourn = int(bool(conf_code and conf_code.endswith('-T')))
    is_ncaa_tourn = int(
        neutral_site_val == 1 and
        conf_game_val == 0 and
        target_date.month in (3, 4) and
        not is_conf_tourn
    )
    row.update({'neutral_site': neutral_site_val, 'conf_game': conf_game_val, 'spread': spread,
                'hca_adj': 3.2, 'rest_diff': 0, 'home_rest': 4, 'away_rest': 4,
                'home_b2b': 0, 'away_b2b': 0,
                'is_conf_tournament': is_conf_tourn,
                'is_ncaa_tournament': is_ncaa_tourn})

    def tvd_snap(team):
        rows = conn.execute("""
            SELECT snapshot_date, adj_em, adj_o, adj_d, barthag,
                   efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d,
                   two_p_o, two_p_d, three_p_o, three_p_d, blk_pct, ast_pct
            FROM torvik_daily
            WHERE team=? ORDER BY snapshot_date DESC""", (team,)).fetchall()
        for r in rows:
            snap = str(r[0])
            if len(snap) == 8: snap = f"{snap[:4]}-{snap[4:6]}-{snap[6:8]}"
            if snap[:10] < gd_str: return r[1:]  # strip snapshot_date
        return None

    def kpd_snap(team):
        rows = conn.execute("""
            SELECT snapshot_date, adj_em, adj_o, adj_d, adj_tempo, luck, sos, sos_o, sos_d,
                   rank_adj_em, pythag FROM kenpom_daily
            WHERE team=? ORDER BY snapshot_date DESC""", (team,)).fetchall()
        for r in rows:
            if str(r[0])[:10] < gd_str: return r[1:]
        return None

    # TVD
    th, ta = tvd_snap(home), tvd_snap(away)
    if th:
        for i, k in enumerate(['h_tvd_adj_em','h_tvd_adj_o','h_tvd_adj_d','h_tvd_barthag']):
            row[k] = th[i]
        row['has_tvd_home'] = 1
    else: row['has_tvd_home'] = 0
    if ta:
        for i, k in enumerate(['a_tvd_adj_em','a_tvd_adj_o','a_tvd_adj_d','a_tvd_barthag']):
            row[k] = ta[i]
        row['has_tvd_away'] = 1
    else: row['has_tvd_away'] = 0
    if th and ta:
        row['tvd_em_gap']  = (th[0] or 0) - (ta[0] or 0)
        row['tvd_bar_gap'] = (th[3] or 0) - (ta[3] or 0)

    # KPD
    kh, ka = kpd_snap(home), kpd_snap(away)
    kpd_keys_h = ['h_kpd_adj_em','h_kpd_adj_o','h_kpd_adj_d','h_kpd_adj_tempo',
                  'h_kpd_luck','h_kpd_sos','h_kpd_sos_o','h_kpd_sos_d','h_kpd_rank_adj_em','h_kpd_pythag']
    kpd_keys_a = ['a_kpd_adj_em','a_kpd_adj_o','a_kpd_adj_d','a_kpd_adj_tempo',
                  'a_kpd_luck','a_kpd_sos','a_kpd_sos_o','a_kpd_sos_d','a_kpd_rank_adj_em','a_kpd_pythag']
    if kh:
        for i, k in enumerate(kpd_keys_h): row[k] = kh[i]
        row['has_kpd_home'] = 1
    else: row['has_kpd_home'] = 0
    if ka:
        for i, k in enumerate(kpd_keys_a): row[k] = ka[i]
        row['has_kpd_away'] = 1
    else: row['has_kpd_away'] = 0
    if kh and ka:
        row['kpd_em_gap']   = (kh[0] or 0) - (ka[0] or 0)
        row['kpd_luck_gap'] = (kh[4] or 0) - (ka[4] or 0)
        row['kpd_sos_gap']  = (kh[5] or 0) - (ka[5] or 0)

    # KP fanmatch
    fm = conn.execute("""
        SELECT home_team, home_pred, away_pred, home_wp, pred_tempo FROM kenpom_fanmatch
        WHERE game_date=? AND ((home_team=? AND away_team=?) OR (home_team=? AND away_team=?))
        LIMIT 1""", (gd_str, home, away, away, home)).fetchone()
    if fm:
        flipped = (fm[0] != home)
        h_pred = float(fm[2] if flipped else fm[1]) if fm[1] else None
        a_pred = float(fm[1] if flipped else fm[2]) if fm[2] else None
        h_wp   = (1 - float(fm[3])/100 if flipped else float(fm[3])/100) if fm[3] else None
        row.update({'kp_home_pred': h_pred, 'kp_away_pred': a_pred, 'kp_home_wp': h_wp,
                    'kp_pred_margin': (h_pred - a_pred) if h_pred and a_pred else None,
                    'kp_pred_tempo': float(fm[4]) if fm[4] else None, 'has_kp_fanmatch': 1})
    else:
        row['has_kp_fanmatch'] = 0

    return row, home, away


def compute_ev(p): return p * (1 + PAYOUT) - 1

def compute_ml_ev(p_win, ml):
    """EV for a moneyline bet given P(win) and American odds."""
    ml = float(ml)
    payout = (ml / 100) if ml > 0 else (100 / abs(ml))
    return p_win * payout - (1 - p_win)

def p_cover_to_p_win(p_cover, spread, sigma):
    """
    Convert P(home covers spread) → P(home wins outright) using normal distribution.
    spread is negative for home favorites (e.g., -3.5).
    """
    from scipy.stats import norm
    cover_threshold = -spread   # home needs to win by this many points
    z  = norm.ppf(1 - p_cover)
    mu = cover_threshold - sigma * z
    return float(np.clip(norm.sf(0, loc=mu, scale=sigma), 0.01, 0.99))

def ml_kelly_size(ev, bankroll, kelly_frac=KELLY_FRAC, max_pct=MAX_BET_PCT):
    """Kelly sizing for ML bets (payout varies, not fixed -110)."""
    # ev = p*payout - (1-p), solve for p: p = (ev+1)/(payout+1)
    # We approximate payout from ev (not perfect but close enough for sizing)
    return kelly_size(ev, bankroll, kelly_frac, max_pct)

def kelly_size(ev, bankroll, kelly_frac=None):
    p = (ev + 1) / (1 + PAYOUT)
    q = 1 - p
    b = PAYOUT
    kelly = max(0, (b * p - q) / b)
    frac = kelly_frac if kelly_frac is not None else KELLY_FRAC
    return round(bankroll * min(kelly * frac, MAX_BET_PCT), 2)


def format_bet_line(b):
    btype = b.get('bet_type', 'spread')
    if btype == 'total':
        direction = b.get('total_dir', 'Over')
        line = f"{direction} {b.get('total','?')}"
        line_short = f"{direction[0]}{b.get('total','?')}"
        return line_short, line
    elif btype == 'ml':
        team = b['home_norm'] if b['bet_side'] == 'home' else b['away_norm']
        ml = b.get('ml_home') if b['bet_side'] == 'home' else b.get('ml_away')
        ml_str = f"{ml:+d}" if ml else "ML"
        return ml_str, f"{team} ML {ml_str}"
    else:  # spread
        team = b['home_norm'] if b['bet_side'] == 'home' else b['away_norm']
        side = 'HOME' if b['bet_side'] == 'home' else 'AWAY'
        sprd = b['spread'] if b['bet_side'] == 'home' else -b['spread']
        return f"{sprd:+.1f}", f"{team} {sprd:+.1f} ({side})"


def print_card(bets, target_date, bankroll, ev_thresh):
    print("\n" + "="*90)
    print(f"  NCAAB BET CARD — {target_date.strftime('%A, %B %d %Y')}")
    print(f"  Bankroll: ${bankroll:,.0f} | EV≥{ev_thresh*100:.0f}%")
    print("="*90)
    if not bets:
        print("  No qualifying bets today.")
        print("="*90)
        return
    total_risk = sum(b['bet_size'] for b in bets)
    print(f"  {len(bets)} bet(s) | Total at risk: ${total_risk:,.0f} ({total_risk/bankroll*100:.1f}%)")

    # Sort by game time first, then by EV descending within same game
    def sort_key(b):
        try:
            gt = str(b.get('game_time', ''))
            return (gt[:19], -b['ev'])  # truncate to second for stable sort
        except Exception:
            return ('', -b['ev'])

    print(f"\n  {'TIME':<8} {'MATCHUP':<30} {'TYPE':<8} {'LINE':>7} {'P(W)':>6} {'EDGE':>6} {'EV':>7}  {'BET ON':<24} {'SIZE':>7}")
    print("  " + "-"*94)
    for b in sorted(bets, key=sort_key):
        matchup = f"{b['away_norm']} @ {b['home_norm']}"
        if len(matchup) > 29: matchup = matchup[:26] + "..."
        btype = b.get('bet_type', 'SPREAD').upper()
        line_short, bet_desc = format_bet_line(b)
        if len(bet_desc) > 23: bet_desc = bet_desc[:20] + "..."
        fm = " ★" if b.get('has_fanmatch') else "  "
        inj = " ⚠" if b.get('injury_flag') else ""
        edge = b.get('edge_pts', 0)
        # Format game time as HH:MM ET
        try:
            from zoneinfo import ZoneInfo
            gt_raw = str(b.get('game_time', ''))
            gt_dt  = datetime.fromisoformat(gt_raw)
            if gt_dt.tzinfo is not None:
                et = gt_dt.astimezone(ZoneInfo('America/New_York'))
                time_str = et.strftime('%I:%M%p').lstrip('0')
            else:
                time_str = gt_raw[11:16]
        except Exception:
            time_str = '?'
        print(f"  {time_str:<8} {matchup:<30} {btype:<8} {line_short:>7} {b['p_cover']:>6.3f} "
              f"{edge:>+5.1f}% {b['ev']:>+7.3f}  {bet_desc:<24} ${b['bet_size']:>6,.0f}{fm}")
    print("  " + "-"*94)
    print("  ★ KenPom fanmatch  |  EDGE = P(win) − 52.38% breakeven  |  ¼-Kelly, max 2%")
    print("="*94)


def fetch_scores(target_date):
    """
    Fetch final scores from OddsAPI for games on target_date.
    Cost: 2 credits per call. Returns completed games only.
    """
    if not ODDS_KEY:
        print("  ERROR: ODDS_API_KEY not set")
        return []
    url = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/scores/"
    params = {'apiKey': ODDS_KEY, 'daysFrom': 3}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            print(f"  OddsAPI scores HTTP {r.status_code}")
            return []
        data = r.json()
        remaining = r.headers.get('X-Requests-Remaining', '?')
        print(f"  OddsAPI scores: {len(data)} events | remaining: {remaining}")
        # Filter to completed games on target_date
        results = []
        for g in data:
            if not g.get('completed'):
                continue
            gdt = datetime.fromisoformat(g['commence_time'].replace('Z', '+00:00'))
            if gdt.date() != target_date:
                continue
            scores = {s['name']: int(s['score']) for s in (g.get('scores') or [])}
            if len(scores) == 2:
                results.append({
                    'home_team': g['home_team'],
                    'away_team': g['away_team'],
                    'scores':    scores,
                })
        return results
    except Exception as e:
        print(f"  OddsAPI scores error: {e}")
        return []


def update_results(target_date, norm_fn):
    """
    Load saved bets for target_date, fetch final scores,
    mark each bet won/lost, print P&L summary, save results JSON.
    """
    bets_path    = os.path.join(OUT_DIR, f'bets_{target_date}.json')
    results_path = os.path.join(OUT_DIR, f'results_{target_date}.json')

    if not os.path.exists(bets_path):
        print(f"  No bets file found: {bets_path}")
        print(f"  Run without --update-results first to generate today's card.")
        return

    with open(bets_path) as f:
        bets = json.load(f)

    if not bets:
        print("  No bets to update.")
        return

    print(f"  Loaded {len(bets)} bets from {bets_path}")
    scores = fetch_scores(target_date)

    if not scores:
        print("  No completed scores available yet — try again after games finish.")
        return

    # Build scores lookup keyed by (home_norm, away_norm)
    scores_map = {}
    for g in scores:
        h_norm = norm_fn(g['home_team'])
        a_norm = norm_fn(g['away_team'])
        scores_map[(h_norm, a_norm)] = g['scores']
        scores_map[(a_norm, h_norm)] = g['scores']  # both directions

    results   = []
    total_wagered = 0
    total_pnl     = 0
    wins = losses = pushes = pending = 0

    PAYOUT_FIXED = 100 / 110

    for b in bets:
        h = b.get('home_norm', norm_fn(b.get('home_team', '')))
        a = b.get('away_norm', norm_fn(b.get('away_team', '')))
        game_scores = scores_map.get((h, a))

        outcome = b.copy()

        if game_scores is None:
            outcome['result'] = 'pending'
            outcome['pnl']    = 0
            pending += 1
            results.append(outcome)
            continue

        # Get home/away scores
        home_raw = b.get('home_team', h)
        away_raw = b.get('away_team', a)
        h_score = game_scores.get(home_raw) or game_scores.get(h)
        a_score = game_scores.get(away_raw) or game_scores.get(a)

        # Try all keys if still None
        if h_score is None or a_score is None:
            for name, score in game_scores.items():
                n = norm_fn(name)
                if n == h: h_score = score
                if n == a: a_score = score

        if h_score is None or a_score is None:
            outcome['result'] = 'pending'
            outcome['pnl']    = 0
            pending += 1
            results.append(outcome)
            continue

        margin      = h_score - a_score   # positive = home won
        bet_type    = b.get('bet_type', 'spread')
        bet_side    = b.get('bet_side', 'home')
        spread      = b.get('spread', 0) or 0
        total_line  = b.get('total', 0)   or 0
        bet_size    = b.get('bet_size', 0)
        ml_home     = b.get('ml_home')
        ml_away     = b.get('ml_away')

        won = False
        if bet_type == 'spread':
            if bet_side == 'home':
                won = (margin + spread) > 0   # home covers
            else:
                won = (margin + spread) < 0   # away covers
            payout_rate = PAYOUT_FIXED

        elif bet_type == 'total':
            actual_total = h_score + a_score
            if bet_side == 'over':
                won = actual_total > total_line
            else:
                won = actual_total < total_line
            payout_rate = PAYOUT_FIXED

        elif bet_type == 'ml':
            if bet_side == 'home':
                won = margin > 0
                ml  = ml_home
            else:
                won = margin < 0
                ml  = ml_away
            ml = float(ml) if ml else -110
            payout_rate = (ml / 100) if ml > 0 else (100 / abs(ml))

        else:
            won = False
            payout_rate = PAYOUT_FIXED

        # Check push (spread/total lands exactly on line)
        is_push = False
        if bet_type == 'spread' and (margin + spread) == 0:
            is_push = True
        elif bet_type == 'total' and (h_score + a_score) == total_line:
            is_push = True

        if is_push:
            pnl = 0
            outcome['result'] = 'push'
            pushes += 1
        elif won:
            pnl = bet_size * payout_rate
            outcome['result'] = 'win'
            wins += 1
        else:
            pnl = -bet_size
            outcome['result'] = 'loss'
            losses += 1

        outcome['home_score']  = h_score
        outcome['away_score']  = a_score
        outcome['actual_margin'] = margin
        outcome['pnl']           = round(pnl, 2)
        total_wagered += bet_size
        total_pnl     += pnl
        results.append(outcome)

    # Print summary
    settled = wins + losses + pushes
    print()
    print("=" * 60)
    print(f"  RESULTS — {target_date}")
    print("=" * 60)
    print(f"  {wins}W / {losses}L / {pushes}P  ({pending} pending)")
    if settled > 0:
        wr = wins / settled
        roi = total_pnl / total_wagered if total_wagered > 0 else 0
        print(f"  Win rate: {wr:.1%} | P&L: ${total_pnl:+,.2f} | ROI: {roi:+.1%}")
        print(f"  Total wagered: ${total_wagered:,.0f}")
    print()

    # Per-bet breakdown
    print(f"  {'MATCHUP':<30} {'TYPE':<8} {'SIDE':<20} {'SIZE':>6} {'RESULT':<7} {'P&L':>8}")
    print("  " + "-"*82)
    for r in sorted(results, key=lambda x: x.get('result','') == 'pending'):
        matchup = f"{r.get('away_norm','')} @ {r.get('home_norm','')}"
        if len(matchup) > 29: matchup = matchup[:26] + "..."
        btype   = r.get('bet_type','').upper()
        side    = r.get('bet_side','')
        size    = r.get('bet_size', 0)
        result  = r.get('result', 'pending').upper()
        pnl     = r.get('pnl', 0)
        score   = f"({r.get('home_score','?')}-{r.get('away_score','?')})" if r.get('home_score') is not None else ''
        print(f"  {matchup:<30} {btype:<8} {side:<20} ${size:>5,.0f} {result:<7} ${pnl:>+8.2f} {score}")
    print("=" * 60)

    # Save results
    with open(results_path, 'w') as f:
        json.dump({
            'date':          str(target_date),
            'wins':          wins,
            'losses':        losses,
            'pushes':        pushes,
            'pending':       pending,
            'total_wagered': round(total_wagered, 2),
            'total_pnl':     round(total_pnl, 2),
            'bets':          results,
        }, f, indent=2, default=str)
    print(f"\nSaved: {results_path}")

    # Update season P&L tracker
    update_season_pnl(target_date, wins, losses, pushes, total_wagered, total_pnl, bankroll, results=results)


def update_season_pnl(target_date, wins, losses, pushes, wagered, pnl, bankroll, results=None):
    """
    Accumulate daily results into outputs/season_pnl.json.
    Tracks running P&L, ROI, win rate across the season.
    """
    pnl_path = os.path.join(OUT_DIR, 'season_pnl.json')

    # Load existing
    if os.path.exists(pnl_path):
        with open(pnl_path) as f:
            tracker = json.load(f)
    else:
        tracker = {
            'season_start': str(target_date),
            'bankroll_start': bankroll,
            'days': [],
            'totals': {'wins': 0, 'losses': 0, 'pushes': 0,
                       'wagered': 0.0, 'pnl': 0.0},
            'by_type': {'spread': {'w':0,'l':0,'p':0,'pnl':0.0},
                        'total':  {'w':0,'l':0,'p':0,'pnl':0.0},
                        'ml':     {'w':0,'l':0,'p':0,'pnl':0.0}},
        }

    # Ensure by_type exists in older tracker files
    if 'by_type' not in tracker:
        tracker['by_type'] = {'spread': {'w':0,'l':0,'p':0,'pnl':0.0},
                              'total':  {'w':0,'l':0,'p':0,'pnl':0.0},
                              'ml':     {'w':0,'l':0,'p':0,'pnl':0.0}}

    # Remove existing entry for this date if re-running
    tracker['days'] = [d for d in tracker['days'] if d['date'] != str(target_date)]

    # Add today
    settled = wins + losses + pushes
    tracker['days'].append({
        'date':    str(target_date),
        'wins':    wins,
        'losses':  losses,
        'pushes':  pushes,
        'wagered': round(wagered, 2),
        'pnl':     round(pnl, 2),
        'roi':     round(pnl / wagered, 4) if wagered > 0 else 0,
        'wr':      round(wins / settled, 4) if settled > 0 else 0,
    })
    tracker['days'].sort(key=lambda x: x['date'])

    # Recompute totals from all days
    t = tracker['totals']
    t['wins']    = sum(d['wins']    for d in tracker['days'])
    t['losses']  = sum(d['losses']  for d in tracker['days'])
    t['pushes']  = sum(d['pushes']  for d in tracker['days'])
    t['wagered'] = round(sum(d['wagered'] for d in tracker['days']), 2)
    t['pnl']     = round(sum(d['pnl']     for d in tracker['days']), 2)
    total_settled = t['wins'] + t['losses'] + t['pushes']
    t['roi']     = round(t['pnl'] / t['wagered'], 4) if t['wagered'] > 0 else 0
    t['wr']      = round(t['wins'] / total_settled, 4) if total_settled > 0 else 0
    t['bets']    = total_settled

    # Update by-type breakdown from today's results
    if results:
        for r in results:
            btype = r.get('bet_type', 'spread').lower()
            if btype not in tracker['by_type']:
                tracker['by_type'][btype] = {'w':0,'l':0,'p':0,'pnl':0.0}
            result = r.get('result','pending').lower()
            p = r.get('pnl', 0)
            if result == 'win':   tracker['by_type'][btype]['w']   += 1
            elif result == 'loss': tracker['by_type'][btype]['l']  += 1
            elif result == 'push': tracker['by_type'][btype]['p']  += 1
            tracker['by_type'][btype]['pnl'] = round(
                tracker['by_type'][btype]['pnl'] + p, 2)

    with open(pnl_path, 'w') as f:
        json.dump(tracker, f, indent=2, default=str)

    # Print season summary
    print()
    print("=" * 55)
    print("  SEASON P&L SUMMARY")
    print("=" * 55)
    print(f"  Record:    {t['wins']}W / {t['losses']}L / {t['pushes']}P  ({t['bets']} bets)")
    print(f"  Win rate:  {t['wr']:.1%}")
    print(f"  P&L:       ${t['pnl']:+,.2f}")
    print(f"  ROI:       {t['roi']:+.1%}")
    print(f"  Wagered:   ${t['wagered']:,.0f}")

    # By-type breakdown
    print(f"\n  {'Type':<10} {'W-L-P':<12} {'P&L':>10} {'ROI':>8}")
    print(f"  {'─'*42}")
    for btype, s in tracker['by_type'].items():
        total_s = s['w'] + s['l'] + s['p']
        if total_s == 0: continue
        # Estimate wagered (approximate from P&L and win rate)
        wl = s['w'] + s['l']
        approx_wagered = wl * 100 if wl > 0 else 1
        roi_str = f"{s['pnl']/approx_wagered:+.1%}" if approx_wagered > 0 else 'N/A'
        print(f"  {btype:<10} {s['w']}-{s['l']}-{s['p']:<8} ${s['pnl']:>+9.2f} {roi_str:>8}")

    if len(tracker['days']) > 1:
        running = 0
        print(f"\n  {'Date':<12} {'W-L-P':<10} {'Day P&L':>9} {'Running':>10}")
        print(f"  {'─'*44}")
        for d in tracker['days']:
            running += d['pnl']
            wlp = f"{d['wins']}-{d['losses']}-{d['pushes']}"
            print(f"  {d['date']:<12} {wlp:<10} ${d['pnl']:>+8.2f} ${running:>+9.2f}")
    print("=" * 55)
    print(f"  Saved: {pnl_path}")
    """
    Accumulate daily results into outputs/season_pnl.json.
    Tracks running P&L, ROI, win rate across the season.
    """
    pnl_path = os.path.join(OUT_DIR, 'season_pnl.json')

    # Load existing
    if os.path.exists(pnl_path):
        with open(pnl_path) as f:
            tracker = json.load(f)
    else:
        tracker = {
            'season_start': str(target_date),
            'bankroll_start': bankroll,
            'days': [],
            'totals': {'wins': 0, 'losses': 0, 'pushes': 0,
                       'wagered': 0.0, 'pnl': 0.0}
        }

    # Remove existing entry for this date if re-running
    tracker['days'] = [d for d in tracker['days'] if d['date'] != str(target_date)]

    # Add today
    settled = wins + losses + pushes
    tracker['days'].append({
        'date':    str(target_date),
        'wins':    wins,
        'losses':  losses,
        'pushes':  pushes,
        'wagered': round(wagered, 2),
        'pnl':     round(pnl, 2),
        'roi':     round(pnl / wagered, 4) if wagered > 0 else 0,
        'wr':      round(wins / settled, 4) if settled > 0 else 0,
    })
    tracker['days'].sort(key=lambda x: x['date'])

    # Recompute totals from all days
    t = tracker['totals']
    t['wins']    = sum(d['wins']    for d in tracker['days'])
    t['losses']  = sum(d['losses']  for d in tracker['days'])
    t['pushes']  = sum(d['pushes']  for d in tracker['days'])
    t['wagered'] = round(sum(d['wagered'] for d in tracker['days']), 2)
    t['pnl']     = round(sum(d['pnl']     for d in tracker['days']), 2)
    total_settled = t['wins'] + t['losses'] + t['pushes']
    t['roi']     = round(t['pnl'] / t['wagered'], 4) if t['wagered'] > 0 else 0
    t['wr']      = round(t['wins'] / total_settled, 4) if total_settled > 0 else 0
    t['bets']    = total_settled

    with open(pnl_path, 'w') as f:
        json.dump(tracker, f, indent=2, default=str)

    # Print season summary
    print()
    print("=" * 50)
    print("  SEASON P&L SUMMARY")
    print("=" * 50)
    print(f"  Record:    {t['wins']}W / {t['losses']}L / {t['pushes']}P  ({t['bets']} bets)")
    print(f"  Win rate:  {t['wr']:.1%}")
    print(f"  P&L:       ${t['pnl']:+,.2f}")
    print(f"  ROI:       {t['roi']:+.1%}")
    print(f"  Wagered:   ${t['wagered']:,.0f}")
    if len(tracker['days']) > 1:
        # Running P&L by day
        running = 0
        print(f"\n  {'Date':<12} {'W-L-P':<10} {'Day P&L':>9} {'Running':>10}")
        print(f"  {'─'*44}")
        for d in tracker['days']:
            running += d['pnl']
            wlp = f"{d['wins']}-{d['losses']}-{d['pushes']}"
            print(f"  {d['date']:<12} {wlp:<10} ${d['pnl']:>+8.2f} ${running:>+9.2f}")
    print("=" * 50)
    print(f"  Saved: {pnl_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',           type=str,  help='YYYY-MM-DD (default: today)')
    parser.add_argument('--bankroll',       type=float, default=BANKROLL)
    parser.add_argument('--ev',             type=float, default=EV_MIN)
    parser.add_argument('--demo',           action='store_true', help='Use demo games for testing')
    parser.add_argument('--update-results', action='store_true', help='Fetch final scores and mark bet outcomes')
    parser.add_argument('--kelly',          type=float, default=None, help='Kelly fraction (default: 0.25)')
    parser.add_argument('--replay',         action='store_true', help='Load lines from DB instead of OddsAPI (for past dates)')
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    bankroll    = args.bankroll
    ev_thresh   = args.ev
    kelly_frac  = args.kelly  # None = use default KELLY_FRAC (0.25)

    # --update-results: fetch scores and mark outcomes for a saved bet card
    if args.update_results:
        print(f"\nNCAAB Results Update — {target_date}")
        print("=" * 50)
        try:
            norm_fn = get_norm_func()
        except Exception:
            norm_fn = lambda x: x
        update_results(target_date, norm_fn)
        sys.exit(0)

    print(f"\nNCAAB Daily Bets — {target_date}")
    print("="*50)

    # Load all models
    result = load_model()
    model, imputer, feature_cols = result[0], result[1], result[2]
    totals_model    = result[3] if len(result) > 3 else None
    totals_imputer  = result[4] if len(result) > 4 else None
    totals_features = result[5] if len(result) > 5 else None
    ml_params       = result[6] if len(result) > 6 else None
    ml_sigma        = ml_params['margin_std'] if ml_params else 11.5  # fallback

    if not model: sys.exit(1)

    try:
        norm_fn = get_norm_func()
    except Exception as e:
        print(f"  Warning: could not load norm() from build_features: {e}")
        norm_fn = lambda x: x

    if args.demo:
        games = [
            {'home_team': 'Duke',    'away_team': 'North Carolina', 'spread': -4.5,
             'total': 145.5, 'ml_home': -180, 'ml_away': 150, 'game_time': datetime.now()},
            {'home_team': 'Kansas',  'away_team': 'Baylor',         'spread': -3.0,
             'total': 152.0, 'ml_home': -145, 'ml_away': 122, 'game_time': datetime.now()},
            {'home_team': 'Gonzaga', 'away_team': "Saint Mary's",   'spread': -6.5,
             'total': 148.5, 'ml_home': -240, 'ml_away': 198, 'game_time': datetime.now()},
        ]
        print(f"  DEMO MODE: {len(games)} games")
        injury_map = {}
    elif args.replay:
        # Load lines from DB for a past date (no OddsAPI call)
        _conn = sqlite3.connect(DB)
        _rows = _conn.execute("""
            SELECT gl.home_team, gl.away_team, gl.spread, gl.over_under,
                   gl.home_moneyline, gl.away_moneyline, g.neutral_site
            FROM game_lines gl
            JOIN games g ON gl.game_date=g.game_date
                AND gl.home_team=g.home_team AND gl.away_team=g.away_team
            WHERE gl.game_date=? AND gl.spread IS NOT NULL
        """, (str(target_date),)).fetchall()
        _conn.close()
        games = []
        for home, away, spread, total, ml_home, ml_away, neutral in _rows:
            games.append({
                'home_team': home, 'away_team': away,
                'spread': spread, 'total': total,
                'ml_home': ml_home, 'ml_away': ml_away,
                'game_time': datetime.now(),
            })
        print(f"  REPLAY MODE: {len(games)} games from DB for {target_date}")
        injury_map = {}
    else:
        odds_data = fetch_todays_lines(target_date)
        games = parse_lines(odds_data, target_date)
        print(f"  DraftKings: {len(games)} games on {target_date}")

        # Store opening/closing lines for future LINE_MOVE feature use
        if games:
            lm_conn = sqlite3.connect(DB)
            n_stored = store_line_movement(games, target_date, lm_conn, norm_fn)
            lm_conn.close()
            print(f"  Line movement: {n_stored} games logged")

        # Fetch today's referee assignments from ESPN
        n_refs = fetch_todays_refs(games, target_date, norm_fn)
        if n_refs > 0:
            print(f"  Refs: {n_refs} games scraped")

        # Fetch injury reports
        injury_map = fetch_injury_flags(games, norm_fn)
        if injury_map:
            print(f"  Injuries: {len(injury_map)} teams with flagged players")
            for team, players in injury_map.items():
                for p in players:
                    print(f"    ⚠  {team}: {p['name']} ({p['position']}) — {p['status']}")
        else:
            injury_map = {}

    if not games:
        print("No games found. Use --demo for testing.")
        sys.exit(0)

    # Fetch Torvik schedule for conf_game / neutral_site flags
    print("  Fetching schedule context from Torvik...")
    schedule_info = fetch_torvik_schedule(target_date)
    print(f"  Schedule: {len(schedule_info)} games found")

    conn = sqlite3.connect(DB)
    bets = []

    print(f"\nScoring {len(games)} game(s) across SPREAD / TOTAL / ML markets...")
    print(f"  {'Game':<40} {'MKT':<7} {'P':>6} {'EDGE':>7} {'EV':>8}  Status")
    print("  " + "-"*74)

    for g in games:
        spread  = g['spread']
        total   = g.get('total')
        ml_home = g.get('ml_home')
        ml_away = g.get('ml_away')

        label = f"{norm_fn(g['away_team'])} @ {norm_fn(g['home_team'])}"
        if len(label) > 39: label = label[:36] + "..."

        # Injury warning flag
        h_norm = norm_fn(g['home_team'])
        a_norm = norm_fn(g['away_team'])
        h_injured = injury_map.get(h_norm, [])
        a_injured = injury_map.get(a_norm, [])
        inj_flag = ''
        if h_injured or a_injured:
            inj_flag = ' ⚠'

        # ── SPREAD ────────────────────────────────────────────────────────
        if SPREAD_LO <= abs(spread) <= SPREAD_HI:
            try:
                row, hn, an = build_features(
                    g['home_team'], g['away_team'],
                    spread, target_date, conn, feature_cols, norm_fn, schedule_info
                )
                X    = pd.DataFrame([row])[feature_cols]
                Ximp = pd.DataFrame(imputer.transform(X), columns=feature_cols)
                p    = float(model.predict_proba(Ximp)[0][1])  # P(home covers)

                ev_h = compute_ev(p)
                ev_a = compute_ev(1 - p)
                best_spread_ev  = max(ev_h, ev_a)
                spread_side = 'home' if ev_h >= ev_a else 'away'
                p_spread = p if spread_side == 'home' else (1 - p)
                edge_spread = (p_spread - 0.5238) * 100

                qualifies = best_spread_ev >= ev_thresh
                status = f"✓ BET {'HOME' if spread_side=='home' else 'AWAY'}" if qualifies else "—"
                print(f"  {label:<40} {'SPREAD':<7} {p_spread:>6.3f} {edge_spread:>+6.1f}% {best_spread_ev:>+8.3f}  {status}")

                if qualifies:
                    sz = kelly_size(best_spread_ev, bankroll, kelly_frac)
                    if sz >= 10:
                        sprd_display = spread if spread_side == 'home' else -spread
                        bets.append({
                            'home_team': g['home_team'], 'away_team': g['away_team'],
                            'home_norm': hn, 'away_norm': an,
                            'spread': sprd_display, 'total': total,
                            'ml_home': ml_home, 'ml_away': ml_away,
                            'p_cover': p_spread, 'ev': best_spread_ev,
                            'edge_pts': round(edge_spread, 1),
                            'bet_side': spread_side, 'bet_size': sz,
                            'bet_type': 'spread', 'game_time': str(g['game_time']),
                            'has_fanmatch': bool(row.get('has_kp_fanmatch')),
                            'injury_flag': bool(h_injured or a_injured),
                            'injury_detail': [(h_norm, h_injured), (a_norm, a_injured)],
                        })

                # ── MONEYLINE (derived from spread model) ─────────────────
                # ML odds filter based on walk-forward backtest (11_backtest_ml_model.py):
                #   Big favorites (<-200):      ROI +6.7%, WR 76.9% ✓
                #   Mid favorites (-200 to -100): ROI -2.0%         ✗
                #   Near pick-ems (-100 to +100): ROI -10%          ✗
                #   Underdogs (+100 to +200):    ROI +8.2%, WR 44.3% ✓
                # Only bet in the two profitable zones.
                ML_FAV_MAX  = -200  # big favorites only (more negative = bigger fav)
                ML_FAV_MIN  = -400  # cap at -400 (liquidity/non-model info risk)
                ML_DOG_MIN  = +100  # underdogs only (not near pick-ems)
                ML_DOG_MAX  = +200  # cap at +200
                if ml_home is not None and ml_away is not None:
                    p_win = p_cover_to_p_win(p, spread, ml_sigma)
                    ev_ml_h = compute_ml_ev(p_win,       ml_home)
                    ev_ml_a = compute_ml_ev(1 - p_win,   ml_away)

                    # Determine best side and check odds are in range
                    if ev_ml_h >= ev_ml_a:
                        ml_side, p_ml, best_ml_ev, ml_odds = 'home', p_win, ev_ml_h, ml_home
                    else:
                        ml_side, p_ml, best_ml_ev, ml_odds = 'away', 1-p_win, ev_ml_a, ml_away

                    # Apply two-zone odds filter
                    is_big_fav = (ml_odds < 0) and (ML_FAV_MIN <= ml_odds <= ML_FAV_MAX)
                    is_dog     = (ml_odds > 0) and (ML_DOG_MIN <= ml_odds <= ML_DOG_MAX)
                    ml_in_range = is_big_fav or is_dog
                    edge_ml = (p_ml - 0.5238) * 100

                    qual_ml = best_ml_ev >= ev_thresh and ml_in_range
                    if ml_in_range:
                        status_ml = f"✓ BET {'HOME' if ml_side=='home' else 'AWAY'}" if qual_ml else "—"
                        print(f"  {label:<40} {'ML':<7} {p_ml:>6.3f} {edge_ml:>+6.1f}% {best_ml_ev:>+8.3f}  {status_ml}")
                    # Skip printing lines outside odds range (too short or too long)

                    if qual_ml:
                        sz_ml = kelly_size(best_ml_ev, bankroll, kelly_frac)
                        if sz_ml >= 10:
                            bets.append({
                                'home_team': g['home_team'], 'away_team': g['away_team'],
                                'home_norm': hn, 'away_norm': an,
                                'spread': spread, 'total': total,
                                'ml_home': ml_home, 'ml_away': ml_away,
                                'p_cover': p_ml, 'ev': best_ml_ev,
                                'edge_pts': round(edge_ml, 1),
                                'bet_side': ml_side, 'bet_size': sz_ml,
                                'bet_type': 'ml', 'game_time': str(g['game_time']),
                                'has_fanmatch': bool(row.get('has_kp_fanmatch')),
                            })

            except Exception as e:
                print(f"  {label:<40} {'SPREAD':<7} ERROR: {e}")

        else:
            print(f"  {label:<40} {'SPREAD':<7} {'—':>6} {'—':>7} {'—':>8}  SKIP ({spread:+.1f} outside {SPREAD_LO}-{SPREAD_HI}pt)")

        # ── TOTALS ────────────────────────────────────────────────────────
        if totals_model is not None and total is not None:
            try:
                # Build features for totals — reuse spread feature row, add total
                row_t, hn_t, an_t = build_features(
                    g['home_team'], g['away_team'],
                    spread, target_date, conn, totals_features, norm_fn, schedule_info
                )
                row_t['over_under'] = total
                X_t    = pd.DataFrame([row_t])[totals_features]
                Ximp_t = pd.DataFrame(totals_imputer.transform(X_t), columns=totals_features)
                p_over = float(totals_model.predict_proba(Ximp_t)[0][1])

                # ── Tournament pace adjustments ──────────────────────────
                # Based on empirical analysis (analyze_tournament_pace.py):
                # Conf tournament: DK underprices totals by ~+2.0 pts residual
                #   → over rate 56.4% vs 49.3% reg season (N=94 games)
                #   → boost P(over) conservatively using +1.5pt adjustment
                # NCAA tournament: residual -2.16 pts but over rate 48.9%
                #   → not meaningfully different from reg season, skip
                # Adjustment: +1.5pt edge on ~141pt total with ~10pt std
                #   → P boost ≈ norm.cdf(1.5/10) - 0.5 ≈ +0.060
                is_conf_tourn = bool(row_t.get('is_conf_tournament', 0))
                if is_conf_tourn:
                    from scipy.stats import norm as _norm
                    CONF_TOURN_ADJ_PTS = 1.5   # conservative half of +2.0 residual
                    TOTAL_STD          = 10.0   # approx std of total scoring
                    p_boost = _norm.cdf(CONF_TOURN_ADJ_PTS / TOTAL_STD) - 0.5
                    p_over  = float(np.clip(p_over + p_boost, 0.01, 0.99))

                ev_over  = compute_ev(p_over)
                ev_under = compute_ev(1 - p_over)
                best_tot_ev = max(ev_over, ev_under)
                tot_dir  = 'over' if ev_over >= ev_under else 'under'
                p_tot    = p_over if tot_dir == 'over' else (1 - p_over)
                edge_tot = (p_tot - 0.5238) * 100

                qual_tot = best_tot_ev >= ev_thresh
                status_tot = f"✓ BET {'OVER' if tot_dir=='over' else 'UNDER'}" if qual_tot else "—"
                tot_label = f"{('O' if tot_dir=='over' else 'U')}{total}"
                print(f"  {label:<40} {f'TOTAL {tot_label}':<7} {p_tot:>6.3f} {edge_tot:>+6.1f}% {best_tot_ev:>+8.3f}  {status_tot}")

                if qual_tot:
                    sz_t = kelly_size(best_tot_ev, bankroll, kelly_frac)
                    if sz_t >= 10:
                        bets.append({
                            'home_team': g['home_team'], 'away_team': g['away_team'],
                            'home_norm': hn_t, 'away_norm': an_t,
                            'spread': spread, 'total': total,
                            'ml_home': ml_home, 'ml_away': ml_away,
                            'p_cover': p_tot, 'ev': best_tot_ev,
                            'edge_pts': round(edge_tot, 1),
                            'bet_side': tot_dir, 'bet_size': sz_t,
                            'bet_type': 'total', 'total_dir': tot_dir.capitalize(),
                            'game_time': str(g['game_time']),
                            'has_fanmatch': bool(row_t.get('has_kp_fanmatch')),
                        })
            except Exception as e:
                print(f"  {label:<40} {'TOTAL':<7} ERROR: {e}")

    conn.close()
    print()
    print_card(bets, target_date, bankroll, ev_thresh)

    if bets:
        out = os.path.join(OUT_DIR, f'bets_{target_date}.json')
        with open(out, 'w') as f:
            json.dump(bets, f, indent=2, default=str)
        print(f"\nSaved: {out}")
