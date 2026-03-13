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
from datetime import date, datetime
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



def fetch_torvik_schedule(target_date):
    """
    Fetch today's schedule from Torvik to get conf_game and neutral_site flags.
    Torvik cell format: "3 Arizona vs 7 Iowa St. B12-T ESPN"
    Returns dict keyed by frozenset of normalized team names.
    """
    import re
    url = f"https://barttorvik.com/schedule.php?date={target_date.strftime('%Y%m%d')}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return {}
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
                    # Find the cell containing ' vs ' — matchup + conf are in same cell
                    # Format: "3 Arizona vs 7 Iowa St. B12-T ESPN"
                    for cell in self._row:
                        if ' vs ' not in cell:
                            continue
                        # Split on ' vs '
                        parts = cell.split(' vs ', 1)
                        left  = parts[0].strip()   # "3 Arizona"
                        right = parts[1].strip()   # "7 Iowa St. B12-T ESPN"
                        # Strip leading rank number from left team
                        t1 = re.sub(r'^\d+\s+', '', left).strip()
                        # Right side has: rank, team name, conf code, network
                        # Conf code looks like B12-T, SEC-T, ACC-T, B10-T, Non-con etc.
                        # Match pattern: optional rank, team name, then CONF-SUFFIX
                        right_no_rank = re.sub(r'^\d+\s+', '', right)
                        # Extract conf code (all-caps with hyphen, e.g. B12-T, SEC-T)
                        conf_match = re.search(r'([A-Z][A-Z0-9]*-[A-Z])', right_no_rank)
                        conf_code = conf_match.group(1) if conf_match else ''
                        # Strip conf code and anything after from team name
                        if conf_code:
                            t2 = right_no_rank[:right_no_rank.index(conf_code)].strip()
                        else:
                            # No conf code — strip trailing network name (all caps)
                            t2 = re.sub(r'\s+[A-Z0-9]+\s*$', '', right_no_rank).strip()
                        # Determine conf_game and neutral_site from conf code
                        is_conf    = 1 if conf_code and 'Non' not in conf_code else 0
                        is_neutral = 1 if conf_code.endswith('-T') or conf_code.endswith('-N') else 0
                        if t1 and t2:
                            key = frozenset([t1, t2])
                            self.games[key] = {
                                'conf_game':    is_conf,
                                'neutral_site': is_neutral,
                                'conf_code':    conf_code,
                            }
                        break
            def handle_data(self, data):
                if self._in_td: self._cells.append(data)

        parser = ScheduleParser()
        parser.feed(r.text)
        return parser.games
    except Exception as e:
        print(f"  Warning: could not fetch Torvik schedule: {e}")
        return {}


def build_features(home_raw, away_raw, spread, target_date, conn, feature_cols, norm_fn, schedule_info=None):
    home = norm_fn(home_raw)
    away = norm_fn(away_raw)
    gd_str = str(target_date)

    row = {c: None for c in feature_cols}
    # Look up conf_game and neutral_site from Torvik schedule
    sched_key  = frozenset([home, away])
    sched_data = (schedule_info or {}).get(sched_key, {})
    conf_game_val    = sched_data.get('conf_game', 0)
    neutral_site_val = sched_data.get('neutral_site', 0)
    row.update({'neutral_site': neutral_site_val, 'conf_game': conf_game_val, 'spread': spread,
                'hca_adj': 3.2, 'rest_diff': 0, 'home_rest': 4, 'away_rest': 4,
                'home_b2b': 0, 'away_b2b': 0})

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

def kelly_size(ev, bankroll):
    p = (ev + 1) / (1 + PAYOUT)
    q = 1 - p
    b = PAYOUT
    kelly = max(0, (b * p - q) / b)
    return round(bankroll * min(kelly * KELLY_FRAC, MAX_BET_PCT), 2)


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
    print("\n" + "="*78)
    print(f"  NCAAB BET CARD — {target_date.strftime('%A, %B %d %Y')}")
    print(f"  Bankroll: ${bankroll:,.0f} | EV≥{ev_thresh*100:.0f}%")
    print("="*78)
    if not bets:
        print("  No qualifying bets today.")
        print("="*78)
        return
    total_risk = sum(b['bet_size'] for b in bets)
    print(f"  {len(bets)} bet(s) | Total at risk: ${total_risk:,.0f} ({total_risk/bankroll*100:.1f}%)")
    print(f"\n  {'MATCHUP':<32} {'TYPE':<8} {'LINE':>7} {'P(W)':>6} {'EDGE':>6} {'EV':>7}  {'BET ON':<24} {'SIZE':>7}")
    print("  " + "-"*82)
    for b in sorted(bets, key=lambda x: -x['ev']):
        matchup = f"{b['away_norm']} @ {b['home_norm']}"
        if len(matchup) > 31: matchup = matchup[:28] + "..."
        btype = b.get('bet_type', 'SPREAD').upper()
        line_short, bet_desc = format_bet_line(b)
        if len(bet_desc) > 23: bet_desc = bet_desc[:20] + "..."
        fm = " ★" if b.get('has_fanmatch') else "  "
        edge = b.get('edge_pts', 0)
        print(f"  {matchup:<32} {btype:<8} {line_short:>7} {b['p_cover']:>6.3f} {edge:>+5.1f}% {b['ev']:>+7.3f}  {bet_desc:<24} ${b['bet_size']:>6,.0f}{fm}")
    print("  " + "-"*82)
    print("  ★ KenPom fanmatch  |  EDGE = P(win) − 52.38% breakeven  |  ¼-Kelly, max 2%")
    print("="*82)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',     type=str,   help='YYYY-MM-DD (default: today)')
    parser.add_argument('--bankroll', type=float, default=BANKROLL)
    parser.add_argument('--ev',       type=float, default=EV_MIN)
    parser.add_argument('--demo',     action='store_true', help='Use demo games for testing')
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    bankroll    = args.bankroll
    ev_thresh   = args.ev

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
    else:
        odds_data = fetch_todays_lines(target_date)
        games = parse_lines(odds_data, target_date)
        print(f"  DraftKings: {len(games)} games on {target_date}")

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
                    sz = kelly_size(best_spread_ev, bankroll)
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
                        })

                # ── MONEYLINE (derived from spread model) ─────────────────
                if ml_home is not None and ml_away is not None:
                    p_win = p_cover_to_p_win(p, spread, ml_sigma)
                    ev_ml_h = compute_ml_ev(p_win,       ml_home)
                    ev_ml_a = compute_ml_ev(1 - p_win,   ml_away)
                    best_ml_ev  = max(ev_ml_h, ev_ml_a)
                    ml_side = 'home' if ev_ml_h >= ev_ml_a else 'away'
                    p_ml    = p_win if ml_side == 'home' else (1 - p_win)
                    edge_ml = (p_ml - 0.5238) * 100  # vs fair 50/50 baseline

                    qual_ml = best_ml_ev >= ev_thresh
                    status_ml = f"✓ BET {'HOME' if ml_side=='home' else 'AWAY'}" if qual_ml else "—"
                    ml_display = ml_home if ml_side == 'home' else ml_away
                    print(f"  {label:<40} {'ML':<7} {p_ml:>6.3f} {edge_ml:>+6.1f}% {best_ml_ev:>+8.3f}  {status_ml}")

                    if qual_ml:
                        sz_ml = kelly_size(best_ml_ev, bankroll)
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
                    sz_t = kelly_size(best_tot_ev, bankroll)
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
