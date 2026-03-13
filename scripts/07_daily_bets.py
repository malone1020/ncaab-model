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
        return None, None, None
    with open(MODEL_PKL, 'rb') as f:
        model = pickle.load(f)
    with open(IMPUTER_F, 'rb') as f:
        imputer = pickle.load(f)
    with open(FEAT_F) as f:
        meta = json.load(f)
    features = meta['features'] if isinstance(meta, dict) else meta
    combo = meta.get('combo', 'unknown') if isinstance(meta, dict) else 'unknown'
    print(f"  Model: {combo} | {len(features)} features")
    return model, imputer, features


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
    params = {'apiKey': ODDS_KEY, 'regions': 'us', 'markets': 'spreads',
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
        home, away, spread = game.get('home_team',''), game.get('away_team',''), None
        for book in game.get('bookmakers', []):
            if book['key'] != 'draftkings': continue
            for mkt in book.get('markets', []):
                if mkt['key'] != 'spreads': continue
                for oc in mkt.get('outcomes', []):
                    if oc['name'] == home:
                        try: spread = float(oc['point'])
                        except: pass
        if spread is not None:
            games.append({'home_team': home, 'away_team': away,
                          'spread': spread, 'game_time': gdt})
    return games


def build_features(home_raw, away_raw, spread, target_date, conn, feature_cols, norm_fn):
    home = norm_fn(home_raw)
    away = norm_fn(away_raw)
    gd_str = str(target_date)

    row = {c: None for c in feature_cols}
    row.update({'neutral_site': 0, 'conf_game': 0, 'spread': spread,
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

def kelly_size(ev, bankroll):
    p = (ev + 1) / (1 + PAYOUT)
    q = 1 - p
    b = PAYOUT
    kelly = max(0, (b * p - q) / b)
    return round(bankroll * min(kelly * KELLY_FRAC, MAX_BET_PCT), 2)


def print_card(bets, target_date, bankroll, ev_thresh):
    print("\n" + "="*72)
    print(f"  NCAAB BET CARD — {target_date.strftime('%A, %B %d %Y')}")
    print(f"  Bankroll: ${bankroll:,.0f} | EV≥{ev_thresh*100:.0f}% | Spreads {SPREAD_LO}-{SPREAD_HI}pt")
    print("="*72)
    if not bets:
        print("  No qualifying bets today.")
        print("="*72)
        return
    total = sum(b['bet_size'] for b in bets)
    print(f"  {len(bets)} bet(s) | Total exposure: ${total:,.0f} ({total/bankroll*100:.1f}%)")
    print(f"\n  {'MATCHUP':<38} {'LINE':>5} {'P':>6} {'EV':>7}  {'BET':<22} {'SIZE':>7}")
    print("  " + "-"*70)
    for b in sorted(bets, key=lambda x: -x['ev']):
        matchup = f"{b['away_norm']} @ {b['home_norm']}"
        if len(matchup) > 37: matchup = matchup[:34] + "..."
        side  = 'HOME' if b['bet_side'] == 'home' else 'AWAY'
        team  = b['home_norm'] if b['bet_side'] == 'home' else b['away_norm']
        if len(team) > 21: team = team[:18] + "..."
        fm = " ★" if b.get('has_fanmatch') else "  "
        print(f"  {matchup:<38} {b['spread']:>+5.1f} {b['p_cover']:>6.3f} {b['ev']:>+7.3f}  {team+' ('+side+')' :<22} ${b['bet_size']:>6,.0f}{fm}")
    print("  " + "-"*70)
    print("  ★ KenPom fanmatch available  |  ¼-Kelly sizing, max 2% per game")
    print("="*72)


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

    model, imputer, feature_cols = load_model()
    if not model: sys.exit(1)

    try:
        norm_fn = get_norm_func()
    except Exception as e:
        print(f"  Warning: could not load norm() from build_features: {e}")
        norm_fn = lambda x: x

    if args.demo:
        games = [
            {'home_team': 'Duke',    'away_team': 'North Carolina', 'spread': -4.5, 'game_time': datetime.now()},
            {'home_team': 'Kansas',  'away_team': 'Baylor',         'spread': -3.0, 'game_time': datetime.now()},
            {'home_team': 'Gonzaga', 'away_team': "Saint Mary's",   'spread': -6.5, 'game_time': datetime.now()},
        ]
        print(f"  DEMO MODE: {len(games)} games")
    else:
        odds_data = fetch_todays_lines(target_date)
        games = parse_lines(odds_data, target_date)
        print(f"  DraftKings: {len(games)} games on {target_date}")

    if not games:
        print("No games found. Use --demo for testing.")
        sys.exit(0)

    conn = sqlite3.connect(DB)
    bets = []

    print(f"\nScoring {len(games)} game(s)...")
    print(f"  {'Game':<44} {'P(cvr)':>7} {'EV':>8}  Status")
    print("  " + "-"*68)

    for g in games:
        spread = g['spread']
        if not (SPREAD_LO <= abs(spread) <= SPREAD_HI):
            continue
        try:
            row, hn, an = build_features(g['home_team'], g['away_team'],
                                         spread, target_date, conn, feature_cols, norm_fn)
        except Exception as e:
            print(f"  ERROR {g['home_team']} vs {g['away_team']}: {e}")
            continue

        X    = pd.DataFrame([row])[feature_cols]
        Ximp = pd.DataFrame(imputer.transform(X), columns=feature_cols)
        p    = float(model.predict_proba(Ximp)[0][1])

        ev_h = compute_ev(p)
        ev_a = compute_ev(1 - p)
        best_ev  = max(ev_h, ev_a)
        bet_side = 'home' if ev_h >= ev_a else 'away'
        p_disp   = p if bet_side == 'home' else (1 - p)

        label = f"{an} @ {hn}"
        if len(label) > 43: label = label[:40] + "..."
        qualifies = best_ev >= ev_thresh
        status = f"✓ BET {('HOME' if bet_side=='home' else 'AWAY')}" if qualifies else "—"
        print(f"  {label:<44} {p_disp:>7.3f} {best_ev:>+8.3f}  {status}")

        if qualifies:
            sz = kelly_size(best_ev, bankroll)
            if sz >= 10:
                bets.append({
                    'home_team': g['home_team'], 'away_team': g['away_team'],
                    'home_norm': hn, 'away_norm': an,
                    'bet_norm':  hn if bet_side=='home' else an,
                    'spread': spread, 'p_cover': p_disp, 'ev': best_ev,
                    'bet_side': bet_side, 'bet_size': sz,
                    'game_time': str(g['game_time']),
                    'has_fanmatch': bool(row.get('has_kp_fanmatch')),
                })

    conn.close()
    print_card(bets, target_date, bankroll, ev_thresh)

    if bets:
        out = os.path.join(OUT_DIR, f'bets_{target_date}.json')
        with open(out, 'w') as f:
            json.dump(bets, f, indent=2, default=str)
        print(f"\nSaved: {out}")
