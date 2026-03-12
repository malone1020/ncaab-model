"""
07_daily_bets.py
================
Generate today's bet card using the production model.
Fetches today's DraftKings lines and projects each game.

Run: python scripts/07_daily_bets.py
     python scripts/07_daily_bets.py --date 2025-01-15
"""

import sqlite3, os, json, sys, argparse, warnings
from datetime import date, datetime
import pandas as pd
import numpy as np
import requests
from xgboost import XGBRegressor

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

warnings.filterwarnings('ignore')

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
MODEL   = os.path.join(ROOT, 'models', 'production_model.json')
FEAT_F  = os.path.join(ROOT, 'models', 'feature_cols.json')
OUT     = os.path.join(ROOT, 'outputs')

ODDS_KEY     = os.getenv('ODDS_API_KEY', '')
EDGE_MIN     = 3.0
SPREAD_LO    = 0.5
SPREAD_HI    = 9.0
EXCLUDE_LO   = 9.0
EXCLUDE_HI   = 12.0
KELLY_FRAC   = 0.25
MAX_BET_PCT  = 0.02   # max 2% of bankroll per game
BANKROLL     = 10000  # default bankroll


def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def load_model_and_features():
    if not os.path.exists(MODEL):
        print(f"ERROR: Model not found at {MODEL}")
        print("Run python scripts/06_train_final_model.py first")
        return None, None

    model = XGBRegressor()
    model.load_model(MODEL)

    with open(FEAT_F) as f:
        feature_cols = json.load(f)

    return model, feature_cols


def fetch_todays_lines(target_date=None):
    """Fetch NCAA basketball lines from OddsAPI."""
    if not ODDS_KEY:
        print("WARNING: No ODDS_API_KEY found. Using demo data.")
        return []

    url = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/"
    params = {
        'apiKey': ODDS_KEY,
        'regions': 'us',
        'markets': 'spreads',
        'bookmakers': 'draftkings',
        'oddsFormat': 'american',
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"OddsAPI error: {r.status_code}")
            return []
    except Exception as e:
        print(f"OddsAPI error: {e}")
        return []


def parse_lines(odds_data, target_date=None):
    """Extract game spreads from OddsAPI response."""
    games = []
    today = target_date or date.today()

    for game in odds_data:
        game_time = datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00'))
        if game_time.date() != today:
            continue

        home = game.get('home_team', '')
        away = game.get('away_team', '')

        spread = None
        for book in game.get('bookmakers', []):
            if book['key'] != 'draftkings':
                continue
            for market in book.get('markets', []):
                if market['key'] != 'spreads':
                    continue
                for outcome in market.get('outcomes', []):
                    if outcome['name'] == home:
                        try:
                            spread = float(outcome['point'])
                        except:
                            pass

        if spread is not None:
            games.append({
                'home_team': home,
                'away_team': away,
                'spread': spread,
                'game_time': str(game_time),
            })

    return games


def build_game_features(home, away, spread, season, conn, feature_cols):
    """Build feature row for a single today's game."""
    from scripts.team_name_map import normalize  # use your normalization
    home = normalize(home)
    away = normalize(away)

    # Load latest ratings for each team
    # (Use current season-1 for static ratings, and latest snapshot for daily Torvik)
    # This is simplified - in production you'd build full features same as 04_build_features.py

    row = {c: None for c in feature_cols}
    row['neutral_site'] = 0
    row['conf_game'] = 0
    row['spread'] = spread

    # TODO: populate from DB using same logic as 04_build_features.py
    # This is a stub - the full implementation joins all rating tables

    return row


def kelly_size(edge, win_prob, bankroll, kelly_frac=KELLY_FRAC, max_pct=MAX_BET_PCT):
    """Quarter-Kelly bet sizing."""
    # Convert edge to estimated win probability
    # edge = our_line - dk_spread (positive = we like home)
    # Use empirical win rate from backtest: roughly 54% at edge=3+
    p = min(0.58, 0.50 + edge * 0.015)  # rough calibration
    q = 1 - p
    b = 100 / 110  # payout at -110

    kelly = (b * p - q) / b
    fraction = kelly * kelly_frac
    bet = bankroll * min(fraction, max_pct)

    return max(0, round(bet, 2))


def print_bet_card(bets, target_date=None):
    today = target_date or date.today()
    print("\n" + "="*65)
    print(f"  NCAAB BET CARD — {today.strftime('%A, %B %d %Y')}")
    print("="*65)

    if not bets:
        print("  No qualifying bets today (edge < threshold or no games).")
        print("="*65)
        return

    total_exposure = sum(b['bet_size'] for b in bets)
    print(f"  Games: {len(bets)} | Total exposure: ${total_exposure:,.0f}")
    print("-"*65)
    print(f"  {'MATCHUP':<35} {'SPREAD':>7} {'PROJ':>7} {'EDGE':>6} {'BET':>8}")
    print("-"*65)

    for b in sorted(bets, key=lambda x: -abs(x['edge'])):
        side = "HOME" if b['bet_side'] == 'home' else "AWAY"
        team = b['home_team'] if b['bet_side'] == 'home' else b['away_team']
        matchup = f"{b['away_team']} @ {b['home_team']}"
        if len(matchup) > 34: matchup = matchup[:31] + "..."

        spread_str = f"{b['spread']:+.1f}"
        proj_str   = f"{b['proj_margin']:+.1f}"
        edge_str   = f"{b['edge']:+.1f}"
        bet_str    = f"${b['bet_size']:,.0f}"
        bet_on     = f"{team} ({side})"

        print(f"  {matchup:<35} {spread_str:>7} {proj_str:>7} {edge_str:>6} {bet_str:>8}")
        print(f"  {'→ BET: ' + bet_on:<50}")
        print()

    print("="*65)
    print("  Kelly sizing @ 1/4 Kelly, max 2% bankroll")
    print("  Exclude spreads 9-12pt (historically poor bucket)")
    print("="*65)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, help='Target date YYYY-MM-DD')
    parser.add_argument('--bankroll', type=float, default=BANKROLL)
    parser.add_argument('--edge', type=float, default=EDGE_MIN)
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    bankroll = args.bankroll

    print(f"Generating bet card for {target_date}...")

    model, feature_cols = load_model_and_features()
    if not model:
        sys.exit(1)

    odds_data = fetch_todays_lines(target_date)
    games = parse_lines(odds_data, target_date)
    print(f"Found {len(games)} games on DraftKings for {target_date}")

    bets = []
    conn = get_db()
    season = target_date.year if target_date.month >= 9 else target_date.year

    for game in games:
        row = build_game_features(
            game['home_team'], game['away_team'],
            game['spread'], season, conn, feature_cols
        )

        X = pd.DataFrame([row])[feature_cols]
        X = X.fillna(0)  # median fill in production
        proj_margin = float(model.predict(X)[0])

        spread = game['spread']
        # Edge = how much better our projection is vs the line
        # Positive edge = home team outperforms line
        edge = proj_margin - spread

        if abs(edge) < args.edge:
            continue
        if not (SPREAD_LO <= abs(spread) <= SPREAD_HI):
            continue
        if EXCLUDE_LO <= abs(spread) <= EXCLUDE_HI:
            continue

        bet_side = 'home' if edge > 0 else 'away'
        bet_size = kelly_size(abs(edge), None, bankroll)

        if bet_size < 10:
            continue

        bets.append({
            'home_team': game['home_team'],
            'away_team': game['away_team'],
            'spread': spread,
            'proj_margin': proj_margin,
            'edge': edge,
            'bet_side': bet_side,
            'bet_size': bet_size,
            'game_time': game['game_time'],
        })

    conn.close()
    print_bet_card(bets, target_date)

    # Save
    if bets:
        out_path = os.path.join(OUT, f'bets_{target_date}.json')
        with open(out_path, 'w') as f:
            json.dump(bets, f, indent=2, default=str)
        print(f"\nBets saved: {out_path}")
