"""
13_team_totals_model.py
=======================
Team totals model — predicts whether each team scores over/under
their individual team total line (e.g. "Duke Over 42.5").

Team totals are often more beatable than full-game totals because:
- Books price them with less precision
- Pace/style matchup effects are more concentrated
- Less sharp money focuses on team totals

Markets:
- Home team total (over/under)
- Away team total (over/under)

Run: python scripts/13_team_totals_model.py --backtest   (validate first)
     python scripts/13_team_totals_model.py --train       (train production model)
"""

import sqlite3, os, sys, json, warnings, argparse, requests, time
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.calibration import CalibratedClassifierCV
from xgboost import XGBClassifier

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except ImportError:
    pass

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
MODELS   = os.path.join(ROOT, 'models')
ODDS_KEY = os.getenv('ODDS_API_KEY', '')
PAYOUT   = 100 / 110
EV_MIN   = 0.03

# Team total features — same as game features but from one team's perspective
# We create home-team and away-team versions
HOME_FEATURES = [
    # Offense efficiency
    'h_kpd_adj_o', 'h_tvd_adj_o', 'h_tvd_efg_o', 'h_tvd_tov_o',
    'h_tvd_orb', 'h_tvd_ftr_o',
    # Defense of opponent
    'a_kpd_adj_d', 'a_tvd_adj_d', 'a_tvd_efg_d', 'a_tvd_tov_d',
    'a_tvd_drb', 'a_tvd_ftr_d',
    # Pace
    'h_kpd_adj_tempo', 'a_kpd_adj_tempo',
    'h_tvd_adj_t', 'a_tvd_adj_t',
    # Context
    'neutral_site', 'conf_game', 'hca_adj',
    'home_rest', 'away_rest', 'home_b2b',
    # KP fanmatch
    'kp_home_pred', 'kp_pred_tempo',
    # Recency
    'h_recency_rew_adj_o', 'h_recency_trend_o',
    'a_recency_rew_adj_d', 'a_recency_trend_d',
    # Tournament
    'is_conf_tournament', 'is_ncaa_tournament',
]

AWAY_FEATURES = [
    # Offense efficiency
    'a_kpd_adj_o', 'a_tvd_adj_o', 'a_tvd_efg_o', 'a_tvd_tov_o',
    'a_tvd_orb', 'a_tvd_ftr_o',
    # Defense of opponent
    'h_kpd_adj_d', 'h_tvd_adj_d', 'h_tvd_efg_d', 'h_tvd_tov_d',
    'h_tvd_drb', 'h_tvd_ftr_d',
    # Pace
    'h_kpd_adj_tempo', 'a_kpd_adj_tempo',
    'h_tvd_adj_t', 'a_tvd_adj_t',
    # Context
    'neutral_site', 'conf_game',
    'home_rest', 'away_rest', 'away_b2b',
    # KP fanmatch
    'kp_away_pred', 'kp_pred_tempo',
    # Recency
    'a_recency_rew_adj_o', 'a_recency_trend_o',
    'h_recency_rew_adj_d', 'h_recency_trend_d',
    # Tournament
    'is_conf_tournament', 'is_ncaa_tournament',
]


def compute_ev(p): return p * (1 + PAYOUT) - 1


def load_team_totals_data():
    """
    Load team total lines from OddsAPI historical data.
    Team totals use market key 'player_props' or 'team_totals' depending on book.
    DraftKings uses 'team_totals' market.
    """
    conn = sqlite3.connect(DB)

    # Load game features
    gf = pd.read_sql("SELECT * FROM game_features_v2", conn)

    # Load team totals from game_lines if available, else from line_movement
    # Check if team total columns exist
    cols = [c[1] for c in conn.execute("PRAGMA table_info(game_lines)").fetchall()]
    has_team_totals = 'home_team_total' in cols

    if has_team_totals:
        lines = pd.read_sql("""
            SELECT game_date, home_team, away_team, season,
                   home_team_total, away_team_total,
                   home_score, away_score
            FROM game_lines
            WHERE home_team_total IS NOT NULL
        """, conn)
        print(f"  Team total lines in game_lines: {len(lines):,}")
    else:
        print("  ⚠ No team total columns in game_lines yet")
        print("  Need to scrape team totals from OddsAPI")
        print("  Run: python scripts/13_team_totals_model.py --scrape-lines")
        conn.close()
        return None, None, None

    conn.close()

    # Merge with features
    merged = gf.merge(lines, on=['game_date','home_team','away_team'], how='inner',
                     suffixes=('','_lines'))

    # Create home team dataset
    home_df = merged.copy()
    home_df['team_total'] = home_df['home_team_total']
    home_df['team_score'] = home_df['home_score']
    home_df['went_over_team'] = (home_df['team_score'] > home_df['team_total']).astype(int)
    home_df['side'] = 'home'
    home_feats = [f for f in HOME_FEATURES if f in home_df.columns]

    # Create away team dataset
    away_df = merged.copy()
    away_df['team_total'] = away_df['away_team_total']
    away_df['team_score'] = away_df['away_score']
    away_df['went_over_team'] = (away_df['team_score'] > away_df['team_total']).astype(int)
    away_df['side'] = 'away'
    away_feats = [f for f in AWAY_FEATURES if f in away_df.columns]

    return home_df, home_feats, away_df, away_feats


def scrape_team_total_lines(seasons=None):
    """
    Scrape historical team total lines from OddsAPI.
    Team totals use market: 'alternate_team_totals' or 'team_totals'
    Cost: ~30 credits per date (same as regular scrape)
    """
    print("\nScraping team total lines from OddsAPI...")
    print("NOTE: DraftKings team totals use 'team_totals' market key")
    print("This requires checking OddsAPI for available markets first.")

    # First check what markets are available
    r = requests.get(
        "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/",
        params={
            'apiKey': ODDS_KEY,
            'regions': 'us',
            'markets': 'team_totals',
            'bookmakers': 'draftkings',
            'oddsFormat': 'american',
        },
        timeout=15
    )
    print(f"  Current team totals check: HTTP {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  Games with team totals today: {len(data)}")
        if data:
            # Show sample
            g = data[0]
            print(f"  Sample: {g['away_team']} @ {g['home_team']}")
            for book in g.get('bookmakers', []):
                if book['key'] == 'draftkings':
                    for mkt in book.get('markets', []):
                        print(f"    Market: {mkt['key']}")
                        for oc in mkt.get('outcomes', [])[:4]:
                            print(f"      {oc}")

    remaining = r.headers.get('X-Requests-Remaining', '?')
    print(f"  Credits remaining: {remaining}")


def add_team_total_columns():
    """Add team total columns to game_lines table."""
    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    cols = [c[1] for c in conn.execute("PRAGMA table_info(game_lines)").fetchall()]
    if 'home_team_total' not in cols:
        conn.execute("ALTER TABLE game_lines ADD COLUMN home_team_total REAL")
        conn.execute("ALTER TABLE game_lines ADD COLUMN away_team_total REAL")
        conn.execute("ALTER TABLE game_lines ADD COLUMN home_over_team_total INTEGER")
        conn.execute("ALTER TABLE game_lines ADD COLUMN away_over_team_total INTEGER")
        conn.commit()
        print("  Added team total columns to game_lines")
    else:
        print("  Team total columns already exist")
    conn.close()


def backtest(n_seasons=4):
    """Walk-forward backtest of team totals model."""
    print("\nRunning team totals backtest...")
    result = load_team_totals_data()
    if result[0] is None:
        return

    home_df, home_feats, away_df, away_feats = result
    print(f"  Home dataset: {len(home_df):,} games")
    print(f"  Away dataset: {len(away_df):,} games")

    # TODO: implement walk-forward backtest
    print("  Backtest not yet implemented — need team total lines first")
    print("  Run --scrape-lines to get team total data from OddsAPI")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--backtest',     action='store_true')
    parser.add_argument('--train',        action='store_true')
    parser.add_argument('--scrape-lines', action='store_true',
                        help='Check OddsAPI team total market availability')
    parser.add_argument('--add-columns',  action='store_true',
                        help='Add team total columns to game_lines table')
    args = parser.parse_args()

    if args.add_columns:
        add_team_total_columns()
    elif args.scrape_lines:
        scrape_team_total_lines()
    elif args.backtest:
        backtest()
    elif args.train:
        print("Train not yet implemented — need team total lines first")
        print("Steps:")
        print("  1. python scripts/13_team_totals_model.py --add-columns")
        print("  2. python scripts/13_team_totals_model.py --scrape-lines")
        print("  3. Scrape historical team total lines (similar to 10_scrape_historical_lines.py)")
        print("  4. python scripts/13_team_totals_model.py --backtest")
        print("  5. python scripts/13_team_totals_model.py --train")
    else:
        parser.print_help()
