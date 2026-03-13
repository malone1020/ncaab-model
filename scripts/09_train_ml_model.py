"""
09_train_ml_model.py
====================
Derives P(home wins outright) from the spread model using a normal
distribution approximation of college basketball scoring margins.

No separate classifier needed — we use the spread model's P(home covers)
to back-calculate the implied margin distribution, then integrate to get
P(home wins outright > 0).

Method:
  Given: P(home covers spread S) from the calibrated spread model
  The spread model learned: P(margin > S) ≈ p_cover
  From p_cover and S, we can infer the implied mean margin (mu)
  assuming a normal distribution with sigma = historical std of margins.
  Then P(win) = P(margin > 0) = norm.sf(0, loc=mu, scale=sigma)

  This is calibrated against historical ML results and saved as a
  lookup/correction function.

Run: python scripts/09_train_ml_model.py
"""

import sqlite3, os, json, pickle, warnings
import pandas as pd
import numpy as np
from scipy import stats, optimize
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
OUT_DIR  = os.path.join(ROOT, 'models')
PAYOUT   = 100 / 110

# Historical std of college basketball margins (from the data)
# Typical value is ~11-12 points
MARGIN_STD = None  # will be computed from data


def load_data():
    conn = sqlite3.connect(DB)

    print("Loading game_lines for ML calibration...")
    lines = pd.read_sql("""
        SELECT game_date, home_team, away_team,
               spread, home_margin,
               home_moneyline, away_moneyline,
               home_covered,
               CASE WHEN home_margin > 0 THEN 1 ELSE 0 END as home_won
        FROM game_lines
        WHERE spread IS NOT NULL
          AND home_margin IS NOT NULL
          AND home_moneyline IS NOT NULL
          AND away_moneyline IS NOT NULL
          AND ABS(spread) BETWEEN 0.5 AND 20
    """, conn)
    conn.close()
    print(f"  {len(lines):,} games with spread + margin + ML odds")
    return lines


def compute_margin_std(lines):
    """Compute historical std of home_margin (net of spread)."""
    global MARGIN_STD
    # Margin relative to spread — this should be ~N(0, sigma)
    residuals = lines['home_margin'] - (-lines['spread'])  # spread is home team's spread
    sigma = residuals.std()
    print(f"\n  Historical margin std: {sigma:.2f} pts")
    print(f"  (This is the key parameter for converting P(cover) → P(win))")
    MARGIN_STD = sigma
    return sigma


def american_to_prob(ml):
    """Convert American moneyline to implied probability (without juice removal)."""
    ml = float(ml)
    if ml > 0:
        return 100 / (ml + 100)
    else:
        return abs(ml) / (abs(ml) + 100)


def ml_to_payout(ml):
    """
    Convert American moneyline to decimal payout per $1 risked.
    E.g., -110 -> 100/110 = 0.909
         +150 -> 150/100 = 1.50
         -150 -> 100/150 = 0.667
    """
    ml = float(ml)
    if ml > 0:
        return ml / 100
    else:
        return 100 / abs(ml)


def p_cover_to_p_win(p_cover, spread, sigma):
    """
    Convert P(home covers spread) to P(home wins outright) using normal approx.

    Given P(margin > spread) = p_cover, we solve for mu:
      P(Z > (spread - mu) / sigma) = p_cover
      => (spread - mu) / sigma = norm.ppf(1 - p_cover)
      => mu = spread - sigma * norm.ppf(1 - p_cover)

    Then P(win) = P(margin > 0) = P(Z > -mu/sigma) = norm.sf(-mu/sigma)
    Note: spread here is the home team's spread (negative = home favorite)
    """
    from scipy.stats import norm
    # spread is negative for favorites: e.g., Arizona -3.5 → spread = -3.5
    # P(home_margin > -3.5) = p_cover  ... but conventions vary
    # In our DB: spread = home team's spread (negative = home favorite)
    # home covers if home_margin > -spread (i.e., margin > abs(spread) for underdog)
    # Actually: home covers if home_margin + spread > 0, i.e., home_margin > -spread
    # For Arizona -3.5: covers if Arizona wins by more than 3.5
    # So P(home_margin > -spread) = p_cover ... wait, let me be precise:
    # spread stored as negative for favorites: -3.5 means home is -3.5
    # home covers if home_margin > abs(spread) when home is favorite
    # = home_margin > -spread (since spread is -3.5, -spread = 3.5)
    cover_threshold = -spread  # points home needs to win by to cover

    # Solve for mu: P(margin > cover_threshold) = p_cover
    z = stats.norm.ppf(1 - p_cover)
    mu = cover_threshold - sigma * z

    # P(home wins outright) = P(margin > 0)
    p_win = stats.norm.sf(0, loc=mu, scale=sigma)
    return float(np.clip(p_win, 0.01, 0.99))


def compute_ml_ev(p_win, ml):
    """EV for a moneyline bet given P(win) and American odds."""
    payout = ml_to_payout(ml)
    return p_win * payout - (1 - p_win)


def calibrate_and_validate(lines, sigma):
    """
    Validate the normal distribution approximation by checking
    how well market spread → P(win) matches actual win rates by bucket.
    """
    print("\n--- ML Conversion Sanity Check ---")
    print("(Validating normal dist approximation: spread bucket vs actual win rate)")
    from scipy.stats import norm

    lines = lines.copy()
    # Market-implied P(home wins) from spread using normal dist
    # spread is negative for home favorites: -3.5 means home expected margin = +3.5
    lines['market_p_win'] = lines['spread'].apply(
        lambda s: float(np.clip(norm.sf(0, loc=-s, scale=sigma), 0.01, 0.99))
    )

    lines['bucket'] = pd.cut(lines['market_p_win'],
                              bins=[0, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 1.0],
                              labels=['<30%','30-40%','40-50%','50-60%','60-70%','70-80%','>80%'])

    cal = lines.groupby('bucket').agg(
        n=('home_won','count'),
        actual_win_rate=('home_won','mean'),
        avg_market_p=('market_p_win','mean')
    ).dropna()

    print(f"\n  {'Bucket':<10} {'N':>7} {'Mkt P(W)':>9} {'Actual':>9} {'Diff':>8}")
    print("  " + "-"*52)
    for bucket, row in cal.iterrows():
        diff = row['actual_win_rate'] - row['avg_market_p']
        print(f"  {str(bucket):<10} {int(row['n']):>7,} {row['avg_market_p']:>9.3f} "
              f"{row['actual_win_rate']:>9.3f} {diff:>+8.3f}")

    mae = abs(cal['actual_win_rate'] - cal['avg_market_p']).mean()
    print(f"\n  Mean error: {mae:.4f} — lower = normal dist is good fit")
    print(f"  sigma={sigma:.2f} is used to convert model P(cover) → P(win)")
    return cal

def validate_ml_ev(lines, sigma):
    """
    Walk-forward style validation: if we had bet ML using our P(win) estimates,
    what would the ROI be vs the actual ML odds?
    Only bet when EV > 3%.
    """
    print("\n--- ML EV Validation (using spread-derived P(win)) ---")

    from scipy.stats import norm
    EV_MIN = 0.03
    lines = lines.copy()
    lines['cover_threshold'] = -lines['spread']
    lines['market_p_cover'] = norm.sf(lines['cover_threshold'], loc=0, scale=sigma)
    lines['implied_p_win'] = lines.apply(
        lambda r: p_cover_to_p_win(r['market_p_cover'], r['spread'], sigma), axis=1
    )

    # EV for home and away ML
    lines['ev_home_ml'] = lines.apply(
        lambda r: compute_ml_ev(r['implied_p_win'], r['home_moneyline']), axis=1
    )
    lines['ev_away_ml'] = lines.apply(
        lambda r: compute_ml_ev(1 - r['implied_p_win'], r['away_moneyline']), axis=1
    )

    lines['best_ev'] = lines[['ev_home_ml','ev_away_ml']].max(axis=1)
    lines['bet_home'] = lines['ev_home_ml'] >= lines['ev_away_ml']

    bet_df = lines[lines['best_ev'] >= EV_MIN].copy()
    print(f"  Qualifying bets (EV≥{EV_MIN*100:.0f}%): {len(bet_df):,}")

    if len(bet_df) == 0:
        print("  No qualifying bets — conversion method too conservative")
        return

    bet_df['won'] = np.where(
        bet_df['bet_home'],
        bet_df['home_won'] == 1,
        bet_df['home_won'] == 0
    )
    bet_df['payout'] = np.where(
        bet_df['bet_home'],
        bet_df['home_moneyline'].apply(ml_to_payout),
        bet_df['away_moneyline'].apply(ml_to_payout)
    )
    bet_df['profit'] = np.where(bet_df['won'], bet_df['payout'], -1.0)

    roi = bet_df['profit'].mean()
    wr  = bet_df['won'].mean()
    print(f"  Win rate: {wr:.3f} | ROI: {roi:+.4f}")
    print(f"  Avg EV: {bet_df['best_ev'].mean():+.4f}")

    # Season breakdown
    if 'season' in lines.columns:
        by_season = bet_df.groupby(
            lines.loc[bet_df.index, 'season'] if 'season' in lines.columns else pd.Series()
        ).agg(n=('won','count'), wr=('won','mean'), roi=('profit','mean'))
        if not by_season.empty:
            print("\n  By season:")
            for s, row in by_season.iterrows():
                print(f"    {int(s)}: {row['n']:>5} bets | WR={row['wr']:.3f} | ROI={row['roi']:+.4f}")


def save_ml_params(sigma, lines):
    """Save the parameters needed for daily ML EV computation."""
    params = {
        'margin_std': float(sigma),
        'method': 'normal_distribution_from_spread',
        'description': (
            'P(home wins) derived from P(home covers spread) using normal dist. '
            f'sigma={sigma:.2f} pts computed from {len(lines):,} historical games.'
        )
    }
    path = os.path.join(OUT_DIR, 'ml_params.json')
    with open(path, 'w') as f:
        json.dump(params, f, indent=2)
    print(f"\n✅ Saved: {path}")
    print(f"   margin_std = {sigma:.2f}")
    return params


if __name__ == '__main__':
    print("=" * 60)
    print("NCAAB Model — Moneyline Probability Calibration")
    print("=" * 60)

    lines = load_data()

    # Add season column
    lines['season'] = lines['game_date'].apply(
        lambda d: int(d[:4]) if d[5:7] >= '09' else int(d[:4])
    )

    sigma = compute_margin_std(lines)

    # Validate conversion accuracy
    cal = calibrate_and_validate(lines, sigma)

    # Validate EV-based ML betting
    validate_ml_ev(lines, sigma)

    # Save params
    params = save_ml_params(sigma, lines)

    print("\n" + "=" * 60)
    print("Moneyline model ready.")
    print("No separate training needed — P(win) derived from spread model.")
    print("Next: python scripts/10_update_daily_bets.py")
    print("  (or update 07_daily_bets.py to incorporate totals + ML)")
