"""
11_backtest_ml_model.py
=======================
Walk-forward backtest of the moneyline model.

Method:
  1. For each test season, load spread model predictions (same combo as production)
  2. Convert P(home covers spread) -> P(home wins outright) via normal dist
  3. Compute ML EV against historical DK moneyline odds
  4. Filter: ML odds between -400 and +200, EV >= 3%
  5. Report ROI at actual ML payout

This validates the conversion math empirically, not just analytically.

Run: python scripts/11_backtest_ml_model.py
"""

import sqlite3, os, json, warnings, pickle
import pandas as pd
import numpy as np
from scipy.stats import norm
from sklearn.impute import SimpleImputer
from xgboost import XGBClassifier
from sklearn.calibration import CalibratedClassifierCV

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
MODEL_F  = os.path.join(ROOT, 'models', 'production_model.pkl')
FEAT_F   = os.path.join(ROOT, 'models', 'feature_cols.json')
PARAMS_F = os.path.join(ROOT, 'models', 'ml_params.json')

PAYOUT   = 100 / 110
EV_MIN   = 0.03
# Two-zone filter based on backtest findings
ML_FAV_MIN = -400
ML_FAV_MAX = -200  # big favorites only
ML_DOG_MIN = +100  # underdogs only
ML_DOG_MAX = +200
SPREAD_MIN = 0.5
SPREAD_MAX = 9.0


def ml_payout(ml):
    ml = float(ml)
    return ml / 100 if ml > 0 else 100 / abs(ml)


def p_cover_to_p_win(p_cover, spread, sigma):
    """Convert P(home covers spread) -> P(home wins outright)."""
    cover_threshold = -spread
    z  = norm.ppf(1 - p_cover)
    mu = cover_threshold - sigma * z
    return float(np.clip(norm.sf(0, loc=mu, scale=sigma), 0.01, 0.99))


def ml_in_range(ml):
    if ml is None: return False
    ml = float(ml)
    is_big_fav = (ml < 0) and (ML_FAV_MIN <= ml <= ML_FAV_MAX)
    is_dog     = (ml > 0) and (ML_DOG_MIN <= ml <= ML_DOG_MAX)
    return is_big_fav or is_dog


def load_data():
    conn = sqlite3.connect(DB)

    # Load features
    df = pd.read_sql("SELECT * FROM game_features_v2", conn)
    df['game_date'] = pd.to_datetime(df['game_date'])

    # Load ML odds + outcomes from game_lines
    lines = pd.read_sql("""
        SELECT game_date, home_team, away_team,
               spread, home_moneyline, away_moneyline,
               home_margin,
               CASE WHEN home_margin > 0 THEN 1 ELSE 0 END as home_won
        FROM game_lines
        WHERE home_moneyline IS NOT NULL
          AND away_moneyline IS NOT NULL
          AND home_margin IS NOT NULL
          AND ABS(spread) BETWEEN ? AND ?
    """, conn, params=(SPREAD_MIN, SPREAD_MAX))
    conn.close()

    lines['game_date'] = pd.to_datetime(lines['game_date'])

    merged = df.merge(lines, on=['game_date','home_team','away_team'], how='inner',
                      suffixes=('','_lines'))

    # Use spread from lines (more reliable)
    if 'spread_lines' in merged.columns:
        merged['spread'] = merged['spread_lines'].fillna(merged['spread'])
        merged = merged.drop(columns=['spread_lines'], errors='ignore')

    print(f"  Merged: {len(merged):,} games with features + ML odds")
    return merged


def make_model():
    return CalibratedClassifierCV(
        XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            min_child_weight=10, reg_lambda=2.0,
            use_label_encoder=False, eval_metric='logloss',
            random_state=42, n_jobs=-1, verbosity=0,
        ), method='isotonic', cv=3
    )


if __name__ == '__main__':
    print("=" * 65)
    print("NCAAB — Moneyline Model Walk-Forward Backtest")
    print("=" * 65)

    # Load feature cols from production model
    with open(FEAT_F) as f:
        meta = json.load(f)
    feature_cols = meta['features'] if isinstance(meta, dict) else meta
    print(f"  Spread model features: {len(feature_cols)}")

    # Load sigma
    if os.path.exists(PARAMS_F):
        with open(PARAMS_F) as f:
            sigma = json.load(f)['margin_std']
    else:
        sigma = 11.45
    print(f"  Sigma: {sigma:.2f}")

    df = load_data()

    seasons = sorted(df['season'].unique())
    test_seasons = [s for s in seasons if seasons.index(s) >= 4]

    print(f"\n--- Walk-Forward ML Backtest ---")
    print(f"  ML odds filter: big favs ({ML_FAV_MIN} to {ML_FAV_MAX}) + dogs (+{ML_DOG_MIN} to +{ML_DOG_MAX}) | EV: {EV_MIN:.0%}")
    print()
    print(f"  {'Season':<8} {'Train':>7} {'Test':>7} {'Bets':>6} "
          f"{'WR':>7} {'ROI':>8} {'AvgEV':>8} {'AvgOdds':>9}")
    print("  " + "-" * 65)

    all_results = []

    for test_s in test_seasons:
        train_ss = [s for s in seasons if s < test_s][-4:]
        train = df[df['season'].isin(train_ss) & df['ats_win'].notna()].copy()
        test  = df[df['season'] == test_s].copy()

        if len(train) < 200 or len(test) < 50:
            continue

        valid = [c for c in feature_cols if c in train.columns
                 and train[c].notna().sum() > 50]

        imp = SimpleImputer(strategy='median')
        X_tr = imp.fit_transform(train[valid])
        X_te = imp.transform(test[valid])
        y_tr = train['ats_win'].astype(int).values

        model = make_model()
        model.fit(X_tr, y_tr)

        p_home_cover = model.predict_proba(X_te)[:, 1]

        test = test.copy()
        test['p_cover'] = p_home_cover

        # Convert to P(win)
        test['p_home_win'] = test.apply(
            lambda r: p_cover_to_p_win(r['p_cover'], r['spread'], sigma), axis=1
        )
        test['p_away_win'] = 1 - test['p_home_win']

        # EV for each side
        test['ev_home_ml'] = test.apply(
            lambda r: r['p_home_win'] * ml_payout(r['home_moneyline']) - (1 - r['p_home_win'])
            if pd.notna(r['home_moneyline']) else np.nan, axis=1
        )
        test['ev_away_ml'] = test.apply(
            lambda r: r['p_away_win'] * ml_payout(r['away_moneyline']) - (1 - r['p_away_win'])
            if pd.notna(r['away_moneyline']) else np.nan, axis=1
        )

        # Select best side
        test['bet_home'] = test['ev_home_ml'] >= test['ev_away_ml']
        test['best_ev']  = test.apply(
            lambda r: r['ev_home_ml'] if r['bet_home'] else r['ev_away_ml'], axis=1
        )
        test['bet_ml'] = test.apply(
            lambda r: r['home_moneyline'] if r['bet_home'] else r['away_moneyline'], axis=1
        )

        # Apply filters
        mask = (
            (test['best_ev'] >= EV_MIN) &
            (test['bet_ml'].apply(lambda x: ml_in_range(x) if pd.notna(x) else False))
        )
        bets = test[mask].copy()

        if len(bets) < 10:
            continue

        # Compute outcomes
        bets['won'] = np.where(
            bets['bet_home'],
            bets['home_won'] == 1,
            bets['home_won'] == 0
        )
        bets['payout'] = bets['bet_ml'].apply(ml_payout)
        bets['profit'] = np.where(bets['won'], bets['payout'], -1.0)

        n   = len(bets)
        wr  = bets['won'].mean()
        roi = bets['profit'].mean()
        avg_ev   = bets['best_ev'].mean()
        avg_odds = bets['bet_ml'].mean()

        print(f"  {int(test_s):<8} {len(train):>7,} {len(test):>7,} {n:>6} "
              f"{wr:>7.3f} {roi:>+8.3f} {avg_ev:>+8.3f} {avg_odds:>+9.0f}")

        all_results.append(bets)

    if all_results:
        all_bets = pd.concat(all_results)
        n    = len(all_bets)
        wr   = all_bets['won'].mean()
        roi  = all_bets['profit'].mean()
        avg_ev = all_bets['best_ev'].mean()

        print("  " + "-" * 65)
        print(f"  {'TOTAL':<8} {'':>7} {'':>7} {n:>6} "
              f"{wr:>7.3f} {roi:>+8.3f} {avg_ev:>+8.3f}")

        # Breakdown by odds bucket
        print(f"\n  ROI by odds bucket:")
        all_bets['odds_bucket'] = pd.cut(
            all_bets['bet_ml'],
            bins=[-400, -200, -100, 100, 200],
            labels=['Big fav (<-200)', 'Fav (-200 to -100)',
                    'Pick (-100 to +100)', 'Dog (+100 to +200)']
        )
        for bucket, grp in all_bets.groupby('odds_bucket', observed=True):
            if len(grp) < 10: continue
            b_roi = grp['profit'].mean()
            b_wr  = grp['won'].mean()
            print(f"    {str(bucket):<22} N={len(grp):>5} WR={b_wr:.3f} ROI={b_roi:>+.3f}")

        print(f"\n  Breakeven WR varies by odds. At -110: 52.38%")
        print(f"  Overall ROI {roi:+.3f} {'✓ POSITIVE — ML model adds value' if roi > 0 else '✗ NEGATIVE — consider disabling ML bets'}")

        # Save results
        out = os.path.join(ROOT, 'outputs', 'ml_backtest_results.json')
        summary = {
            'n_bets': n, 'win_rate': round(wr,4), 'roi': round(roi,4),
            'avg_ev': round(avg_ev,4), 'sigma': sigma,
            'ml_filter': f"{ML_MIN} to +{ML_MAX}",
            'ev_threshold': EV_MIN,
        }
        with open(out, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\n  Saved: {out}")
    else:
        print("  No qualifying bets found.")
