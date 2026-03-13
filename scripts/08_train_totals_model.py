"""
08_train_totals_model.py
========================
Train a calibrated XGBClassifier to predict whether the total goes Over.
Target: went_over = (home_score + away_score) > over_under

Feature philosophy:
  - Both teams' offensive AND defensive efficiency (pace matters both ways)
  - Tempo is the most important single feature for totals
  - Use same clean sources as spread model: TVD, KPD, KP_FANMATCH, CONTEXT

Run: python scripts/08_train_totals_model.py
"""

import sqlite3, os, json, pickle, warnings
import pandas as pd
import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from sklearn.metrics import brier_score_loss, roc_auc_score
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
OUT_DIR  = os.path.join(ROOT, 'models')
os.makedirs(OUT_DIR, exist_ok=True)

PAYOUT      = 100 / 110   # -110 juice
EV_THRESH   = 0.03
SPREAD_LO   = 0.5
SPREAD_HI   = 9.0
MIN_TRAIN   = 3000
KELLY_FRAC  = 0.25
MAX_BET_PCT = 0.02

# ── Feature groups for totals ──────────────────────────────────────────────
# Key insight: for totals, BOTH teams' offense AND defense matter equally.
# Tempo (pace) is the dominant signal — fast teams = more possessions = higher totals.
# We use the same clean sources as the spread model.

TOTALS_FEATURES = [
    # Context
    'neutral_site', 'conf_game', 'over_under',
    'home_rest', 'away_rest', 'home_b2b', 'away_b2b',

    # Torvik daily — offensive and defensive efficiency + tempo proxy
    'h_tvd_adj_o', 'h_tvd_adj_d', 'h_tvd_barthag',
    'a_tvd_adj_o', 'a_tvd_adj_d', 'a_tvd_barthag',
    'tvd_em_gap',
    # Pace proxies from Torvik
    'h_tvd_efg_o', 'h_tvd_efg_d',
    'a_tvd_efg_o', 'a_tvd_efg_d',
    'has_tvd_home', 'has_tvd_away',

    # KenPom daily — tempo is directly measured here
    'h_kpd_adj_o', 'h_kpd_adj_d', 'h_kpd_adj_tempo',
    'a_kpd_adj_o', 'a_kpd_adj_d', 'a_kpd_adj_tempo',
    'h_kpd_pythag', 'a_kpd_pythag',
    'kpd_em_gap',
    # Combined pace signal
    'has_kpd_home', 'has_kpd_away',

    # KenPom fanmatch — predicted tempo is gold for totals
    'kp_pred_tempo',   # KenPom's game-specific pace prediction
    'kp_home_pred',    # predicted scores proxy total directly
    'kp_away_pred',
    'kp_pred_margin',  # less relevant for totals but keep for interaction
    'has_kp_fanmatch',
]

# Derived features we'll compute on the fly
DERIVED = [
    'h_kpd_adj_o_minus_a_kpd_adj_d',   # home offense vs away defense
    'a_kpd_adj_o_minus_h_kpd_adj_d',   # away offense vs home defense
    'avg_kpd_tempo',                    # average tempo of both teams
    'kp_pred_total',                    # sum of KP predicted scores
]


def load_data():
    conn = sqlite3.connect(DB)
    print("Loading game_features_v2...")
    df = pd.read_sql("SELECT * FROM game_features_v2", conn)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    print("Loading over_under targets from game_lines...")
    lines = pd.read_sql("""
        SELECT game_date, home_team, away_team,
               over_under, went_over
        FROM game_lines
        WHERE over_under IS NOT NULL
          AND went_over IS NOT NULL
    """, conn)
    conn.close()
    print(f"  {len(lines):,} games with O/U + result")
    return df, lines


def build_totals_features(df, lines):
    """Merge features with totals target and engineer pace features."""
    # Merge on game_date + home_team + away_team
    merged = df.merge(
        lines[['game_date', 'home_team', 'away_team', 'over_under', 'went_over']],
        on=['game_date', 'home_team', 'away_team'],
        how='inner'
    )
    print(f"  Merged: {len(merged):,} games with both features and O/U target")

    # Use over_under from lines (more reliable than from features table)
    if 'over_under_x' in merged.columns:
        merged['over_under'] = merged['over_under_x'].fillna(merged.get('over_under_y'))
        merged = merged.drop(columns=['over_under_x', 'over_under_y'], errors='ignore')

    # Engineer derived pace features
    merged['h_kpd_adj_o_minus_a_kpd_adj_d'] = (
        merged.get('h_kpd_adj_o', pd.Series(dtype=float)) -
        merged.get('a_kpd_adj_d', pd.Series(dtype=float))
    )
    merged['a_kpd_adj_o_minus_h_kpd_adj_d'] = (
        merged.get('a_kpd_adj_o', pd.Series(dtype=float)) -
        merged.get('h_kpd_adj_d', pd.Series(dtype=float))
    )
    merged['avg_kpd_tempo'] = (
        merged.get('h_kpd_adj_tempo', pd.Series(dtype=float)) +
        merged.get('a_kpd_adj_tempo', pd.Series(dtype=float))
    ) / 2
    merged['kp_pred_total'] = (
        merged.get('kp_home_pred', pd.Series(dtype=float)) +
        merged.get('kp_away_pred', pd.Series(dtype=float))
    )

    # Select only features that exist in the dataframe
    all_feats = TOTALS_FEATURES + DERIVED
    available = [f for f in all_feats if f in merged.columns]
    missing   = [f for f in all_feats if f not in merged.columns]
    if missing:
        print(f"  Missing features (will use NaN): {missing}")

    # Add any missing as NaN
    for f in missing:
        merged[f] = np.nan

    return merged, all_feats


def walk_forward_validate(df, features, target='went_over'):
    """Walk-forward validation matching spread model methodology."""
    seasons = sorted(df['season'].dropna().unique())
    results = []
    print(f"\n--- Walk-Forward Validation (totals O/U classifier) ---")
    print(f"{'Season':>8} {'Train':>8} {'Test':>7} {'Bets':>6} {'WR':>7} {'ROI':>8}")
    print("-" * 55)

    for i, test_season in enumerate(seasons):
        if i < 4:  # need at least 4 seasons of training data
            continue

        train_df = df[df['season'] < test_season].copy()
        test_df  = df[df['season'] == test_season].copy()

        if len(train_df) < MIN_TRAIN or len(test_df) < 100:
            continue

        # Filter to games with O/U data
        train_df = train_df[train_df['over_under'].notna() & train_df[target].notna()]
        test_df  = test_df[test_df['over_under'].notna() & test_df[target].notna()]

        if len(train_df) < MIN_TRAIN:
            continue

        X_train = train_df[features]
        y_train = train_df[target].astype(int)
        X_test  = test_df[features]
        y_test  = test_df[target].astype(int)

        # Impute
        imp = SimpleImputer(strategy='median')
        X_train_imp = pd.DataFrame(imp.fit_transform(X_train), columns=features)
        X_test_imp  = pd.DataFrame(imp.transform(X_test),  columns=features)

        # Train calibrated XGBClassifier
        base = XGBClassifier(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            gamma=0.1,
            use_label_encoder=False,
            eval_metric='logloss',
            random_state=42,
            verbosity=0,
        )
        model = CalibratedClassifierCV(base, cv=5, method='isotonic')
        model.fit(X_train_imp, y_train)

        # Predict P(over)
        p_over = model.predict_proba(X_test_imp)[:, 1]
        ev_over  = p_over * (1 + PAYOUT) - 1
        ev_under = (1 - p_over) * (1 + PAYOUT) - 1
        best_ev  = np.maximum(ev_over, ev_under)

        # Only bet when EV >= threshold and O/U in reasonable range
        bet_mask = best_ev >= EV_THRESH
        if bet_mask.sum() == 0:
            continue

        test_sub  = test_df[bet_mask].copy()
        p_sub     = p_over[bet_mask]
        ev_sub    = best_ev[bet_mask]
        bet_over  = ev_over[bet_mask] >= ev_under[bet_mask]

        # Actual result: 1 if bet won
        actual_over = y_test.values[bet_mask]
        won = np.where(bet_over, actual_over == 1, actual_over == 0)

        n_bets   = bet_mask.sum()
        win_rate = won.mean()
        # ROI at -110
        roi = (won.sum() * PAYOUT - (~won).sum()) / n_bets

        results.append({
            'season':    test_season,
            'n_train':   len(train_df),
            'n_test':    len(test_df),
            'n_bets':    n_bets,
            'win_rate':  win_rate,
            'roi':       roi,
        })
        print(f"  {int(test_season):>6}  {len(train_df):>8,}  {len(test_df):>7,}  {n_bets:>6}  {win_rate:>7.3f}  {roi:>+8.3f}")

    if not results:
        print("  No valid seasons for validation.")
        return None

    total_bets = sum(r['n_bets'] for r in results)
    total_wins = sum(r['n_bets'] * r['win_rate'] for r in results)
    total_wr   = total_wins / total_bets if total_bets > 0 else 0
    total_roi  = (total_wins * PAYOUT - (total_bets - total_wins)) / total_bets if total_bets > 0 else 0
    print("-" * 55)
    print(f"  {'TOTAL':>6}  {'':>8}  {'':>7}  {total_bets:>6}  {total_wr:>7.3f}  {total_roi:>+8.3f}")
    print(f"  Breakeven WR: 52.38%")
    return results


def train_production_model(df, features, target='went_over'):
    """Train final model on all labeled data."""
    print("\nTraining production totals model on full dataset...")
    labeled = df[df['over_under'].notna() & df[target].notna()].copy()
    print(f"  Training on {len(labeled):,} games, {len(features)} features")

    X = labeled[features]
    y = labeled[target].astype(int)

    imp = SimpleImputer(strategy='median')
    X_imp = pd.DataFrame(imp.fit_transform(X), columns=features)

    base = XGBClassifier(
        n_estimators=400,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        gamma=0.1,
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42,
        verbosity=0,
    )
    model = CalibratedClassifierCV(base, cv=5, method='isotonic')
    model.fit(X_imp, y)

    # Feature importance
    try:
        fi = model.estimator.feature_importances_
        fi_df = pd.Series(fi, index=features).sort_values(ascending=False)
        print("\n--- Top 15 Features ---")
        for feat, imp_val in fi_df.head(15).items():
            bar = '█' * int(imp_val * 300)
            print(f"  {feat:<45} {imp_val:.4f} {bar}")
    except Exception:
        pass

    # Save
    model_path   = os.path.join(OUT_DIR, 'totals_model.pkl')
    imputer_path = os.path.join(OUT_DIR, 'totals_imputer.pkl')
    feat_path    = os.path.join(OUT_DIR, 'totals_feature_cols.json')

    with open(model_path,   'wb') as f: pickle.dump(model, f)
    with open(imputer_path, 'wb') as f: pickle.dump(imp, f)
    with open(feat_path,    'w')  as f: json.dump({'features': features, 'target': target}, f)

    print(f"\n✅ Saved: {model_path}")
    print(f"   {imputer_path}")
    print(f"   {feat_path}")
    return model, imp


if __name__ == '__main__':
    print("=" * 60)
    print("NCAAB Model — Training Totals (Over/Under) Model")
    print("=" * 60)

    df, lines = load_data()
    merged, features = build_totals_features(df, lines)

    over_rate = merged['went_over'].mean()
    print(f"\n  Target: went_over | Over rate: {over_rate:.3f} ({over_rate*100:.1f}%)")
    print(f"  Features: {len(features)}")
    print(f"  Games with O/U target: {merged['went_over'].notna().sum():,}")

    results = walk_forward_validate(merged, features)

    if results:
        model, imp = train_production_model(merged, features)
        print("\nNext: python scripts/09_train_ml_model.py")
    else:
        print("\nWARNING: Validation failed — check data coverage")
