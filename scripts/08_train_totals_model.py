"""
08_train_totals_model.py
========================
Train a calibrated XGBClassifier to predict whether the total goes Over.
Target: went_over = (home_score + away_score) > over_under

Uses the same feature group combo system as 05b_backtest_totals_combos.py.
Best combo determined empirically via walk-forward backtest.

Run: python scripts/08_train_totals_model.py
     python scripts/08_train_totals_model.py --combo "CONTEXT+TVD+KPD+RECENCY+REFS+TRAVEL"
"""

import sqlite3, os, json, pickle, warnings, argparse
import pandas as pd
import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')

ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB       = os.path.join(ROOT, 'data', 'basketball.db')
OUT_DIR  = os.path.join(ROOT, 'models')
os.makedirs(OUT_DIR, exist_ok=True)

PAYOUT    = 100 / 110
EV_THRESH = 0.03
MIN_TRAIN = 3000

# Default combo — empirically best from 05b_backtest_totals_combos.py
DEFAULT_COMBO = "CONTEXT+TVD+KPD+RECENCY+REFS+TRAVEL"

# ── Feature groups — must match 05b_backtest_totals_combos.py exactly ──────
CONTEXT_FEATURES = [
    'neutral_site', 'conf_game', 'over_under',
    'home_rest', 'away_rest', 'home_b2b', 'away_b2b',
    'is_conf_tournament', 'is_ncaa_tournament',
]

FEATURE_GROUPS = {
    'TVD': [
        'h_tvd_adj_o', 'h_tvd_adj_d', 'h_tvd_barthag',
        'a_tvd_adj_o', 'a_tvd_adj_d', 'a_tvd_barthag',
        'tvd_em_gap',
        'h_tvd_efg_o', 'h_tvd_efg_d',
        'a_tvd_efg_o', 'a_tvd_efg_d',
        'has_tvd_home', 'has_tvd_away',
    ],
    'KPD': [
        'h_kpd_adj_o', 'h_kpd_adj_d', 'h_kpd_adj_tempo',
        'a_kpd_adj_o', 'a_kpd_adj_d', 'a_kpd_adj_tempo',
        'kpd_em_gap', 'has_kpd_home', 'has_kpd_away',
        'avg_kpd_tempo',
        'h_kpd_adj_o_minus_a_kpd_adj_d',
        'a_kpd_adj_o_minus_h_kpd_adj_d',
    ],
    'KP_FANMATCH': [
        'kp_pred_tempo', 'kp_home_pred', 'kp_away_pred',
        'kp_pred_margin', 'kp_pred_total', 'has_kp_fanmatch',
    ],
    'ROLLING': [
        'h_rol_r5_pts_off', 'h_rol_r5_pts_def', 'h_rol_r5_pace',
        'h_rol_r5_efg', 'h_rol_r5_margin',
        'h_rol_r10_margin', 'h_rol_ew_margin', 'h_rol_trend_margin',
        'a_rol_r5_pts_off', 'a_rol_r5_pts_def', 'a_rol_r5_pace',
        'a_rol_r5_efg', 'a_rol_r5_margin',
        'a_rol_r10_margin', 'a_rol_ew_margin', 'a_rol_trend_margin',
        'rol_margin_gap', 'rol_efg_gap', 'rol_trend_gap',
        'has_rol_home', 'has_rol_away',
    ],
    'RECENCY': [
        'h_rew_adj_em', 'h_rew_adj_o', 'h_rew_adj_d',
        'a_rew_adj_em', 'a_rew_adj_o', 'a_rew_adj_d',
        'h_trend_adj_em', 'a_trend_adj_em',
        'rew_em_gap', 'rew_o_gap', 'rew_d_gap', 'trend_em_gap', 'has_rew',
    ],
    'REFS': [
        'ref_avg_fpg', 'ref_home_bias', 'ref_ftr_home_avg',
        'ref_ftr_away_avg', 'ref_ftr_gap', 'has_ref_data',
        'ref_high_foul', 'ref_low_foul',
    ],
    'TRAVEL': [
        'away_travel_miles', 'tz_crossings', 'east_to_west', 'west_to_east',
        'away_road_game_n', 'away_long_trip', 'away_tz_change',
    ],
    'LINE_MOVE': [
        'spread_open', 'total_open', 'spread_close', 'total_close',
        'spread_move', 'total_move', 'ml_home_move',
    ],
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--optuna', action='store_true',
                   help='Use Optuna-tuned hyperparameters')
    p.add_argument('--combo', type=str, default=DEFAULT_COMBO,
                   help=f'Feature group combo (default: {DEFAULT_COMBO})')
    return p.parse_args()


def get_feature_cols(df, combo_str):
    """Resolve combo string to list of available feature columns."""
    groups = [g.strip() for g in combo_str.split('+')]
    cols = list(CONTEXT_FEATURES)
    for g in groups:
        if g == 'CONTEXT':
            continue  # already included
        for c in FEATURE_GROUPS.get(g, []):
            if c in df.columns and c not in cols:
                cols.append(c)
    return [c for c in cols if c in df.columns]


def load_data(combo_str):
    conn = sqlite3.connect(DB)
    print("Loading game_features_v2...")
    df = pd.read_sql("SELECT * FROM game_features_v2", conn)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    print("Loading over_under targets from game_lines...")
    lines = pd.read_sql("""
        SELECT game_date, home_team, away_team, over_under, went_over
        FROM game_lines
        WHERE over_under IS NOT NULL AND went_over IS NOT NULL
    """, conn)
    print(f"  {len(lines):,} games with O/U + result")

    # Merge target
    merged = df.merge(
        lines[['game_date', 'home_team', 'away_team', 'over_under', 'went_over']],
        on=['game_date', 'home_team', 'away_team'], how='inner'
    )
    print(f"  Merged: {len(merged):,} games with features + O/U target")

    if 'over_under_x' in merged.columns:
        merged['over_under'] = merged['over_under_x'].fillna(merged.get('over_under_y', pd.Series(dtype=float)))
        merged = merged.drop(columns=['over_under_x', 'over_under_y'], errors='ignore')

    # Engineer derived features
    merged['avg_kpd_tempo'] = (
        merged.get('h_kpd_adj_tempo', pd.Series(dtype=float)) +
        merged.get('a_kpd_adj_tempo', pd.Series(dtype=float))
    ) / 2
    merged['h_kpd_adj_o_minus_a_kpd_adj_d'] = (
        merged.get('h_kpd_adj_o', pd.Series(dtype=float)) -
        merged.get('a_kpd_adj_d', pd.Series(dtype=float))
    )
    merged['a_kpd_adj_o_minus_h_kpd_adj_d'] = (
        merged.get('a_kpd_adj_o', pd.Series(dtype=float)) -
        merged.get('h_kpd_adj_d', pd.Series(dtype=float))
    )
    merged['kp_pred_total'] = (
        merged.get('kp_home_pred', pd.Series(dtype=float)) +
        merged.get('kp_away_pred', pd.Series(dtype=float))
    )

    # Merge line movement if available
    try:
        lm = pd.read_sql("""
            SELECT game_date, home_team, away_team,
                   spread_open, total_open, spread_close, total_close,
                   spread_move, total_move, ml_home_move
            FROM line_movement
        """, conn)
        if len(lm) > 0:
            merged = merged.merge(lm, on=['game_date', 'home_team', 'away_team'], how='left')
            print(f"  Line movement: {lm['total_move'].notna().sum():,} games with data")
    except Exception:
        pass

    conn.close()

    features = get_feature_cols(merged, combo_str)
    print(f"  Combo: {combo_str}")
    print(f"  Features: {len(features)}")
    return merged, features


def walk_forward_validate(df, features, target='went_over'):
    seasons = sorted(df['season'].dropna().unique())
    results = []
    print(f"\n--- Walk-Forward Validation (totals O/U classifier) ---")
    print(f"{'Season':>8} {'Train':>8} {'Test':>7} {'Bets':>6} {'WR':>7} {'ROI':>8}")
    print("-" * 55)

    for i, test_season in enumerate(seasons):
        if i < 4:
            continue

        train_df = df[df['season'] < test_season].copy()
        test_df  = df[df['season'] == test_season].copy()

        train_df = train_df[train_df['over_under'].notna() & train_df[target].notna()]
        test_df  = test_df[test_df['over_under'].notna()  & test_df[target].notna()]

        if len(train_df) < MIN_TRAIN or len(test_df) < 50:
            continue

        valid_feats = [f for f in features if train_df[f].notna().any()]
        X_train = train_df[valid_feats]
        y_train = train_df[target].astype(int)
        X_test  = test_df[valid_feats]
        y_test  = test_df[target].astype(int)

        imp = SimpleImputer(strategy='median')
        X_train_imp = pd.DataFrame(imp.fit_transform(X_train), columns=valid_feats)
        X_test_imp  = pd.DataFrame(imp.transform(X_test), columns=valid_feats)

        base = make_xgb(getattr(args, "optuna", False))
        model = CalibratedClassifierCV(base, cv=5, method='isotonic')
        model.fit(X_train_imp, y_train)

        p_over   = model.predict_proba(X_test_imp)[:, 1]
        ev_over  = p_over * (1 + PAYOUT) - 1
        ev_under = (1 - p_over) * (1 + PAYOUT) - 1
        best_ev  = np.maximum(ev_over, ev_under)
        bet_mask = best_ev >= EV_THRESH

        if bet_mask.sum() == 0:
            continue

        bet_over    = ev_over[bet_mask] >= ev_under[bet_mask]
        actual_over = y_test.values[bet_mask]
        won = np.where(bet_over, actual_over == 1, actual_over == 0)

        n_bets   = bet_mask.sum()
        win_rate = won.mean()
        roi      = (won.sum() * PAYOUT - (~won).sum()) / n_bets

        results.append({'season': test_season, 'n_train': len(train_df),
                        'n_test': len(test_df), 'n_bets': n_bets,
                        'win_rate': win_rate, 'roi': roi})
        print(f"  {int(test_season):>6}  {len(train_df):>8,}  {len(test_df):>7,}  "
              f"{n_bets:>6}  {win_rate:>7.3f}  {roi:>+8.3f}")

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


def train_production_model(df, features, combo_str, target='went_over'):
    print("\nTraining production totals model on full dataset...")
    labeled = df[df['over_under'].notna() & df[target].notna()].copy()
    print(f"  Training on {len(labeled):,} games, {len(features)} features")

    valid_feats = [f for f in features if labeled[f].notna().any()]
    dropped = set(features) - set(valid_feats)
    if dropped:
        print(f"  Dropped {len(dropped)} all-NaN features: {dropped}")
    features = valid_feats

    X = labeled[features]
    y = labeled[target].astype(int)

    imp = SimpleImputer(strategy='median')
    X_imp = pd.DataFrame(imp.fit_transform(X), columns=features)

    base = XGBClassifier(
        n_estimators=400, max_depth=4, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=3, gamma=0.1,
        use_label_encoder=False, eval_metric='logloss',
        random_state=42, verbosity=0,
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

    model_path   = os.path.join(OUT_DIR, 'totals_model.pkl')
    imputer_path = os.path.join(OUT_DIR, 'totals_imputer.pkl')
    feat_path    = os.path.join(OUT_DIR, 'totals_feature_cols.json')

    with open(model_path,   'wb') as f: pickle.dump(model, f)
    with open(imputer_path, 'wb') as f: pickle.dump(imp, f)
    with open(feat_path,    'w')  as f:
        json.dump({'combo': combo_str, 'features': features, 'target': target}, f, indent=2)

    print(f"\n✅ Saved: {model_path}")
    print(f"   {imputer_path}")
    print(f"   {feat_path}")
    return model, imp


if __name__ == '__main__':
    args = parse_args()
    use_optuna = getattr(args, 'optuna', False)
    combo = args.combo

    print("=" * 60)
    print("NCAAB Model — Training Totals (Over/Under) Model")
    print("=" * 60)

    merged, features = load_data(combo)

    over_rate = merged['went_over'].mean()
    print(f"\n  Target: went_over | Over rate: {over_rate:.3f} ({over_rate*100:.1f}%)")
    print(f"  Games with O/U target: {merged['went_over'].notna().sum():,}")

    results = walk_forward_validate(merged, features)

    if results:
        train_production_model(merged, features, combo)
        print("\nNext: python scripts/07_daily_bets.py")
    else:
        print("\nWARNING: Validation failed — check data coverage")
