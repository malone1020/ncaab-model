"""
06_train_final_model.py
=======================
Train the production model using the best feature combo from 05_backtest_all_combos.py.
Uses XGBClassifier (ATS binary) + CalibratedClassifierCV (isotonic) to produce
reliable P(home covers) probabilities. Saves model + calibrator + feature list.

Run: python scripts/06_train_final_model.py --combo "CONTEXT+TVD+KPD+KP_FANMATCH"
"""

import sqlite3, os, json, argparse, warnings
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
import pickle

warnings.filterwarnings('ignore')

ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB    = os.path.join(ROOT, 'data', 'basketball.db')
OUT   = os.path.join(ROOT, 'outputs')
MODEL = os.path.join(ROOT, 'models')
os.makedirs(OUT, exist_ok=True)
os.makedirs(MODEL, exist_ok=True)

SPREAD_MIN = 0.5
SPREAD_MAX = 9.0
EV_MIN     = 0.03
PAYOUT     = 100 / 110

FEATURE_GROUPS = {
    'CONTEXT': [
        'neutral_site', 'conf_game', 'hca_adj', 'rest_diff',
    ],
    'TVS': [
        'h_tvs_adj_em','h_tvs_adj_o','h_tvs_adj_d','h_tvs_adj_t','h_tvs_barthag',
        'a_tvs_adj_em','a_tvs_adj_o','a_tvs_adj_d','a_tvs_adj_t','a_tvs_barthag',
        'tvs_em_gap', 'has_tvs_home', 'has_tvs_away',
    ],
    'TVD': [
        'h_tvd_adj_em','h_tvd_adj_o','h_tvd_adj_d','h_tvd_adj_t','h_tvd_barthag','h_tvd_wab',
        'a_tvd_adj_em','a_tvd_adj_o','a_tvd_adj_d','a_tvd_adj_t','a_tvd_barthag','a_tvd_wab',
        'tvd_em_gap', 'has_tvd_home', 'has_tvd_away',
    ],
    'KP': [
        'h_kp_adj_em','h_kp_adj_o','h_kp_adj_d','h_kp_adj_t',
        'a_kp_adj_em','a_kp_adj_o','a_kp_adj_d','a_kp_adj_t',
        'kp_em_gap', 'has_kp_home', 'has_kp_away',
    ],
    'HA_CORE': [
        'h_ha_o_eff','h_ha_d_eff','h_ha_pace','h_ha_con','h_ha_sos','h_ha_rq',
        'a_ha_o_eff','a_ha_d_eff','a_ha_pace','a_ha_con','a_ha_sos','a_ha_rq',
        'ha_gap_o_eff','ha_gap_d_eff','ha_pace_avg','h_has_hasla','a_has_hasla',
    ],
    'HA_SHOT': [
        'h_ha_o_3par','h_ha_d_3par','h_ha_o_prox','h_ha_d_prox',
        'h_ha_o_mrar','h_ha_d_mrar','h_ha_o_fg_pct','h_ha_d_fg_pct',
        'a_ha_o_3par','a_ha_d_3par','a_ha_o_prox','a_ha_d_prox',
        'a_ha_o_mrar','a_ha_d_mrar','a_ha_o_fg_pct','a_ha_d_fg_pct',
        'ha_gap_o_3par','ha_gap_d_3par','ha_gap_o_prox','ha_gap_d_prox',
        'ha_gap_o_mrar','ha_gap_d_mrar',
    ],
    'HA_DELTA': [
        'h_ha_delta_o_eff','h_ha_delta_d_eff','h_ha_delta_pace',
        'a_ha_delta_o_eff','a_ha_delta_d_eff','a_ha_delta_pace',
        'ha_momentum_gap','ha_def_mom_gap',
    ],
    'HA_MATCHUP': [
        'ha_3par_matchup_h','ha_3par_matchup_a',
        'ha_prox_matchup_h','ha_prox_matchup_a',
    ],
    'KPD': [
        'h_kpd_adj_em','h_kpd_adj_o','h_kpd_adj_d','h_kpd_adj_tempo','h_kpd_luck',
        'h_kpd_sos','h_kpd_sos_o','h_kpd_sos_d','h_kpd_rank_adj_em','h_kpd_pythag',
        'a_kpd_adj_em','a_kpd_adj_o','a_kpd_adj_d','a_kpd_adj_tempo','a_kpd_luck',
        'a_kpd_sos','a_kpd_sos_o','a_kpd_sos_d','a_kpd_rank_adj_em','a_kpd_pythag',
        'kpd_em_gap','kpd_luck_gap','kpd_sos_gap','has_kpd_home','has_kpd_away',
    ],
    'KP_FANMATCH': [
        'kp_home_pred','kp_away_pred','kp_home_wp','kp_pred_margin',
        'kp_pred_tempo','has_kp_fanmatch',
    ],
    'ROLLING': [
        'h_rol_r5_efg','h_rol_r5_tov','h_rol_r5_orb','h_rol_r5_pace',
        'h_rol_r5_pts_off','h_rol_r5_pts_def','h_rol_r5_margin',
        'h_rol_r10_margin','h_rol_ew_margin','h_rol_trend_margin',
        'a_rol_r5_efg','a_rol_r5_tov','a_rol_r5_orb','a_rol_r5_pace',
        'a_rol_r5_pts_off','a_rol_r5_pts_def','a_rol_r5_margin',
        'a_rol_r10_margin','a_rol_ew_margin','a_rol_trend_margin',
        'rol_margin_gap','rol_efg_gap','rol_trend_gap','has_rol_home','has_rol_away',
    ],
}

LEAKY_SOURCES = {'TVS', 'KP', 'HA_CORE', 'HA_SHOT', 'HA_DELTA', 'HA_MATCHUP'}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--combo', type=str, default=None)
    return p.parse_args()


def get_feature_cols(df, combo_str):
    groups = [g.strip() for g in combo_str.split('+')] if combo_str else list(FEATURE_GROUPS.keys())
    cols = []
    for g in groups:
        for c in FEATURE_GROUPS.get(g, []):
            if c in df.columns and c not in cols:
                cols.append(c)
    leaky = any(g in LEAKY_SOURCES for g in groups)
    return cols, leaky


def load_data():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT * FROM game_features_v2", conn)
    conn.close()
    df['game_date'] = pd.to_datetime(df['game_date'])
    df['ats_win'] = pd.to_numeric(df['ats_win'], errors='coerce')
    return df


def make_base():
    return XGBClassifier(
        n_estimators=300, max_depth=4, learning_rate=0.04,
        subsample=0.8, colsample_bytree=0.75, min_child_weight=15,
        reg_alpha=0.1, reg_lambda=1.5,
        use_label_encoder=False, eval_metric='logloss',
        random_state=42, n_jobs=-1, verbosity=0,
    )


def walkforward_eval(df, feat_cols):
    seasons = sorted(df['season'].unique())
    test_seasons = [s for s in seasons if seasons.index(s) >= 4]

    print("\n--- Walk-Forward Validation (calibrated ATS classifier) ---")
    print(f"{'Season':<8} {'Train':>7} {'Test':>7} {'Bets':>6} {'WR':>7} {'ROI':>8}")
    print("-" * 50)

    all_results = []
    for test_s in test_seasons:
        train_ss = [s for s in seasons if s < test_s][-4:]
        train = df[df['season'].isin(train_ss) & df['ats_win'].notna()].copy()
        test  = df[
            (df['season'] == test_s) &
            df['spread'].abs().between(SPREAD_MIN, SPREAD_MAX) &
            df['ats_win'].notna()
        ].copy()
        if len(test) < 50 or len(train) < 200:
            continue

        valid = [c for c in feat_cols if train[c].notna().sum() > 50]
        if len(valid) < 3:
            continue

        imp = SimpleImputer(strategy='median')
        X_tr = imp.fit_transform(train[valid])
        X_te = imp.transform(test[valid])
        y_tr = train['ats_win'].astype(int).values

        cal = CalibratedClassifierCV(make_base(), method='isotonic', cv=3)
        cal.fit(X_tr, y_tr)

        p_home = cal.predict_proba(X_te)[:, 1]
        p_away = 1 - p_home
        test = test.copy()
        test['ev_home'] = p_home * (1 + PAYOUT) - 1
        test['ev_away'] = p_away * (1 + PAYOUT) - 1
        test['best_ev']  = test[['ev_home','ev_away']].max(axis=1)
        test['bet_home'] = test['ev_home'] >= test['ev_away']

        bets = test[test['best_ev'] >= EV_MIN].copy()
        bets['bet_won'] = (
            (bets['bet_home'] & (bets['ats_win'] == 1)) |
            (~bets['bet_home'] & (bets['ats_win'] == 0))
        ).astype(int)

        n = len(bets)
        if n >= 10:
            wins = bets['bet_won'].sum()
            wr  = wins / n
            roi = (wins * PAYOUT - (n - wins)) / n
            print(f"  {test_s:<8} {len(train):>7} {len(test):>7} {n:>6} {wr:>7.3f} {roi:>+8.3f}")
        else:
            print(f"  {test_s:<8} {len(train):>7} {len(test):>7} {'<10':>6}")
        all_results.append(bets)

    if all_results:
        all_bets = pd.concat(all_results)
        n = len(all_bets)
        wins = all_bets['bet_won'].sum()
        wr  = wins / n
        roi = (wins * PAYOUT - (n - wins)) / n
        print("-" * 50)
        print(f"  {'TOTAL':<8} {'':>7} {'':>7} {n:>6} {wr:>7.3f} {roi:>+8.3f}")
        print(f"\n  Breakeven WR: 52.38% | Bets with EV≥{EV_MIN:.0%}: {n}")


def train_production_model(df, feat_cols):
    labeled = df[df['ats_win'].notna()].copy()
    valid = [c for c in feat_cols if labeled[c].notna().mean() > 0.1]

    imp = SimpleImputer(strategy='median')
    X = imp.fit_transform(labeled[valid])
    y = labeled['ats_win'].astype(int).values

    print(f"  Training on {len(labeled):,} labeled games, {len(valid)} features...")
    cal = CalibratedClassifierCV(make_base(), method='isotonic', cv=5)
    cal.fit(X, y)

    # Feature importances averaged across CV folds
    try:
        fi = np.mean([e.estimator.feature_importances_
                      for e in cal.calibrated_classifiers_], axis=0)
        fi_df = pd.DataFrame({'feature': valid, 'importance': fi}).sort_values('importance', ascending=False)

        def tag(f):
            for src, cols in FEATURE_GROUPS.items():
                if f in cols: return src
            return 'other'
        fi_df['source'] = fi_df['feature'].apply(tag)

        print("\n--- Top 20 Features ---")
        for _, row in fi_df.head(20).iterrows():
            bar = '█' * int(row['importance'] * 300)
            print(f"  {row['feature']:<45} [{row['source']:<14}] {row['importance']:.4f} {bar}")

        src = fi_df.groupby('source')['importance'].sum().sort_values(ascending=False)
        print("\n--- Source Importance ---")
        for s, v in src.items():
            print(f"  {s:<20} {v:.3f} {'█' * int(v * 100)}")

        fi_df.to_csv(os.path.join(OUT, 'feature_importance.csv'), index=False)
    except Exception as e:
        print(f"  (importance unavailable: {e})")

    return cal, imp, valid


if __name__ == '__main__':
    args = parse_args()
    combo = args.combo

    print("=" * 60)
    print("NCAAB Model — Training Production Model")
    print("=" * 60)

    df = load_data()
    print(f"Loaded {len(df):,} games")

    feat_cols, is_leaky = get_feature_cols(df, combo)
    print(f"Combo: {combo or 'ALL'}")
    print(f"Features: {len(feat_cols)} cols from specified groups")
    if is_leaky:
        print("⚠  WARNING: combo includes leaky season-final sources")

    walkforward_eval(df, feat_cols)

    print("\nTraining production model on full dataset...")
    cal_model, imputer, final_feats = train_production_model(df, feat_cols)

    with open(os.path.join(MODEL, 'production_model.pkl'), 'wb') as f:
        pickle.dump(cal_model, f)
    with open(os.path.join(MODEL, 'imputer.pkl'), 'wb') as f:
        pickle.dump(imputer, f)
    with open(os.path.join(MODEL, 'feature_cols.json'), 'w') as f:
        json.dump({'combo': combo, 'features': final_feats}, f, indent=2)

    print(f"\n✅ Saved: models/production_model.pkl ({len(final_feats)} features)")
    print(f"   models/imputer.pkl | models/feature_cols.json")
    print(f"\nNext: python scripts/07_daily_bets.py")
