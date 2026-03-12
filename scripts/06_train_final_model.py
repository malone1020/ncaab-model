"""
06_train_final_model.py
=======================
Train the final production model using the best feature combination
identified by 05_backtest_all_combos.py.

Saves: models/production_model.json
       outputs/feature_importance.csv
       outputs/final_backtest_summary.txt

Run: python scripts/06_train_final_model.py
"""

import sqlite3, os, json, warnings
import pandas as pd
import numpy as np
from xgboost import XGBRegressor

warnings.filterwarnings('ignore')

ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB    = os.path.join(ROOT, 'data', 'basketball.db')
OUT   = os.path.join(ROOT, 'outputs')
MODEL = os.path.join(ROOT, 'models')
os.makedirs(OUT, exist_ok=True)
os.makedirs(MODEL, exist_ok=True)

# ── Paste the best combination from 05_backtest output here ──
# Or set to None to use ALL features (safest default)
BEST_COMBO = None  # e.g. 'kenpom+torvik_season+haslametrics+rolling'

# Feature group definitions (must match 05_backtest_all_combos.py)
FEATURE_GROUPS = {
    'kenpom': [
        'kp_em_gap','kp_o_gap','kp_d_gap',
        'h_kp_em','a_kp_em','h_kp_o','a_kp_o','h_kp_d','a_kp_d',
    ],
    'torvik_season': [
        'tv_em_gap','tv_barthag_gap','tv_efg_gap','tv_tov_gap','tv_orb_gap',
        'h_tv_adj_em','a_tv_adj_em','h_tv_barthag','a_tv_barthag',
        'h_tv_efg_o','a_tv_efg_o','h_tv_efg_d','a_tv_efg_d',
        'h_tv_tov_o','a_tv_tov_o','h_tv_ftr_o','a_tv_ftr_o',
        'h_tv_adj_t','a_tv_adj_t',
    ],
    'torvik_extended': [
        'tv_height_gap','tv_exp_gap','tv_talent_gap','tv_wab_gap',
        'h_tv_avg_hgt','a_tv_avg_hgt','h_tv_experience','a_tv_experience',
        'h_tv_talent','a_tv_talent','h_tv_blk_pct','a_tv_blk_pct',
        'h_tv_ast_pct','a_tv_ast_pct','h_tv_elite_sos','a_tv_elite_sos',
        'h_tv_pake','a_tv_pake','h_tv_pase','a_tv_pase',
    ],
    'torvik_daily': [
        'tvd_em_gap','tvd_efg_gap','tvd_bar_gap',
        'h_tvd_adj_em','a_tvd_adj_em','h_tvd_barthag','a_tvd_barthag',
        'h_tvd_efg_o','a_tvd_efg_o','h_tvd_adj_t','a_tvd_adj_t',
    ],
    'torvik_preds': ['torvik_pred','torvik_prob','torvik_vs_spread'],
    'haslametrics': [
        'ha_eff_gap','ha_prox_gap','ha_ap_gap','ha_mrar_gap',
        'ha_ppsc_gap','ha_scc_gap',
        'h_ha_eff','a_ha_eff','h_ha_prox','a_ha_prox',
        'h_ha_ap_pct','a_ha_ap_pct','h_ha_mrar','a_ha_mrar',
        'h_ha_ppsc','a_ha_ppsc','h_ha_scc_pct','a_ha_scc_pct',
        'h_ha_pct_3pa','a_ha_pct_3pa','h_ha_pct_mra','a_ha_pct_mra',
    ],
    'rolling': [
        'roll_ppp_gap','roll_efg_gap','roll_pts_gap',
        'h_r_ppp','a_r_ppp','h_r_efg','a_r_efg',
        'h_r_tov','a_r_tov','h_r_orb','a_r_orb',
        'h_r_pts','a_r_pts',
    ],
    'context': [
        'hca','hca_adj','rest_diff','home_b2b','away_b2b',
        'neutral_site','conf_game',
    ],
    'line_movement': ['line_move'],
}

VIG = -110
EDGE_THRESHOLD = 3.0
SPREAD_FILTER  = (0.5, 9.0)
EXCLUDE_BUCKET = (9.0, 12.0)  # historically poor


def ats_roi(wins, n):
    if n == 0: return 0
    wr = wins / n
    return wr * (100 / abs(VIG)) - (1 - wr)


def load_data():
    conn = sqlite3.connect(DB)
    df = pd.read_sql("SELECT * FROM game_features_v2", conn)
    conn.close()
    return df


def get_feature_cols(df, combo=None):
    all_sources = list(FEATURE_GROUPS.keys())
    if combo:
        active = ['context'] + combo.split('+')
    else:
        active = all_sources

    cols = []
    for src in active:
        if src in FEATURE_GROUPS:
            cols += [c for c in FEATURE_GROUPS[src] if c in df.columns]
    return list(dict.fromkeys(cols))


def walkforward_eval(df, feature_cols):
    """Full walk-forward evaluation with per-season reporting."""
    seasons = sorted(df['season'].unique())
    all_preds = []

    print("\n--- Walk-Forward Validation ---")
    print(f"{'Season':<8} {'Train':>7} {'Test':>7} {'MAE':>7} {'E3-Filt WR':>12} {'ROI':>8} {'Bets':>6}")

    for test_s in seasons[2:]:
        train = df[df['season'] < test_s]
        test  = df[df['season'] == test_s].copy()

        avail = [c for c in feature_cols if c in train.columns and train[c].notna().mean() > 0.35]
        if len(avail) < 3: continue

        X_tr = train[avail].fillna(train[avail].median())
        y_tr = train['actual_margin'].dropna()
        X_tr = X_tr.loc[y_tr.index]
        X_te = test[avail].fillna(train[avail].median())

        model = XGBRegressor(
            n_estimators=300, max_depth=4, learning_rate=0.04,
            subsample=0.8, colsample_bytree=0.75, min_child_weight=15,
            reg_alpha=0.1, reg_lambda=1.5, random_state=42, verbosity=0
        )
        model.fit(X_tr, y_tr)
        test['pred'] = model.predict(X_te)
        all_preds.append(test)

        mae = np.abs(test['actual_margin'] - test['pred']).mean()

        # ATS filtered
        bets = test[
            test['spread'].notna() & test['ats_win'].notna() &
            test['spread'].abs().between(*SPREAD_FILTER) &
            ~test['spread'].abs().between(*EXCLUDE_BUCKET)
        ].copy()
        bets['edge'] = bets['pred'] - (-bets['spread'])
        bets = bets[bets['edge'].abs() >= EDGE_THRESHOLD]

        if len(bets) >= 10:
            wins = bets['ats_win'].sum()
            n = len(bets)
            wr = wins / n
            roi = ats_roi(wins, n)
            print(f"  {test_s:<8} {len(train):>7} {len(test):>7} {mae:>7.2f} {wr:>11.1%} {roi:>+8.1%} {n:>6}")
        else:
            print(f"  {test_s:<8} {len(train):>7} {len(test):>7} {mae:>7.2f} {'—':>12} {'—':>8} {len(bets):>6}")

    return pd.concat(all_preds, ignore_index=True) if all_preds else pd.DataFrame()


def train_production_model(df, feature_cols):
    """Train final model on all available data."""
    avail = [c for c in feature_cols if c in df.columns and df[c].notna().mean() > 0.35]
    X = df[avail].fillna(df[avail].median())
    y = df['actual_margin'].dropna()
    X = X.loc[y.index]

    model = XGBRegressor(
        n_estimators=400, max_depth=4, learning_rate=0.04,
        subsample=0.8, colsample_bytree=0.75, min_child_weight=15,
        reg_alpha=0.1, reg_lambda=1.5, random_state=42, verbosity=0
    )
    model.fit(X, y)

    # Feature importances
    importances = pd.DataFrame({
        'feature': avail,
        'importance': model.feature_importances_,
    }).sort_values('importance', ascending=False)

    # Tag source
    def tag_source(feat):
        for src, cols in FEATURE_GROUPS.items():
            if feat in cols: return src
        return 'unknown'
    importances['source'] = importances['feature'].apply(tag_source)

    print("\n--- Top 25 Features ---")
    for _, row in importances.head(25).iterrows():
        bar = '█' * int(row['importance'] * 300)
        print(f"  {row['feature']:<40} [{row['source']:<18}] {row['importance']:.4f} {bar}")

    # Source importance summary
    src_imp = importances.groupby('source')['importance'].sum().sort_values(ascending=False)
    print("\n--- Source Importance Summary ---")
    for src, imp in src_imp.items():
        bar = '█' * int(imp * 100)
        print(f"  {src:<20} {imp:.3f} {bar}")

    # Save
    model_path = os.path.join(MODEL, 'production_model.json')
    model.save_model(model_path)
    importances.to_csv(os.path.join(OUT, 'feature_importance.csv'), index=False)

    # Save feature list for daily bet card
    with open(os.path.join(MODEL, 'feature_cols.json'), 'w') as f:
        json.dump(avail, f)

    print(f"\nModel saved: {model_path}")
    print(f"Features saved: {os.path.join(MODEL, 'feature_cols.json')}")

    return model, avail


def final_summary(combined_df, edge=EDGE_THRESHOLD, spread_range=SPREAD_FILTER):
    """Print final ATS summary stats."""
    df = combined_df.copy()
    df = df[df['spread'].notna() & df['ats_win'].notna()]

    print("\n" + "="*60)
    print("FINAL PRODUCTION MODEL — ATS SUMMARY")
    print("="*60)

    for edge_val in [2.0, 3.0, 4.0, 5.0]:
        df['edge'] = df['pred'] - (-df['spread'])
        bets_all  = df[df['edge'].abs() >= edge_val]
        bets_filt = bets_all[
            bets_all['spread'].abs().between(*spread_range) &
            ~bets_all['spread'].abs().between(*EXCLUDE_BUCKET)
        ]

        for label, bets in [('ALL spreads', bets_all), (f'{spread_range[0]}-{spread_range[1]}pt', bets_filt)]:
            if len(bets) < 20: continue
            wins = bets['ats_win'].sum()
            n = len(bets)
            wr = wins / n
            roi = ats_roi(wins, n)
            print(f"  Edge≥{edge_val:.0f} {label:<18} WR={wr:.1%}  ROI={roi:+.1%}  ({n} bets)")

    print()


if __name__ == '__main__':
    print("="*60)
    print("NCAAB Model — Training Production Model")
    print("="*60)

    df = load_data()
    print(f"Loaded {len(df):,} games")

    feature_cols = get_feature_cols(df, BEST_COMBO)
    print(f"Using {len(feature_cols)} candidate features from: {BEST_COMBO or 'ALL sources'}")

    # Walk-forward validation
    combined = walkforward_eval(df, feature_cols)
    if not combined.empty:
        final_summary(combined)

    # Train production model on all data
    print("\nTraining production model on full dataset...")
    model, final_features = train_production_model(df, feature_cols)
    print(f"\n✅ Production model trained ({len(final_features)} features)")
    print("Next: python scripts/07_daily_bets.py")
