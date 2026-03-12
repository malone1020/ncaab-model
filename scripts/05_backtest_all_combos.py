"""
05_backtest_all_combos.py
=========================
Walk-forward backtesting across all feature group combinations.
Train on seasons T-4 through T-1, test on season T.
Ranks combos by out-of-sample ATS ROI at -110 with edge >= 3.

Feature groups:
  CONTEXT    - game context (always included)
  TVS        - Torvik season efficiency
  TVD        - Torvik daily pre-game snapshot
  KP         - KenPom pretourney ratings
  HA_CORE    - Haslametrics TI efficiency (o_eff, d_eff, pace)
  HA_SHOT    - Haslametrics shot quality (3par, prox, mrar)
  HA_DELTA   - Haslametrics momentum delta (td - ti)
  HA_MATCHUP - Haslametrics cross-team matchup features

Run: python scripts/05_backtest_all_combos.py
"""
import sqlite3, os, json, itertools
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.impute import SimpleImputer

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
OUT_DIR = os.path.join(ROOT, 'outputs')
os.makedirs(OUT_DIR, exist_ok=True)

# ── Feature group definitions ────────────────────────────────────────────────
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
        'ha_gap_o_eff','ha_gap_d_eff','ha_pace_avg',
        'h_has_hasla','a_has_hasla',
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
}

OPTIONAL_SOURCES = ['TVS','TVD','KP','HA_CORE','HA_SHOT','HA_DELTA','HA_MATCHUP']
SPREAD_MIN, SPREAD_MAX = 0.5, 9.0
EDGE_MIN   = 3.0
PAYOUT     = 100 / 110  # -110 juice

# ── Load data ────────────────────────────────────────────────────────────────
print("Loading game_features_v2...")
conn = sqlite3.connect(DB)
df   = pd.read_sql("SELECT * FROM game_features_v2", conn)
conn.close()
df['game_date'] = pd.to_datetime(df['game_date'])
print(f"  {len(df):,} rows, {len(df.columns)} columns, seasons {df.season.min()}-{df.season.max()}")

# ── Walk-forward backtest ────────────────────────────────────────────────────
def backtest_combo(group_names):
    feat_cols = []
    for g in group_names:
        for c in FEATURE_GROUPS[g]:
            if c in df.columns and c not in feat_cols:
                feat_cols.append(c)

    seasons   = sorted(df['season'].unique())
    min_train = 4  # need at least 4 prior seasons
    test_seasons = [s for s in seasons if seasons.index(s) >= min_train]
    if len(test_seasons) < 2:
        return None

    all_preds = []
    for test_s in test_seasons:
        train_ss = [s for s in seasons if s < test_s][-4:]
        train    = df[df['season'].isin(train_ss)].copy()
        test     = df[df['season'] == test_s].copy()

        # Filter to spread range
        test = test[(test['spread'].abs() >= SPREAD_MIN) &
                    (test['spread'].abs() <= SPREAD_MAX)]
        if len(test) < 50:
            continue

        X_train = train[feat_cols].copy()
        y_train = train['actual_margin'].copy()
        X_test  = test[feat_cols].copy()

        # Drop cols all-null in train
        valid = [c for c in feat_cols if X_train[c].notna().sum() > 50]
        X_train = X_train[valid]
        X_test  = X_test[valid]

        imp = SimpleImputer(strategy='median')
        X_train_imp = imp.fit_transform(X_train)
        X_test_imp  = imp.transform(X_test)

        model = XGBRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            min_child_weight=10, reg_lambda=2.0,
            random_state=42, n_jobs=-1, verbosity=0,
        )
        model.fit(X_train_imp, y_train)
        preds = model.predict(X_test_imp)

        test = test.copy()
        test['pred_margin'] = preds
        # Edge: how much model disagrees with line (home perspective)
        # Positive edge = model thinks home covers
        test['edge'] = test['pred_margin'] - (-test['spread'])
        all_preds.append(test)

    if not all_preds:
        return None

    result_df = pd.concat(all_preds)
    bet = result_df[result_df['edge'].abs() >= EDGE_MIN].copy()
    if len(bet) < 100:
        return None

    wins  = bet['ats_win'].sum()
    total = len(bet)
    wr    = wins / total
    roi   = (wins * PAYOUT - (total - wins)) / total
    return {
        'combo': '+'.join(group_names),
        'n_bets': total,
        'win_rate': round(wr, 4),
        'roi': round(roi, 4),
        'n_seasons': len(test_seasons),
    }

# ── Run all combinations ─────────────────────────────────────────────────────
print(f"\nTesting all combinations of {len(OPTIONAL_SOURCES)} optional feature groups...")
print(f"(always including CONTEXT)\n")

results = []
total_combos = sum(len(list(itertools.combinations(OPTIONAL_SOURCES, r)))
                   for r in range(1, len(OPTIONAL_SOURCES)+1))
print(f"Total combos: {total_combos}")

done = 0
for r in range(1, len(OPTIONAL_SOURCES)+1):
    for combo in itertools.combinations(OPTIONAL_SOURCES, r):
        groups = ['CONTEXT'] + list(combo)
        res = backtest_combo(groups)
        if res:
            results.append(res)
            if res['roi'] > 0:
                print(f"  ✓ {res['combo']:50s} | {res['n_bets']:5d} bets | "
                      f"WR={res['win_rate']:.3f} | ROI={res['roi']:+.3f}")
        done += 1
        if done % 10 == 0:
            print(f"  ... {done}/{total_combos} combos done", end='\r')

print(f"\nCompleted {done} combos, {len(results)} with sufficient data")

# ── Save and rank results ────────────────────────────────────────────────────
res_df = pd.DataFrame(results).sort_values('roi', ascending=False)
res_df.to_csv(os.path.join(OUT_DIR, 'combo_backtest_results.csv'), index=False)

print("\n" + "="*80)
print("TOP 15 COMBOS BY OUT-OF-SAMPLE ROI")
print("="*80)
print(f"{'Combo':<55} {'Bets':>6} {'WR':>6} {'ROI':>7}")
print("-"*80)
for _, r in res_df.head(15).iterrows():
    print(f"{r['combo']:<55} {r['n_bets']:>6} {r['win_rate']:>6.3f} {r['roi']:>+7.3f}")

best = res_df.iloc[0]
print(f"\nBest combo: {best['combo']}")
print(f"  Win rate: {best['win_rate']:.3f} | ROI: {best['roi']:+.4f} | Bets: {best['n_bets']}")
print(f"\nNext: python scripts/06_train_final_model.py --combo \"{best['combo']}\"")
