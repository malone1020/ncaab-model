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
from xgboost import XGBClassifier
from sklearn.impute import SimpleImputer

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
OUT_DIR = os.path.join(ROOT, 'outputs')
os.makedirs(OUT_DIR, exist_ok=True)

# ── Feature group definitions ────────────────────────────────────────────────
FEATURE_GROUPS = {
    'CONTEXT': [
        'neutral_site', 'conf_game', 'hca_adj', 'rest_diff',
        'is_conf_tournament', 'is_ncaa_tournament',
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
    # ── NEW CLEAN SOURCES ────────────────────────────────────────────────────
    'TRAVEL': [
        'away_travel_miles', 'tz_crossings', 'east_to_west', 'west_to_east',
        'away_road_game_n', 'away_long_trip', 'away_tz_change',
        'neutral_home_miles',
    ],
    'REFS': [
        'ref_avg_fpg', 'ref_home_bias', 'ref_ftr_home_avg',
        'ref_ftr_away_avg', 'ref_ftr_gap', 'has_ref_data',
        'ref_high_foul', 'ref_low_foul',
    ],
    'RECENCY': [
        'h_rew_adj_em', 'h_rew_adj_o', 'h_rew_adj_d',
        'a_rew_adj_em', 'a_rew_adj_o', 'a_rew_adj_d',
        'h_trend_adj_em', 'a_trend_adj_em',
        'rew_em_gap', 'rew_o_gap', 'rew_d_gap', 'trend_em_gap', 'has_rew',
    ],
    'EXPERIENCE': [
        'h_experience', 'a_experience', 'exp_gap', 'has_experience',
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
        'rol_margin_gap','rol_efg_gap','rol_trend_gap',
        'has_rol_home','has_rol_away',
    ],
}

# Clean sources (no look-ahead leakage) — use for primary backtest
CLEAN_SOURCES   = ['TVD','KPD','KP_FANMATCH','ROLLING','TRAVEL','REFS','RECENCY','EXPERIENCE']
# Leaky sources (season-final ratings) — NOT included in backtest.
# Their ~22% ROI is ~3x inflated vs clean combos and unusable in production.
LEAKY_SOURCES   = ['TVS','KP','HA_CORE','HA_SHOT','HA_DELTA','HA_MATCHUP']
# Only test clean combos: 2^8 - 1 = 255 combos, ~20 min runtime
OPTIONAL_SOURCES = CLEAN_SOURCES
SPREAD_MIN, SPREAD_MAX = 0.5, 9.0
EV_MIN     = 0.03       # minimum EV threshold: P(cover)*1.909 - 1 >= 0.03
PAYOUT     = 100 / 110  # -110 juice
JUICE_IMPL = 110 / 210  # implied prob at -110 = 52.38%

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
    min_train = 4
    test_seasons = [s for s in seasons if seasons.index(s) >= min_train]
    if len(test_seasons) < 2:
        return None

    all_preds = []
    for test_s in test_seasons:
        train_ss = [s for s in seasons if s < test_s][-4:]
        train    = df[df['season'].isin(train_ss)].copy()
        test     = df[df['season'] == test_s].copy()

        # Filter: spread range + must have ats_win label
        test = test[(test['spread'].abs() >= SPREAD_MIN) &
                    (test['spread'].abs() <= SPREAD_MAX) &
                    test['ats_win'].notna()]
        train = train[train['ats_win'].notna()]
        if len(test) < 50 or len(train) < 200:
            continue

        X_train = train[feat_cols].copy()
        y_train = train['ats_win'].astype(int)
        X_test  = test[feat_cols].copy()

        # Drop cols all-null in train
        valid = [c for c in feat_cols if X_train[c].notna().sum() > 50]
        X_train = X_train[valid]
        X_test  = X_test[valid]

        imp = SimpleImputer(strategy='median')
        X_train_imp = imp.fit_transform(X_train)
        X_test_imp  = imp.transform(X_test)

        model = XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            min_child_weight=10, reg_lambda=2.0,
            use_label_encoder=False, eval_metric='logloss',
            random_state=42, n_jobs=-1, verbosity=0,
        )
        model.fit(X_train_imp, y_train)
        # P(home covers spread)
        p_home = model.predict_proba(X_test_imp)[:, 1]
        p_away = 1 - p_home

        test = test.copy()
        test['p_home_cover'] = p_home
        # EV for betting home: P(cover)*1.909 - 1
        test['ev_home'] = p_home * (1 + PAYOUT) - 1
        # EV for betting away: P(away cover)*1.909 - 1
        test['ev_away'] = p_away * (1 + PAYOUT) - 1
        # Best side and its EV
        test['best_ev']   = test[['ev_home','ev_away']].max(axis=1)
        test['bet_home']  = test['ev_home'] >= test['ev_away']
        all_preds.append(test)

    if not all_preds:
        return None

    result_df = pd.concat(all_preds)
    # Only bet when best EV exceeds threshold
    bet = result_df[result_df['best_ev'] >= EV_MIN].copy()
    if len(bet) < 100:
        return None

    bet['bet_won'] = (
        (bet['bet_home'] & (bet['ats_win'] == 1)) |
        (~bet['bet_home'] & (bet['ats_win'] == 0))
    ).astype(int)

    wins  = bet['bet_won'].sum()
    total = len(bet)
    wr    = wins / total
    roi   = (wins * PAYOUT - (total - wins)) / total
    avg_ev = bet['best_ev'].mean()
    return {
        'combo': '+'.join(group_names),
        'n_bets': total,
        'win_rate': round(wr, 4),
        'roi': round(roi, 4),
        'avg_ev': round(avg_ev, 4),
        'n_seasons': len(test_seasons),
        'leaky': any(g in LEAKY_SOURCES for g in group_names),
    }

# ── Run all combinations ─────────────────────────────────────────────────────
print(f"\nTesting all combinations of {len(OPTIONAL_SOURCES)} optional feature groups...")
print(f"(always including CONTEXT; EV threshold = {EV_MIN:.0%})\n")

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
            flag = '⚠ LEAKY' if res['leaky'] else '✓ CLEAN'
            if res['roi'] > 0:
                print(f"  {flag} {res['combo']:55s} | {res['n_bets']:5d} bets | "
                      f"WR={res['win_rate']:.3f} | ROI={res['roi']:+.3f} | EV={res['avg_ev']:+.3f}")
        done += 1
        if done % 10 == 0:
            print(f"  ... {done}/{total_combos} combos done", end='\r')

print(f"\nCompleted {done} combos, {len(results)} with sufficient data")

# ── Save and rank results ────────────────────────────────────────────────────
res_df = pd.DataFrame(results).sort_values('roi', ascending=False)
res_df.to_csv(os.path.join(OUT_DIR, 'combo_backtest_results.csv'), index=False)

print("\n" + "="*90)
print("TOP 10 CLEAN COMBOS — BY OUT-OF-SAMPLE ROI (all combos are clean in this run)")
print("="*90)
print(f"{'Combo':<60} {'Bets':>6} {'WR':>6} {'ROI':>7} {'AvgEV':>7}")
print("-"*90)
for _, r in res_df.head(10).iterrows():
    print(f"{r['combo']:<60} {r['n_bets']:>6} {r['win_rate']:>6.3f} {r['roi']:>+7.3f} {r['avg_ev']:>+7.3f}")

best = res_df.iloc[0]
print(f"\nBest combo: {best['combo']}")
print(f"  Win rate: {best['win_rate']:.3f} | ROI: {best['roi']:+.4f} | AvgEV: {best['avg_ev']:+.4f} | Bets: {best['n_bets']}")
print(f"\nNext: python scripts/06_train_final_model.py --combo \"{best['combo']}\"")
