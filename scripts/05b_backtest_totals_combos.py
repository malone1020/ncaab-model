"""
05b_backtest_totals_combos.py
==============================
Walk-forward backtesting across all feature group combinations for the
totals (Over/Under) model. Mirrors the methodology of 05_backtest_all_combos.py
but targets went_over instead of ats_win.

Feature groups tested (CONTEXT always included):
  TVD        - Torvik daily efficiency + pace proxy
  KPD        - KenPom daily (best direct tempo measure)
  KP_FANMATCH - Game-specific pace/score predictions
  ROLLING    - Recent scoring trends (pts_off, pts_def, pace)
  RECENCY    - Recency-weighted efficiency
  REFS       - Referee foul tendencies (affects pace directly)
  TRAVEL     - Fatigue effects on scoring output
  LINE_MOVE  - Opening/closing line movement (if line_movement table populated)

255 combos (2^8 - 1), ~20 min runtime.

Run: python scripts/05b_backtest_totals_combos.py
     python scripts/05b_backtest_totals_combos.py --min-seasons 4
"""

import sqlite3, os, json, itertools, argparse
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.impute import SimpleImputer

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB      = os.path.join(ROOT, 'data', 'basketball.db')
OUT_DIR = os.path.join(ROOT, 'outputs')
os.makedirs(OUT_DIR, exist_ok=True)

PAYOUT   = 100 / 110
EV_MIN   = 0.03
MIN_BETS = 100

# ── Feature group definitions ────────────────────────────────────────────────
# CONTEXT is always included — these are game-level modifiers that affect scoring
CONTEXT_FEATURES = [
    'neutral_site', 'conf_game',
    'home_rest', 'away_rest', 'home_b2b', 'away_b2b',
    'over_under',   # the line itself is a feature — market's best guess at total
    'is_conf_tournament', 'is_ncaa_tournament',  # tournament type flags
]

FEATURE_GROUPS = {
    # Torvik daily — offensive/defensive efficiency, EFG as pace proxy
    'TVD': [
        'h_tvd_adj_o', 'h_tvd_adj_d', 'h_tvd_barthag',
        'a_tvd_adj_o', 'a_tvd_adj_d', 'a_tvd_barthag',
        'tvd_em_gap',
        'h_tvd_efg_o', 'h_tvd_efg_d',
        'a_tvd_efg_o', 'a_tvd_efg_d',
        'has_tvd_home', 'has_tvd_away',
    ],

    # KenPom daily — best direct tempo measure in the dataset
    'KPD': [
        'h_kpd_adj_o', 'h_kpd_adj_d', 'h_kpd_adj_tempo',
        'a_kpd_adj_o', 'a_kpd_adj_d', 'a_kpd_adj_tempo',
        'kpd_em_gap',
        'has_kpd_home', 'has_kpd_away',
        # Derived pace features (computed in build_totals_features)
        'avg_kpd_tempo',                     # (h_tempo + a_tempo) / 2
        'h_kpd_adj_o_minus_a_kpd_adj_d',    # home off vs away def
        'a_kpd_adj_o_minus_h_kpd_adj_d',    # away off vs home def
    ],

    # KenPom fanmatch — game-specific predicted scores and pace
    'KP_FANMATCH': [
        'kp_pred_tempo',    # single best totals feature — game-specific pace projection
        'kp_home_pred',     # predicted scores proxy total directly
        'kp_away_pred',
        'kp_pred_margin',
        'kp_pred_total',    # derived: kp_home_pred + kp_away_pred
        'has_kp_fanmatch',
    ],

    # Rolling box score — recent scoring pace and efficiency trends
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

    # Recency-weighted efficiency — recent form on offense/defense
    'RECENCY': [
        'h_rew_adj_em', 'h_rew_adj_o', 'h_rew_adj_d',
        'a_rew_adj_em', 'a_rew_adj_o', 'a_rew_adj_d',
        'h_trend_adj_em', 'a_trend_adj_em',
        'rew_em_gap', 'rew_o_gap', 'rew_d_gap', 'trend_em_gap', 'has_rew',
    ],

    # Referee tendencies — foul rate directly affects pace and scoring
    'REFS': [
        'ref_avg_fpg', 'ref_home_bias', 'ref_ftr_home_avg',
        'ref_ftr_away_avg', 'ref_ftr_gap', 'has_ref_data',
        'ref_high_foul', 'ref_low_foul',
    ],

    # Travel/fatigue — tired teams score differently
    'TRAVEL': [
        'away_travel_miles', 'tz_crossings', 'east_to_west', 'west_to_east',
        'away_road_game_n', 'away_long_trip', 'away_tz_change',
    ],

    # Line movement — sharp money signal (only available if 10_scrape_historical_lines.py has run)
    'LINE_MOVE': [
        'spread_open', 'total_open',
        'spread_close', 'total_close',
        'spread_move',   # spread_close - spread_open
        'total_move',    # total_close - total_open (sharp signal for totals)
        'ml_home_move',
    ],
}

OPTIONAL_SOURCES = list(FEATURE_GROUPS.keys())


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--min-seasons', type=int, default=4,
                   help='Minimum training seasons before first test (default: 4)')
    return p.parse_args()


def load_data():
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
    print(f"  {len(lines):,} games with O/U target")

    # Merge target onto features
    df = df.merge(
        lines[['game_date', 'home_team', 'away_team', 'over_under', 'went_over']],
        on=['game_date', 'home_team', 'away_team'],
        how='inner'
    )
    # Resolve over_under column collision
    if 'over_under_x' in df.columns:
        df['over_under'] = df['over_under_x'].fillna(df.get('over_under_y', pd.Series(dtype=float)))
        df = df.drop(columns=['over_under_x', 'over_under_y'], errors='ignore')

    print(f"  Merged: {len(df):,} games with features + O/U target")

    # Engineer derived pace features
    df['avg_kpd_tempo'] = (
        df.get('h_kpd_adj_tempo', pd.Series(dtype=float)) +
        df.get('a_kpd_adj_tempo', pd.Series(dtype=float))
    ) / 2
    df['h_kpd_adj_o_minus_a_kpd_adj_d'] = (
        df.get('h_kpd_adj_o', pd.Series(dtype=float)) -
        df.get('a_kpd_adj_d', pd.Series(dtype=float))
    )
    df['a_kpd_adj_o_minus_h_kpd_adj_d'] = (
        df.get('a_kpd_adj_o', pd.Series(dtype=float)) -
        df.get('h_kpd_adj_d', pd.Series(dtype=float))
    )
    df['kp_pred_total'] = (
        df.get('kp_home_pred', pd.Series(dtype=float)) +
        df.get('kp_away_pred', pd.Series(dtype=float))
    )

    # Merge line movement if available
    try:
        lm = pd.read_sql("""
            SELECT game_date, home_team, away_team,
                   spread_open, total_open, ml_home_open, ml_away_open,
                   spread_close, total_close, ml_home_close, ml_away_close,
                   spread_move, total_move, ml_home_move
            FROM line_movement
        """, conn)
        if len(lm) > 0:
            df = df.merge(lm, on=['game_date', 'home_team', 'away_team'], how='left')
            print(f"  Line movement: {lm['spread_move'].notna().sum():,} games with open/close data")
        else:
            print("  Line movement: table empty (run 10_scrape_historical_lines.py)")
    except Exception:
        print("  Line movement: table not found (run 10_scrape_historical_lines.py)")

    conn.close()
    return df


def get_feature_cols(df, group_names):
    """Get available feature columns for a given set of groups."""
    cols = list(CONTEXT_FEATURES)  # always include context
    for g in group_names:
        for c in FEATURE_GROUPS.get(g, []):
            if c in df.columns and c not in cols:
                cols.append(c)
    # Return only columns that actually exist in df
    return [c for c in cols if c in df.columns]


def backtest_combo(df, group_names, min_seasons=4):
    feat_cols = get_feature_cols(df, group_names)
    if len(feat_cols) < 3:
        return None

    seasons = sorted(df['season'].dropna().unique())
    test_seasons = [s for s in seasons if seasons.index(s) >= min_seasons]
    if len(test_seasons) < 2:
        return None

    all_preds = []
    for test_s in test_seasons:
        train_df = df[df['season'] < test_s].copy()
        test_df  = df[df['season'] == test_s].copy()

        # Filter to games with O/U data
        train_df = train_df[train_df['over_under'].notna() & train_df['went_over'].notna()]
        test_df  = test_df[test_df['over_under'].notna()  & test_df['went_over'].notna()]

        if len(train_df) < 500 or len(test_df) < 50:
            continue

        # Drop features that are all-NaN in training data
        valid = [c for c in feat_cols if train_df[c].notna().sum() > 50]
        if len(valid) < 3:
            continue

        X_train = train_df[valid]
        y_train = train_df['went_over'].astype(int)
        X_test  = test_df[valid]

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

        p_over = model.predict_proba(X_test_imp)[:, 1]
        ev_over  = p_over * (1 + PAYOUT) - 1
        ev_under = (1 - p_over) * (1 + PAYOUT) - 1

        test_df = test_df.copy()
        test_df['p_over']    = p_over
        test_df['ev_over']   = ev_over
        test_df['ev_under']  = ev_under
        test_df['best_ev']   = np.maximum(ev_over, ev_under)
        test_df['bet_over']  = ev_over >= ev_under
        all_preds.append(test_df)

    if not all_preds:
        return None

    result_df = pd.concat(all_preds)
    bet = result_df[result_df['best_ev'] >= EV_MIN].copy()
    if len(bet) < MIN_BETS:
        return None

    actual_over = bet['went_over'].astype(int).values
    bet_over    = bet['bet_over'].values
    won = np.where(bet_over, actual_over == 1, actual_over == 0)

    n     = len(bet)
    wins  = won.sum()
    wr    = wins / n
    roi   = (wins * PAYOUT - (n - wins)) / n
    avg_ev = bet['best_ev'].mean()

    # Season-by-season breakdown for top combos
    seasons_positive = 0
    for s in sorted(result_df['season'].unique()):
        s_bet = bet[bet['season'] == s]
        if len(s_bet) < 10:
            continue
        s_won = np.where(
            s_bet['bet_over'].values,
            s_bet['went_over'].astype(int).values == 1,
            s_bet['went_over'].astype(int).values == 0,
        )
        if s_won.mean() > 0.5238:
            seasons_positive += 1

    return {
        'combo':            '+'.join(['CONTEXT'] + list(group_names)),
        'n_bets':           n,
        'win_rate':         round(wr, 4),
        'roi':              round(roi, 4),
        'avg_ev':           round(avg_ev, 4),
        'n_seasons':        len(test_seasons),
        'seasons_positive': seasons_positive,
        'has_line_move':    'LINE_MOVE' in group_names,
    }


if __name__ == '__main__':
    args = parse_args()

    print("=" * 90)
    print("NCAAB Totals — Walk-Forward Feature Combo Backtest")
    print("=" * 90)

    df = load_data()
    over_rate = df['went_over'].mean()
    print(f"\n  Target: went_over | Over rate: {over_rate:.3f} | "
          f"Breakeven WR: 52.38% | EV threshold: {EV_MIN:.0%}")
    print(f"  Seasons: {sorted(df['season'].dropna().unique())}")
    print(f"  Optional groups: {OPTIONAL_SOURCES}")

    # Check which groups have sufficient coverage
    print("\n  Feature group coverage:")
    for grp, cols in FEATURE_GROUPS.items():
        available = [c for c in cols if c in df.columns]
        if available:
            coverage = df[available[0]].notna().mean() if available else 0
            print(f"    {grp:<15} {len(available):>3}/{len(cols)} cols | "
                  f"~{coverage:.0%} coverage")
        else:
            print(f"    {grp:<15}   0/{len(cols)} cols | NOT AVAILABLE")

    total_combos = sum(
        len(list(itertools.combinations(OPTIONAL_SOURCES, r)))
        for r in range(1, len(OPTIONAL_SOURCES) + 1)
    )
    print(f"\n  Total combos: {total_combos} | Min training seasons: {args.min_seasons}")
    print()
    print(f"  {'Combo':<65} {'Bets':>6} {'WR':>6} {'ROI':>7} {'AvgEV':>7} {'S+':>4}")
    print("  " + "-" * 100)

    results = []
    done = 0
    for r in range(1, len(OPTIONAL_SOURCES) + 1):
        for combo in itertools.combinations(OPTIONAL_SOURCES, r):
            res = backtest_combo(df, list(combo), min_seasons=args.min_seasons)
            if res and res['roi'] > 0:
                results.append(res)
                lm_flag = '★' if res['has_line_move'] else ' '
                print(f"  {lm_flag} {res['combo']:<64} {res['n_bets']:>6} "
                      f"{res['win_rate']:>6.3f} {res['roi']:>+7.3f} "
                      f"{res['avg_ev']:>+7.3f} {res['seasons_positive']:>4}")
            done += 1
            if done % 20 == 0:
                print(f"  ... {done}/{total_combos} combos tested", end='\r')

    print(f"\n  Completed {done} combos, {len(results)} with positive ROI")

    # Save results
    if results:
        res_df = pd.DataFrame(results).sort_values('roi', ascending=False)
        out_path = os.path.join(OUT_DIR, 'totals_combo_backtest_results.csv')
        res_df.to_csv(out_path, index=False)

        print("\n" + "=" * 90)
        print("TOP 10 TOTALS COMBOS — BY OUT-OF-SAMPLE ROI")
        print("=" * 90)
        print(f"  {'Combo':<65} {'Bets':>6} {'WR':>6} {'ROI':>7} {'AvgEV':>7} {'S+':>4}")
        print("  " + "-" * 100)
        for _, r in res_df.head(10).iterrows():
            lm = '★' if r['has_line_move'] else ' '
            print(f"  {lm} {r['combo']:<64} {r['n_bets']:>6} "
                  f"{r['win_rate']:>6.3f} {r['roi']:>+7.3f} "
                  f"{r['avg_ev']:>+7.3f} {r['seasons_positive']:>4}")

        best = res_df.iloc[0]
        print(f"\n  Best combo: {best['combo']}")
        print(f"  ROI: {best['roi']:+.4f} | WR: {best['win_rate']:.4f} | "
              f"Bets: {best['n_bets']} | Seasons+: {best['seasons_positive']}")
        print(f"\n  Saved: {out_path}")
        print(f"\nNext: python scripts/08_train_totals_model.py --combo \"{best['combo']}\"")
    else:
        print("\n  No combos with positive ROI found — check data coverage.")
