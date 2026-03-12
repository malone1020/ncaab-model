"""
Script 05: Model Training — XGBoost Margin Predictor
======================================================
Trains an XGBoost regression model to predict game margin (home - away).
Uses walk-forward validation to simulate real betting conditions.

Betting logic:
  - Model predicts margin: e.g. home wins by 4
  - Market spread: e.g. home -6.5
  - Edge = predicted_margin - (-spread) = 4 - 6.5 = -2.5  → bet AWAY
  - Bet when |edge| > threshold (default 3 points)

Outputs:
  - Feature importance chart
  - ATS performance by season and edge threshold
  - Saved model file (models/margin_model.json)

Usage:
    python scripts/05_train_model.py
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

try:
    import xgboost as xgb
except ImportError:
    print("Install xgboost: pip install xgboost")
    raise

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR / "data" / "basketball.db"
MODEL_DIR  = BASE_DIR / "models"
MODEL_DIR.mkdir(exist_ok=True)

# Spread filter: only bet games where model has demonstrated edge
# 9-12pt range loses -7.1% ROI, 20+pt also weak — exclude both
SPREAD_MIN = 0.5
SPREAD_MAX = 9.0

# ── Feature columns used for training ─────────────────────────────────────────
FEATURE_COLS = [
    # ── Torvik ratings (PRIMARY signal — daily, no leakage) ──────────────────
    "torvik_em_gap",      # Torvik adj_em gap (home - away)
    "torvik_barthag_gap", # Win probability gap (most predictive single metric)
    "torvik_o_gap",       # Offensive efficiency gap
    "torvik_d_gap",       # Defensive efficiency gap
    "torvik_tempo_gap",   # Tempo gap (pace matchup)
    "torvik_efg_o_gap",   # Effective FG% gap
    "torvik_efg_d_gap",   # Defensive eFG% gap
    "torvik_tov_gap",     # Turnover rate gap
    "torvik_orb_gap",     # Offensive rebounding gap
    "home_torvik_adj_em", "away_torvik_adj_em",   # Raw ratings (not just gap)
    "home_barthag",       "away_barthag",

    # ── KenPom (supplemental — cross-check against Torvik) ───────────────────
    "em_gap",             # KenPom efficiency gap (prior season)
    "kp_em_gap",          # Same as em_gap (explicit name)
    "o_gap", "d_gap",     # KenPom O/D gaps
    "home_kp_em", "away_kp_em",

    # ── Rolling four-factor stats (opponent-quality adjusted) ─────────────────
    "adj_margin_gap",     # Adjusted margin gap (our strongest raw signal)
    "home_adj_margin", "away_adj_margin",
    "margin_gap",         # Unadjusted rolling margin gap
    "efg_gap", "tov_gap", "orb_gap", "ftr_gap", "pts_gap",
    "home_roll_efg",  "away_roll_efg",
    "home_roll_tov",  "away_roll_tov",
    "home_roll_orb",  "away_roll_orb",
    "home_roll_ftr",  "away_roll_ftr",
    "home_roll_pts",  "away_roll_pts",
    "home_roll_margin","away_roll_margin",
    "home_roll_pace", "away_roll_pace",
    "home_sos", "away_sos", "sos_gap",

    # ── Torvik game prediction (use disagreement as signal) ───────────────────
    "torvik_pred_margin", # Torvik's own projected margin
    "torvik_vs_spread",   # Torvik margin vs DK spread (Torvik's own edge estimate)

    # ── Home court advantage ──────────────────────────────────────────────────
    "home_court_margin", "home_court_cover", "home_court_edge",

    # ── Rest / back-to-back ───────────────────────────────────────────────────
    "rest_diff", "home_b2b", "away_b2b", "b2b_diff",

    # ── Haslametrics shot quality (unique signal vs KenPom/Torvik) ──────────
    "hasla_ap_gap",    # adjusted performance % gap (their primary metric)
    "hasla_prox_gap",  # avg shot distance gap (interior vs perimeter team)
    "hasla_mrar_gap",  # mid-range attempt rate gap
    "hasla_scc_gap",   # scoring chance conversion gap
    "hasla_ppsc_gap",  # points per scoring chance gap
    "hasla_3pt_gap",   # 3pt attempt % gap
    "hasla_ftar_gap",  # free throw attempt rate gap
    "home_hasla_ap_pct", "away_hasla_ap_pct",
    "home_hasla_prox",   "away_hasla_prox",

    # ── Game context ─────────────────────────────────────────────────────────
    "neutral_site", "conf_game",
]

TARGET = "actual_margin"


# ══════════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════

def load_features(conn):
    df = pd.read_sql("""
        SELECT *
        FROM game_features
        WHERE actual_margin IS NOT NULL
          AND home_adj_em IS NOT NULL
          AND home_roll_efg IS NOT NULL
          AND away_roll_efg IS NOT NULL
          AND spread IS NOT NULL
    """, conn)
    print(f"Loaded {len(df):,} fully-featured games")
    print(f"Seasons: {sorted(df['season'].unique())}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# WALK-FORWARD VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def walk_forward(df, min_train_seasons=3):
    """
    For each test season, train on all prior seasons.
    Returns dataframe with predictions appended.
    """
    seasons = sorted(df["season"].unique())
    all_preds = []

    for i, test_season in enumerate(seasons):
        if i < min_train_seasons:
            continue  # need enough training data

        train_seasons = seasons[:i]
        train = df[df["season"].isin(train_seasons)].copy()
        test  = df[df["season"] == test_season].copy()

        X_train = train[FEATURE_COLS].fillna(0)
        y_train = train[TARGET]
        X_test  = test[FEATURE_COLS].fillna(0)

        model = xgb.XGBRegressor(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=10,   # regularize — avoid overfitting small samples
            random_state=42,
            verbosity=0,
        )
        model.fit(X_train, y_train)

        test = test.copy()
        test["predicted_margin"] = model.predict(X_test)
        all_preds.append(test)

        mae = np.mean(np.abs(test["predicted_margin"] - test[TARGET]))
        print(f"  {test_season}: train={len(train):,}  test={len(test):,}  MAE={mae:.2f}")

    return pd.concat(all_preds, ignore_index=True), model


# ══════════════════════════════════════════════════════════════════════════════
# BETTING EVALUATION
# ══════════════════════════════════════════════════════════════════════════════

def evaluate_ats(df, edge_threshold=3.0):
    """
    Evaluate ATS performance.

    spread: negative = home favored (e.g. -6.5 means home favored by 6.5)
    Model predicts home_margin.

    Market implied margin = -spread (e.g. spread=-6.5 → market says home wins by 6.5)
    Edge = predicted_margin - market_implied_margin
      + edge → bet HOME (we think home wins by more than market)
      - edge → bet AWAY (we think away covers)
    """
    df = df.copy()
    df["market_margin"]    = -df["spread"]
    df["edge"]             = df["predicted_margin"] - df["market_margin"]
    df["bet_home"]         = df["edge"] >  edge_threshold
    df["bet_away"]         = df["edge"] < -edge_threshold
    df["has_bet"]          = df["bet_home"] | df["bet_away"]

    # Did our bet win?
    # home_covered=1 means home covered the spread
    df["bet_won"] = np.where(
        df["bet_home"], df["home_covered"],
        np.where(df["bet_away"], 1 - df["home_covered"], np.nan)
    )

    return df


def ats_summary(df, label=""):
    bets  = df[df["has_bet"] & df["bet_won"].notna()]
    total = len(bets)
    wins  = bets["bet_won"].sum()
    pct   = wins / total * 100 if total > 0 else 0
    # At -110 juice: need 52.4% to break even
    roi   = (wins * (100/110) - (total - wins)) / total * 100 if total > 0 else 0
    print(f"  {label:<20} bets={total:5,}  W={int(wins):5,}  "
          f"L={int(total-wins):5,}  pct={pct:.1f}%  ROI={roi:+.1f}%")
    return {"label": label, "bets": total, "wins": int(wins),
            "pct": pct, "roi": roi}


def full_ats_report(df):
    print("\n── ATS Performance by Edge Threshold ────────────────────────")
    print(f"  {'Threshold':<20} {'Bets':>5}  {'W':>5}  {'L':>5}  {'Pct':>6}  {'ROI':>7}")
    print(f"  {'-'*20} {'-'*5}  {'-'*5}  {'-'*5}  {'-'*6}  {'-'*7}")
    results = []
    for thresh in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]:
        ev = evaluate_ats(df, thresh)
        r  = ats_summary(ev, f"Edge > {thresh}")
        results.append(r)

    # Also show filtered results (0.5-9pt spreads only) for comparison
    print("\n── ATS Filtered: Spreads 0.5-9pt only ──────────────────────")
    print(f"  {'Threshold':<20} {'Bets':>5}  {'W':>5}  {'L':>5}  {'Pct':>6}  {'ROI':>7}")
    print(f"  {'-'*20} {'-'*5}  {'-'*5}  {'-'*5}  {'-'*6}  {'-'*7}")
    df_filt = df[df["spread"].abs().between(0.5, 9.0)]
    for thresh in [3.0, 4.0, 5.0, 6.0]:
        ev = evaluate_ats(df_filt, thresh)
        ats_summary(ev, f"Edge > {thresh} (filt)")

    print("\n── ATS Performance by Season (Edge > 3) ─────────────────────")
    print(f"  {'Season':<20} {'Bets':>5}  {'W':>5}  {'L':>5}  {'Pct':>6}  {'ROI':>7}")
    print(f"  {'-'*20} {'-'*5}  {'-'*5}  {'-'*5}  {'-'*6}  {'-'*7}")
    ev3 = evaluate_ats(df, 3.0)
    for season in sorted(ev3["season"].unique()):
        s = ev3[ev3["season"] == season]
        ats_summary(s, str(season))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE IMPORTANCE
# ══════════════════════════════════════════════════════════════════════════════

def plot_feature_importance(model, path):
    imp = pd.Series(model.feature_importances_, index=FEATURE_COLS)
    imp = imp.sort_values(ascending=True).tail(20)

    fig, ax = plt.subplots(figsize=(10, 7))
    imp.plot(kind="barh", ax=ax, color="steelblue")
    ax.set_title("XGBoost Feature Importance (top 20)", fontsize=14)
    ax.set_xlabel("Importance Score")
    ax.axvline(0, color="black", linewidth=0.8)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    plt.close()


# ══════════════════════════════════════════════════════════════════════════════
# BASELINE: KenPom-only model
# ══════════════════════════════════════════════════════════════════════════════

def baseline_kenpom(df):
    """Simple linear baseline: predicted margin = em_gap * scale_factor."""
    print("\n── Baseline: KenPom EM Gap vs Actual Margin ─────────────────")
    corr = df["em_gap"].corr(df["actual_margin"])
    print(f"  Correlation (em_gap vs margin): {corr:.3f}")

    # Naive prediction: use spread as prediction (market efficiency check)
    df2 = df.copy()
    df2["predicted_margin"] = -df2["spread"]  # market implied
    df2["market_margin"]    = -df2["spread"]
    df2["edge"]             = 0  # market = model → no bets
    mae = np.mean(np.abs(df2["predicted_margin"] - df2["actual_margin"]))
    print(f"  MAE if we just use the spread as our prediction: {mae:.2f} pts")
    print(f"  (This is the benchmark our model needs to beat)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    conn = sqlite3.connect(DB_PATH)
    df   = load_features(conn)
    conn.close()

    if len(df) < 1000:
        print("⚠  Not enough data — re-run Script 03b and 04 first")
        return

    baseline_kenpom(df)

    print("\n── Walk-Forward Validation ───────────────────────────────────")
    predictions, final_model = walk_forward(df, min_train_seasons=3)

    print(f"\n  Total predicted games: {len(predictions):,}")
    overall_mae = np.mean(np.abs(predictions["predicted_margin"] - predictions[TARGET]))
    print(f"  Overall MAE: {overall_mae:.2f} pts")

    full_ats_report(predictions)

    # Feature importance from final model
    imp_path = MODEL_DIR / "feature_importance.png"
    plot_feature_importance(final_model, imp_path)

    # Save final model
    model_path = MODEL_DIR / "margin_model.json"
    final_model.save_model(str(model_path))
    print(f"  Saved model: {model_path}")

    # Save predictions for further analysis
    out_path = BASE_DIR / "outputs" / "predictions.csv"
    out_path.parent.mkdir(exist_ok=True)
    predictions[["game_id", "season", "game_date", "home_team", "away_team",
                 "spread", "actual_margin", "predicted_margin",
                 "home_covered", "went_over"]].to_csv(out_path, index=False)
    print(f"  Saved predictions: {out_path}")

    print("\n✓ Done")


if __name__ == "__main__":
    main()
