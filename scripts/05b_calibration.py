"""
Script 05b: Model Calibration & Diagnostics
=============================================
Runs after 05_train_model.py. Loads predictions.csv and answers:

  1. Is the model calibrated? (when it says +8, does +8 actually happen?)
  2. Where does it have the most edge? (by conf, season type, spread range)
  3. Is the edge real or just home-bias? (does it just always bet favorites?)
  4. Profit curve — cumulative ROI over time

Usage:
    python scripts/05b_calibration.py
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

BASE_DIR  = Path(__file__).resolve().parent.parent
DB_PATH   = BASE_DIR / "data" / "basketball.db"
PRED_PATH = BASE_DIR / "outputs" / "predictions.csv"
OUT_DIR   = BASE_DIR / "outputs"
OUT_DIR.mkdir(exist_ok=True)

EDGE_THRESHOLD = 3.0


def load_predictions():
    if not PRED_PATH.exists():
        print(f"⚠  {PRED_PATH} not found — run 05_train_model.py first")
        return None
    df = pd.read_csv(PRED_PATH)
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["market_margin"]  = -df["spread"]
    df["edge"]           = df["predicted_margin"] - df["market_margin"]
    df["abs_edge"]       = df["edge"].abs()
    df["bet_home"]       = df["edge"] >  EDGE_THRESHOLD
    df["bet_away"]       = df["edge"] < -EDGE_THRESHOLD
    df["has_bet"]        = df["bet_home"] | df["bet_away"]
    df["bet_won"]        = np.where(
        df["bet_home"], df["home_covered"],
        np.where(df["bet_away"], 1 - df["home_covered"], np.nan)
    )
    print(f"Loaded {len(df):,} predictions ({df['has_bet'].sum()} bets flagged)")
    return df


# ── 1. Calibration ────────────────────────────────────────────────────────────

def calibration_plot(df, ax):
    """Bin predictions and compare to actual margins."""
    df2 = df.copy()
    df2["pred_bin"] = pd.cut(df2["predicted_margin"],
                              bins=range(-30, 35, 5), right=False)
    grouped = df2.groupby("pred_bin", observed=True).agg(
        actual_mean=("actual_margin", "mean"),
        count=("actual_margin", "size"),
        pred_mean=("predicted_margin", "mean"),
    ).dropna()

    ax.scatter(grouped["pred_mean"], grouped["actual_mean"],
               s=grouped["count"] / 2, alpha=0.7, color="steelblue")
    lim = max(abs(grouped["pred_mean"].max()), abs(grouped["actual_mean"].max())) + 3
    ax.plot([-lim, lim], [-lim, lim], "r--", alpha=0.5, label="Perfect calibration")
    ax.set_xlabel("Predicted Margin")
    ax.set_ylabel("Actual Margin (bin average)")
    ax.set_title("Model Calibration\n(bubble size = # games)")
    ax.legend()
    ax.grid(True, alpha=0.3)


# ── 2. Edge vs Cover Rate ─────────────────────────────────────────────────────

def edge_vs_cover(df, ax):
    """Does higher edge → higher cover rate?"""
    bets = df[df["has_bet"] & df["bet_won"].notna()].copy()
    bets["edge_bin"] = pd.cut(bets["abs_edge"],
                               bins=[3, 4, 5, 6, 7, 8, 10, 15, 30])
    grouped = bets.groupby("edge_bin", observed=True).agg(
        cover_pct=("bet_won", "mean"),
        count=("bet_won", "size"),
    ).dropna()

    bars = ax.bar(range(len(grouped)),
                  grouped["cover_pct"] * 100,
                  color=["green" if x > 52.4 else "salmon"
                         for x in grouped["cover_pct"] * 100])
    ax.axhline(52.4, color="red", linestyle="--", alpha=0.7, label="Break-even (52.4%)")
    ax.axhline(50.0, color="gray", linestyle=":", alpha=0.5, label="Random (50%)")
    ax.set_xticks(range(len(grouped)))
    ax.set_xticklabels([str(b) for b in grouped.index], rotation=30, ha="right")
    ax.set_ylabel("Cover %")
    ax.set_title("Cover Rate by Edge Size")
    ax.set_ylim(40, 70)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")

    # Add count labels
    for i, (_, row) in enumerate(grouped.iterrows()):
        ax.text(i, row["cover_pct"] * 100 + 0.5,
                f"n={int(row['count'])}", ha="center", fontsize=8)


# ── 3. Cumulative ROI ─────────────────────────────────────────────────────────

def cumulative_roi(df, ax):
    """Cumulative profit curve at -110 juice."""
    bets = df[df["has_bet"] & df["bet_won"].notna()].copy()
    bets = bets.sort_values("game_date")
    bets["profit"] = np.where(bets["bet_won"] == 1,
                               100/110,   # win: +0.909 units
                               -1.0)      # loss: -1 unit
    bets["cum_profit"] = bets["profit"].cumsum()
    bets["cum_roi"]    = bets["cum_profit"] / (np.arange(1, len(bets)+1)) * 100

    ax.plot(bets["game_date"], bets["cum_profit"],
            color="steelblue", linewidth=1.5)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.fill_between(bets["game_date"], bets["cum_profit"], 0,
                     where=bets["cum_profit"] >= 0,
                     alpha=0.2, color="green")
    ax.fill_between(bets["game_date"], bets["cum_profit"], 0,
                     where=bets["cum_profit"] < 0,
                     alpha=0.2, color="red")
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Units")
    ax.set_title(f"Cumulative P&L (Edge > {EDGE_THRESHOLD} pts, flat 1u bets)")
    ax.grid(True, alpha=0.3)


# ── 4. Bias Check ─────────────────────────────────────────────────────────────

def bias_check(df):
    print("\n── Bias Check ────────────────────────────────────────────────")
    bets = df[df["has_bet"] & df["bet_won"].notna()]

    home_bets = bets[bets["bet_home"]]
    away_bets = bets[bets["bet_away"]]
    fav_bets  = bets[bets["spread"] < 0]   # betting home favorite
    dog_bets  = bets[bets["spread"] > 0]   # betting home underdog

    print(f"  Home bets : {len(home_bets):4,}  cover: "
          f"{home_bets['bet_won'].mean()*100:.1f}%")
    print(f"  Away bets : {len(away_bets):4,}  cover: "
          f"{away_bets['bet_won'].mean()*100:.1f}%")
    print(f"  Fav bets  : {len(fav_bets):4,}  cover: "
          f"{fav_bets['bet_won'].mean()*100:.1f}%")
    print(f"  Dog bets  : {len(dog_bets):4,}  cover: "
          f"{dog_bets['bet_won'].mean()*100:.1f}%")

    # Spread range breakdown
    print("\n── ATS by Spread Range ───────────────────────────────────────")
    df2 = bets.copy()
    df2["spread_abs"] = df2["spread"].abs()
    bins = [0, 3, 6, 9, 12, 20, 50]
    labels = ["0-3", "3-6", "6-9", "9-12", "12-20", "20+"]
    df2["spread_range"] = pd.cut(df2["spread_abs"], bins=bins, labels=labels)
    for rng, grp in df2.groupby("spread_range", observed=True):
        if len(grp) > 10:
            print(f"  {rng:>6} pt spread:  n={len(grp):4,}  "
                  f"cover={grp['bet_won'].mean()*100:.1f}%")


# ── 5. Season breakdown ───────────────────────────────────────────────────────

def season_breakdown(df):
    print("\n── Full Season Breakdown (Edge > 3) ──────────────────────────")
    print(f"  {'Season':<8} {'Bets':>5}  {'W':>4}  {'L':>4}  "
          f"{'Pct':>6}  {'ROI':>7}  {'Units':>7}")
    print(f"  {'-'*8} {'-'*5}  {'-'*4}  {'-'*4}  {'-'*6}  {'-'*7}  {'-'*7}")

    bets = df[df["has_bet"] & df["bet_won"].notna()]
    total_units = 0
    for season in sorted(bets["season"].unique()):
        s = bets[bets["season"] == season]
        w = s["bet_won"].sum()
        l = len(s) - w
        pct = w / len(s) * 100
        units = w * (100/110) - l
        roi = units / len(s) * 100
        total_units += units
        print(f"  {season:<8} {len(s):>5}  {int(w):>4}  {int(l):>4}  "
              f"{pct:>6.1f}%  {roi:>+7.1f}%  {units:>+7.1f}u")

    print(f"  {'TOTAL':<8} {len(bets):>5}  "
          f"{int(bets['bet_won'].sum()):>4}  "
          f"{int(len(bets)-bets['bet_won'].sum()):>4}  "
          f"{bets['bet_won'].mean()*100:>6.1f}%  "
          f"{'':>7}  {total_units:>+7.1f}u")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    df = load_predictions()
    if df is None:
        return

    bias_check(df)
    season_breakdown(df)

    # Plots
    fig = plt.figure(figsize=(15, 10))
    gs  = gridspec.GridSpec(2, 2, figure=fig, hspace=0.4, wspace=0.35)

    calibration_plot(df, fig.add_subplot(gs[0, 0]))
    edge_vs_cover(df,   fig.add_subplot(gs[0, 1]))
    cumulative_roi(df,  fig.add_subplot(gs[1, :]))

    fig.suptitle("NCAAB Model Diagnostics", fontsize=16, fontweight="bold")
    out_path = OUT_DIR / "model_diagnostics.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"\nSaved diagnostics chart: {out_path}")
    plt.close()

    print("\n✓ Done")


if __name__ == "__main__":
    main()
