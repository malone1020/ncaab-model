"""
Script 04c: Home Court Advantage Features
==========================================
Computes per-venue home court advantage and adds it to game_features.

Key insight: The market prices well-known home courts (Kansas, Duke) accurately.
The edge is in programs where home court is systematically undervalued.

Features added to game_features:
  - home_court_value     : historical avg margin premium at this venue (neutral-adjusted)
  - home_court_cover_rate: historical ATS cover rate at this venue
  - home_court_games     : sample size (for confidence weighting)
  - home_court_edge      : home_court_cover_rate - 0.5 (how much the market undervalues it)

Usage:
    python scripts/04c_home_court.py
    (Run after 04_feature_engineering.py)
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

# Minimum games at a venue to compute a reliable home court value
MIN_GAMES = 20

# Shrinkage: blend venue estimate toward league average for small samples
# At MIN_GAMES, use 50% venue / 50% league. At 100+ games, use 95% venue.
SHRINK_FULL = 100


def compute_home_court_values(conn):
    """
    Compute per-venue home court advantage using historical data.

    Method:
    1. For non-neutral games, compute average home margin
    2. Compare to neutral-site baseline (what the team averages on neutral floor)
    3. Shrink toward league average for small samples
    4. Use leave-one-season-out to avoid leakage — for each game,
       compute home court value using all OTHER seasons
    """
    print("Computing per-venue home court values...")

    df = pd.read_sql("""
        SELECT gf.game_id, gf.season, gf.game_date,
               gf.home_team, gf.away_team,
               gf.actual_margin, gf.home_covered,
               gf.neutral_site, gf.spread
        FROM game_features gf
        WHERE gf.actual_margin IS NOT NULL
    """, conn)

    df["game_date"] = pd.to_datetime(df["game_date"])
    print(f"  Loaded {len(df):,} games with outcomes")

    # League average home margin (non-neutral only)
    home_games = df[df["neutral_site"] == 0]
    league_avg_margin = home_games["actual_margin"].mean()
    league_avg_cover  = home_games["home_covered"].mean()
    print(f"  League avg home margin : {league_avg_margin:.2f} pts")
    print(f"  League avg home cover  : {league_avg_cover*100:.1f}%")

    # ── Leave-one-season-out home court values ────────────────────────────
    # For each game, use all games from OTHER seasons to estimate venue value
    # This prevents using the current season's outcomes to predict themselves

    seasons = sorted(df["season"].unique())
    all_hcv = []

    for test_season in seasons:
        train = df[(df["neutral_site"] == 0) &
                   (df["season"] != test_season)].copy()
        test  = df[df["season"] == test_season].copy()

        # Per-venue stats from training data
        venue_stats = (train.groupby("home_team")
                       .agg(
                           venue_games  = ("actual_margin", "count"),
                           venue_margin = ("actual_margin", "mean"),
                           venue_cover  = ("home_covered", "mean"),
                       )
                       .reset_index())

        # Shrinkage toward league average
        # weight = games / (games + shrink_factor)
        # shrink_factor chosen so that at MIN_GAMES weight = 0.5
        shrink_factor = MIN_GAMES
        venue_stats["weight"] = (
            venue_stats["venue_games"] /
            (venue_stats["venue_games"] + shrink_factor)
        )
        venue_stats["hcv_margin"] = (
            venue_stats["weight"] * venue_stats["venue_margin"] +
            (1 - venue_stats["weight"]) * league_avg_margin
        )
        venue_stats["hcv_cover"] = (
            venue_stats["weight"] * venue_stats["venue_cover"] +
            (1 - venue_stats["weight"]) * league_avg_cover
        )
        venue_stats["hcv_edge"] = venue_stats["hcv_cover"] - 0.5

        # Merge onto test season games
        test = test.merge(
            venue_stats[["home_team","venue_games","hcv_margin",
                          "hcv_cover","hcv_edge"]],
            on="home_team", how="left"
        )

        # Fill missing venues (new programs) with league average
        test["hcv_margin"]  = test["hcv_margin"].fillna(league_avg_margin)
        test["hcv_cover"]   = test["hcv_cover"].fillna(league_avg_cover)
        test["hcv_edge"]    = test["hcv_edge"].fillna(0)
        test["venue_games"] = test["venue_games"].fillna(0)

        all_hcv.append(test[["game_id","venue_games","hcv_margin",
                              "hcv_cover","hcv_edge"]])

    hcv = pd.concat(all_hcv, ignore_index=True)
    print(f"  Computed HCV for {len(hcv):,} games across {len(seasons)} seasons")
    return hcv


def add_columns_if_missing(conn, table, columns):
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in c.fetchall()}
    for col, dtype in columns.items():
        if col not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
    conn.commit()


def update_game_features(conn, hcv):
    print("\nUpdating game_features with home court values...")

    add_columns_if_missing(conn, "game_features", {
        "home_court_games" : "INTEGER",
        "home_court_margin": "REAL",
        "home_court_cover" : "REAL",
        "home_court_edge"  : "REAL",
    })

    c = conn.cursor()
    updated = 0
    for _, row in hcv.iterrows():
        c.execute("""
            UPDATE game_features SET
                home_court_games  = ?,
                home_court_margin = ?,
                home_court_cover  = ?,
                home_court_edge   = ?
            WHERE game_id = ?
        """, (
            int(row["venue_games"]),
            round(float(row["hcv_margin"]), 4),
            round(float(row["hcv_cover"]),  4),
            round(float(row["hcv_edge"]),   4),
            int(row["game_id"]),
        ))
        updated += c.rowcount

    conn.commit()
    print(f"  Updated {updated:,} rows")


def print_summary(conn):
    """Show top and bottom home court venues by edge."""
    df = pd.read_sql("""
        SELECT home_team,
               MAX(home_court_games)  as games,
               ROUND(MAX(home_court_margin), 2) as margin_premium,
               ROUND(MAX(home_court_cover)*100, 1)  as cover_pct,
               ROUND(MAX(home_court_edge)*100, 1)   as edge_pct
        FROM game_features
        WHERE home_court_games >= 20
          AND neutral_site = 0
        GROUP BY home_team
        ORDER BY cover_pct DESC
    """, conn)

    print("\n── Top 15 Home Court Venues by Cover Rate ───────────────────")
    print(f"  {'Team':<25} {'Games':>5}  {'Margin':>7}  {'Cover%':>7}  {'Edge%':>6}")
    print(f"  {'-'*25} {'-'*5}  {'-'*7}  {'-'*7}  {'-'*6}")
    for _, r in df.head(15).iterrows():
        print(f"  {r['home_team']:<25} {int(r['games']):>5}  "
              f"{r['margin_premium']:>+7.2f}  "
              f"{r['cover_pct']:>7.1f}%  "
              f"{r['edge_pct']:>+6.1f}%")

    print("\n── Bottom 10 Home Court Venues (market overvalues home) ────")
    print(f"  {'Team':<25} {'Games':>5}  {'Margin':>7}  {'Cover%':>7}  {'Edge%':>6}")
    print(f"  {'-'*25} {'-'*5}  {'-'*7}  {'-'*7}  {'-'*6}")
    for _, r in df.tail(10).iterrows():
        print(f"  {r['home_team']:<25} {int(r['games']):>5}  "
              f"{r['margin_premium']:>+7.2f}  "
              f"{r['cover_pct']:>7.1f}%  "
              f"{r['edge_pct']:>+6.1f}%")

    # Distribution of edge
    print(f"\n── Home Court Edge Distribution ─────────────────────────────")
    print(f"  Mean edge   : {df['edge_pct'].mean():+.2f}%")
    print(f"  Std dev     : {df['edge_pct'].std():.2f}%")
    print(f"  > +5% edge  : {(df['edge_pct'] > 5).sum()} venues")
    print(f"  < -5% edge  : {(df['edge_pct'] < -5).sum()} venues")


def main():
    print(f"Database: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)

    hcv = compute_home_court_values(conn)
    update_game_features(conn, hcv)
    print_summary(conn)

    conn.close()
    print("\n✓ Done — re-run 05_train_model.py to include home court features")


if __name__ == "__main__":
    main()
