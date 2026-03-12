"""
Script 04d: Opponent-Quality Adjusted Rolling Margins
======================================================
Improves the rolling margin features by weighting each game's margin
by the opponent's strength (KenPom adj_em).

Why this matters:
  - margin_gap is currently the #1 feature at 25% importance
  - But raw margin doesn't distinguish quality of opponent
  - Team A going +8 vs top-50 opponents >> Team A going +8 vs bottom-100
  - Adjusting for opponent quality makes margin_gap significantly sharper

New features added to game_features:
  - home_adj_margin     : rolling avg margin weighted by opponent KenPom
  - away_adj_margin     : same for away team
  - home_sos            : avg opponent KenPom em in last N games (strength of schedule)
  - away_sos            : same for away team
  - adj_margin_gap      : home_adj_margin - away_adj_margin (replaces margin_gap)
  - sos_gap             : home_sos - away_sos

Method:
  For each game in rolling window:
    raw_margin_i = points_scored - points_allowed
    opp_em_i     = opponent's prior-season KenPom adj_em
    weight_i     = (opp_em_i - min_em) / (max_em - min_em)  [0-1 scale]
    adj_margin   = weighted average of raw_margin_i

  Use prior-season KenPom for opponent ratings to avoid leakage.

Usage:
    python scripts/04d_adj_margins.py
    (Run after 04_feature_engineering.py)
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from team_name_map import normalize_for_kenpom
except ImportError:
    def normalize_for_kenpom(name): return name

ROLLING_WINDOW = 10
MIN_GAMES      = 3   # minimum games to compute adjusted margin


def load_data(conn):
    print("Loading data...")

    stats = pd.read_sql("""
        SELECT s.game_id, s.season, s.game_date, s.team, s.opponent,
               s.is_home, s.points, s.efg_pct, s.tov_pct, s.orb_pct,
               s.ft_rate, s.pace
        FROM game_team_stats s
        WHERE s.points IS NOT NULL
        ORDER BY s.game_date
    """, conn)
    stats["game_date"] = pd.to_datetime(stats["game_date"])
    print(f"  Stats: {len(stats):,} team-game rows")

    kenpom = pd.read_sql("""
        SELECT season, team, adj_em
        FROM kenpom_ratings
        WHERE snapshot_type = 'final'
    """, conn)
    print(f"  KenPom: {len(kenpom):,} team-season rows")

    return stats, kenpom


def build_opponent_ratings(stats, kenpom):
    """
    For each game-team row, look up the opponent's prior-season KenPom rating.
    Uses season-1 to match how we handle KenPom in Script 04 (no leakage).
    """
    print("  Building opponent quality lookup...")

    # Normalize KenPom team names
    kenpom = kenpom.copy()
    kenpom["team_norm"] = kenpom["team"].apply(normalize_for_kenpom)

    # For each stats row, opponent's prior-season KenPom
    stats = stats.copy()
    stats["opp_norm"]      = stats["opponent"].apply(normalize_for_kenpom)
    stats["kenpom_season"] = stats["season"] - 1

    opp_kp = kenpom.rename(columns={
        "team_norm": "opp_norm",
        "season"   : "kenpom_season",
        "adj_em"   : "opp_adj_em",
    })[["opp_norm","kenpom_season","opp_adj_em"]]

    stats = stats.merge(opp_kp, on=["opp_norm","kenpom_season"], how="left")

    matched = stats["opp_adj_em"].notna().sum()
    total   = len(stats)
    print(f"  Opponent KenPom match rate: {matched:,}/{total:,} "
          f"({matched/total*100:.1f}%)")

    return stats


def compute_adjusted_rolling(stats, window=10):
    """
    Compute opponent-quality-adjusted rolling margins.

    Weight = sigmoid of opponent adj_em (stronger opponents = higher weight)
    Adjusted margin = sum(margin_i * weight_i) / sum(weight_i)

    Also compute raw SOS (avg opponent adj_em in rolling window).
    """
    print(f"  Computing adjusted rolling stats (window={window})...")

    # Get opponent stats for margin calculation
    opp_pts = stats[["game_id","team","points"]].rename(columns={
        "team": "opponent", "points": "opp_points"
    })
    stats = stats.merge(opp_pts, on=["game_id","opponent"], how="left")
    stats["margin"] = stats["points"] - stats["opp_points"]

    # League-wide em stats for normalization
    em_mean = stats["opp_adj_em"].mean()
    em_std  = stats["opp_adj_em"].std()

    # Weight: standardized opponent strength, then softmax to [0.5, 1.5]
    # This means strong opponents count 3x more than weak ones
    stats["opp_weight"] = np.where(
        stats["opp_adj_em"].notna(),
        1.0 + (stats["opp_adj_em"] - em_mean) / (em_std * 2),
        1.0  # use weight=1 when opponent KenPom not available
    )
    stats["opp_weight"] = stats["opp_weight"].clip(0.25, 2.0)

    # Weighted margin = margin * weight (for weighted average)
    stats["weighted_margin"] = stats["margin"] * stats["opp_weight"]

    parts = []
    for team, grp in stats.groupby("team"):
        grp = grp.sort_values("game_date").copy()

        # Rolling weighted margin (shift 1 to avoid leakage)
        wm_shifted = grp["weighted_margin"].shift(1)
        w_shifted  = grp["opp_weight"].shift(1)
        em_shifted = grp["opp_adj_em"].shift(1)

        adj_margins = []
        sos_values  = []
        for i in range(len(grp)):
            start = max(0, i - window)
            wm_window = wm_shifted.iloc[start:i]
            w_window  = w_shifted.iloc[start:i]
            em_window = em_shifted.iloc[start:i]

            valid_w = w_window.dropna()
            valid_wm = wm_window.dropna()
            valid_em = em_window.dropna()

            if len(valid_w) >= MIN_GAMES:
                adj_margin = valid_wm.sum() / valid_w.sum()
                sos        = valid_em.mean()
            else:
                adj_margin = np.nan
                sos        = np.nan

            adj_margins.append(adj_margin)
            sos_values.append(sos)

        grp["roll_adj_margin"] = adj_margins
        grp["roll_sos"]        = sos_values
        parts.append(grp[["game_id","team","is_home",
                           "roll_adj_margin","roll_sos"]])

    result = pd.concat(parts, ignore_index=True)
    print(f"  Computed adjusted margins for {len(result):,} game-team rows")
    return result


def add_columns_if_missing(conn, table, columns):
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in c.fetchall()}
    for col, dtype in columns.items():
        if col not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
    conn.commit()


def update_game_features(conn, rolled):
    print("\nUpdating game_features with adjusted margin features...")

    add_columns_if_missing(conn, "game_features", {
        "home_adj_margin" : "REAL",
        "away_adj_margin" : "REAL",
        "home_sos"        : "REAL",
        "away_sos"        : "REAL",
        "adj_margin_gap"  : "REAL",
        "sos_gap"         : "REAL",
    })

    # Split home and away
    home = rolled[rolled["is_home"] == 1][
        ["game_id","roll_adj_margin","roll_sos"]
    ].rename(columns={
        "roll_adj_margin": "home_adj_margin",
        "roll_sos"       : "home_sos",
    })
    away = rolled[rolled["is_home"] == 0][
        ["game_id","roll_adj_margin","roll_sos"]
    ].rename(columns={
        "roll_adj_margin": "away_adj_margin",
        "roll_sos"       : "away_sos",
    })

    merged = home.merge(away, on="game_id", how="outer")
    merged["adj_margin_gap"] = merged["home_adj_margin"] - merged["away_adj_margin"]
    merged["sos_gap"]        = merged["home_sos"] - merged["away_sos"]

    c = conn.cursor()
    updated = 0
    for _, row in merged.iterrows():
        c.execute("""
            UPDATE game_features SET
                home_adj_margin = ?,
                away_adj_margin = ?,
                home_sos        = ?,
                away_sos        = ?,
                adj_margin_gap  = ?,
                sos_gap         = ?
            WHERE game_id = ?
        """, (
            round(float(row["home_adj_margin"]), 4) if pd.notna(row["home_adj_margin"]) else None,
            round(float(row["away_adj_margin"]), 4) if pd.notna(row["away_adj_margin"]) else None,
            round(float(row["home_sos"]),        4) if pd.notna(row["home_sos"])        else None,
            round(float(row["away_sos"]),        4) if pd.notna(row["away_sos"])        else None,
            round(float(row["adj_margin_gap"]),  4) if pd.notna(row["adj_margin_gap"])  else None,
            round(float(row["sos_gap"]),         4) if pd.notna(row["sos_gap"])         else None,
            int(row["game_id"]),
        ))
        updated += c.rowcount

    conn.commit()
    print(f"  Updated {updated:,} rows")


def print_summary(conn):
    df = pd.read_sql("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN adj_margin_gap IS NOT NULL THEN 1 ELSE 0 END) as has_adj,
            ROUND(CORR(adj_margin_gap, actual_margin), 3) as adj_corr,
            ROUND(CORR(margin_gap,     actual_margin), 3) as raw_corr
        FROM game_features
        WHERE actual_margin IS NOT NULL
    """, conn)

    print("\n── Adjusted Margin Summary ──────────────────────────────────")
    print(f"  Total games          : {df['total'].iloc[0]:,}")
    print(f"  Has adj_margin_gap   : {df['has_adj'].iloc[0]:,}")
    print(f"  Corr (adj_margin_gap vs actual): {df['adj_corr'].iloc[0]}")
    print(f"  Corr (raw margin_gap vs actual): {df['raw_corr'].iloc[0]}")
    print(f"  (Higher correlation = more predictive)")

    # SOS distribution
    sos = pd.read_sql("""
        SELECT home_team,
               ROUND(AVG(home_sos), 2) as avg_sos,
               COUNT(*) as games
        FROM game_features
        WHERE home_sos IS NOT NULL
        GROUP BY home_team
        HAVING games >= 20
        ORDER BY avg_sos DESC
        LIMIT 10
    """, conn)
    print("\n── Toughest Schedules (avg opponent KenPom EM) ──────────────")
    print(sos.to_string(index=False))


def main():
    print(f"Database: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)

    stats, kenpom = load_data(conn)
    stats = build_opponent_ratings(stats, kenpom)
    rolled = compute_adjusted_rolling(stats, ROLLING_WINDOW)
    update_game_features(conn, rolled)
    print_summary(conn)

    conn.close()
    print("\n✓ Done — add adj_margin_gap and sos_gap to FEATURE_COLS in 05_train_model.py")


if __name__ == "__main__":
    main()
