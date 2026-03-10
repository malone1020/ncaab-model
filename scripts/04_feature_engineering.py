"""
Script 04: Feature Engineering
================================
Builds the game_features table used for model training.

For each game that has a betting line, we:
  1. Pull KenPom season ratings for both teams (pre-game snapshot)
  2. Compute rolling 10-game averages of four factors for both teams
  3. Build matchup features (gaps, advantages, tempo)
  4. Store actual outcomes and cover results

Output table: game_features
  - One row per game (home team perspective)
  - All features available BEFORE the game tip-off
  - Target variables: actual_margin, home_covered, went_over

Usage:
    python scripts/04_feature_engineering.py
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

ROLLING_WINDOW = 10   # games to look back for rolling stats


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

def ensure_schema(conn):
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS game_features")
    c.execute("""
        CREATE TABLE game_features (
            -- Identity
            game_id         INTEGER PRIMARY KEY,
            season          INTEGER,
            game_date       TEXT,
            home_team       TEXT,
            away_team       TEXT,
            season_type     TEXT,
            neutral_site    INTEGER,
            conf_game       INTEGER,

            -- Betting line (consensus spread across providers)
            spread          REAL,       -- negative = home favored
            over_under      REAL,
            home_moneyline  INTEGER,
            away_moneyline  INTEGER,
            n_providers     INTEGER,    -- how many books had this game

            -- KenPom season ratings (end-of-season)
            home_adj_em     REAL,
            home_adj_o      REAL,
            home_adj_d      REAL,
            home_adj_t      REAL,
            away_adj_em     REAL,
            away_adj_o      REAL,
            away_adj_d      REAL,
            away_adj_t      REAL,

            -- KenPom matchup gaps (home minus away)
            em_gap          REAL,       -- efficiency margin gap
            o_gap           REAL,       -- offensive efficiency gap
            d_gap           REAL,       -- defensive efficiency gap (+ = home better D)
            tempo_gap       REAL,       -- tempo gap

            -- Rolling 10-game four factors: home team
            home_roll_efg       REAL,
            home_roll_tov       REAL,
            home_roll_orb       REAL,
            home_roll_ftr       REAL,
            home_roll_pts       REAL,
            home_roll_margin    REAL,
            home_roll_pace      REAL,
            home_roll_opp_efg   REAL,   -- opponent EFG allowed (defensive proxy)
            home_roll_opp_tov   REAL,   -- opponent TOV forced
            home_roll_opp_orb   REAL,   -- opponent ORB allowed

            -- Rolling 10-game four factors: away team
            away_roll_efg       REAL,
            away_roll_tov       REAL,
            away_roll_orb       REAL,
            away_roll_ftr       REAL,
            away_roll_pts       REAL,
            away_roll_margin    REAL,
            away_roll_pace      REAL,
            away_roll_opp_efg   REAL,
            away_roll_opp_tov   REAL,
            away_roll_opp_orb   REAL,

            -- Matchup four-factor gaps (home minus away)
            efg_gap         REAL,       -- home EFG - away EFG
            tov_gap         REAL,       -- home TOV - away TOV (lower is better)
            orb_gap         REAL,       -- home ORB - away ORB
            ftr_gap         REAL,       -- home FTR - away FTR
            pts_gap         REAL,       -- home avg pts - away avg pts
            margin_gap      REAL,       -- home avg margin - away avg margin

            -- Defensive four-factor gaps (home D vs away D)
            def_efg_gap     REAL,       -- home opp_efg_allowed vs away opp_efg_allowed
            def_tov_gap     REAL,
            def_orb_gap     REAL,

            -- Outcomes (targets)
            home_score      INTEGER,
            away_score      INTEGER,
            actual_margin   INTEGER,    -- home_score - away_score
            home_covered    INTEGER,    -- 1 if home covered spread
            went_over       INTEGER     -- 1 if total went over
        )
    """)
    conn.commit()
    print("✓ game_features table created")


# ══════════════════════════════════════════════════════════════════════════════
# LOAD BASE DATA
# ══════════════════════════════════════════════════════════════════════════════

def load_data(conn):
    print("Loading data...")

    # Consensus lines: average spread/total across providers per game
    lines = pd.read_sql("""
        SELECT
            game_id, season, game_date, home_team, away_team,
            home_score, away_score, home_margin as actual_margin,
            AVG(spread) as spread,
            AVG(over_under) as over_under,
            AVG(home_moneyline) as home_moneyline,
            AVG(away_moneyline) as away_moneyline,
            COUNT(DISTINCT provider) as n_providers,
            -- Use any row's home_covered/went_over (same for all providers of same game)
            MAX(home_covered) as home_covered,
            MAX(went_over) as went_over
        FROM game_lines
        WHERE spread IS NOT NULL
        GROUP BY game_id
    """, conn)
    print(f"  Lines: {len(lines):,} games with spreads")

    # Game-level metadata from games table
    games_meta = pd.read_sql("""
        SELECT cbbd_id as game_id, neutral_site, conf_game, season_type, tournament
        FROM games
    """, conn)

    # KenPom ratings (final season snapshot)
    kenpom = pd.read_sql("""
        SELECT season, team, adj_em, adj_o, adj_d, adj_t
        FROM kenpom_ratings
        WHERE snapshot_type = 'final'
    """, conn)
    print(f"  KenPom: {len(kenpom):,} team-season rows")

    # Per-game team stats for rolling calculations
    stats = pd.read_sql("""
        SELECT game_id, season, game_date, team, opponent,
               is_home, points, efg_pct, tov_pct, orb_pct, ft_rate, pace
        FROM game_team_stats
        WHERE efg_pct IS NOT NULL
        ORDER BY game_date
    """, conn)
    print(f"  Game stats: {len(stats):,} team-game rows")

    return lines, games_meta, kenpom, stats


# ══════════════════════════════════════════════════════════════════════════════
# ROLLING STATS
# ══════════════════════════════════════════════════════════════════════════════

def build_rolling_stats(stats: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """
    For each team-game, compute rolling averages of the previous N games.
    Returns a dataframe indexed by (game_id, team) with rolling features.
    IMPORTANT: We use shift(1) so no same-game data leaks into features.
    """
    print(f"  Computing rolling {window}-game averages...")

    stats = stats.copy()
    stats["game_date"] = pd.to_datetime(stats["game_date"])
    stats = stats.sort_values(["team", "game_date"])

    # Opponent stats — join to get what the opponent did in THIS game
    # (used to measure a team's defensive performance)
    opp_stats = stats[["game_id", "team", "efg_pct", "tov_pct", "orb_pct"]].copy()
    opp_stats = opp_stats.rename(columns={
        "team"   : "opponent",
        "efg_pct": "opp_efg",
        "tov_pct": "opp_tov",
        "orb_pct": "opp_orb",
    })
    stats = stats.merge(opp_stats, on=["game_id", "opponent"], how="left")

    # Margin: need opponent points
    opp_pts = stats[["game_id", "team", "points"]].copy()
    opp_pts.columns = ["game_id", "opponent", "opp_points"]
    stats = stats.merge(opp_pts, on=["game_id", "opponent"], how="left")
    stats["margin"] = stats["points"] - stats["opp_points"]

    # Rolling features — shift(1) to exclude current game
    roll_cols = {
        "efg_pct" : "roll_efg",
        "tov_pct" : "roll_tov",
        "orb_pct" : "roll_orb",
        "ft_rate"  : "roll_ftr",
        "points"  : "roll_pts",
        "margin"  : "roll_margin",
        "pace"    : "roll_pace",
        "opp_efg" : "roll_opp_efg",
        "opp_tov" : "roll_opp_tov",
        "opp_orb" : "roll_opp_orb",
    }

    result_parts = []
    for team, grp in stats.groupby("team"):
        grp = grp.sort_values("game_date").copy()
        for src_col, dst_col in roll_cols.items():
            if src_col in grp.columns:
                grp[dst_col] = (
                    grp[src_col]
                    .shift(1)                              # exclude current game
                    .rolling(window, min_periods=3)        # need at least 3 games
                    .mean()
                )
        result_parts.append(grp)

    rolled = pd.concat(result_parts, ignore_index=True)
    roll_feature_cols = list(roll_cols.values()) + ["game_id", "team", "game_date", "is_home"]
    return rolled[roll_feature_cols]


# ══════════════════════════════════════════════════════════════════════════════
# BUILD FEATURES
# ══════════════════════════════════════════════════════════════════════════════

def build_features(lines, games_meta, kenpom, stats):
    print("\nBuilding features...")

    # Rolling stats
    rolling = build_rolling_stats(stats, ROLLING_WINDOW)

    # Split rolling into home / away perspective
    home_roll = rolling[rolling["is_home"] == 1].copy()
    away_roll = rolling[rolling["is_home"] == 0].copy()

    home_roll = home_roll.add_prefix("home_").rename(columns={
        "home_game_id"   : "game_id",
        "home_game_date" : "game_date",
        "home_team"      : "home_team_check",
        "home_is_home"   : "is_home_check",
    })
    away_roll = away_roll.add_prefix("away_").rename(columns={
        "away_game_id"   : "game_id",
        "away_game_date" : "game_date",
        "away_team"      : "away_team_check",
        "away_is_home"   : "is_home_check",
    })

    # Start with lines as the base (one row per game)
    df = lines.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    # Merge game metadata
    df = df.merge(games_meta, on="game_id", how="left")

    # Merge KenPom for home team
    df = df.merge(
        kenpom.rename(columns={
            "team"  : "home_team",
            "adj_em": "home_adj_em",
            "adj_o" : "home_adj_o",
            "adj_d" : "home_adj_d",
            "adj_t" : "home_adj_t",
        }),
        on=["season", "home_team"], how="left"
    )

    # Merge KenPom for away team
    df = df.merge(
        kenpom.rename(columns={
            "team"  : "away_team",
            "adj_em": "away_adj_em",
            "adj_o" : "away_adj_o",
            "adj_d" : "away_adj_d",
            "adj_t" : "away_adj_t",
        }),
        on=["season", "away_team"], how="left"
    )

    # Merge rolling stats
    df = df.merge(home_roll.drop(columns=["home_team_check", "is_home_check", "game_date"]),
                  on="game_id", how="left")
    df = df.merge(away_roll.drop(columns=["away_team_check", "is_home_check", "game_date"]),
                  on="game_id", how="left")

    # KenPom matchup gaps
    df["em_gap"]    = df["home_adj_em"] - df["away_adj_em"]
    df["o_gap"]     = df["home_adj_o"]  - df["away_adj_o"]
    df["d_gap"]     = df["away_adj_d"]  - df["home_adj_d"]   # lower D = better; flip so + = home better D
    df["tempo_gap"] = df["home_adj_t"]  - df["away_adj_t"]

    # Four-factor matchup gaps
    df["efg_gap"]    = df["home_roll_efg"]     - df["away_roll_efg"]
    df["tov_gap"]    = df["home_roll_tov"]     - df["away_roll_tov"]   # lower TOV is better
    df["orb_gap"]    = df["home_roll_orb"]     - df["away_roll_orb"]
    df["ftr_gap"]    = df["home_roll_ftr"]     - df["away_roll_ftr"]
    df["pts_gap"]    = df["home_roll_pts"]     - df["away_roll_pts"]
    df["margin_gap"] = df["home_roll_margin"]  - df["away_roll_margin"]

    # Defensive four-factor gaps
    df["def_efg_gap"] = df["away_roll_opp_efg"] - df["home_roll_opp_efg"]   # home allows less = +
    df["def_tov_gap"] = df["home_roll_opp_tov"] - df["away_roll_opp_tov"]   # home forces more = +
    df["def_orb_gap"] = df["away_roll_opp_orb"] - df["home_roll_opp_orb"]

    print(f"  Total rows: {len(df):,}")
    print(f"  Rows with KenPom home: {df['home_adj_em'].notna().sum():,}")
    print(f"  Rows with KenPom away: {df['away_adj_em'].notna().sum():,}")
    print(f"  Rows with rolling stats: {df['home_roll_efg'].notna().sum():,}")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# SAVE TO DB
# ══════════════════════════════════════════════════════════════════════════════

def save_features(conn, df: pd.DataFrame):
    cols = [
        "game_id", "season", "game_date", "home_team", "away_team",
        "season_type", "neutral_site", "conf_game",
        "spread", "over_under", "home_moneyline", "away_moneyline", "n_providers",
        "home_adj_em", "home_adj_o", "home_adj_d", "home_adj_t",
        "away_adj_em", "away_adj_o", "away_adj_d", "away_adj_t",
        "em_gap", "o_gap", "d_gap", "tempo_gap",
        "home_roll_efg", "home_roll_tov", "home_roll_orb", "home_roll_ftr",
        "home_roll_pts", "home_roll_margin", "home_roll_pace",
        "home_roll_opp_efg", "home_roll_opp_tov", "home_roll_opp_orb",
        "away_roll_efg", "away_roll_tov", "away_roll_orb", "away_roll_ftr",
        "away_roll_pts", "away_roll_margin", "away_roll_pace",
        "away_roll_opp_efg", "away_roll_opp_tov", "away_roll_opp_orb",
        "efg_gap", "tov_gap", "orb_gap", "ftr_gap", "pts_gap", "margin_gap",
        "def_efg_gap", "def_tov_gap", "def_orb_gap",
        "home_score", "away_score", "actual_margin", "home_covered", "went_over",
    ]

    # Only keep columns that exist in df
    available = [c for c in cols if c in df.columns]
    missing   = [c for c in cols if c not in df.columns]
    if missing:
        print(f"  ⚠  Missing columns (will be NULL): {missing}")

    out = df[available].copy()
    out["game_date"] = out["game_date"].astype(str)

    # Replace NaN with None for SQLite
    out = out.where(pd.notnull(out), None)

    out.to_sql("game_features", conn, if_exists="replace", index=False)
    print(f"  Saved {len(out):,} rows to game_features")


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(conn):
    df = pd.read_sql("""
        SELECT season,
               COUNT(*) as games,
               SUM(CASE WHEN home_adj_em IS NOT NULL THEN 1 ELSE 0 END) as has_kenpom,
               SUM(CASE WHEN home_roll_efg IS NOT NULL THEN 1 ELSE 0 END) as has_rolling,
               SUM(CASE WHEN home_adj_em IS NOT NULL AND home_roll_efg IS NOT NULL THEN 1 ELSE 0 END) as fully_featured,
               ROUND(AVG(actual_margin), 2) as avg_margin,
               ROUND(AVG(CASE WHEN home_covered=1 THEN 1.0 ELSE 0 END)*100, 1) as home_cover_pct
        FROM game_features
        GROUP BY season ORDER BY season
    """, conn)

    print("\n── game_features summary ─────────────────────────────────────")
    print(df.to_string(index=False))

    total_full = df["fully_featured"].sum()
    print(f"\n  Total fully-featured rows (model-ready): {total_full:,}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"Database: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)

    ensure_schema(conn)
    lines, games_meta, kenpom, stats = load_data(conn)
    df = build_features(lines, games_meta, kenpom, stats)
    save_features(conn, df)
    print_summary(conn)

    conn.close()
    print("\n✓ Done")


if __name__ == "__main__":
    main()
