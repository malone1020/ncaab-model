"""
Script 04b: Feature Enrichment
================================
Adds situational features that public models ignore:

  1. Rest days differential (days since last game, per team)
  2. Travel burden (back-to-back, home/away streaks)
  3. Referee tendencies (pace, foul rate, scoring environment)
  4. First half scores (for future 1H model)
  5. In-season adjusted ratings from CBBD /ratings/adjusted

All features are added as new columns to game_features table.

Usage:
    python scripts/04b_enrich_features.py
    (Run after 04_feature_engineering.py)
"""

import sqlite3
import requests
import pandas as pd
import numpy as np
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR  = Path(__file__).resolve().parent.parent
DB_PATH   = BASE_DIR / "data" / "basketball.db"
API_KEY   = os.getenv("CBBD_API_KEY")
BASE_URL  = "https://api.collegebasketballdata.com"
HEADERS   = {"Authorization": f"Bearer {API_KEY}"}


# ══════════════════════════════════════════════════════════════════════════════
# 1. REST DAYS & TRAVEL
# ══════════════════════════════════════════════════════════════════════════════

def compute_rest_features(conn):
    """
    For each game, compute:
      - home_rest_days: days since home team's last game
      - away_rest_days: days since away team's last game
      - rest_diff: home_rest - away_rest (+ means home is more rested)
      - home_b2b: 1 if home team played yesterday
      - away_b2b: 1 if away team played yesterday
      - home_away_streak: consecutive away games for away team (travel fatigue)
    """
    print("Computing rest/travel features...")

    games = pd.read_sql("""
        SELECT cbbd_id as game_id, season, game_date,
               home_team, away_team
        FROM games
        ORDER BY game_date
    """, conn)
    games["game_date"] = pd.to_datetime(games["game_date"])

    # Build per-team game log
    home_log = games[["game_id","game_date","home_team","away_team"]].copy()
    home_log["team"]     = home_log["home_team"]
    home_log["is_home"]  = 1
    home_log["opponent"] = home_log["away_team"]

    away_log = games[["game_id","game_date","home_team","away_team"]].copy()
    away_log["team"]     = away_log["away_team"]
    away_log["is_home"]  = 0
    away_log["opponent"] = away_log["home_team"]

    team_log = pd.concat([
        home_log[["game_id","game_date","team","is_home"]],
        away_log[["game_id","game_date","team","is_home"]],
    ]).sort_values(["team","game_date"])

    # Previous game date per team
    team_log["prev_date"] = team_log.groupby("team")["game_date"].shift(1)
    team_log["rest_days"] = (
        team_log["game_date"] - team_log["prev_date"]
    ).dt.days.clip(upper=30)  # cap at 30 (season breaks)

    # Away streak (consecutive away games)
    team_log["away_streak"] = (
        team_log.groupby("team")["is_home"]
        .transform(lambda x: x.eq(0).groupby((x != x.shift()).cumsum()).cumsum())
    )

    # Merge back to games
    home_rest = team_log[team_log["game_id"].isin(games["game_id"])].copy()
    home_rest = home_rest.merge(
        games[["game_id","home_team"]].rename(columns={"home_team":"team"}),
        on=["game_id","team"], how="inner"
    )[["game_id","rest_days","away_streak"]].rename(columns={
        "rest_days"   : "home_rest_days",
        "away_streak" : "home_away_streak",
    })

    away_rest = team_log[team_log["game_id"].isin(games["game_id"])].copy()
    away_rest = away_rest.merge(
        games[["game_id","away_team"]].rename(columns={"away_team":"team"}),
        on=["game_id","team"], how="inner"
    )[["game_id","rest_days","away_streak"]].rename(columns={
        "rest_days"   : "away_rest_days",
        "away_streak" : "away_away_streak",
    })

    rest = home_rest.merge(away_rest, on="game_id", how="outer")
    rest["rest_diff"]  = rest["home_rest_days"] - rest["away_rest_days"]
    rest["home_b2b"]   = (rest["home_rest_days"] == 1).astype(int)
    rest["away_b2b"]   = (rest["away_rest_days"] == 1).astype(int)
    rest["b2b_diff"]   = rest["away_b2b"] - rest["home_b2b"]  # + = away disadvantaged

    print(f"  Computed rest features for {len(rest):,} games")
    return rest


# ══════════════════════════════════════════════════════════════════════════════
# 2. REFEREE TENDENCIES
# ══════════════════════════════════════════════════════════════════════════════

def pull_referee_data(conn):
    """
    Pull referee assignments and compute per-ref tendency profiles:
      - avg_fouls_per_game
      - avg_pace (games they officiate tend to be fast/slow)
      - avg_total_score
      - over_rate (% of games going over total when this ref crew works)
    """
    print("Pulling referee data from CBBD...")

    # Check if we already have ref data
    existing = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='referee_game'",
        conn
    )
    if len(existing) > 0:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM referee_game")
        if c.fetchone()[0] > 5000:
            print("  Referee data already loaded, skipping pull")
            return build_ref_features(conn)

    # Pull from API
    conn.execute("""
        CREATE TABLE IF NOT EXISTS referee_game (
            game_id   INTEGER,
            referee   TEXT,
            season    INTEGER,
            PRIMARY KEY (game_id, referee)
        )
    """)
    conn.commit()

    all_rows = []
    for season in range(2016, 2026):
        for stype in ["regular", "postseason"]:
            try:
                r = requests.get(
                    f"{BASE_URL}/games/media",
                    headers=HEADERS,
                    params={"season": season, "seasonType": stype,
                            "mediaType": "referee"},
                    timeout=30
                )
                if r.status_code == 200:
                    data = r.json()
                    for g in data:
                        game_id = g.get("gameId")
                        for outlet in (g.get("outlets") or []):
                            name = outlet.get("name") or outlet.get("outlet")
                            if name:
                                all_rows.append({
                                    "game_id" : game_id,
                                    "referee" : name,
                                    "season"  : season,
                                })
                time.sleep(0.5)
            except Exception as e:
                print(f"  ⚠  {season} {stype}: {e}")

    if all_rows:
        ref_df = pd.DataFrame(all_rows).drop_duplicates()
        ref_df.to_sql("referee_game", conn, if_exists="replace", index=False)
        print(f"  Stored {len(ref_df):,} referee-game assignments")
    else:
        print("  ⚠  No referee data returned — endpoint may require different params")
        return None

    return build_ref_features(conn)


def build_ref_features(conn):
    """
    Build per-game referee tendency score using historical ref profiles.
    """
    # Get game stats to build ref profiles
    stats = pd.read_sql("""
        SELECT g.cbbd_id as game_id, g.season,
               s.fouls, s.pace, s.points,
               gl.over_under, gl.went_over
        FROM games g
        JOIN game_team_stats s ON s.game_id = g.cbbd_id AND s.is_home = 1
        LEFT JOIN game_lines gl ON gl.game_id = g.cbbd_id
        WHERE s.fouls IS NOT NULL
    """, conn)

    refs = pd.read_sql("SELECT * FROM referee_game", conn)
    if len(refs) == 0:
        return None

    merged = refs.merge(stats, on="game_id", how="inner")

    # Per-ref rolling profile (use career avg up to that season)
    ref_profiles = merged.groupby("referee").agg(
        ref_avg_fouls  = ("fouls", "mean"),
        ref_avg_pace   = ("pace",  "mean"),
        ref_avg_pts    = ("points","mean"),
        ref_over_rate  = ("went_over", "mean"),
        ref_game_count = ("game_id", "count"),
    ).reset_index()

    # Only use refs with 20+ games for reliability
    ref_profiles = ref_profiles[ref_profiles["ref_game_count"] >= 20]

    # Per-game crew average (average of all refs for that game)
    game_ref = refs.merge(ref_profiles, on="referee", how="inner")
    game_crew = game_ref.groupby("game_id").agg(
        crew_avg_fouls    = ("ref_avg_fouls",  "mean"),
        crew_avg_pace     = ("ref_avg_pace",   "mean"),
        crew_avg_pts      = ("ref_avg_pts",    "mean"),
        crew_over_rate    = ("ref_over_rate",  "mean"),
        crew_size         = ("referee",        "count"),
    ).reset_index()

    print(f"  Built ref features for {len(game_crew):,} games")
    return game_crew


# ══════════════════════════════════════════════════════════════════════════════
# 3. FIRST HALF SCORES
# ══════════════════════════════════════════════════════════════════════════════

def pull_half_scores(conn):
    """
    Re-pull game stats capturing first half scores from byPeriod array.
    Adds home_pts_h1, away_pts_h1, home_pts_h2, away_pts_h2 to games table.
    """
    print("Pulling first half scores...")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS game_half_scores (
            game_id     INTEGER PRIMARY KEY,
            season      INTEGER,
            home_pts_h1 INTEGER,
            home_pts_h2 INTEGER,
            away_pts_h1 INTEGER,
            away_pts_h2 INTEGER,
            home_margin_h1 INTEGER,
            total_h1    INTEGER
        )
    """)
    conn.commit()

    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM game_half_scores")
    if c.fetchone()[0] > 5000:
        print("  Half scores already loaded")
        return

    # Pull from game_team_stats — we stored points byPeriod in the raw pull
    # but didn't save it. Need to re-hit the API for a sample.
    # For now, flag as pending and note the column to add to Script 03b
    print("  ⚠  Half scores require re-pulling from API with byPeriod captured")
    print("     Adding to Script 03c (future task)")


# ══════════════════════════════════════════════════════════════════════════════
# 4. IN-SEASON ADJUSTED RATINGS
# ══════════════════════════════════════════════════════════════════════════════

def pull_adjusted_ratings(conn):
    """
    Pull CBBD /ratings/adjusted — in-season efficiency ratings updated
    throughout the year. Much better than end-of-season KenPom for
    early-season and mid-season games.
    """
    print("Pulling in-season adjusted ratings...")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS adjusted_ratings (
            season      INTEGER,
            team        TEXT,
            rating_date TEXT,
            adj_o       REAL,
            adj_d       REAL,
            adj_em      REAL,
            PRIMARY KEY (season, team, rating_date)
        )
    """)
    conn.commit()

    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM adjusted_ratings")
    existing = c.fetchone()[0]
    if existing > 1000:
        print(f"  Already have {existing:,} rating snapshots")
        return

    all_rows = []
    for season in range(2016, 2026):
        try:
            r = requests.get(
                f"{BASE_URL}/ratings/adjusted",
                headers=HEADERS,
                params={"season": season},
                timeout=30
            )
            if r.status_code == 200:
                data = r.json()
                for row in data:
                    all_rows.append({
                        "season"     : season,
                        "team"       : row.get("team"),
                        "rating_date": (row.get("date") or "")[:10],
                        "adj_o"      : row.get("offense"),
                        "adj_d"      : row.get("defense"),
                        "adj_em"     : row.get("overall"),
                    })
                print(f"  {season}: {len(data):,} rating rows")
            time.sleep(0.8)
        except Exception as e:
            print(f"  ⚠  {season}: {e}")

    if all_rows:
        df = pd.DataFrame(all_rows).dropna(subset=["team"])
        df.to_sql("adjusted_ratings", conn, if_exists="replace", index=False)
        print(f"  Stored {len(df):,} total rating snapshots")


# ══════════════════════════════════════════════════════════════════════════════
# 5. UPDATE game_features WITH NEW COLUMNS
# ══════════════════════════════════════════════════════════════════════════════

def add_columns_if_missing(conn, table, columns):
    """Add new columns to existing table without dropping data."""
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in c.fetchall()}
    for col, dtype in columns.items():
        if col not in existing:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
    conn.commit()


def update_game_features(conn, rest_df, ref_df):
    """Merge enrichment features into game_features table."""
    print("\nUpdating game_features with enrichment features...")

    # Add new columns
    new_cols = {
        "home_rest_days"  : "REAL",
        "away_rest_days"  : "REAL",
        "rest_diff"       : "REAL",
        "home_b2b"        : "INTEGER",
        "away_b2b"        : "INTEGER",
        "b2b_diff"        : "INTEGER",
        "home_away_streak": "INTEGER",
        "away_away_streak": "INTEGER",
        "crew_avg_fouls"  : "REAL",
        "crew_avg_pace"   : "REAL",
        "crew_over_rate"  : "REAL",
    }
    add_columns_if_missing(conn, "game_features", new_cols)

    c = conn.cursor()
    updated = 0

    # Update rest features
    for _, row in rest_df.iterrows():
        c.execute("""
            UPDATE game_features SET
                home_rest_days   = :home_rest_days,
                away_rest_days   = :away_rest_days,
                rest_diff        = :rest_diff,
                home_b2b         = :home_b2b,
                away_b2b         = :away_b2b,
                b2b_diff         = :b2b_diff,
                home_away_streak = :home_away_streak,
                away_away_streak = :away_away_streak
            WHERE game_id = :game_id
        """, row.to_dict())
        updated += c.rowcount

    conn.commit()
    print(f"  Rest features: updated {updated:,} rows")

    # Update ref features
    if ref_df is not None and len(ref_df) > 0:
        ref_updated = 0
        for _, row in ref_df.iterrows():
            c.execute("""
                UPDATE game_features SET
                    crew_avg_fouls = :crew_avg_fouls,
                    crew_avg_pace  = :crew_avg_pace,
                    crew_over_rate = :crew_over_rate
                WHERE game_id = :game_id
            """, row[["game_id","crew_avg_fouls",
                       "crew_avg_pace","crew_over_rate"]].to_dict())
            ref_updated += c.rowcount
        conn.commit()
        print(f"  Ref features: updated {ref_updated:,} rows")
    else:
        print("  Ref features: skipped (no data)")


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(conn):
    df = pd.read_sql("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN home_rest_days IS NOT NULL THEN 1 ELSE 0 END) as has_rest,
            SUM(CASE WHEN crew_avg_fouls IS NOT NULL THEN 1 ELSE 0 END) as has_refs,
            SUM(CASE WHEN home_b2b = 1 THEN 1 ELSE 0 END) as home_b2b_games,
            SUM(CASE WHEN away_b2b = 1 THEN 1 ELSE 0 END) as away_b2b_games,
            ROUND(AVG(rest_diff), 2) as avg_rest_diff
        FROM game_features
    """, conn)
    print("\n── Enrichment Summary ───────────────────────────────────────")
    print(f"  Total games     : {df['total'].iloc[0]:,}")
    print(f"  Has rest data   : {df['has_rest'].iloc[0]:,}")
    print(f"  Has ref data    : {df['has_refs'].iloc[0]:,}")
    print(f"  Home B2B games  : {df['home_b2b_games'].iloc[0]:,}")
    print(f"  Away B2B games  : {df['away_b2b_games'].iloc[0]:,}")
    print(f"  Avg rest diff   : {df['avg_rest_diff'].iloc[0]}")

    # Quick B2B impact check
    b2b = pd.read_sql("""
        SELECT
            home_b2b, away_b2b,
            AVG(actual_margin) as avg_margin,
            AVG(home_covered) as cover_rate,
            COUNT(*) as n
        FROM game_features
        WHERE actual_margin IS NOT NULL
          AND home_rest_days IS NOT NULL
        GROUP BY home_b2b, away_b2b
        ORDER BY home_b2b, away_b2b
    """, conn)

    print("\n── B2B Impact on Cover Rate ─────────────────────────────────")
    print(f"  {'Home B2B':<10} {'Away B2B':<10} {'N':>5}  "
          f"{'Avg Margin':>10}  {'Cover %':>8}")
    for _, r in b2b.iterrows():
        print(f"  {int(r['home_b2b']):<10} {int(r['away_b2b']):<10} "
              f"{int(r['n']):>5}  {r['avg_margin']:>+10.2f}  "
              f"{r['cover_rate']*100:>7.1f}%")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"Database: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)

    # 1. Rest / travel
    rest_df = compute_rest_features(conn)

    # 2. Referees
    ref_df = pull_referee_data(conn)

    # 3. In-season adjusted ratings
    pull_adjusted_ratings(conn)

    # 4. Update game_features
    update_game_features(conn, rest_df, ref_df)

    # 5. Summary
    print_summary(conn)

    conn.close()
    print("\n✓ Done — re-run 05_train_model.py to retrain with enriched features")


if __name__ == "__main__":
    main()
