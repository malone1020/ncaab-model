"""
Standalone rolling efficiency feature builder.
Computes per-team rolling stats from game_team_stats:
  - Last 5/10 game weighted averages for: off_eff, def_eff, efg, tov, orb, pace
  - ATS trend (last 5/10 games vs spread)
  - Recency-weighted vs flat average

Results stored in: rolling_efficiency table
Used by 04_build_features.py as GROUP: ROLLING
"""
import sqlite3, os, pandas as pd, numpy as np
DB = os.path.join(os.getcwd(), "data", "basketball.db")

def db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def build():
    conn = db()

    print("Loading game_team_stats...")
    # Check schema first
    cols = [c[1] for c in conn.execute("PRAGMA table_info(game_team_stats)").fetchall()]
    print(f"  Columns: {cols}")

    stats = pd.read_sql("""
        SELECT gts.*, g.game_date, g.season, g.home_team, g.away_team,
               g.home_score, g.away_score
        FROM game_team_stats gts
        JOIN games g ON g.id = gts.game_id
        WHERE g.home_score IS NOT NULL
        ORDER BY g.game_date, gts.game_id
    """, conn)
    print(f"  {len(stats):,} team-game rows")
    print(f"  Columns: {list(stats.columns)}")
    print(f"  Sample:\n{stats.head(2).to_string()}")

    conn.close()

if __name__ == "__main__":
    build()
