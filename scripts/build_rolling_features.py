"""
build_rolling_features.py
=========================
Builds per-team rolling efficiency features from game_team_stats.
Computes as-of-game-date rolling windows (no leakage) for:
  - Last 5 and 10 game averages: efg_pct, tov_pct, orb_pct, ft_rate,
    three_pct, pace, points (off), points allowed (def)
  - Recency-weighted (exponential decay) versions
  - Implied offensive/defensive efficiency from box scores

Stored in: rolling_efficiency table
  (season, game_date, team, <features>)

Each row represents a team's rolling stats ENTERING that game
(computed from prior games only — strict no-leakage).
"""
import sqlite3, os, pandas as pd, numpy as np
from datetime import datetime

DB = os.path.join(os.getcwd(), "data", "basketball.db")

def db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def ensure_table(conn):
    conn.execute("DROP TABLE IF EXISTS rolling_efficiency")
    conn.execute("""
        CREATE TABLE rolling_efficiency (
            season        INTEGER,
            game_date     TEXT,
            team          TEXT,
            -- Last 5 game rolling averages
            r5_efg        REAL,
            r5_tov        REAL,
            r5_orb        REAL,
            r5_ftr        REAL,
            r5_3pct       REAL,
            r5_pace       REAL,
            r5_pts_off    REAL,
            r5_pts_def    REAL,
            r5_margin     REAL,
            -- Last 10 game rolling averages
            r10_efg       REAL,
            r10_tov       REAL,
            r10_orb       REAL,
            r10_ftr       REAL,
            r10_3pct      REAL,
            r10_pace      REAL,
            r10_pts_off   REAL,
            r10_pts_def   REAL,
            r10_margin    REAL,
            -- Exponentially weighted (recent games weighted more)
            ew_efg        REAL,
            ew_tov        REAL,
            ew_orb        REAL,
            ew_pts_off    REAL,
            ew_pts_def    REAL,
            ew_margin     REAL,
            -- Trend: last 5 minus last 10 (momentum direction)
            trend_efg     REAL,
            trend_margin  REAL,
            -- Sample size
            games_played  INTEGER,
            PRIMARY KEY (season, game_date, team)
        )
    """)
    conn.commit()

def ewm_mean(series, span=5):
    """Exponentially weighted mean — more weight to recent games."""
    if len(series) == 0:
        return None
    weights = np.array([np.exp(-i/span) for i in range(len(series)-1, -1, -1)])
    weights /= weights.sum()
    return float(np.dot(series, weights))

def rolling_mean(series, n):
    if len(series) == 0:
        return None
    vals = series[-n:] if len(series) >= n else series
    return float(np.mean(vals))

def build():
    conn = db()
    ensure_table(conn)

    print("Loading game data...")
    # Use games table for authoritative game_date, join stats
    df = pd.read_sql("""
        SELECT
            g.id        AS game_id,
            g.game_date,
            g.season,
            gts.team,
            gts.opponent,
            gts.is_home,
            gts.efg_pct,
            gts.tov_pct,
            gts.orb_pct,
            gts.ft_rate,
            gts.three_pct,
            gts.pace,
            gts.points   AS pts_off
        FROM game_team_stats gts
        JOIN games g ON g.id = gts.game_id
        WHERE g.home_score IS NOT NULL
          AND gts.points IS NOT NULL
        ORDER BY gts.team, g.game_date, g.id
    """, conn)

    print(f"  {len(df):,} team-game rows")
    df['game_date'] = pd.to_datetime(df['game_date'])

    # Add points allowed (opponent's points in same game)
    # Self-join: for each team-game, find the opponent row
    pts_lookup = df[['game_id','team','pts_off']].copy()
    pts_lookup.columns = ['game_id','opponent','pts_def']
    df = df.merge(pts_lookup, on=['game_id','opponent'], how='left')
    df['margin'] = df['pts_off'] - df['pts_def']

    print("Computing rolling features per team...")
    all_rows = []

    for team, grp in df.groupby('team'):
        grp = grp.sort_values('game_date').reset_index(drop=True)

        for i in range(len(grp)):
            row = grp.iloc[i]
            # Only use games BEFORE this game (strict no-leakage)
            prior = grp.iloc[:i]

            if len(prior) == 0:
                # No prior games — skip (can't compute rolling stats)
                continue

            def col(c):
                return prior[c].dropna().values

            r5_efg      = rolling_mean(col('efg_pct'), 5)
            r5_tov      = rolling_mean(col('tov_pct'), 5)
            r5_orb      = rolling_mean(col('orb_pct'), 5)
            r5_ftr      = rolling_mean(col('ft_rate'),  5)
            r5_3pct     = rolling_mean(col('three_pct'),5)
            r5_pace     = rolling_mean(col('pace'),     5)
            r5_pts_off  = rolling_mean(col('pts_off'),  5)
            r5_pts_def  = rolling_mean(col('pts_def'),  5)
            r5_margin   = rolling_mean(col('margin'),   5)

            r10_efg     = rolling_mean(col('efg_pct'), 10)
            r10_tov     = rolling_mean(col('tov_pct'), 10)
            r10_orb     = rolling_mean(col('orb_pct'), 10)
            r10_ftr     = rolling_mean(col('ft_rate'),  10)
            r10_3pct    = rolling_mean(col('three_pct'),10)
            r10_pace    = rolling_mean(col('pace'),     10)
            r10_pts_off = rolling_mean(col('pts_off'),  10)
            r10_pts_def = rolling_mean(col('pts_def'),  10)
            r10_margin  = rolling_mean(col('margin'),   10)

            ew_efg      = ewm_mean(col('efg_pct'))
            ew_tov      = ewm_mean(col('tov_pct'))
            ew_orb      = ewm_mean(col('orb_pct'))
            ew_pts_off  = ewm_mean(col('pts_off'))
            ew_pts_def  = ewm_mean(col('pts_def'))
            ew_margin   = ewm_mean(col('margin'))

            trend_efg    = (r5_efg - r10_efg)    if r5_efg    and r10_efg    else None
            trend_margin = (r5_margin - r10_margin) if r5_margin and r10_margin else None

            all_rows.append((
                int(row['season']), row['game_date'].strftime('%Y-%m-%d'), team,
                r5_efg, r5_tov, r5_orb, r5_ftr, r5_3pct, r5_pace,
                r5_pts_off, r5_pts_def, r5_margin,
                r10_efg, r10_tov, r10_orb, r10_ftr, r10_3pct, r10_pace,
                r10_pts_off, r10_pts_def, r10_margin,
                ew_efg, ew_tov, ew_orb, ew_pts_off, ew_pts_def, ew_margin,
                trend_efg, trend_margin,
                len(prior)
            ))

    print(f"  Computed {len(all_rows):,} rolling rows")
    print("  Inserting into rolling_efficiency...")

    conn.executemany("""
        INSERT OR REPLACE INTO rolling_efficiency VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, all_rows)
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM rolling_efficiency").fetchone()[0]
    teams = conn.execute("SELECT COUNT(DISTINCT team) FROM rolling_efficiency").fetchone()[0]
    print(f"\n✅ rolling_efficiency: {total:,} rows, {teams} teams")
    print("Next: python scripts/04_build_features.py")
    conn.close()

if __name__ == "__main__":
    build()
