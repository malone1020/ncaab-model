"""
03g_recency_features.py
=======================
Builds two new clean feature sources:

1. RECENCY_EFF — Exponential decay on Torvik daily ratings.
   Captures trend direction that point-in-time snapshots miss.
   A team improving from 15th → 5th AdjD is very different from
   one declining from 5th → 15th, even if today's rating is similar.

2. EXPERIENCE — Torvik experience rating (avg years of experience
   per player, weighted by minutes). Available from torvik_season.
   Stored pre-game using prior season value (no leakage).

Tables created:
  recency_eff      — per team per game: decay-weighted AdjEM, AdjO, AdjD trend
  team_experience  — per team per season: experience rating from torvik_season

Run: python scripts/03g_recency_features.py
"""

import sqlite3, os, math
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')


# ── 1. RECENCY-WEIGHTED EFFICIENCY ─────────────────────────────────────────

def build_recency_eff(conn):
    """
    For each game, for each team:
      - Gather all Torvik daily snapshots UP TO (not including) game date
      - Apply exponential decay: weight = exp(-lambda * days_ago)
      - lambda = ln(2) / 21  →  half-life of 21 days
      - Compute weighted avg and TREND (slope of last 30 days)
    """
    print("Building recency-weighted efficiency...")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS recency_eff (
            game_date   TEXT,
            team        TEXT,
            rew_adj_em  REAL,   -- decay-weighted AdjEM
            rew_adj_o   REAL,   -- decay-weighted AdjO
            rew_adj_d   REAL,   -- decay-weighted AdjD
            trend_adj_em REAL,  -- slope of AdjEM over last 30 days (pts/day)
            trend_adj_o  REAL,
            trend_adj_d  REAL,
            n_snaps     INTEGER,
            PRIMARY KEY (game_date, team)
        )
    """)

    # Load all torvik daily snapshots
    tvd = pd.read_sql("""
        SELECT snapshot_date, team, adj_em, adj_o, adj_d
        FROM torvik_daily
        WHERE adj_em IS NOT NULL
        ORDER BY snapshot_date, team
    """, conn)

    # Normalize snapshot_date to YYYY-MM-DD
    def norm_date(s):
        s = str(s).strip()
        if len(s) == 8 and '-' not in s:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s[:10]

    tvd['snapshot_date'] = tvd['snapshot_date'].apply(norm_date)
    tvd['snap_dt'] = pd.to_datetime(tvd['snapshot_date'])

    # Load games we need to compute features for
    games = pd.read_sql("""
        SELECT DISTINCT game_date, home_team as team FROM games
        UNION
        SELECT DISTINCT game_date, away_team as team FROM games
        ORDER BY game_date, team
    """, conn)
    games['game_dt'] = pd.to_datetime(games['game_date'])

    LAMBDA = math.log(2) / 21.0  # 21-day half-life

    rows = []
    teams = tvd['team'].unique()
    team_set = set(teams)

    # Build per-team snapshot index for fast lookup
    team_snaps = {}
    for team, grp in tvd.groupby('team'):
        team_snaps[team] = grp.sort_values('snap_dt').reset_index(drop=True)

    total = len(games)
    for i, (_, row) in enumerate(games.iterrows()):
        if i % 10000 == 0:
            print(f"  {i:,}/{total:,}...")

        gd   = row['game_date']
        team = row['team']
        gdt  = row['game_dt']

        if team not in team_snaps:
            continue

        snaps = team_snaps[team]
        # Only snapshots BEFORE game date
        prior = snaps[snaps['snap_dt'] < gdt]

        if len(prior) < 3:
            continue

        # Compute days ago for each snapshot
        days_ago = (gdt - prior['snap_dt']).dt.days.values.astype(float)
        weights  = np.exp(-LAMBDA * days_ago)

        em = prior['adj_em'].values
        ao = prior['adj_o'].values
        ad = prior['adj_d'].values

        w_sum    = weights.sum()
        rew_em   = float(np.dot(weights, em) / w_sum)
        rew_ao   = float(np.dot(weights, ao) / w_sum)
        rew_ad   = float(np.dot(weights, ad) / w_sum)

        # Trend: linear slope over last 30 days
        recent_mask = days_ago <= 30
        trend_em = trend_ao = trend_ad = None
        if recent_mask.sum() >= 4:
            x = -days_ago[recent_mask]   # x increases toward game date
            trend_em = float(np.polyfit(x, em[recent_mask], 1)[0])
            trend_ao = float(np.polyfit(x, ao[recent_mask], 1)[0])
            trend_ad = float(np.polyfit(x, ad[recent_mask], 1)[0])

        rows.append((gd, team, rew_em, rew_ao, rew_ad,
                     trend_em, trend_ao, trend_ad, len(prior)))

    conn.execute("DELETE FROM recency_eff")
    conn.executemany("""
        INSERT OR REPLACE INTO recency_eff VALUES (?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    print(f"  ✅ recency_eff: {len(rows):,} rows")
    return len(rows)


# ── 2. EXPERIENCE FEATURES ──────────────────────────────────────────────────

def build_experience(conn):
    """
    Pull experience rating from torvik_season.
    Use PRIOR SEASON value for each game to avoid leakage.
    (A team's experience in season S is known before season S+1 starts.)

    Experience = avg years of experience per player weighted by minutes.
    Higher = more experienced. Freshman-heavy teams = lower.
    """
    print("Building experience features...")

    # Check what experience columns exist in torvik_season
    cols = [r[1] for r in conn.execute("PRAGMA table_info(torvik_season)").fetchall()]
    exp_candidates = [c for c in cols if 'exp' in c.lower() or 'experience' in c.lower()]
    print(f"  Experience columns found: {exp_candidates}")

    if not exp_candidates:
        print("  ⚠️  No experience column in torvik_season — skipping")
        return 0

    exp_col = exp_candidates[0]  # e.g. 'exp' or 'experience'

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS team_experience (
            season      INTEGER,
            team        TEXT,
            experience  REAL,
            PRIMARY KEY (season, team)
        )
    """)

    rows = conn.execute(f"""
        SELECT season, team, {exp_col}
        FROM torvik_season
        WHERE {exp_col} IS NOT NULL
    """).fetchall()

    conn.execute("DELETE FROM team_experience")
    conn.executemany("INSERT OR REPLACE INTO team_experience VALUES (?,?,?)", rows)
    conn.commit()
    print(f"  ✅ team_experience: {len(rows):,} rows")
    return len(rows)


# ── 3. LINE MOVEMENT ────────────────────────────────────────────────────────

def check_line_movement(conn):
    """
    Check if game_lines already has open/close spread.
    Line movement = spread_close - spread_open.
    Sharp money signal: closing line differs significantly from open.
    """
    print("Checking line movement data...")

    cols = [r[1] for r in conn.execute("PRAGMA table_info(game_lines)").fetchall()]
    print(f"  game_lines columns: {cols}")

    open_col  = next((c for c in cols if 'open' in c.lower()), None)
    close_col = next((c for c in cols if c == 'spread' or 'close' in c.lower()), None)

    if open_col and close_col:
        n = conn.execute(f"""
            SELECT COUNT(*) FROM game_lines
            WHERE {open_col} IS NOT NULL AND {close_col} IS NOT NULL
        """).fetchone()[0]
        print(f"  ✅ Line movement available: {n:,} games with open+close spread")
        print(f"     Open col: {open_col}, Close col: {close_col}")

        # Compute movement stats
        stats = conn.execute(f"""
            SELECT
              AVG(ABS({close_col} - {open_col})) as avg_move,
              MAX(ABS({close_col} - {open_col})) as max_move,
              SUM(CASE WHEN ABS({close_col} - {open_col}) >= 1.0 THEN 1 ELSE 0 END) as moved_1pt
            FROM game_lines
            WHERE {open_col} IS NOT NULL AND {close_col} IS NOT NULL
        """).fetchone()
        print(f"     Avg move: {stats[0]:.2f} pts | Max: {stats[1]:.1f} pts | Moved ≥1pt: {stats[2]:,}")
    else:
        print("  ⚠️  No open line column found — line movement not available in current data")
        print("     To add: scrape opening lines from OddsPortal, Pinnacle, or Action Network API")
        print("     Feature: line_move = spread_close - spread_open")
        print("     Signal:  |line_move| ≥ 1.5 + direction vs public % = sharp money indicator")

    return open_col, close_col


if __name__ == '__main__':
    conn = sqlite3.connect(DB)

    build_recency_eff(conn)
    build_experience(conn)
    check_line_movement(conn)

    conn.close()
    print("\n✅ 03g complete.")
    print("Next: python scripts/04_build_features.py (to wire new features in)")
