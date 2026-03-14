"""
03h_compute_trends.py
=====================
Fills NULL trend_adj_em, trend_adj_o, trend_adj_d columns in recency_eff.

Method: for each (game_date, team) row, pull the last N torvik_daily
snapshots strictly before game_date and fit a linear slope to adj_em/o/d.
Positive slope = team improving, negative = declining.

The slope is normalized by the number of snapshots so it's comparable
across teams with different data density.

torvik_daily.snapshot_date is in YYYYMMDD format.
recency_eff.game_date is in YYYY-MM-DD format.

Run: python scripts/03h_compute_trends.py
     python scripts/03h_compute_trends.py --n-snaps 8  (use last 8 snapshots)
     python scripts/03h_compute_trends.py --overwrite   (recompute all, not just NULLs)
"""

import sqlite3, os, argparse, warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

DEFAULT_N_SNAPS = 10   # last N snapshots before game date
MIN_SNAPS       = 3    # need at least this many to compute a meaningful slope
BATCH_COMMIT    = 1000


def linear_slope(values):
    """Return linear regression slope of values over equally-spaced x."""
    n = len(values)
    if n < 2:
        return None
    x = np.arange(n, dtype=float)
    y = np.array(values, dtype=float)
    # Remove NaNs
    mask = ~np.isnan(y)
    if mask.sum() < 2:
        return None
    x, y = x[mask], y[mask]
    # Simple OLS slope
    xm, ym = x.mean(), y.mean()
    denom = ((x - xm) ** 2).sum()
    if denom == 0:
        return None
    return float(((x - xm) * (y - ym)).sum() / denom)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--n-snaps', type=int, default=DEFAULT_N_SNAPS,
                   help=f'Number of snapshots to use (default: {DEFAULT_N_SNAPS})')
    p.add_argument('--overwrite', action='store_true',
                   help='Recompute all rows, not just NULLs')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()

    print("=" * 60)
    print("NCAAB — Computing Recency Trend Features")
    print("=" * 60)
    print(f"  Using last {args.n_snaps} Torvik snapshots per game")
    print(f"  Min snapshots required: {MIN_SNAPS}")

    conn = sqlite3.connect(DB)

    # Load recency_eff rows that need trend computation
    if args.overwrite:
        rew = pd.read_sql("SELECT game_date, team FROM recency_eff", conn)
        print(f"  Rows to process (overwrite): {len(rew):,}")
    else:
        rew = pd.read_sql("""
            SELECT game_date, team FROM recency_eff
            WHERE trend_adj_em IS NULL
        """, conn)
        print(f"  Rows with NULL trends: {len(rew):,}")

    if len(rew) == 0:
        print("  Nothing to do — all trends already computed.")
        conn.close()
        exit(0)

    # Load all torvik_daily snapshots into memory — fast dict lookup
    # Key: (team, snapshot_date_int) -> {adj_em, adj_o, adj_d}
    print("\n  Loading torvik_daily...")
    tvd = pd.read_sql("""
        SELECT team, snapshot_date, adj_em, adj_o, adj_d
        FROM torvik_daily
        WHERE adj_em IS NOT NULL
        ORDER BY team, snapshot_date
    """, conn)
    print(f"  {len(tvd):,} snapshots loaded")

    # Convert snapshot_date (YYYYMMDD str) to int for fast comparison
    tvd['snap_int'] = tvd['snapshot_date'].astype(int)

    # Group by team for fast lookup
    tvd_by_team = {}
    for team, grp in tvd.groupby('team'):
        # Sort by date ascending
        grp_sorted = grp.sort_values('snap_int')
        tvd_by_team[team] = grp_sorted[['snap_int','adj_em','adj_o','adj_d']].values

    print(f"  {len(tvd_by_team):,} unique teams indexed")

    # Compute trends
    updates = []
    skipped = 0

    for i, (_, row) in enumerate(rew.iterrows()):
        game_date = row['game_date']   # YYYY-MM-DD
        team      = row['team']

        # Convert game_date to YYYYMMDD int for comparison
        game_int = int(game_date.replace('-', ''))

        snaps = tvd_by_team.get(team)
        if snaps is None:
            skipped += 1
            continue

        # Get last N snapshots strictly before game_date
        before = snaps[snaps[:, 0] < game_int]
        if len(before) < MIN_SNAPS:
            skipped += 1
            continue

        recent = before[-args.n_snaps:]   # last N rows

        slope_em = linear_slope(recent[:, 1])
        slope_o  = linear_slope(recent[:, 2])
        slope_d  = linear_slope(recent[:, 3])

        updates.append((slope_em, slope_o, slope_d, game_date, team))

        if (i + 1) % BATCH_COMMIT == 0:
            conn.executemany("""
                UPDATE recency_eff
                SET trend_adj_em = ?,
                    trend_adj_o  = ?,
                    trend_adj_d  = ?
                WHERE game_date = ? AND team = ?
            """, updates)
            conn.commit()
            updates = []
            print(f"  [{i+1}/{len(rew)}] committed batch | skipped={skipped}")

    # Final batch
    if updates:
        conn.executemany("""
            UPDATE recency_eff
            SET trend_adj_em = ?,
                trend_adj_o  = ?,
                trend_adj_d  = ?
            WHERE game_date = ? AND team = ?
        """, updates)
        conn.commit()

    # Report
    filled = conn.execute("""
        SELECT COUNT(*) FROM recency_eff WHERE trend_adj_em IS NOT NULL
    """).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM recency_eff").fetchone()[0]

    conn.close()

    print()
    print("=" * 60)
    print(f"Done. {filled:,} / {total:,} rows now have trend values")
    print(f"Skipped (insufficient snapshots): {skipped:,}")
    print()
    print("Next: python scripts/04_build_features.py")
    print("      python scripts/08_train_totals_model.py "
          "--combo \"CONTEXT+TVD+KPD+RECENCY+REFS+TRAVEL\"")
