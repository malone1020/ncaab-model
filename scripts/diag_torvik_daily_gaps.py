"""
diag_torvik_daily_gaps.py
Identifies exactly which game dates are missing from torvik_daily,
and what the scraper would need to fetch to fill them.
"""
import sqlite3, os, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# All game dates that have spread data (our betting universe)
game_dates = pd.read_sql("""
    SELECT DISTINCT g.season, g.game_date
    FROM games g
    JOIN game_lines gl ON gl.game_date = g.game_date
        AND gl.home_team = g.home_team
        AND gl.away_team = g.away_team
    WHERE gl.spread IS NOT NULL
    ORDER BY g.game_date
""", conn)
game_dates['game_date'] = pd.to_datetime(game_dates['game_date'])

# All snapshot dates in torvik_daily
snap_dates = pd.read_sql("""
    SELECT DISTINCT season, snapshot_date
    FROM torvik_daily
    ORDER BY snapshot_date
""", conn)
snap_dates['snapshot_date'] = pd.to_datetime(snap_dates['snapshot_date'])

conn.close()

print(f"Game dates with spread data: {len(game_dates):,} across {game_dates['season'].nunique()} seasons")
print(f"Torvik daily snapshot dates: {len(snap_dates):,}")

# For each game date, check if there's a snapshot within 1 day before
snap_set = set(snap_dates['snapshot_date'].dt.date)
game_dates['has_snapshot'] = game_dates['game_date'].dt.date.apply(
    lambda d: any((d - pd.Timedelta(days=i)).date() in snap_set for i in range(0, 3))
)

covered   = game_dates['has_snapshot'].sum()
uncovered = (~game_dates['has_snapshot']).sum()
print(f"\nGame dates WITH nearby snapshot:    {covered:,} ({covered/len(game_dates):.1%})")
print(f"Game dates WITHOUT nearby snapshot: {uncovered:,} ({uncovered/len(game_dates):.1%})")

print("\nBy season:")
for s, g in game_dates.groupby('season'):
    c = g['has_snapshot'].sum()
    print(f"  {s}: {c:>5}/{len(g):>5} covered ({c/len(g):.0%})")

# What dates are missing? Sample them
missing = game_dates[~game_dates['has_snapshot']].copy()
print(f"\nSample missing dates (first 20):")
print(missing[['season','game_date']].head(20).to_string(index=False))

# Are missing dates clustered (early season) or spread throughout?
missing['month'] = missing['game_date'].dt.month
print(f"\nMissing dates by month:")
print(missing['month'].value_counts().sort_index().to_string())

# What does torvik_daily actually have?
print(f"\nTorvik daily snapshot date range:")
print(f"  Earliest: {snap_dates['snapshot_date'].min().date()}")
print(f"  Latest:   {snap_dates['snapshot_date'].max().date()}")
print(f"  Total unique dates: {snap_dates['snapshot_date'].nunique()}")

# How many teams per snapshot date?
conn2 = sqlite3.connect(DB)
teams_per_snap = pd.read_sql("""
    SELECT snapshot_date, COUNT(DISTINCT team) n_teams
    FROM torvik_daily
    GROUP BY snapshot_date
    ORDER BY snapshot_date
""", conn2)
conn2.close()
teams_per_snap['snapshot_date'] = pd.to_datetime(teams_per_snap['snapshot_date'])
print(f"\nTeams per snapshot: min={teams_per_snap['n_teams'].min()}, max={teams_per_snap['n_teams'].max()}, median={teams_per_snap['n_teams'].median():.0f}")
print(f"Snapshots with full coverage (>340 teams): {(teams_per_snap['n_teams']>340).sum()}")
print(f"Snapshots with partial coverage (<100 teams): {(teams_per_snap['n_teams']<100).sum()}")

# Show gaps — consecutive date ranges that are missing
all_snap_dates = sorted(snap_dates['snapshot_date'].dt.date.unique())
if all_snap_dates:
    gaps = []
    prev = all_snap_dates[0]
    for d in all_snap_dates[1:]:
        diff = (d - prev).days
        if diff > 7:
            gaps.append((prev, d, diff))
        prev = d
    print(f"\nGaps > 7 days in torvik_daily snapshots ({len(gaps)} gaps):")
    for start, end, days in gaps[:20]:
        print(f"  {start} → {end}  ({days} days gap)")
