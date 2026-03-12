"""
diag_torvik_daily_gaps.py — v2
Diagnoses torvik_daily coverage gaps without relying on the broken join.
"""
import sqlite3, os, pandas as pd
from datetime import timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# ── All game dates directly from games table ──────────────────────────────────
game_dates = pd.read_sql("""
    SELECT DISTINCT season, game_date FROM games
    ORDER BY game_date
""", conn)
game_dates['game_date'] = pd.to_datetime(game_dates['game_date'])

# ── All snapshot dates in torvik_daily ───────────────────────────────────────
snap_df = pd.read_sql("""
    SELECT DISTINCT snapshot_date, COUNT(DISTINCT team) n_teams
    FROM torvik_daily
    GROUP BY snapshot_date
    ORDER BY snapshot_date
""", conn)
snap_df['snapshot_date'] = pd.to_datetime(snap_df['snapshot_date'])
snap_set = set(snap_df['snapshot_date'].dt.date.tolist())

conn.close()

print(f"Total game dates in games table:    {len(game_dates):,}")
print(f"Torvik daily unique snapshot dates: {len(snap_df):,}")
print(f"Teams per snapshot: min={snap_df['n_teams'].min()}, "
      f"max={snap_df['n_teams'].max()}, median={snap_df['n_teams'].median():.0f}")

# ── For each game date, is there a snapshot within 3 days prior? ──────────────
def has_nearby_snap(d):
    for i in range(0, 4):
        if (d - timedelta(days=i)) in snap_set:
            return True
    return False

game_dates['has_snapshot'] = game_dates['game_date'].dt.date.map(has_nearby_snap)

covered   = game_dates['has_snapshot'].sum()
uncovered = (~game_dates['has_snapshot']).sum()
print(f"\nGame dates WITH nearby snapshot (within 3 days):    {covered:,} ({covered/len(game_dates):.1%})")
print(f"Game dates WITHOUT nearby snapshot:                {uncovered:,} ({uncovered/len(game_dates):.1%})")

print("\nCoverage by season:")
for s, g in game_dates.groupby('season'):
    c = g['has_snapshot'].sum()
    print(f"  {s}: {c:>5}/{len(g):>5} game dates covered ({c/len(g):.0%})")

# ── What date range does torvik_daily cover? ──────────────────────────────────
print(f"\nTorvik daily date range:")
print(f"  Earliest snapshot: {snap_df['snapshot_date'].min().date()}")
print(f"  Latest snapshot:   {snap_df['snapshot_date'].max().date()}")
print(f"\nAll snapshot dates in DB:")
for _, row in snap_df.iterrows():
    print(f"  {row['snapshot_date'].date()}  ({row['n_teams']} teams)")

# ── Find gaps > 7 days between consecutive snapshots ─────────────────────────
snap_dates_sorted = sorted(snap_set)
print(f"\nGaps > 7 days between snapshots ({len(snap_dates_sorted)} total snapshots):")
gap_count = 0
for i in range(1, len(snap_dates_sorted)):
    diff = (snap_dates_sorted[i] - snap_dates_sorted[i-1]).days
    if diff > 7:
        print(f"  {snap_dates_sorted[i-1]} → {snap_dates_sorted[i]}  ({diff} days)")
        gap_count += 1
if gap_count == 0:
    print("  No gaps > 7 days found")

# ── What game dates are completely unrepresented? ────────────────────────────
missing = game_dates[~game_dates['has_snapshot']]
print(f"\nMissing coverage by month (all seasons):")
missing['month'] = missing['game_date'].dt.month
print(missing['month'].value_counts().sort_index().rename({
    11:'Nov',12:'Dec',1:'Jan',2:'Feb',3:'Mar',4:'Apr'
}).to_string())

print(f"\nSample missing game dates:")
print(missing[['season','game_date']].head(30).to_string(index=False))
