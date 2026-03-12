"""
diag_coverage_gaps.py — Diagnose exactly what's missing and why
Covers:
  1. Torvik daily: which game dates have no snapshot within 7 days
  2. 2016/2017 season name normalization failures
  3. torvik_game_preds join failure rate + sample mismatches
  4. game_lines team name mismatches vs games table
Run from project root: python scripts/diag_coverage_gaps.py
"""
import sqlite3, os, pandas as pd
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

SEP = "=" * 65
def section(t): print(f"\n{SEP}\n{t}\n{SEP}")

# ── helper: normalize team names same way 04_build_features does ─────────────
import re
REPLACEMENTS = {
    'St.': 'St', 'Saint': 'St', 'State': 'St',
    'University': '', 'College': '',
}
def norm(name):
    if not isinstance(name, str): return ''
    n = name.strip()
    n = re.sub(r'[^\w\s]', '', n)
    n = re.sub(r'\s+', ' ', n).strip().lower()
    return n

# ── 1. Torvik daily coverage vs game dates ────────────────────────────────────
section("TORVIK DAILY — snapshot coverage vs actual game dates")

# What snapshot dates do we have?
snap_dates = set(
    r[0] for r in conn.execute("SELECT DISTINCT snapshot_date FROM torvik_daily")
)
print(f"  Unique snapshot dates in DB: {len(snap_dates)}")
print(f"  Date range: {min(snap_dates)} → {max(snap_dates)}")

# What game dates exist?
game_dates = pd.read_sql(
    "SELECT DISTINCT game_date, season FROM games WHERE home_score IS NOT NULL",
    conn
)
game_dates['game_date'] = pd.to_datetime(game_dates['game_date'])

# For each game date, find nearest snapshot within 7 days prior
def nearest_snap(gd):
    gd_str = gd.strftime('%Y%m%d')
    # Check within 7 days prior
    for delta in range(0, 8):
        candidate = (gd - timedelta(days=delta)).strftime('%Y%m%d')
        if candidate in snap_dates:
            return delta
    return None

print("\n  Games with no snapshot within 7 days, by season:")
for season, grp in game_dates.groupby('season'):
    missing = 0
    for gd in grp['game_date']:
        if nearest_snap(gd) is None:
            missing += 1
    pct = missing / len(grp)
    if pct > 0.01:
        print(f"    {season}: {missing}/{len(grp)} games missing ({pct:.0%})")

# Show which specific date ranges have no snapshots
print("\n  Snapshot date gaps (periods with no snapshot > 10 days):")
sorted_snaps = sorted(snap_dates)
for i in range(1, len(sorted_snaps)):
    d1 = datetime.strptime(sorted_snaps[i-1], '%Y%m%d')
    d2 = datetime.strptime(sorted_snaps[i], '%Y%m%d')
    gap = (d2 - d1).days
    if gap > 10:
        print(f"    Gap: {sorted_snaps[i-1]} → {sorted_snaps[i]} ({gap} days)")

# ── 2. 2016/2017 name normalization failures ──────────────────────────────────
section("2016/2017 NAME NORMALIZATION — why 0% Torvik/KenPom/Hasla match")

# Get team names from games 2016
games_2016 = pd.read_sql(
    "SELECT DISTINCT home_team FROM games WHERE season=2016 LIMIT 30", conn
)
# Get team names from torvik_season 2016
torvik_2016 = pd.read_sql(
    "SELECT DISTINCT team FROM torvik_season WHERE season=2016 LIMIT 30", conn
)
print("  games.home_team samples (2016):")
print("  ", games_2016['home_team'].tolist()[:15])
print("\n  torvik_season.team samples (2016):")
print("  ", torvik_2016['team'].tolist()[:15])

# Check if norm() brings them together
games_2016_norm = set(norm(t) for t in games_2016['home_team'])
torvik_2016_norm = set(norm(t) for t in torvik_2016['team'])
overlap = games_2016_norm & torvik_2016_norm
print(f"\n  After norm(): {len(overlap)}/{len(games_2016_norm)} match")

# Try exact match (pre-norm)
games_all_2016 = set(r[0] for r in conn.execute(
    "SELECT DISTINCT home_team FROM games WHERE season=2016"
))
torvik_all_2016 = set(r[0] for r in conn.execute(
    "SELECT DISTINCT team FROM torvik_season WHERE season=2016"
))
exact_match = games_all_2016 & torvik_all_2016
print(f"  Exact match (no norm): {len(exact_match)}/{len(games_all_2016)} games teams in torvik")
print(f"  Sample games teams NOT in torvik: {list(games_all_2016 - torvik_all_2016)[:10]}")
print(f"  Sample torvik teams NOT in games: {list(torvik_all_2016 - games_all_2016)[:10]}")

# ── 3. How 04_build_features.py currently normalizes ─────────────────────────
section("HOW norm() WORKS IN 04_build_features.py — check for 2016 bug")

# Read the actual norm function from 04
with open(os.path.join(ROOT, 'scripts', '04_build_features.py')) as f:
    content = f.read()
# Find norm function
start = content.find('def norm(')
end = content.find('\ndef ', start + 1)
norm_func = content[start:end]
print(norm_func[:500])

# ── 4. torvik_game_preds join failures ───────────────────────────────────────
section("TORVIK_GAME_PREDS — join failure analysis")

tp = pd.read_sql(
    "SELECT game_date, home_team, away_team FROM torvik_game_preds WHERE season=2024 LIMIT 5",
    conn
)
g = pd.read_sql(
    "SELECT game_date, home_team, away_team FROM games WHERE season=2024 LIMIT 5",
    conn
)
print("  torvik_game_preds sample (2024):")
print(tp.to_string(index=False))
print("\n  games sample (2024):")
print(g.to_string(index=False))

# Check overlap for 2024
tp_keys = set(
    (r[0], norm(r[1]), norm(r[2]))
    for r in conn.execute(
        "SELECT game_date, home_team, away_team FROM torvik_game_preds WHERE season=2024"
    )
)
g_keys = set(
    (r[0], norm(r[1]), norm(r[2]))
    for r in conn.execute(
        "SELECT game_date, home_team, away_team FROM games WHERE season=2024"
    )
)
matched = tp_keys & g_keys
print(f"\n  2024 torvik_game_preds: {len(tp_keys)} entries")
print(f"  2024 games: {len(g_keys)} entries")
print(f"  Matched on date+norm(team): {len(matched)} ({len(matched)/len(g_keys):.0%} of games)")

# Sample mismatches
tp_only = list(tp_keys - g_keys)[:5]
print(f"\n  In torvik_preds but NOT in games (date, home, away):")
for x in tp_only:
    print(f"    {x}")
g_only_with_tp_date = [k for k in (g_keys - tp_keys) if k[0] in {x[0] for x in tp_only}][:5]

# ── 5. game_lines name mismatches ─────────────────────────────────────────────
section("GAME_LINES TEAM NAME MISMATCHES — pre-2019 vs post-2019")

for season in [2017, 2018, 2024]:
    gl_teams = set(r[0] for r in conn.execute(
        f"SELECT DISTINCT home_team FROM game_lines WHERE season={season}"
    ))
    g_teams = set(r[0] for r in conn.execute(
        f"SELECT DISTINCT home_team FROM games WHERE season={season}"
    ))
    exact = gl_teams & g_teams
    print(f"\n  {season}: game_lines has {len(gl_teams)} teams, games has {len(g_teams)} teams")
    print(f"  Exact match: {len(exact)} teams ({len(exact)/len(g_teams):.0%})")
    # Sample mismatches
    in_lines_not_games = list(gl_teams - g_teams)[:8]
    in_games_not_lines = list(g_teams - gl_teams)[:8]
    print(f"  In game_lines NOT games: {in_lines_not_games}")
    print(f"  In games NOT game_lines: {in_games_not_lines}")

conn.close()
