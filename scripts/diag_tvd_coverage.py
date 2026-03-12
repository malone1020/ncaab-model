"""Check TVD coverage across all games — both teams must hit for a game to count."""
import sqlite3, os, sys, pandas as pd
sys.path.insert(0, 'scripts')
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

import importlib.util
spec = importlib.util.spec_from_file_location("fb", "scripts/04_build_features.py")
fb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fb)

games = fb.load_games(conn)
tv_d  = fb.load_torvik_daily(conn)
lines = fb.load_lines(conn)
if not lines.empty:
    games = games.merge(lines, on=['game_date','home_team','away_team'], how='left')

total = both_hit = home_hit = away_hit = no_hit = 0
key_missing = date_too_early = 0

for _, g in games.iterrows():
    s  = int(g['season'])
    gd = pd.Timestamp(g['game_date'])
    gdate_int = int(gd.strftime('%Y%m%d'))
    total += 1

    h_hit = a_hit = False
    for team, flag in [(g['home_team'], 'h'), (g['away_team'], 'a')]:
        snaps = tv_d.get((s, team))
        if snaps is None:
            key_missing += 1
        elif snaps[0][0] >= gdate_int:
            date_too_early += 1
        else:
            if flag == 'h': h_hit = True
            else:           a_hit = True

    if h_hit and a_hit: both_hit += 1
    elif h_hit or a_hit: (home_hit if h_hit else away_hit).__class__  # dummy
    else: no_hit += 1
    if h_hit: home_hit += 1
    if a_hit: away_hit += 1

print(f"Total games:         {total:,}")
print(f"Both teams hit:      {both_hit:,}  ({100*both_hit/total:.1f}%)")
print(f"Home team hit:       {home_hit:,}  ({100*home_hit/total:.1f}%)")
print(f"Away team hit:       {away_hit:,}  ({100*away_hit/total:.1f}%)")
print(f"Key missing (team-sides): {key_missing:,}")
print(f"Key exists but game before first snap: {date_too_early:,}")

# Per-season both-hit rate
print("\nPer-season both-hit rate:")
season_counts = {}
for _, g in games.iterrows():
    s  = int(g['season'])
    gd = pd.Timestamp(g['game_date'])
    gdate_int = int(gd.strftime('%Y%m%d'))
    h = tv_d.get((s, g['home_team']))
    a = tv_d.get((s, g['away_team']))
    h_ok = h is not None and h[0][0] < gdate_int
    a_ok = a is not None and a[0][0] < gdate_int
    if s not in season_counts: season_counts[s] = [0,0]
    season_counts[s][1] += 1
    if h_ok and a_ok: season_counts[s][0] += 1

for s in sorted(season_counts):
    hit, tot = season_counts[s]
    print(f"  {s}: {hit:,}/{tot:,}  ({100*hit/tot:.0f}%)")

conn.close()
