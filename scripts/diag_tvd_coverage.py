"""Full TVD coverage check — per season hit rate."""
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

# Per-season: count games where BOTH teams get a TVD hit
season_stats = {}
key_miss_examples = {}
date_miss_examples = {}

for _, g in games.iterrows():
    s  = int(g['season'])
    gd = pd.Timestamp(g['game_date'])
    gdate_int = int(gd.strftime('%Y%m%d'))
    if s not in season_stats:
        season_stats[s] = {'total':0,'both':0,'key_miss':0,'date_miss':0}
    season_stats[s]['total'] += 1

    h_ok = a_ok = False
    for team in [g['home_team'], g['away_team']]:
        snaps = tv_d.get((s, team))
        if snaps is None:
            season_stats[s]['key_miss'] += 1
            if s not in key_miss_examples:
                key_miss_examples[s] = team
        elif snaps[0][0] >= gdate_int:
            season_stats[s]['date_miss'] += 1
            if s not in date_miss_examples:
                date_miss_examples[s] = f"team={team} game={gdate_int} earliest_snap={snaps[0][0]}"
        else:
            if team == g['home_team']: h_ok = True
            else: a_ok = True
    if h_ok and a_ok:
        season_stats[s]['both'] += 1

print(f"{'Season':>8} {'Total':>7} {'Both':>7} {'%':>6} {'KeyMiss':>8} {'DateMiss':>9}")
print("-"*55)
for s in sorted(season_stats):
    d = season_stats[s]
    pct = 100*d['both']/d['total'] if d['total'] else 0
    print(f"  {s}   {d['total']:>6} {d['both']:>7} {pct:>5.0f}%  {d['key_miss']:>7}  {d['date_miss']:>8}")
    if s in key_miss_examples:
        print(f"           key_miss ex: '{key_miss_examples[s]}'")
    if s in date_miss_examples:
        print(f"           date_miss ex: {date_miss_examples[s]}")

conn.close()
