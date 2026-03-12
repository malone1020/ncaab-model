"""Instrument the exact torvik_as_of call path from the real game loop."""
import sqlite3, os, sys, pandas as pd
sys.path.insert(0, 'scripts')

DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Replicate load_games exactly as 04_build_features does
import importlib.util
spec = importlib.util.spec_from_file_location("fb", "scripts/04_build_features.py")
fb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fb)

games  = fb.load_games(conn)
tv_d   = fb.load_torvik_daily(conn)
lines  = fb.load_lines(conn)
norm   = fb.norm

# Apply the same merge that build_features does
if not lines.empty:
    lines_reset = lines.copy()
    games = games.merge(lines_reset, on=['game_date','home_team','away_team'], how='left')

print(f"games.game_date dtype after merge: {games['game_date'].dtype}")
print(f"Sample game_date values: {games['game_date'].head(3).tolist()}")
print(f"Index has {len(tv_d)} keys")

# Try first 20 games — print key attempted vs whether it hit
hits, misses = 0, 0
miss_examples = []
for _, g in games.head(200).iterrows():
    s    = int(g['season'])
    home = g['home_team']
    away = g['away_team']
    gd   = pd.Timestamp(g['game_date'])
    gdate_int = int(gd.strftime('%Y%m%d'))
    
    for team in [home, away]:
        key = (s, team)
        snaps = tv_d.get(key)
        if snaps:
            before = [x[0] for x in snaps if x[0] < gdate_int]
            if before:
                hits += 1
            else:
                misses += 1
                if len(miss_examples) < 5:
                    miss_examples.append(f"  KEY EXISTS but no snap before {gdate_int}: key={key}, earliest_snap={snaps[0][0]}")
        else:
            misses += 1
            if len(miss_examples) < 5:
                miss_examples.append(f"  KEY MISSING: ({s}, '{team}') — norm='{norm(team)}'")

print(f"\nFirst 200 games: hits={hits}, misses={misses}")
print("Miss examples:")
for e in miss_examples:
    print(e)

# Also check: are the keys in tv_d using normed or raw team names?
sample_keys = list(tv_d.keys())[:5]
print(f"\nSample index keys: {sample_keys}")
conn.close()
