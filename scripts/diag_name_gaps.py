"""Find all team names in games that don't match any torvik_daily team name."""
import sqlite3, os, pandas as pd
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Get norm function from 04_build_features by importing it
import sys; sys.path.insert(0, 'scripts')
import importlib.util
spec = importlib.util.spec_from_file_location("fb", "scripts/04_build_features.py")
fb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fb)
norm = fb.norm

tvd_teams = set(r[0] for r in conn.execute("SELECT DISTINCT team FROM torvik_daily"))
game_teams_raw = set(r[0] for r in conn.execute("SELECT DISTINCT home_team FROM games"))

# Apply norm to both
tvd_normed   = set(norm(t) for t in tvd_teams)
game_normed  = set(norm(t) for t in game_teams_raw)

unmatched = sorted(game_normed - tvd_normed)
print(f"torvik_daily unique teams (normed): {len(tvd_normed)}")
print(f"games unique teams (normed):        {len(game_normed)}")
print(f"games teams with NO match in torvik_daily: {len(unmatched)}")
print()

# Show mismatches with closest torvik name
print("UNMATCHED game teams (first 60):")
for name in unmatched[:60]:
    # Find closest torvik name
    candidates = [t for t in tvd_normed if t[:4].lower() == name[:4].lower()]
    hint = candidates[:2] if candidates else ['(no close match)']
    print(f"  '{name}'  →  torvik has: {hint}")

conn.close()
