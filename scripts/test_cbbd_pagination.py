"""Check torvik_game_preds as games table source + test /games/teams endpoint."""
import sqlite3, requests, os
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except: pass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
KEY = os.getenv('CBBD_API_KEY','')
HDR = {'Authorization': f'Bearer {KEY}', 'Accept': 'application/json'}
BASE = 'https://api.collegebasketballdata.com'

conn = sqlite3.connect(DB)

# Check torvik_game_preds schema and coverage
cols = [c[1] for c in conn.execute("PRAGMA table_info(torvik_game_preds)").fetchall()]
print(f"torvik_game_preds cols: {cols}")

r = conn.execute("""
    SELECT MIN(game_date), MAX(game_date), COUNT(*)
    FROM torvik_game_preds WHERE season=2026
""").fetchone()
print(f"2026 coverage: {r[0]} to {r[1]} ({r[2]:,} games)")

# Sample rows
rows = conn.execute("""
    SELECT game_date, home_team, away_team, actual_home, actual_away, neutral_site
    FROM torvik_game_preds WHERE season=2026 AND game_date > '2026-01-06'
    LIMIT 5
""").fetchall()
print(f"\nSample post-Jan-6 games:")
for r in rows: print(f"  {r}")

# Check how many are missing from games table
missing = conn.execute("""
    SELECT COUNT(*) FROM torvik_game_preds tp
    LEFT JOIN games g ON tp.game_date=g.game_date 
        AND tp.home_team=g.home_team AND tp.away_team=g.away_team
    WHERE tp.season=2026 AND tp.game_date > '2026-01-06'
    AND g.game_date IS NULL
""").fetchone()[0]
print(f"\nGames in torvik_preds after Jan 6 but missing from games: {missing:,}")

conn.close()

# Test /games/teams endpoint
print(f"\nTesting /games/teams endpoint...")
r = requests.get(f'{BASE}/games/teams', headers=HDR,
    params={'season': 2026, 'seasonType': 'regular'}, timeout=15)
print(f"HTTP {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"Games returned: {len(data)}")
    if data:
        dates = sorted(set(g.get('date','')[:10] for g in data if g.get('date')))
        print(f"Date range: {dates[0]} to {dates[-1]}")
