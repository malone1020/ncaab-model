"""Test CBBD date chunking to get past 3000 game cap."""
import requests, os
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except: pass

KEY = os.getenv('CBBD_API_KEY','')
HDR = {'Authorization': f'Bearer {KEY}', 'Accept': 'application/json'}
BASE = 'https://api.collegebasketballdata.com'

print("Testing date chunking for 2026 season...")
total = set()

# Try fetching in monthly chunks with startDate only
for start in ['2025-11-01', '2025-12-01', '2026-01-01', '2026-02-01', '2026-03-01']:
    r = requests.get(f'{BASE}/games', headers=HDR,
        params={'season': 2026, 'seasonType': 'regular', 'startDate': start},
        timeout=15)
    if r.status_code == 200:
        data = r.json()
        dates = sorted(set(g.get('startDate','')[:10] for g in data if g.get('startDate')))
        new_games = set(g.get('id') for g in data) - total
        total.update(g.get('id') for g in data)
        print(f"  startDate={start}: {len(data)} games | {len(new_games)} new | "
              f"range: {dates[0] if dates else 'N/A'} to {dates[-1] if dates else 'N/A'}")
    else:
        print(f"  startDate={start}: HTTP {r.status_code}")

print(f"\nTotal unique games found: {len(total)}")
