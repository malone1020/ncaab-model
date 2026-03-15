"""Test CBBD API pagination options."""
import requests, os
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except: pass

KEY = os.getenv('CBBD_API_KEY','')
HDR = {'Authorization': f'Bearer {KEY}', 'Accept': 'application/json'}
BASE = 'https://api.collegebasketballdata.com'

print("Testing CBBD API date range pagination...")

# Test 1: date range
r = requests.get(f'{BASE}/games', headers=HDR,
    params={'season': 2026, 'seasonType': 'regular',
            'startDate': '2026-01-07', 'endDate': '2026-03-14'}, timeout=15)
print(f"\nWith startDate/endDate: HTTP {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"  Games returned: {len(data)}")
    if data:
        dates = sorted(set(g.get('startDate','')[:10] for g in data))
        print(f"  Date range: {dates[0]} to {dates[-1]}")

# Test 2: no date range (baseline)
r2 = requests.get(f'{BASE}/games', headers=HDR,
    params={'season': 2026, 'seasonType': 'regular'}, timeout=15)
print(f"\nNo date filter: HTTP {r2.status_code}")
if r2.status_code == 200:
    data2 = r2.json()
    print(f"  Games returned: {len(data2)}")
    if data2:
        dates2 = sorted(set(g.get('startDate','')[:10] for g in data2))
        print(f"  Date range: {dates2[0]} to {dates2[-1]}")
