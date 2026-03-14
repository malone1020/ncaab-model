"""Debug what OddsAPI returns for a specific tournament date."""
import os, requests
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except: pass

ODDS_KEY = os.getenv('ODDS_API_KEY', '')
HIST_URL = "https://api.the-odds-api.com/v4/historical/sports/basketball_ncaab/odds/"

def fetch(date_str):
    params = {
        'apiKey': ODDS_KEY, 'regions': 'us',
        'markets': 'spreads,totals,h2h', 'bookmakers': 'draftkings',
        'oddsFormat': 'american', 'date': f"{date_str}T23:00:00Z",
    }
    r = requests.get(HIST_URL, params=params, timeout=20)
    print(f"HTTP {r.status_code} | remaining: {r.headers.get('X-Requests-Remaining','?')}")
    if r.status_code == 200:
        data = r.json()
        games = data.get('data', data) if isinstance(data, dict) else data
        print(f"Games returned: {len(games)}")
        for g in games:
            home = g.get('home_team','')
            away = g.get('away_team','')
            has_dk = any(b['key']=='draftkings' for b in g.get('bookmakers',[]))
            print(f"  {away} @ {home}  {'[DK]' if has_dk else '[no DK]'}")

# Test a known bad date
print("=== 2016-04-02 (Final Four) ===")
fetch('2016-04-02')
print()
print("=== 2023-03-16 (NCAA tournament) ===")
fetch('2023-03-16')
