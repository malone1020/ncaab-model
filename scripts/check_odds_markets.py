"""Check what markets OddsAPI has for NCAAB."""
import os, requests
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
except: pass

ODDS_KEY = os.getenv('ODDS_API_KEY', '')

# First check available markets for NCAAB
r = requests.get(
    "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/",
    params={
        'apiKey': ODDS_KEY,
        'regions': 'us',
        'markets': 'spreads,totals,h2h,alternate_spreads,alternate_totals,player_props',
        'bookmakers': 'draftkings',
        'oddsFormat': 'american',
    },
    timeout=15
)
print(f"HTTP {r.status_code} | remaining: {r.headers.get('X-Requests-Remaining','?')}")

if r.status_code == 200:
    data = r.json()
    print(f"Games returned: {len(data)}")
    if data:
        g = data[0]
        print(f"\nSample game: {g['away_team']} @ {g['home_team']}")
        for book in g.get('bookmakers', []):
            if book['key'] == 'draftkings':
                print(f"DraftKings markets available:")
                for mkt in book.get('markets', []):
                    print(f"  {mkt['key']}: {len(mkt.get('outcomes',[]))} outcomes")
                    if mkt['key'] not in ('spreads','totals','h2h'):
                        for oc in mkt.get('outcomes', [])[:3]:
                            print(f"    {oc}")
elif r.status_code == 422:
    print(f"422 error — checking valid market keys...")
    # Try each market individually
    for mkt in ['spreads','totals','h2h','team_totals','player_props',
                'alternate_spreads','alternate_totals','btts','draw_no_bet']:
        r2 = requests.get(
            "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/",
            params={'apiKey': ODDS_KEY, 'regions': 'us',
                   'markets': mkt, 'bookmakers': 'draftkings'},
            timeout=10
        )
        status = "✓" if r2.status_code == 200 else f"✗ {r2.status_code}"
        remaining = r2.headers.get('X-Requests-Remaining','?')
        print(f"  {mkt:<25} {status} | remaining={remaining}")
        if r2.status_code == 200 and r2.json():
            g = r2.json()[0]
            for book in g.get('bookmakers',[]):
                if book['key'] == 'draftkings' and book.get('markets'):
                    print(f"    → {len(book['markets'][0].get('outcomes',[]))} outcomes")
                    for oc in book['markets'][0].get('outcomes',[])[:2]:
                        print(f"       {oc}")
        import time; time.sleep(0.3)
