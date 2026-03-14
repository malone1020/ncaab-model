import requests, json

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Test the core events API for a known busy date
url = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/events"
params = {'dates': '20241123', 'limit': 300}
r = requests.get(url, headers=HEADERS, params=params, timeout=15)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    print(f"Count: {data.get('count')}")
    print(f"PageCount: {data.get('pageCount')}")
    items = data.get('items', [])
    print(f"Items returned: {len(items)}")
    if items:
        print(f"Sample item: {items[0]}")
