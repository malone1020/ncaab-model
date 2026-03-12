"""Probe KenPom API to find correct base URL and auth format."""
import requests

API_KEY = 'b59dbfcaa5224d02cea409c73299040637a4d3445da5def3d1f320e313da1571'

# Try different base URLs and auth formats
attempts = [
    # (url, headers, params, label)
    ('https://kenpom.com/api/ratings',         {'Authorization': f'Bearer {API_KEY}'}, {}, 'bearer /api/ratings'),
    ('https://kenpom.com/api/ratings',         {'x-api-key': API_KEY},                {}, 'x-api-key /api/ratings'),
    ('https://kenpom.com/api/v1/ratings',      {'Authorization': f'Bearer {API_KEY}'}, {}, 'bearer /api/v1/ratings'),
    ('https://api.kenpom.com/ratings',         {'Authorization': f'Bearer {API_KEY}'}, {}, 'bearer api.kenpom.com'),
    ('https://kenpom.com/api/ratings',         {'Authorization': f'Bearer {API_KEY}'}, {'season': '2024'}, 'bearer + season param'),
    ('https://kenpom.com/api/archive/ratings', {'Authorization': f'Bearer {API_KEY}'}, {'date': '2024-01-15'}, 'bearer /api/archive/ratings'),
    ('https://kenpom.com/api/archive',         {'Authorization': f'Bearer {API_KEY}'}, {'date': '2024-01-15'}, 'bearer /api/archive'),
    ('https://kenpom.com/api/teams',           {'Authorization': f'Bearer {API_KEY}'}, {}, 'bearer /api/teams'),
    ('https://kenpom.com/api/conferences',     {'Authorization': f'Bearer {API_KEY}'}, {}, 'bearer /api/conferences'),
]

for url, headers, params, label in attempts:
    try:
        r = requests.get(url, headers=headers, params=params, timeout=8)
        print(f"[{r.status_code}] {label}")
        if r.status_code == 200:
            print(f"  SUCCESS! Content-type: {r.headers.get('content-type','?')}")
            print(f"  Body[:200]: {r.text[:200]}")
            break
        elif r.status_code not in (403, 404, 401):
            print(f"  Body[:200]: {r.text[:200]}")
    except Exception as e:
        print(f"[ERR] {label}: {e}")
