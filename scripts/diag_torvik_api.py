"""Quick check: what does the Torvik trankings API return to Python's requests?"""
import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research"}

url = "https://barttorvik.com/trankings.php?date=20240115&top=0&conlimit=All&year=2024&json=1"
try:
    r = requests.get(url, timeout=30, headers=HEADERS)
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('content-type', 'unknown')}")
    print(f"Response length: {len(r.text)}")
    print(f"First 300 chars:\n{r.text[:300]}")
    print()
    
    # Try to parse as JSON
    try:
        data = r.json()
        print(f"JSON parsed OK. Type: {type(data)}, Length: {len(data)}")
        if isinstance(data, list) and len(data) > 0:
            first = data[0]
            print(f"First entry type: {type(first)}, Length: {len(first) if hasattr(first, '__len__') else 'N/A'}")
            print(f"First entry: {first}")
            print()
            # Find Duke
            for item in data[:10]:
                if isinstance(item, (list, tuple)) and len(item) > 0:
                    print(f"  [0]={item[0]!r:25s} all values: {[v for v in item[:18]]}")
    except Exception as e:
        print(f"JSON parse failed: {e}")
        print(f"Raw text: {r.text[:500]}")
        
except Exception as e:
    print(f"Request failed: {e}")
