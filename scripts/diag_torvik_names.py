import sqlite3
from difflib import get_close_matches

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

daily_teams = [t[0] for t in conn.execute("SELECT DISTINCT team FROM torvik_daily ORDER BY team")]
print(f"All {len(daily_teams)} Torvik daily team names:")
print()
# Search for specific ones
lookups = [
    "Omaha", "Nebraska", "UMKC", "Kansas City",
    "IUPUI", "IU Indy", "Indianapolis",
    "Seattle",
    "USC Upstate", "South Carolina Up",
    "FIU", "Florida Inter",
    "Illinois-Chicago", "UIC",
    "LIU", "Long Island",
    "Tennessee-Martin", "UT Martin",
    "Penn", "Pennsylvania",
    "Grambling",
    "Southern U",
    "Mississippi Val",
    "Loyola MD", "Loyola Mar",
    "Saint Francis", "St. Francis",
    "Albany",
    "Maryland-Eastern",
    "San Jose",
    "Bethune",
    "Hawai",
    "Gardner",
    "Kennesaw"
]
for q in lookups:
    matches = [t for t in daily_teams if q.lower() in t.lower()]
    if matches:
        print(f"  {q!r}: {matches}")

print()
print("Full list sample (every 5th):")
for i, t in enumerate(daily_teams):
    if i % 5 == 0:
        print(f"  {t}")
conn.close()
