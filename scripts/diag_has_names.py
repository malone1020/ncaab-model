import sqlite3

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

import sys
sys.path.insert(0, ".")

# Load maps from 04
with open("04_build_features.py") as f:
    content = f.read()

import re
m = re.search(r"CBBD_TO_TORVIK = \{(.*?)\n\}", content, re.DOTALL)
exec("CBBD_TO_TORVIK = {" + m.group(1) + "}")
def norm(name):
    if not name: return name
    return CBBD_TO_TORVIK.get(str(name).strip(), str(name).strip())

has_teams = [t[0] for t in conn.execute("SELECT DISTINCT team FROM haslametrics ORDER BY team")]
daily_teams = set(t[0] for t in conn.execute("SELECT DISTINCT team FROM torvik_daily"))

print(f"Haslametrics unique teams: {len(has_teams)}")
print(f"Torvik daily unique teams: {len(daily_teams)}")
print()

missing = []
for t in has_teams:
    normed = norm(t)
    if normed not in daily_teams:
        missing.append((t, normed))

print(f"Haslametrics teams NOT in Torvik daily after norm(): {len(missing)}")
for t, n in missing[:60]:
    if t == n:
        print(f"  NO MAP: {t!r}")
    else:
        print(f"  WRONG:  {t!r} -> {n!r}")

conn.close()
