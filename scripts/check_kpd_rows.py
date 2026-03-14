import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)
print("kenpom_daily rows by season:")
for r in conn.execute("SELECT season, COUNT(*) FROM kenpom_daily GROUP BY season ORDER BY season").fetchall():
    print(f"  {r}")
print(f"\nTotal: {conn.execute('SELECT COUNT(*) FROM kenpom_daily').fetchone()[0]:,}")
conn.close()
