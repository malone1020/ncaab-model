import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Sample snapshot_date values from both old and new rows
print("Sample snapshot_date values from torvik_daily:")
rows = conn.execute("SELECT snapshot_date, length(snapshot_date), typeof(snapshot_date) FROM torvik_daily LIMIT 20").fetchall()
for r in rows:
    print(f"  value={repr(r[0])}  len={r[1]}  type={r[2]}")

print()
print("Distinct formats (by length):")
for r in conn.execute("SELECT length(snapshot_date), typeof(snapshot_date), COUNT(*), MIN(snapshot_date), MAX(snapshot_date) FROM torvik_daily GROUP BY length(snapshot_date), typeof(snapshot_date)"):
    print(f"  len={r[0]} type={r[1]} count={r[2]:,} min={r[3]} max={r[4]}")

conn.close()
