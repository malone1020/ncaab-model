import sqlite3, os, pandas as pd
DB = os.path.join(os.getcwd(), "data", "basketball.db")
conn = sqlite3.connect(DB)

print("=== referee_game schema ===")
cols = conn.execute("PRAGMA table_info(referee_game)").fetchall()
for c in cols: print(f"  {c}")

print("\n=== row count ===")
n = conn.execute("SELECT COUNT(*) FROM referee_game").fetchone()[0]
print(f"  {n:,} rows")

print("\n=== sample rows ===")
rows = conn.execute("SELECT * FROM referee_game LIMIT 5").fetchall()
for r in rows: print(f"  {r}")

# Try to understand join to games
print("\n=== can we join to games? ===")
try:
    sample = conn.execute("""
        SELECT rg.*, g.home_team, g.away_team, g.season
        FROM referee_game rg
        JOIN games g ON g.id = rg.game_id
        LIMIT 3
    """).fetchall()
    for r in sample: print(f"  {r}")
except Exception as e:
    print(f"  join error: {e}")
    # Try other join cols
    try:
        sample = conn.execute("""
            SELECT rg.*, g.home_team, g.away_team
            FROM referee_game rg
            JOIN games g ON g.id = rg.game_id
            LIMIT 3
        """).fetchall()
    except Exception as e2:
        print(f"  alt join error: {e2}")

conn.close()
