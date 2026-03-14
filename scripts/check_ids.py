import sqlite3
conn = sqlite3.connect('data/basketball.db')
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print('Tables:', [t[0] for t in tables])
print()
print('game_lines cols:', [c[1] for c in conn.execute('PRAGMA table_info(game_lines)').fetchall()])
print()
rows = conn.execute("SELECT * FROM games WHERE game_date > '2025-01-01' LIMIT 2").fetchall()
cols = [c[1] for c in conn.execute('PRAGMA table_info(games)').fetchall()]
for r in rows:
    print(dict(zip(cols, r)))
