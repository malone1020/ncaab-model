"""Check torvik_game_preds coverage for missing games."""
import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

r = conn.execute("""
    SELECT MIN(game_date), MAX(game_date), COUNT(*)
    FROM torvik_game_preds WHERE season=2026
""").fetchone()
print(f"torvik_preds 2026: {r[0]} to {r[1]} ({r[2]:,} games)")

# How many post-Jan-6 games in torvik_preds but missing from games table
missing = conn.execute("""
    SELECT COUNT(*) FROM torvik_game_preds tp
    LEFT JOIN games g ON tp.game_date=g.game_date
        AND tp.home_team=g.home_team AND tp.away_team=g.away_team
    WHERE tp.season=2026 AND tp.game_date > '2026-01-06'
    AND tp.actual_home IS NOT NULL
    AND g.game_date IS NULL
""").fetchone()[0]
print(f"Missing from games table (post Jan-6, with scores): {missing:,}")

# Sample
rows = conn.execute("""
    SELECT tp.game_date, tp.home_team, tp.away_team, tp.actual_home, tp.actual_away
    FROM torvik_game_preds tp
    LEFT JOIN games g ON tp.game_date=g.game_date
        AND tp.home_team=g.home_team AND tp.away_team=g.away_team
    WHERE tp.season=2026 AND tp.game_date > '2026-01-06'
    AND tp.actual_home IS NOT NULL AND g.game_date IS NULL
    LIMIT 5
""").fetchall()
print("\nSample missing games:")
for r in rows: print(f"  {r}")
conn.close()
