import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

total = conn.execute("SELECT COUNT(*) FROM games WHERE season=2025").fetchone()[0]
with_espn = conn.execute("SELECT COUNT(*) FROM games WHERE season=2025 AND espn_id IS NOT NULL AND espn_id != ''").fetchone()[0]
with_lines = conn.execute("""
    SELECT COUNT(*) FROM games g
    JOIN game_lines gl ON g.game_date=gl.game_date
        AND g.home_team=gl.home_team AND g.away_team=gl.away_team
    WHERE g.season=2025
""").fetchone()[0]
with_lines_and_espn = conn.execute("""
    SELECT COUNT(*) FROM games g
    JOIN game_lines gl ON g.game_date=gl.game_date
        AND g.home_team=gl.home_team AND g.away_team=gl.away_team
    WHERE g.season=2025 AND g.espn_id IS NOT NULL AND g.espn_id != ''
""").fetchone()[0]

print(f"Total 2025 games:              {total:,}")
print(f"Games with ESPN ID:            {with_espn:,} ({with_espn/total*100:.0f}%)")
print(f"Games with DK lines:           {with_lines:,}")
print(f"Games with DK lines + ESPN ID: {with_lines_and_espn:,} ({with_lines_and_espn/max(with_lines,1)*100:.0f}% of bettable games)")
conn.close()
