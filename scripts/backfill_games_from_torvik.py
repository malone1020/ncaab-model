"""
backfill_games_from_torvik.py
==============================
Backfill the games table using torvik_game_preds for games
that CBBD's 3000-game cap missed (Jan 7 onwards for 2026).

torvik_game_preds has: game_date, home_team, away_team,
actual_home, actual_away — enough to populate games table.
neutral_site and conf_game will be NULL and updated by
03d_fetch_espn_ids.py / daily update later.

Run: python scripts/backfill_games_from_torvik.py
     python scripts/backfill_games_from_torvik.py --dry-run
"""
import sqlite3, os, argparse
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

def season_for_date(d):
    from datetime import date
    if isinstance(d, str): d = date.fromisoformat(d[:10])
    return d.year + 1 if d.month >= 11 else d.year

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--season', type=int, default=2026)
    args = p.parse_args()

    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    # Find games in torvik_preds not in games table
    rows = conn.execute("""
        SELECT tp.game_date, tp.home_team, tp.away_team,
               tp.actual_home, tp.actual_away, tp.season
        FROM torvik_game_preds tp
        LEFT JOIN games g ON tp.game_date=g.game_date
            AND tp.home_team=g.home_team AND tp.away_team=g.away_team
        WHERE tp.season=?
          AND tp.actual_home IS NOT NULL
          AND tp.actual_away IS NOT NULL
          AND g.game_date IS NULL
        ORDER BY tp.game_date
    """, (args.season,)).fetchall()

    print(f"Games to backfill for {args.season}: {len(rows):,}")
    if rows:
        dates = sorted(set(r[0] for r in rows))
        print(f"Date range: {dates[0]} to {dates[-1]}")

    if args.dry_run:
        print("DRY RUN — no changes made")
        conn.close()
        exit(0)

    inserted = 0
    errors = 0
    for game_date, home, away, home_score, away_score, season in rows:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO games
                (season, game_date, home_team, away_team,
                 home_score, away_score, season_type)
                VALUES (?,?,?,?,?,?,'regular')
            """, (season, game_date, home, away, home_score, away_score))
            inserted += 1
        except Exception as e:
            errors += 1

    conn.commit()
    conn.close()

    print(f"Inserted {inserted:,} games | Errors: {errors}")
    print(f"\nNext: python scripts/01_daily_update.py --since 2026-01-07")
