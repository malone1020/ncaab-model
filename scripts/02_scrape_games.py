"""
Script 02: Pull Historical NCAAB Game Results (Direct Scraper)
==============================================================
Scrapes game results directly from Sports Reference box score index pages.
Does NOT use the sportsreference package (unreliable for historical data).

Strategy:
  - For each season, iterate through every date that had NCAAB games
  - Pull the box score index page for that date
  - Extract game results (teams, scores, location)
  - Store in the games table

This is the most reliable way to get complete game logs.
Expect ~30-60 minutes for a full 10-season run.

Usage:
    python scripts/02_scrape_games.py

CONFIG section below lets you control date range and speed.
"""

import sqlite3
import requests
import pandas as pd
import time
import re
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
START_SEASON  = 2016    # Pull from 2015-16 season onward
END_SEASON    = 2025    # Last completed season
SLEEP_SECONDS = 4       # Be polite to Sports Reference (don't go below 3)
TEST_MODE     = False   # Set True to only pull 2 weeks of one season for testing

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# College basketball season date ranges (approximate)
# Season runs from early November through early April
def get_season_dates(season):
    """
    Return (start_date, end_date) for a given season.
    'season' means the year the season ends (e.g. 2023 = 2022-23 season).
    """
    start = datetime(season - 1, 11, 1)   # Nov 1 of prior year
    end   = datetime(season,     4, 10)   # Apr 10 of season year
    return start, end


# ══════════════════════════════════════════════════════════════════════════════
# SCRAPING
# ══════════════════════════════════════════════════════════════════════════════

def fetch_games_for_date(date: datetime, session: requests.Session):
    """
    Fetch all NCAAB games played on a given date from Sports Reference.
    Returns list of dicts with game data.
    """
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://www.sports-reference.com/cbb/boxscores/index.cgi?date={date_str}"

    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 429:
            print(f"    ⚠  Rate limited on {date_str} — sleeping 60s")
            time.sleep(60)
            resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
    except Exception as e:
        print(f"    ⚠  Request failed for {date_str}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    games = []

    # Each game is in a div.game_summary
    game_divs = soup.find_all("div", class_="game_summary")

    for div in game_divs:
        try:
            # Get the table rows — winner and loser rows
            table = div.find("table", class_="teams")
            if not table:
                continue

            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Row 0 = away team (or loser in some formats)
            # Row 1 = home team (or winner)
            # Sports Reference lists away team first, home team second
            away_row = rows[0]
            home_row = rows[1]

            away_td = away_row.find_all("td")
            home_td = home_row.find_all("td")

            if not away_td or not home_td:
                continue

            away_team  = away_td[0].get_text(strip=True)
            home_team  = home_td[0].get_text(strip=True)

            # Scores — may be empty if game not yet played
            try:
                away_score = int(away_td[1].get_text(strip=True))
                home_score = int(home_td[1].get_text(strip=True))
            except (ValueError, IndexError):
                continue  # Skip unplayed games

            # Check for neutral site indicator
            # Sports Reference shows "(N)" or similar in the game notes
            neutral = 0
            game_note = div.get_text()
            if "(N)" in game_note or "Neutral" in game_note:
                neutral = 1

            # Tournament detection based on date
            tournament = None
            if date.month == 3 and date.day <= 17:
                tournament = "conf_tournament"
            elif (date.month == 3 and date.day > 17) or date.month == 4:
                tournament = "ncaa_tournament"

            games.append({
                "game_date"  : date_str,
                "home_team"  : home_team,
                "away_team"  : away_team,
                "home_score" : home_score,
                "away_score" : away_score,
                "neutral_site": neutral,
                "tournament" : tournament,
            })

        except Exception:
            continue

    return games


def get_season_for_date(date_str):
    """Map game date to season year."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.year + 1 if dt.month >= 11 else dt.year
    except Exception:
        return None


def insert_games(conn, games):
    """Insert list of game dicts, return (inserted, skipped) counts."""
    c = conn.cursor()
    inserted = skipped = 0
    for g in games:
        season = get_season_for_date(g["game_date"])
        try:
            c.execute("""
                INSERT OR IGNORE INTO games
                    (season, game_date, home_team, away_team,
                     home_score, away_score, neutral_site, tournament, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'sports_reference')
            """, (
                season,
                g["game_date"],
                g["home_team"],
                g["away_team"],
                g["home_score"],
                g["away_score"],
                g["neutral_site"],
                g["tournament"],
            ))
            if c.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"    Insert error: {e}")
    conn.commit()
    return inserted, skipped


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def scrape_season(conn, season, session, sleep_seconds, test_mode=False):
    start_date, end_date = get_season_dates(season)

    if test_mode:
        # Only pull 2 weeks for testing
        end_date = start_date + timedelta(days=14)

    total_days     = (end_date - start_date).days + 1
    days_with_games = 0
    total_inserted = 0
    total_skipped  = 0

    print(f"\n  Dates: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    print(f"  Total days to check: {total_days}")

    current = start_date
    day_num = 0

    while current <= end_date:
        day_num += 1
        games = fetch_games_for_date(current, session)

        if games:
            ins, skp = insert_games(conn, games)
            total_inserted += ins
            total_skipped  += skp
            days_with_games += 1

            # Print progress on days with games
            print(f"    {current.strftime('%Y-%m-%d')}  "
                  f"{len(games):3d} games found  "
                  f"+{ins} inserted")

        elif day_num % 30 == 0:
            # Periodic heartbeat even on empty days
            print(f"    {current.strftime('%Y-%m-%d')}  (no games)")

        current += timedelta(days=1)
        time.sleep(sleep_seconds)

    print(f"\n  Season {season} complete:")
    print(f"    Days with games : {days_with_games}")
    print(f"    Inserted        : {total_inserted}")
    print(f"    Skipped         : {total_skipped} (duplicates)")
    return total_inserted


def print_summary(conn):
    c = conn.cursor()
    c.execute("""
        SELECT season,
               COUNT(*) as total,
               SUM(CASE WHEN neutral_site=1 THEN 1 ELSE 0 END) as neutral,
               SUM(CASE WHEN tournament='conf_tournament' THEN 1 ELSE 0 END) as conf_t,
               SUM(CASE WHEN tournament='ncaa_tournament' THEN 1 ELSE 0 END) as ncaa_t
        FROM games
        GROUP BY season ORDER BY season
    """)
    rows = c.fetchall()
    if rows:
        print("\n── Games table summary ───────────────────────────────────")
        print(f"   {'Season':<8} {'Total':>7} {'Neutral':>8} {'ConfT':>7} {'NCAAT':>7}")
        print(f"   {'-'*8} {'-'*7} {'-'*8} {'-'*7} {'-'*7}")
        for r in rows:
            print(f"   {r[0]:<8} {r[1]:>7} {r[2]:>8} {r[3]:>7} {r[4]:>7}")
        print(f"\n   Total: {sum(r[1] for r in rows):,} games")


def main():
    print(f"Database  : {DB_PATH}")
    print(f"Seasons   : {START_SEASON}–{END_SEASON}")
    print(f"Sleep     : {SLEEP_SECONDS}s per day")
    if TEST_MODE:
        print("TEST MODE : only pulling 2 weeks of first season")
    print()

    conn    = sqlite3.connect(DB_PATH)
    session = requests.Session()

    # Show existing game count
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM games")
    existing = c.fetchone()[0]
    if existing:
        print(f"ℹ  {existing:,} games already in DB — duplicates will be skipped\n")

    grand_total = 0
    wall_start  = time.time()

    seasons = [START_SEASON] if TEST_MODE else range(START_SEASON, END_SEASON + 1)

    for season in seasons:
        print(f"\n{'='*60}")
        print(f"  Season {season}")
        print(f"{'='*60}")
        n = scrape_season(conn, season, session, SLEEP_SECONDS, TEST_MODE)
        grand_total += n

    elapsed = time.time() - wall_start
    print(f"\n{'='*60}")
    print(f"  Done in {elapsed/60:.1f} minutes")
    print(f"  Total new games inserted: {grand_total:,}")

    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
