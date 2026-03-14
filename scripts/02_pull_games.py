"""
Script 02: Pull Historical NCAAB Game Results
==============================================
Uses the CollegeBasketballData.com API to pull game results
for seasons 2016-2025 and store them in the games table.

API docs: https://api.collegebasketballdata.com
Requires: CBBD_API_KEY in .env file

Usage:
    python scripts/02_pull_games.py
"""

import sqlite3
import requests
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

# ── Config ─────────────────────────────────────────────────────────────────────
API_KEY     = os.getenv("CBBD_API_KEY")
BASE_URL    = "https://api.collegebasketballdata.com"
START_SEASON = 2016
END_SEASON   = 2026
SLEEP_SEC    = 1.5   # be polite between requests

HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Season types to pull
SEASON_TYPES = ["regular", "postseason"]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_games(season: int, season_type: str) -> list:
    """Fetch all games for a given season and type from CBBD API."""
    url    = f"{BASE_URL}/games"
    params = {
        "season"     : season,
        "seasonType" : season_type,
    }
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            print(f"  ⚠  Rate limited — sleeping 30s")
            time.sleep(30)
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            return r.json() if r.status_code == 200 else []
        else:
            print(f"  ⚠  API error {r.status_code}: {r.text[:200]}")
            return []
    except Exception as e:
        print(f"  ⚠  Request failed: {e}")
        return []


def parse_game(g: dict) -> dict | None:
    """
    Extract the fields we need from a CBBD game object.
    Returns None if the game isn't complete/usable.
    """
    # Skip unfinished games
    if g.get("status") != "final":
        return None
    if g.get("homePoints") is None or g.get("awayPoints") is None:
        return None

    # Parse date (ISO format → YYYY-MM-DD)
    raw_date = g.get("startDate", "")
    game_date = raw_date[:10] if raw_date else None

    # Determine season year from API field
    season = g.get("season")

    # Tournament / game type
    tournament = None
    season_type = g.get("seasonType", "")
    if season_type == "postseason":
        tourney_name = (g.get("tournament") or "").lower()
        if "ncaa" in tourney_name or "march madness" in tourney_name:
            tournament = "ncaa_tournament"
        elif tourney_name:
            tournament = "conf_tournament"
        else:
            tournament = "postseason"

    return {
        "cbbd_id"     : g.get("id"),
        "season"      : season,
        "game_date"   : game_date,
        "home_team"   : g.get("homeTeam"),
        "away_team"   : g.get("awayTeam"),
        "home_score"  : g.get("homePoints"),
        "away_score"  : g.get("awayPoints"),
        "home_conf"   : g.get("homeConference"),
        "away_conf"   : g.get("awayConference"),
        "neutral_site": 1 if g.get("neutralSite") else 0,
        "conf_game"   : 1 if g.get("conferenceGame") else 0,
        "tournament"  : tournament,
        "season_type" : season_type,
        "attendance"  : g.get("attendance"),
        "excitement"  : g.get("excitement"),
        "home_elo_start": g.get("homeTeamEloStart"),
        "home_elo_end"  : g.get("homeTeamEloEnd"),
        "away_elo_start": g.get("awayTeamEloStart"),
        "away_elo_end"  : g.get("awayTeamEloEnd"),
        "venue"       : g.get("venue"),
        "city"        : g.get("city"),
        "state"       : g.get("state"),
    }


def ensure_schema(conn):
    """Add CBBD-specific columns to games table if not present."""
    c = conn.cursor()

    # Drop and recreate with full schema for clean import
    c.execute("DROP TABLE IF EXISTS games")
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cbbd_id       INTEGER UNIQUE,
            season        INTEGER NOT NULL,
            game_date     TEXT,
            home_team     TEXT    NOT NULL,
            away_team     TEXT    NOT NULL,
            home_score    INTEGER,
            away_score    INTEGER,
            home_conf     TEXT,
            away_conf     TEXT,
            neutral_site  INTEGER DEFAULT 0,
            conf_game     INTEGER DEFAULT 0,
            tournament    TEXT,
            season_type   TEXT,
            attendance    INTEGER,
            excitement    REAL,
            home_elo_start REAL,
            home_elo_end   REAL,
            away_elo_start REAL,
            away_elo_end   REAL,
            venue         TEXT,
            city          TEXT,
            state         TEXT,
            source        TEXT DEFAULT 'cbbd'
        )
    """)
    conn.commit()
    print("✓ games table ready")


def insert_games(conn, games: list) -> tuple:
    """Insert parsed game dicts, return (inserted, skipped)."""
    c = conn.cursor()
    inserted = skipped = 0

    for g in games:
        if g is None:
            continue
        try:
            c.execute("""
                INSERT OR IGNORE INTO games (
                    cbbd_id, season, game_date,
                    home_team, away_team, home_score, away_score,
                    home_conf, away_conf,
                    neutral_site, conf_game, tournament, season_type,
                    attendance, excitement,
                    home_elo_start, home_elo_end,
                    away_elo_start, away_elo_end,
                    venue, city, state, source
                ) VALUES (
                    :cbbd_id, :season, :game_date,
                    :home_team, :away_team, :home_score, :away_score,
                    :home_conf, :away_conf,
                    :neutral_site, :conf_game, :tournament, :season_type,
                    :attendance, :excitement,
                    :home_elo_start, :home_elo_end,
                    :away_elo_start, :away_elo_end,
                    :venue, :city, :state, 'cbbd'
                )
            """, g)
            if c.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  Insert error: {e} — {g.get('home_team')} vs {g.get('away_team')}")

    conn.commit()
    return inserted, skipped


def print_summary(conn):
    c = conn.cursor()
    c.execute("""
        SELECT season,
               COUNT(*) as total,
               SUM(CASE WHEN neutral_site=1 THEN 1 ELSE 0 END) as neutral,
               SUM(CASE WHEN season_type='postseason' THEN 1 ELSE 0 END) as postseason
        FROM games
        GROUP BY season ORDER BY season
    """)
    rows = c.fetchall()
    if rows:
        print("\n── games table summary ───────────────────────────────")
        print(f"   {'Season':<8} {'Total':>7} {'Neutral':>8} {'Post':>6}")
        print(f"   {'-'*8} {'-'*7} {'-'*8} {'-'*6}")
        for r in rows:
            print(f"   {r[0]:<8} {r[1]:>7} {r[2]:>8} {r[3]:>6}")
        print(f"\n   Total: {sum(r[1] for r in rows):,} games")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if not API_KEY:
        print("⚠  CBBD_API_KEY not found in .env file")
        return

    print(f"Database : {DB_PATH}")
    print(f"Seasons  : {START_SEASON}–{END_SEASON}")
    print(f"API key  : {API_KEY[:8]}...")
    print()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_schema(conn)
    print()

    grand_total = 0

    for season in range(START_SEASON, END_SEASON + 1):
        print(f"── Season {season} ──────────────────────────────────────")
        season_total = 0

        for stype in SEASON_TYPES:
            raw_games = fetch_games(season, stype)
            parsed    = [parse_game(g) for g in raw_games]
            parsed    = [g for g in parsed if g is not None]

            ins, skp = insert_games(conn, parsed)
            season_total += ins
            print(f"  {stype:<12}  fetched: {len(raw_games):4d}  "
                  f"inserted: {ins:4d}  skipped: {skp:4d}")
            time.sleep(SLEEP_SEC)

        print(f"  Season total: {season_total:,} new games")
        grand_total += season_total

    print(f"\n{'='*55}")
    print(f"  Grand total inserted: {grand_total:,}")
    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
