"""
Script 03b: Fix game_team_stats — pull by team to bypass API row limit
=======================================================================
Gets team list from local games table, filtered to D1 teams only (10+ games).
Resumable — skips teams already in game_team_stats for that season.

Usage:
    python scripts/03b_fix_stats_pagination.py
"""

import sqlite3
import requests
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR     = Path(__file__).resolve().parent.parent
DB_PATH      = BASE_DIR / "data" / "basketball.db"
API_KEY      = os.getenv("CBBD_API_KEY")
BASE_URL     = "https://api.collegebasketballdata.com"
START_SEASON = 2016
END_SEASON   = 2025
SLEEP_SEC    = 0.8
HEADERS      = {"Authorization": f"Bearer {API_KEY}"}

COLS = [
    "game_id", "season", "game_date", "team", "opponent",
    "is_home", "neutral_site", "conf_game", "season_type",
    "pace", "possessions", "game_minutes", "true_shooting",
    "points", "assists", "steals", "blocks", "turnovers",
    "total_rebounds", "off_rebounds", "def_rebounds", "fouls",
    "fg_made", "fg_att", "fg_pct",
    "three_made", "three_att", "three_pct",
    "ft_made", "ft_att", "ft_pct",
    "efg_pct", "tov_pct", "orb_pct", "ft_rate",
    "rating", "game_score",
]
SQL = (f"INSERT OR IGNORE INTO game_team_stats ({', '.join(COLS)}) "
       f"VALUES ({', '.join([':'+c for c in COLS])})")


def ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS game_team_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         INTEGER,
            season          INTEGER,
            game_date       TEXT,
            team            TEXT,
            opponent        TEXT,
            is_home         INTEGER,
            neutral_site    INTEGER,
            conf_game       INTEGER,
            season_type     TEXT,
            pace            REAL,
            possessions     INTEGER,
            game_minutes    INTEGER,
            true_shooting   REAL,
            points          INTEGER,
            assists         INTEGER,
            steals          INTEGER,
            blocks          INTEGER,
            turnovers       INTEGER,
            total_rebounds  INTEGER,
            off_rebounds    INTEGER,
            def_rebounds    INTEGER,
            fouls           INTEGER,
            fg_made         INTEGER,
            fg_att          INTEGER,
            fg_pct          REAL,
            three_made      INTEGER,
            three_att       INTEGER,
            three_pct       REAL,
            ft_made         INTEGER,
            ft_att          INTEGER,
            ft_pct          REAL,
            efg_pct         REAL,
            tov_pct         REAL,
            orb_pct         REAL,
            ft_rate         REAL,
            rating          REAL,
            game_score      REAL,
            UNIQUE(game_id, team)
        )
    """)
    conn.commit()


def get_teams_from_db(conn, season):
    """Only return teams that appear 10+ times — filters out non-D1 opponents."""
    c = conn.cursor()
    c.execute("""
        SELECT team, COUNT(*) as games FROM (
            SELECT home_team as team FROM games WHERE season = ?
            UNION ALL
            SELECT away_team as team FROM games WHERE season = ?
        )
        GROUP BY team
        HAVING games >= 10
        ORDER BY team
    """, (season, season))
    return [r[0] for r in c.fetchall()]


def already_pulled(conn, season, team):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM game_team_stats WHERE season=? AND team=?",
              (season, team))
    return c.fetchone()[0] > 0


def parse_team_stats(g):
    ts  = g.get("teamStats") or {}
    pts = ts.get("points")               or {}
    fg  = ts.get("fieldGoals")           or {}
    tp  = ts.get("threePointFieldGoals") or {}
    ft  = ts.get("freeThrows")           or {}
    rb  = ts.get("rebounds")             or {}
    tov = ts.get("turnovers")            or {}
    fls = ts.get("fouls")                or {}
    ff  = ts.get("fourFactors")          or {}
    return {
        "game_id"       : g.get("gameId"),
        "season"        : g.get("season"),
        "game_date"     : (g.get("startDate") or "")[:10],
        "team"          : g.get("team"),
        "opponent"      : g.get("opponent"),
        "is_home"       : 1 if g.get("isHome") else 0,
        "neutral_site"  : 1 if g.get("neutralSite") else 0,
        "conf_game"     : 1 if g.get("conferenceGame") else 0,
        "season_type"   : g.get("seasonType"),
        "pace"          : g.get("pace"),
        "possessions"   : g.get("possessions"),
        "game_minutes"  : g.get("gameMinutes"),
        "true_shooting" : g.get("trueShooting"),
        "points"        : pts.get("total"),
        "assists"       : ts.get("assists"),
        "steals"        : ts.get("steals"),
        "blocks"        : ts.get("blocks"),
        "turnovers"     : tov.get("total"),
        "total_rebounds": rb.get("total"),
        "off_rebounds"  : rb.get("offensive"),
        "def_rebounds"  : rb.get("defensive"),
        "fouls"         : fls.get("total"),
        "fg_made"       : fg.get("made"),
        "fg_att"        : fg.get("attempted"),
        "fg_pct"        : fg.get("pct"),
        "three_made"    : tp.get("made"),
        "three_att"     : tp.get("attempted"),
        "three_pct"     : tp.get("pct"),
        "ft_made"       : ft.get("made"),
        "ft_att"        : ft.get("attempted"),
        "ft_pct"        : ft.get("pct"),
        "efg_pct"       : ff.get("effectiveFieldGoalPct"),
        "tov_pct"       : ff.get("turnoverRatio"),
        "orb_pct"       : ff.get("offensiveReboundPct"),
        "ft_rate"       : ff.get("freeThrowRate"),
        "rating"        : g.get("rating"),
        "game_score"    : g.get("gameScore"),
    }


def fetch_team_season(season, team, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(
                f"{BASE_URL}/games/teams",
                headers=HEADERS,
                params={"season": season, "team": team},
                timeout=20
            )
            if r.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"\n  Rate limited — sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                return r.json()
            print(f"\n  {r.status_code} for {team} {season}")
            return []
        except Exception as e:
            print(f"\n  Error ({team} {season}): {e}")
            time.sleep(3)
    return []


def insert_rows(conn, rows):
    c = conn.cursor()
    inserted = 0
    for row in rows:
        try:
            c.execute(SQL, row)
            inserted += c.rowcount
        except Exception:
            pass
    conn.commit()
    return inserted


def main():
    if not API_KEY:
        print("CBBD_API_KEY not found in .env")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    grand_total = 0

    for season in range(START_SEASON, END_SEASON + 1):
        teams = get_teams_from_db(conn, season)
        if not teams:
            print(f"{season}: no teams in games table")
            continue

        todo = [t for t in teams if not already_pulled(conn, season, t)]
        done = len(teams) - len(todo)
        print(f"\n── {season} ── {len(teams)} teams ({done} already done, {len(todo)} to pull)")

        season_new = 0
        for i, team in enumerate(todo):
            raw  = fetch_team_season(season, team)
            rows = [parse_team_stats(g) for g in raw]
            ins  = insert_rows(conn, rows)
            season_new  += ins
            grand_total += ins

            if (i + 1) % 50 == 0:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM game_team_stats WHERE season=?", (season,))
                print(f"  [{i+1}/{len(todo)}] {c.fetchone()[0]:,} rows in DB")

            time.sleep(SLEEP_SEC)

        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM game_team_stats WHERE season=?", (season,))
        print(f"  ✓ {season}: {c.fetchone()[0]:,} total (+{season_new} new)")

    print("\n── Final counts ───────────────────────")
    c = conn.cursor()
    c.execute("SELECT season, COUNT(*) FROM game_team_stats GROUP BY season ORDER BY season")
    for r in c.fetchall():
        print(f"  {r[0]}: {r[1]:,}")

    conn.close()
    print(f"\nDone. {grand_total:,} new rows inserted.")
    print("Next: python scripts/04_feature_engineering.py")


if __name__ == "__main__":
    main()
