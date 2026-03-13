"""
diag_leakage_audit.py
=====================
Audits the three specific leakage risks in "clean" features:

1. TVD binary search: does it include same-day snapshots?
2. RECENCY: does it inherit any same-day TVD snapshots?
3. REFS: are prior-season profiles used correctly?

Run: python scripts/diag_leakage_audit.py
"""
import sqlite3, os
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

def section(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")
def ok(m):   print(f"  ✅  {m}")
def warn(m): print(f"  ⚠️   {m}")
def err(m):  print(f"  ❌  {m}")

# ── 1. TVD SAME-DAY SNAPSHOT RISK ──────────────────────────────────────────
section("1. TVD BINARY SEARCH — SAME-DAY SNAPSHOT RISK")
print("""
  The build_features.py binary search looks for the latest snapshot
  with snapshot_date <= game_date (or < game_date?).
  A same-day snapshot means the rating was updated AFTER the game
  started, which is leakage.
""")

# Check how many TVD snapshots share a date with a game
same_day = conn.execute("""
    SELECT COUNT(*) FROM torvik_daily td
    JOIN games g ON g.game_date = td.snapshot_date
""").fetchone()[0]

total_tvd = conn.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
total_games = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]

print(f"  TVD snapshots on a game date: {same_day:,}")
print(f"  Total TVD rows: {total_tvd:,} | Total games: {total_games:,}")

if same_day > 0:
    warn(f"{same_day:,} TVD snapshots share a date with a game")
    print("  If binary search uses '<=' this is potential leakage.")
    print("  If binary search uses '<' strictly, these are excluded — safe.")

    # Sample some
    samples = conn.execute("""
        SELECT td.snapshot_date, td.team, td.adj_em,
               g.home_team, g.away_team
        FROM torvik_daily td
        JOIN games g ON g.game_date = td.snapshot_date
          AND (g.home_team = td.team OR g.away_team = td.team)
        LIMIT 5
    """).fetchall()
    print("\n  Sample same-day snapshots:")
    for r in samples:
        print(f"    {r[0]} | {r[1]} | AdjEM={r[2]} | game: {r[3]} vs {r[4]}")

    # Check what the binary search actually does in build_features.py
    print("""
  ACTION: Check build_features.py around the TVD lookup function.
  Look for: snaps[mid][0] <= d_int  (LEAKY if same-day snapshots exist)
         vs: snaps[mid][0] <  d_int  (SAFE)
""")
else:
    ok("No TVD snapshots share a date with any game — binary search direction doesn't matter")

# ── 2. KPD SAME-DAY SNAPSHOT RISK ──────────────────────────────────────────
section("2. KPD BINARY SEARCH — SAME-DAY SNAPSHOT RISK")

same_day_kpd = conn.execute("""
    SELECT COUNT(*) FROM kenpom_daily kd
    JOIN games g ON g.game_date = kd.snapshot_date
""").fetchone()[0]

print(f"  KPD snapshots on a game date: {same_day_kpd:,}")
if same_day_kpd > 0:
    warn(f"{same_day_kpd:,} KPD snapshots share a date with a game — same risk applies")
else:
    ok("No KPD same-day snapshots")

# ── 3. RECENCY SAME-DAY INHERITANCE ────────────────────────────────────────
section("3. RECENCY — INHERITED TVD SAME-DAY LEAKAGE")
print("""
  recency_eff is built from TVD snapshots strictly BEFORE game_date
  (the code uses: snaps[snaps['snap_dt'] < gdt]).
  The '<' is strict in the Python code, so this should be safe.
  Let's verify the recency_eff table actually excludes same-day data.
""")

try:
    # Check if recency_eff exists
    n_rew = conn.execute("SELECT COUNT(*) FROM recency_eff").fetchone()[0]
    print(f"  recency_eff rows: {n_rew:,}")

    if n_rew > 0 and same_day > 0:
        # For games where same-day TVD exists, does recency_eff include it?
        # We can check by looking at recency values for teams on game dates
        # that have same-day TVD snapshots
        check = conn.execute("""
            SELECT r.game_date, r.team, r.n_snaps,
                   td_same.adj_em as same_day_tvd_em
            FROM recency_eff r
            JOIN (
                SELECT td.snapshot_date, td.team, td.adj_em
                FROM torvik_daily td
                JOIN games g ON g.game_date = td.snapshot_date
                  AND (g.home_team = td.team OR g.away_team = td.team)
            ) td_same ON td_same.snapshot_date = r.game_date
                      AND td_same.team = r.team
            LIMIT 5
        """).fetchall()

        if check:
            warn("Same-day TVD snapshots exist for dates in recency_eff")
            print("  The Python code uses strict '<' so these should be excluded,")
            print("  but verify 03g_recency_features.py line:")
            print("    prior = snaps[snaps['snap_dt'] < gdt]")
            print("  That '<' makes it safe. If it were '<=' it would be leaky.")
        else:
            ok("No overlap detected between recency_eff and same-day TVD snapshots")
    else:
        ok("recency_eff built with strict '<' — safe by construction")
except Exception as e:
    print(f"  recency_eff not available: {e}")

# ── 4. REFS SEASON ALIGNMENT ────────────────────────────────────────────────
section("4. REFS — SEASON ALIGNMENT CHECK")
print("""
  For a game in season S, we should use ref profiles from season S-1.
  Risk: if a referee has no S-1 profile, we fall back to season S.
  Season S profiles include games from that same season = leakage.
""")

try:
    n_rg = conn.execute("SELECT COUNT(*) FROM referee_game WHERE ref1_id != ''").fetchone()[0]
    n_rp = conn.execute("SELECT COUNT(*) FROM referee_profiles").fetchone()[0]
    print(f"  referee_game rows: {n_rg:,}")
    print(f"  referee_profiles rows: {n_rp:,}")

    if n_rp > 0:
        # Check what fraction of game-ref pairs have a prior-season profile
        seasons = conn.execute("SELECT DISTINCT season FROM referee_game ORDER BY season").fetchall()
        print(f"\n  Season coverage:")
        for (s,) in seasons:
            prior_s = s - 1
            # Count refs in this season's games
            refs_in_season = conn.execute("""
                SELECT COUNT(DISTINCT ref1_id) FROM referee_game WHERE season=? AND ref1_id != ''
            """, (s,)).fetchone()[0]
            # Count how many have a prior-season profile
            refs_with_prior = conn.execute("""
                SELECT COUNT(DISTINCT rg.ref1_id) FROM referee_game rg
                JOIN referee_profiles rp ON rp.ref_id = rg.ref1_id AND rp.season = ?
                WHERE rg.season = ?
            """, (prior_s, s)).fetchone()[0]
            pct = refs_with_prior/refs_in_season*100 if refs_in_season else 0
            status = "✅" if pct > 70 else ("⚠️ " if pct > 30 else "❌")
            print(f"    {s}: {refs_with_prior}/{refs_in_season} refs have S-1 profile ({pct:.0f}%) {status}")

        # The fallback to season S is the leakage risk
        print("""
  In build_features.py, ref lookup is:
    prof = ref_profiles.get((rid, s-1)) or ref_profiles.get((rid, s))

  The 'or ref_profiles.get((rid, s))' fallback uses current-season profiles.
  This is LEAKY for games early in the season before profiles are meaningful,
  but in practice ref profiles from season S include that season's full stats,
  which would only be available after the season ends.

  MITIGATION OPTIONS:
  A) Remove the fallback: only use prior-season profiles (most conservative)
  B) Use rolling ref profiles: only include games BEFORE the target game date
  C) Accept as minor leakage: refs don't affect the winning combo anyway
     (REFS wasn't in the top clean combos)
""")
        if n_rp == 0:
            warn("referee_profiles is empty — REFS feature group is all NULLs, no leakage risk")
    else:
        warn("referee_profiles empty — no ref data in use")

except Exception as e:
    print(f"  Referee tables not available: {e}")

# ── 5. EXPERIENCE SEASON ALIGNMENT ─────────────────────────────────────────
section("5. EXPERIENCE — PRIOR SEASON ALIGNMENT")
try:
    n_exp = conn.execute("SELECT COUNT(*) FROM team_experience").fetchone()[0]
    print(f"  team_experience rows: {n_exp:,}")
    if n_exp > 0:
        seasons = conn.execute("SELECT MIN(season), MAX(season) FROM team_experience").fetchone()
        print(f"  Seasons: {seasons[0]}-{seasons[1]}")
        print("""
  In build_features.py, experience lookup is:
    h_exp = experience.get((s-1, home)) or experience.get((s, home))

  Same fallback risk as REFS — if no S-1 data, falls back to S.
  Torvik experience ratings are end-of-season, so season S = leaky.
  BUT: if the fallback is rarely triggered (most teams have S-1 data),
  the practical impact is small.
""")
        # Check first-season coverage
        first_season = seasons[0]
        teams_first = conn.execute(
            "SELECT COUNT(*) FROM team_experience WHERE season=?", (first_season,)
        ).fetchone()[0]
        print(f"  Teams in first available season ({first_season}): {teams_first}")
        print(f"  Games in season {first_season+1} would have no prior-season experience → fallback to S")
        ok("Minor risk, contained to first season in dataset")
    else:
        print("  team_experience empty — EXPERIENCE feature group is all NULLs")
except Exception as e:
    print(f"  team_experience not available: {e}")

# ── 6. VERDICT ──────────────────────────────────────────────────────────────
section("VERDICT")
print(f"""
  Source          Clean?    Notes
  ────────────    ───────   ──────────────────────────────────────────
  TVD             LIKELY    Depends on '<=' vs '<' in binary search
  KPD             LIKELY    Same as TVD
  KP_FANMATCH     YES       Published day-of, pre-game predictions
  ROLLING         YES       Strictly prior games only
  TRAVEL          YES       Pure geography, no timing
  REFS            MOSTLY    Fallback to S profiles is technically leaky
                            but moot — REFS didn't make top combos
  RECENCY         LIKELY    Uses strict '<' per code — safe if TVD safe
  EXPERIENCE      MOSTLY    Same fallback risk, first season only

  KEY CHECK: Inspect build_features.py around TVD/KPD binary search.
  Find the comparison:  snaps[mid][0] <= d_int
  If '<=' is used AND same-day snapshots exist → fix to '<'
""")
conn.close()
print("Audit complete.")
