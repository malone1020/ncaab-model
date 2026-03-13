"""
00_audit_data.py
================
Master data audit. Run before any collection or model rebuild.
Reports: row counts, date ranges, coverage gaps, spot-check accuracy.

Run: python scripts/00_audit_data.py
"""
import sqlite3, os, sys
from datetime import date, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

TODAY = date.today()
CURRENT_SEASON = 2025  # update each year

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def ok(msg):   print(f"  ✅  {msg}")
def warn(msg): print(f"  ⚠️   {msg}")
def err(msg):  print(f"  ❌  {msg}")

def tbl(name):
    try:
        n = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        return n
    except:
        return None

# ── 1. GAMES ────────────────────────────────────────────────────────────────
section("1. GAMES TABLE")
n_games = tbl('games')
print(f"  Total rows: {n_games:,}")

r = conn.execute("SELECT MIN(game_date), MAX(game_date), COUNT(DISTINCT season) FROM games").fetchone()
print(f"  Date range: {r[0]} → {r[1]}")
print(f"  Seasons:    {r[2]}")

# Most recent game
latest = conn.execute("SELECT game_date, home_team, away_team FROM games ORDER BY game_date DESC LIMIT 3").fetchall()
print(f"  Most recent games:")
for g in latest: print(f"    {g[0]}  {g[1]} vs {g[2]}")

# Check for nulls in key fields
nulls = conn.execute("SELECT COUNT(*) FROM games WHERE home_score IS NULL OR away_score IS NULL").fetchone()[0]
if nulls > 100:
    warn(f"{nulls:,} games missing scores (expected for future games)")
else:
    ok(f"Score nulls: {nulls}")

# ── 2. GAME LINES ───────────────────────────────────────────────────────────
section("2. GAME LINES (SPREADS)")
n_lines = tbl('game_lines')
print(f"  Total rows: {n_lines:,}")

r = conn.execute("SELECT MIN(game_date), MAX(game_date) FROM game_lines").fetchone()
print(f"  Date range: {r[0]} → {r[1]}")

spread_null = conn.execute("SELECT COUNT(*) FROM game_lines WHERE spread IS NULL").fetchone()[0]
if spread_null > 500:
    warn(f"{spread_null:,} rows missing spread")
else:
    ok(f"Spread nulls: {spread_null}")

# Coverage vs games
coverage = conn.execute("""
    SELECT COUNT(DISTINCT g.id) FROM games g
    JOIN game_lines gl ON gl.game_date=g.game_date
      AND gl.home_team=g.home_team AND gl.away_team=g.away_team
    WHERE g.game_date >= '2016-01-01'
""").fetchone()[0]
total_since_2016 = conn.execute("SELECT COUNT(*) FROM games WHERE game_date >= '2016-01-01'").fetchone()[0]
pct = coverage/total_since_2016*100
msg = f"Spread coverage since 2016: {coverage:,}/{total_since_2016:,} ({pct:.1f}%)"
if pct > 60: ok(msg)
else: warn(msg)

# ── 3. TORVIK DAILY ─────────────────────────────────────────────────────────
section("3. TORVIK DAILY SNAPSHOTS")
n_tvd = tbl('torvik_daily')
print(f"  Total rows: {n_tvd:,}")

r = conn.execute("SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(DISTINCT snapshot_date) FROM torvik_daily").fetchone()
print(f"  Date range: {r[0]} → {r[1]}")
print(f"  Unique dates: {r[2]}")

# Is it current?
latest_snap = r[1]
if latest_snap:
    latest_dt = datetime.strptime(str(latest_snap)[:10], '%Y-%m-%d').date()
    days_old = (TODAY - latest_dt).days
    msg = f"Latest snapshot: {latest_snap} ({days_old} days ago)"
    if days_old <= 3: ok(msg)
    elif days_old <= 14: warn(msg)
    else: err(msg)

# Null check on key field
nulls = conn.execute("SELECT COUNT(*) FROM torvik_daily WHERE barthag IS NULL").fetchone()[0]
pct_null = nulls/n_tvd*100
msg = f"barthag nulls: {nulls:,} ({pct_null:.1f}%)"
if pct_null < 5: ok(msg)
else: warn(msg)

# ── 4. KENPOM DAILY ─────────────────────────────────────────────────────────
section("4. KENPOM DAILY SNAPSHOTS")
n_kpd = tbl('kenpom_daily')
print(f"  Total rows: {n_kpd:,}")

r = conn.execute("SELECT MIN(snapshot_date), MAX(snapshot_date), COUNT(DISTINCT snapshot_date) FROM kenpom_daily").fetchone()
print(f"  Date range: {r[0]} → {r[1]}")
print(f"  Unique dates: {r[2]}")

latest_snap = r[1]
if latest_snap:
    latest_dt = datetime.strptime(str(latest_snap)[:10], '%Y-%m-%d').date()
    days_old = (TODAY - latest_dt).days
    msg = f"Latest snapshot: {latest_snap} ({days_old} days ago)"
    if days_old <= 3: ok(msg)
    elif days_old <= 14: warn(msg)
    else: err(msg)

nulls = conn.execute("SELECT COUNT(*) FROM kenpom_daily WHERE adj_em IS NULL").fetchone()[0]
msg = f"adj_em nulls: {nulls:,}"
if nulls < 1000: ok(msg)
else: warn(msg)

# ── 5. KENPOM FANMATCH ──────────────────────────────────────────────────────
section("5. KENPOM FANMATCH")
n_fm = tbl('kenpom_fanmatch')
print(f"  Total rows: {n_fm:,}")

r = conn.execute("SELECT MIN(game_date), MAX(game_date) FROM kenpom_fanmatch").fetchone()
print(f"  Date range: {r[0]} → {r[1]}")

latest_snap = r[1]
if latest_snap:
    latest_dt = datetime.strptime(str(latest_snap)[:10], '%Y-%m-%d').date()
    days_old = (TODAY - latest_dt).days
    msg = f"Latest prediction: {latest_snap} ({days_old} days ago)"
    if days_old <= 3: ok(msg)
    elif days_old <= 14: warn(msg)
    else: err(msg)

# ── 6. ROLLING EFFICIENCY ───────────────────────────────────────────────────
section("6. ROLLING EFFICIENCY")
n_rol = tbl('rolling_efficiency')
print(f"  Total rows: {n_rol:,}")

r = conn.execute("SELECT MIN(game_date), MAX(game_date), COUNT(DISTINCT team) FROM rolling_efficiency").fetchone()
print(f"  Date range: {r[0]} → {r[1]}")
print(f"  Teams: {r[2]}")

# Check date alignment vs games
mismatch = conn.execute("""
    SELECT COUNT(*) FROM rolling_efficiency r
    WHERE NOT EXISTS (
        SELECT 1 FROM games g
        WHERE g.game_date = r.game_date
          AND (g.home_team = r.team OR g.away_team = r.team)
    )
""").fetchone()[0]
pct = mismatch/n_rol*100 if n_rol else 0
msg = f"Rows with no matching game: {mismatch:,} ({pct:.1f}%)"
if pct < 10: ok(msg)
else: warn(msg + " — date drift issue, rebuild needed")

# ── 7. HASLAMETRICS ─────────────────────────────────────────────────────────
section("7. HASLAMETRICS")
n_has = tbl('haslametrics_full')
print(f"  Total rows: {n_has:,}")
if n_has:
    r = conn.execute("SELECT MIN(season), MAX(season), COUNT(DISTINCT season) FROM haslametrics_full").fetchone()
    print(f"  Seasons: {r[0]}-{r[1]} ({r[2]} seasons)")
    warn("Season-final only — LEAKY. Daily snapshots needed for clean signal.")

# ── 8. REFEREES ─────────────────────────────────────────────────────────────
section("8. REFEREE DATA")
n_ref = tbl('referee_game')
if n_ref is None:
    err("referee_game table does not exist")
elif n_ref == 0:
    err("referee_game table exists but is EMPTY — needs pull")
else:
    r = conn.execute("SELECT MIN(game_date), MAX(game_date), COUNT(DISTINCT referee_id) FROM referee_game").fetchone()
    print(f"  Total rows: {n_ref:,}")
    print(f"  Date range: {r[0]} → {r[1]}")
    print(f"  Unique referees: {r[2]}")

# ── 9. TRAVEL DISTANCE ──────────────────────────────────────────────────────
section("9. TRAVEL DISTANCE")
n_trav = tbl('team_travel')
if n_trav is None or n_trav == 0:
    err("team_travel table missing or empty — not yet computed")
else:
    ok(f"team_travel: {n_trav:,} rows")

# ── 10. GAME_FEATURES_V2 COVERAGE ───────────────────────────────────────────
section("10. GAME_FEATURES_V2 COVERAGE")
n_gf = tbl('game_features_v2')
print(f"  Total rows: {n_gf:,}")
if n_gf:
    cols_to_check = [
        ('spread',           'Spread'),
        ('tvd_bar_gap',      'Torvik daily'),
        ('kpd_em_gap',       'KenPom daily'),
        ('kp_pred_margin',   'KP fanmatch'),
        ('rol_margin_gap',   'Rolling'),
        ('ha_eff_gap',       'Haslametrics'),
        ('torvik_pred',      'Torvik pred'),
    ]
    for col, label in cols_to_check:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM game_features_v2 WHERE {col} IS NOT NULL").fetchone()[0]
            pct = n/n_gf*100
            msg = f"{label:<20} {n:>6,} / {n_gf:,} ({pct:.1f}%)"
            if pct > 60: ok(msg)
            elif pct > 30: warn(msg)
            else: err(msg)
        except:
            err(f"{label} — column missing")

# ── SUMMARY ─────────────────────────────────────────────────────────────────
section("SUMMARY — COLLECTION PRIORITY")
print("""
  Priority   Task
  ────────   ────────────────────────────────────────────────────
  1 URGENT   Rebuild rolling_efficiency (fix date drift)
  2 URGENT   Pull referee data (03e script)
  3 HIGH      Compute travel distance (new: 03f script)
  4 HIGH      Check TVD/KPD currency — pull today's snapshot
  5 HIGH      Fix rolling date alignment in 04_build_features.py
  6 MEDIUM   Wire experience from torvik_season → features
  7 MEDIUM   Pull line movement / public % (Action Network)
  8 LATER    Haslametrics daily snapshots (if available)
  9 LATER    Recency-weighted efficiency decay

  Run: python scripts/00_audit_data.py  (this script, re-run after fixes)
""")

conn.close()
print("Audit complete.")
