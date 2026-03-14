"""
analyze_tournament_pace.py
==========================
Measure actual scoring in conference tournaments and NCAA tournament
vs regular season to quantify pace inflation for totals adjustments.

NOTE: tournament games have no DK lines in game_lines, so we measure
actual total scoring lift vs regular season. We can compare to the
DK line for regular season games to validate the approach.

Run: python scripts/analyze_tournament_pace.py
"""

import sqlite3, os, warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')


def classify_game_type(row):
    tournament = str(row.get('tournament') or '').lower()
    if 'ncaa' in tournament:
        return 'ncaa_tournament'
    if 'conf' in tournament:
        return 'conf_tournament'
    if 'postseason' in tournament or str(row.get('season_type') or '') == 'postseason':
        return 'other_postseason'
    return 'regular_season'


if __name__ == '__main__':
    print("=" * 65)
    print("NCAAB - Tournament Pace Analysis")
    print("=" * 65)

    conn = sqlite3.connect(DB)

    # Load ALL games with scores (LEFT JOIN so tournament games without lines are included)
    df = pd.read_sql("""
        SELECT g.game_date, g.season, g.home_team, g.away_team,
               g.neutral_site, g.conf_game, g.tournament, g.season_type,
               g.home_score, g.away_score,
               gl.over_under, gl.went_over
        FROM games g
        LEFT JOIN game_lines gl
            ON g.game_date = gl.game_date
            AND g.home_team = gl.home_team
            AND g.away_team = gl.away_team
        WHERE g.home_score IS NOT NULL
          AND g.away_score IS NOT NULL
          AND g.season >= 2019
        ORDER BY g.game_date
    """, conn)
    conn.close()

    df['actual_total'] = df['home_score'] + df['away_score']
    df['has_line']     = df['over_under'].notna()
    df['ou_margin']    = df['actual_total'] - df['over_under']
    df['game_type']    = df.apply(classify_game_type, axis=1)
    df['month']        = pd.to_datetime(df['game_date']).dt.month

    print(f"\nLoaded {len(df):,} games (2019-2025)")
    print("\nGame type distribution:")
    for gt, n in df['game_type'].value_counts().items():
        lined = df[df['game_type']==gt]['has_line'].sum()
        print(f"  {gt:<22} {n:>5} games  ({lined} with DK lines)")

    # ── Actual total scoring by game type ─────────────────────────────────
    print(f"\n{'─'*65}")
    print("AVG ACTUAL TOTAL BY GAME TYPE")
    print(f"{'─'*65}")
    print(f"  {'Type':<22} {'N':>5}  {'Avg Total':>10}  {'vs Reg Season':>14}")
    print(f"  {'─'*55}")

    reg_mean = df[df['game_type']=='regular_season']['actual_total'].mean()

    for gtype in ['regular_season', 'conf_tournament', 'ncaa_tournament', 'other_postseason']:
        sub = df[df['game_type']==gtype]
        if len(sub) < 5:
            continue
        avg = sub['actual_total'].mean()
        lift = avg - reg_mean
        lift_str = f"{lift:+.1f} pts" if gtype != 'regular_season' else "  (baseline)"
        print(f"  {gtype:<22} {len(sub):>5}  {avg:>10.1f}  {lift_str:>14}")

    # ── Season-by-season lift ─────────────────────────────────────────────
    print(f"\n{'─'*65}")
    print("SCORING LIFT BY SEASON (conf + NCAA tournament vs reg season)")
    print(f"{'─'*65}")
    print(f"  {'Season':<8} {'Reg Avg':>8} {'Conf Avg':>10} {'Conf Lift':>10} {'NCAA Avg':>10} {'NCAA Lift':>10}")
    print(f"  {'─'*60}")

    for season in sorted(df['season'].dropna().unique()):
        reg  = df[(df['season']==season) & (df['game_type']=='regular_season')]['actual_total']
        conf = df[(df['season']==season) & (df['game_type']=='conf_tournament')]['actual_total']
        ncaa = df[(df['season']==season) & (df['game_type']=='ncaa_tournament')]['actual_total']
        if len(reg) < 10:
            continue
        reg_avg  = reg.mean()
        conf_str = f"{conf.mean():>8.1f} ({conf.mean()-reg_avg:+.1f})" if len(conf)>0 else "       N/A"
        ncaa_str = f"{ncaa.mean():>8.1f} ({ncaa.mean()-reg_avg:+.1f})" if len(ncaa)>0 else "       N/A"
        print(f"  {int(season):<8} {reg_avg:>8.1f} {conf_str:>20} {ncaa_str:>20}")

    # ── Line accuracy for regular season (validation) ─────────────────────
    reg_lined = df[(df['game_type']=='regular_season') & df['has_line']]
    print(f"\n{'─'*65}")
    print("REGULAR SEASON LINE ACCURACY (validation)")
    print(f"{'─'*65}")
    print(f"  N={len(reg_lined):,} | Avg total={reg_lined['actual_total'].mean():.1f} | "
          f"Avg line={reg_lined['over_under'].mean():.1f} | "
          f"Avg margin={reg_lined['ou_margin'].mean():+.2f} | "
          f"Over%={reg_lined['went_over'].mean()*100:.1f}%")

    # ── Key findings ──────────────────────────────────────────────────────
    conf = df[df['game_type']=='conf_tournament']['actual_total']
    ncaa = df[df['game_type']=='ncaa_tournament']['actual_total']
    reg  = df[df['game_type']=='regular_season']['actual_total']

    print(f"\n{'═'*65}")
    print("KEY FINDINGS")
    print(f"{'═'*65}")
    print(f"  Regular season avg:     {reg.mean():.1f} pts")
    if len(conf)>0:
        print(f"  Conf tournament avg:    {conf.mean():.1f} pts  ({conf.mean()-reg.mean():+.1f} pts vs reg season)")
    if len(ncaa)>0:
        print(f"  NCAA tournament avg:    {ncaa.mean():.1f} pts  ({ncaa.mean()-reg.mean():+.1f} pts vs reg season)")
    print()
    print("  IMPORTANT: These are raw scoring differences, not line-adjusted.")
    print("  The DK line already partially prices in tournament pace.")
    print("  To get the TRUE model adjustment needed, we need DK tournament lines.")
    print("  Recommend: scrape DK tournament lines for 2023-2025 to measure")
    print("  how much the line STILL underestimates after market adjustment.")
