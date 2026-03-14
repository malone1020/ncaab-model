"""
analyze_tournament_pace.py
==========================
Measure actual vs model-predicted totals during conference tournaments
and NCAA tournament vs regular season. Quantifies the pace inflation
we need to account for in 07_daily_bets.py.

Run: python scripts/analyze_tournament_pace.py
"""

import sqlite3, os, warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')


def classify_game_type(row):
    """Classify game using tournament column directly."""
    tournament = str(row.get('tournament') or '').lower()
    if 'ncaa' in tournament:
        return 'ncaa_tournament'
    if 'conf' in tournament:
        return 'conf_tournament'
    if 'postseason' in tournament or row.get('season_type') == 'postseason':
        return 'other_postseason'
    return 'regular_season'


if __name__ == '__main__':
    print("=" * 65)
    print("NCAAB — Tournament Pace Analysis")
    print("=" * 65)

    conn = sqlite3.connect(DB)

    # Load games with scores and lines
    df = pd.read_sql("""
        SELECT g.game_date, g.season, g.home_team, g.away_team,
               g.neutral_site, g.conf_game, g.tournament, g.season_type,
               gl.home_score, gl.away_score, gl.over_under,
               gl.went_over, gl.home_covered
        FROM games g
        JOIN game_lines gl ON g.game_date = gl.game_date
            AND g.home_team = gl.home_team
            AND g.away_team = gl.away_team
        WHERE gl.home_score IS NOT NULL
          AND gl.away_score IS NOT NULL
          AND gl.over_under IS NOT NULL
          AND g.season >= 2019
        ORDER BY g.game_date
    """, conn)
    conn.close()

    print(f"\nLoaded {len(df):,} games with scores + lines (2019-2025)")

    # Compute actual total and over/under margin
    df['actual_total']  = df['home_score'] + df['away_score']
    df['ou_margin']     = df['actual_total'] - df['over_under']  # positive = went over
    df['game_type']     = df.apply(classify_game_type, axis=1)

    # Extract month for seasonality
    df['month'] = pd.to_datetime(df['game_date']).dt.month
    df['is_march'] = df['month'].isin([3, 4])

    print(f"\nGame type distribution:")
    print(df['game_type'].value_counts().to_string())

    # ── Core analysis: actual totals by game type ──────────────────────────
    print(f"\n{'─'*65}")
    print("ACTUAL TOTALS BY GAME TYPE")
    print(f"{'─'*65}")
    print(f"{'Type':<22} {'N':>6} {'Avg Total':>10} {'Avg Line':>10} {'Avg Margin':>12} {'Over%':>7}")
    print(f"{'─'*65}")

    for gtype in ['regular_season', 'neutral_nonconf', 'conf_tournament', 'ncaa_tournament']:
        sub = df[df['game_type'] == gtype]
        if len(sub) < 10:
            continue
        avg_total  = sub['actual_total'].mean()
        avg_line   = sub['over_under'].mean()
        avg_margin = sub['ou_margin'].mean()
        over_pct   = sub['went_over'].mean() * 100
        print(f"  {gtype:<20} {len(sub):>6,} {avg_total:>10.1f} {avg_line:>10.1f} "
              f"{avg_margin:>+12.2f} {over_pct:>6.1f}%")

    # ── Month-by-month breakdown ───────────────────────────────────────────
    print(f"\n{'─'*65}")
    print("OVER% AND AVG MARGIN BY MONTH (all game types)")
    print(f"{'─'*65}")
    print(f"{'Month':<10} {'N':>6} {'Avg Total':>10} {'Avg Line':>10} {'Margin':>8} {'Over%':>7}")
    print(f"{'─'*65}")

    month_names = {11:'Nov', 12:'Dec', 1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr'}
    for month in [11, 12, 1, 2, 3, 4]:
        sub = df[df['month'] == month]
        if len(sub) < 10:
            continue
        avg_total  = sub['actual_total'].mean()
        avg_line   = sub['over_under'].mean()
        avg_margin = sub['ou_margin'].mean()
        over_pct   = sub['went_over'].mean() * 100
        print(f"  {month_names.get(month,'?'):<10} {len(sub):>6,} {avg_total:>10.1f} "
              f"{avg_line:>10.1f} {avg_margin:>+8.2f} {over_pct:>6.1f}%")

    # ── Neutral site breakdown ─────────────────────────────────────────────
    print(f"\n{'─'*65}")
    print("NEUTRAL SITE vs HOME/AWAY GAMES")
    print(f"{'─'*65}")
    print(f"{'Type':<25} {'N':>6} {'Avg Total':>10} {'Avg Line':>10} {'Margin':>8} {'Over%':>7}")
    print(f"{'─'*65}")

    for label, mask in [
        ('Regular season',         df['game_type']=='regular_season'),
        ('Conf tournament',        df['game_type']=='conf_tournament'),
        ('NCAA tournament',        df['game_type']=='ncaa_tournament'),
        ('Other postseason',       df['game_type']=='other_postseason'),
        ('Neutral site (reg ssn)', (df['neutral_site']==1) & (df['game_type']=='regular_season')),
    ]:
        sub = df[mask]
        if len(sub) < 10:
            continue
        avg_total  = sub['actual_total'].mean()
        avg_line   = sub['over_under'].mean()
        avg_margin = sub['ou_margin'].mean()
        over_pct   = sub['went_over'].mean() * 100
        print(f"  {label:<25} {len(sub):>6,} {avg_total:>10.1f} {avg_line:>10.1f} "
              f"{avg_margin:>+8.2f} {over_pct:>6.1f}%")

    # ── Conference tournament specific ────────────────────────────────────
    print(f"\n{'─'*65}")
    print("CONFERENCE TOURNAMENT: LINE ACCURACY BY SEASON")
    print(f"{'─'*65}")
    conf_t = df[df['game_type'] == 'conf_tournament']
    if len(conf_t) > 0:
        print(f"{'Season':<10} {'N':>6} {'Avg Margin':>12} {'Over%':>8}")
        for season, grp in conf_t.groupby('season'):
            print(f"  {int(season):<10} {len(grp):>6} {grp['ou_margin'].mean():>+12.2f} "
                  f"{grp['went_over'].mean()*100:>7.1f}%")

    # ── Key finding summary ────────────────────────────────────────────────
    reg  = df[df['game_type'] == 'regular_season']
    conf = df[df['game_type'] == 'conf_tournament']
    ncaa = df[df['game_type'] == 'ncaa_tournament']

    print(f"\n{'═'*65}")
    print("KEY FINDINGS")
    print(f"{'═'*65}")
    if len(conf) > 0 and len(reg) > 0:
        conf_lift = conf['actual_total'].mean() - reg['actual_total'].mean()
        conf_margin_diff = conf['ou_margin'].mean() - reg['ou_margin'].mean()
        print(f"  Conf tournament vs regular season:")
        print(f"    Actual total lift:    {conf_lift:+.1f} pts")
        print(f"    Model margin diff:    {conf_margin_diff:+.2f} pts (positive = model underpredicts)")
        print(f"    Over rate: conf={conf['went_over'].mean()*100:.1f}% vs reg={reg['went_over'].mean()*100:.1f}%")

    if len(ncaa) > 0 and len(reg) > 0:
        ncaa_lift = ncaa['actual_total'].mean() - reg['actual_total'].mean()
        ncaa_margin_diff = ncaa['ou_margin'].mean() - reg['ou_margin'].mean()
        print(f"\n  NCAA tournament vs regular season:")
        print(f"    Actual total lift:    {ncaa_lift:+.1f} pts")
        print(f"    Model margin diff:    {ncaa_margin_diff:+.2f} pts")
        print(f"    Over rate: ncaa={ncaa['went_over'].mean()*100:.1f}% vs reg={reg['went_over'].mean()*100:.1f}%")

    print(f"\n  Interpretation:")
    print(f"    'Model margin diff' = how much the line UNDERESTIMATES the actual total")
    print(f"    Positive = overs are hitting more than expected = model should adjust UP")
    print(f"    This is the adjustment to apply to P(over) during tournament play")
