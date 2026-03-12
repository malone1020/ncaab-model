import pandas as pd

df = pd.read_csv('outputs/predictions.csv')
df['market_margin'] = -df['spread']
df['edge'] = df['predicted_margin'] - df['market_margin']
df['spread_abs'] = df['spread'].abs()
df['bet_home'] = df['edge'] > 3
df['bet_away'] = df['edge'] < -3
df['has_bet'] = df['bet_home'] | df['bet_away']
df['bet_won'] = df.apply(
    lambda r: r['home_covered'] if r['bet_home']
    else (1 - r['home_covered']) if r['bet_away']
    else None, axis=1
)
bets = df[df['has_bet'] & df['bet_won'].notna()].copy()

excl = bets[~bets['spread_abs'].between(9, 12)]
incl = bets[bets['spread_abs'].between(9, 12)]

def units(b):
    w = b['bet_won'].sum()
    l = len(b) - w
    return w * (100/110) - l

print(f"All bets    : n={len(bets):,}  cover={bets['bet_won'].mean():.3f}  units={units(bets):+.1f}")
print(f"Excl 9-12pt : n={len(excl):,}  cover={excl['bet_won'].mean():.3f}  units={units(excl):+.1f}")
print(f"Only 9-12pt : n={len(incl):,}  cover={incl['bet_won'].mean():.3f}  units={units(incl):+.1f}")

print()
print("By spread bucket:")
bins   = [0, 3, 6, 9, 12, 15, 20, 50]
labels = ['0-3','3-6','6-9','9-12','12-15','15-20','20+']
bets['bucket'] = pd.cut(bets['spread_abs'], bins=bins, labels=labels)
for label, grp in bets.groupby('bucket', observed=True):
    if len(grp) < 50:
        continue
    cov = grp['bet_won'].mean()
    roi = (cov * (100/110) - (1 - cov)) * 100
    print(f"  {label:>6}pt  n={len(grp):5,}  cover={cov:.3f}  ROI={roi:+.1f}%")
