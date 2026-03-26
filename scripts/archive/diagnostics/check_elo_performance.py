"""Check how LOW Elo teams performed vs MED/HIGH teams in NCAAB"""
import sqlite3

conn = sqlite3.connect('../data/betting_model.db')

bets = conn.execute("""
    SELECT selection, result, pnl_units, created_at
    FROM graded_bets
    WHERE sport='basketball_ncaab' AND result NOT IN ('DUPLICATE','PENDING')
    AND DATE(created_at) >= '2026-03-04'
    ORDER BY created_at
""").fetchall()

elo = dict(conn.execute(
    "SELECT team, confidence FROM elo_ratings WHERE sport='basketball_ncaab'"
).fetchall())

low_w = low_l = low_pnl = 0
ok_w = ok_l = ok_pnl = 0

for sel, result, pnl, dt in bets:
    is_low = False
    for team, conf in elo.items():
        if conf == 'LOW' and team.split()[-1] in sel:
            is_low = True
            break
    if is_low:
        if result == 'WIN': low_w += 1
        elif result == 'LOSS': low_l += 1
        low_pnl += (pnl or 0)
        print(f'  LOW: {sel} | {result} | {pnl:+.1f}u')
    else:
        if result == 'WIN': ok_w += 1
        elif result == 'LOSS': ok_l += 1
        ok_pnl += (pnl or 0)

print()
print(f'LOW Elo teams:  {low_w}W-{low_l}L | {low_pnl:+.1f}u')
print(f'MED/HIGH teams: {ok_w}W-{ok_l}L | {ok_pnl:+.1f}u')
conn.close()
