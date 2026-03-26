"""
fix_sub45.py — Taint all graded bets below 4.5 units

The public threshold is 4.5u (MAX PLAY only). 
Anything below shouldn't be in the record.
"""
import sqlite3

conn = sqlite3.connect('../data/betting_model.db')

# Find all sub-4.5u graded bets that aren't already tainted/duplicate
sub = conn.execute("""
    SELECT selection, units, result, pnl_units, sport, created_at 
    FROM graded_bets 
    WHERE units < 4.5 
    AND result NOT IN ('DUPLICATE', 'TAINTED', 'PENDING')
    AND DATE(created_at) >= '2026-03-04'
    ORDER BY created_at
""").fetchall()

print(f"Found {len(sub)} bets below 4.5u:\n")
for sel, units, result, pnl, sport, dt in sub:
    icon = "W" if result == 'WIN' else "L"
    print(f"  {icon} {units:.1f}u  {sel:45s} {pnl:+.1f}u  {dt[:10]}")

# Show impact
wins = sum(1 for b in sub if b[2] == 'WIN')
losses = sum(1 for b in sub if b[2] == 'LOSS')
pnl = sum(b[3] or 0 for b in sub)
print(f"\n  Removing: {wins}W-{losses}L | {pnl:+.1f}u")

# Current record
cur = conn.execute("""
    SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
           SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
           ROUND(SUM(pnl_units),2)
    FROM graded_bets 
    WHERE DATE(created_at)>='2026-03-04' 
    AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
""").fetchone()
print(f"\n  Current:  {cur[0]}W-{cur[1]}L | {cur[2]:+.1f}u")
print(f"  After:    {cur[0]-wins}W-{cur[1]-losses}L | {cur[2]-pnl:+.1f}u")

# Apply
conn.execute("""
    UPDATE graded_bets SET result='TAINTED' 
    WHERE units < 4.5 
    AND result NOT IN ('DUPLICATE', 'TAINTED', 'PENDING')
    AND DATE(created_at) >= '2026-03-04'
""")
conn.commit()
print(f"\n  ✅ Tainted {len(sub)} sub-4.5u bets")

conn.close()
