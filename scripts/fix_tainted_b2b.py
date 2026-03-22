"""
fix_tainted_b2b.py — Identify and mark NCAAB bets tainted by faulty B2B context

During March 9-20 (conference tournaments), "Away on B2B" was adding +1.0 
to spreads when both teams were at the same venue. This inflated edges 
on picks that wouldn't have been recommended without the adjustment.

Data: "Away on B2B" in NCAAB was 4W-12L, -34.3u

This script:
  1. Shows all affected bets
  2. With --apply, marks them as 'TAINTED' so they're excluded from the record

Usage:
    python fix_tainted_b2b.py              # Preview
    python fix_tainted_b2b.py --apply      # Mark as tainted
"""
import sqlite3, sys

conn = sqlite3.connect('../data/betting_model.db')

# Find NCAAB bets from March 9+ with "Away on B2B" in context
tainted = conn.execute("""
    SELECT selection, result, pnl_units, context_factors, created_at, units
    FROM graded_bets
    WHERE sport = 'basketball_ncaab'
    AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
    AND DATE(created_at) >= '2026-03-09'
    AND DATE(created_at) <= '2026-03-20'
    AND context_factors LIKE '%Away on B2B%'
    ORDER BY created_at
""").fetchall()

# Also find "Away bounce-back" tainted bets in same window
tainted_bb = conn.execute("""
    SELECT selection, result, pnl_units, context_factors, created_at, units
    FROM graded_bets
    WHERE sport = 'basketball_ncaab'
    AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
    AND DATE(created_at) >= '2026-03-09'
    AND DATE(created_at) <= '2026-03-20'
    AND context_factors LIKE '%Away bounce-back%'
    AND context_factors NOT LIKE '%Away on B2B%'
    ORDER BY created_at
""").fetchall()

all_tainted = tainted + tainted_bb

if not all_tainted:
    print("  No tainted B2B bets found in tournament window.")
    conn.close()
    exit()

wins = sum(1 for b in all_tainted if b[1] == 'WIN')
losses = sum(1 for b in all_tainted if b[1] == 'LOSS')
pnl = sum(b[2] or 0 for b in all_tainted)

print(f"{'='*65}")
print(f"  TAINTED B2B BETS — NCAAB Conference Tournament Window")
print(f"  March 9-20, 2026 | 'Away on B2B' or 'Away bounce-back'")
print(f"{'='*65}")
print(f"\n  {len(all_tainted)} tainted bets: {wins}W-{losses}L | {pnl:+.1f}u\n")

for sel, result, p, ctx, dt, units in all_tainted:
    icon = "W" if result == 'WIN' else "L"
    ctx_short = ctx[:50] if ctx else ""
    print(f"  {icon} {sel:45s} {p:+.1f}u  {dt[:10]}  {ctx_short}")

# Show what record looks like without them
clean = conn.execute("""
    SELECT 
        SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
        SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
        ROUND(SUM(pnl_units), 2)
    FROM graded_bets
    WHERE DATE(created_at) >= '2026-03-04'
    AND result NOT IN ('DUPLICATE', 'PENDING')
""").fetchone()

print(f"\n  Current record:  {clean[0]}W-{clean[1]}L | {clean[2]:+.1f}u")
print(f"  After removing:  {clean[0]-wins}W-{clean[1]-losses}L | {clean[2]-pnl:+.1f}u")

if '--apply' in sys.argv:
    conn.execute("""
        UPDATE graded_bets SET result = 'TAINTED'
        WHERE sport = 'basketball_ncaab'
        AND result NOT IN ('DUPLICATE', 'PENDING', 'TAINTED')
        AND DATE(created_at) >= '2026-03-09'
        AND DATE(created_at) <= '2026-03-20'
        AND (context_factors LIKE '%Away on B2B%' OR context_factors LIKE '%Away bounce-back%')
    """)
    conn.commit()
    print(f"\n  ✅ Marked {len(all_tainted)} bets as TAINTED — excluded from record")
else:
    print(f"\n  Run with --apply to mark these as TAINTED")

conn.close()
