import sqlite3
conn = sqlite3.connect('../data/betting_model.db')
r = conn.execute("""
    SELECT 
        SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
        SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END),
        ROUND(SUM(pnl_units),2)
    FROM graded_bets 
    WHERE DATE(created_at)>='2026-03-04' 
    AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
""").fetchone()
total = r[0] + r[1]
wp = r[0]/total*100 if total > 0 else 0
wagered = conn.execute("""
    SELECT ROUND(SUM(units),2) FROM graded_bets
    WHERE DATE(created_at)>='2026-03-04' AND result NOT IN ('DUPLICATE','PENDING','TAINTED')
""").fetchone()[0]
roi = (r[2]/wagered*100) if wagered else 0
print(f"Record: {r[0]}W-{r[1]}L | {r[2]:+.1f}u | {wp:.1f}% | ROI {roi:+.1f}%")
conn.close()
