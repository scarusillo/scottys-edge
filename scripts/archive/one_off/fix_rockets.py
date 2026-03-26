import sqlite3
conn = sqlite3.connect('../data/betting_model.db')
conn.execute("UPDATE graded_bets SET result='TAINTED' WHERE selection LIKE '%Houston Rockets%' AND units < 4.5 AND DATE(created_at) >= '2026-03-16'")
conn.commit()
r = conn.execute("SELECT selection, result, units FROM graded_bets WHERE selection LIKE '%Rockets%' ORDER BY created_at DESC LIMIT 3").fetchall()
for row in r:
    print(row)
conn.close()
print("Done")
