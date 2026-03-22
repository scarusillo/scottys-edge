"""
performance.py — Track and report model performance over time.

This is how you know if the model actually works.
Key metrics:
- ROI (return on investment)
- CLV (closing line value) — the #1 predictor of long-term profit
- ATS record by confidence tier
- Model calibration (do 60% predictions win 60% of the time?)
"""
import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def performance_report(days=30, sport=None):
    """Generate a comprehensive performance report."""
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    where = "WHERE b.created_at >= ?"
    params = [cutoff]
    if sport:
        where += " AND b.sport = ?"
        params.append(sport)

    # Overall record
    bets = conn.execute(f"""
        SELECT b.result, b.profit, b.units, b.edge_pct, b.confidence,
               b.clv, b.market_type, b.sport, b.odds
        FROM bets b
        {where} AND b.result IS NOT NULL
    """, params).fetchall()

    if not bets:
        print(f"\n  No graded bets in the last {days} days.")
        conn.close()
        return

    print(f"\n{'='*70}")
    print(f"  PERFORMANCE REPORT — Last {days} days")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    print(f"{'='*70}")

    wins = sum(1 for b in bets if b[0] == 'WIN')
    losses = sum(1 for b in bets if b[0] == 'LOSS')
    pushes = sum(1 for b in bets if b[0] == 'PUSH')
    total = wins + losses + pushes
    win_pct = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

    total_profit = sum(b[1] * b[2] for b in bets if b[1] and b[2])
    total_risked = sum(b[2] for b in bets if b[2])
    roi = total_profit / total_risked * 100 if total_risked > 0 else 0

    print(f"\n  📊 OVERALL RECORD: {wins}-{losses}-{pushes} ({win_pct:.1f}%)")
    print(f"     Total bets: {total}")
    print(f"     Profit: {total_profit:+.2f} units")
    print(f"     ROI: {roi:+.1f}%")

    # CLV analysis
    clv_bets = [b for b in bets if b[5] is not None]
    if clv_bets:
        avg_clv = sum(b[5] for b in clv_bets) / len(clv_bets)
        positive_clv = sum(1 for b in clv_bets if b[5] > 0)
        print(f"\n  📈 CLOSING LINE VALUE:")
        print(f"     Average CLV: {avg_clv:+.2f}%")
        print(f"     Positive CLV rate: {positive_clv}/{len(clv_bets)} ({positive_clv/len(clv_bets)*100:.0f}%)")
        if avg_clv > 0:
            print(f"     ✅ Positive CLV = model is finding genuine edges")
        else:
            print(f"     ⚠️  Negative CLV = model may be chasing stale lines")

    # By confidence tier
    print(f"\n  📋 BY CONFIDENCE TIER:")
    for tier in ['HIGH', 'MEDIUM', 'LOW']:
        tier_bets = [b for b in bets if b[4] == tier]
        if not tier_bets:
            continue
        t_wins = sum(1 for b in tier_bets if b[0] == 'WIN')
        t_losses = sum(1 for b in tier_bets if b[0] == 'LOSS')
        t_profit = sum(b[1] * b[2] for b in tier_bets if b[1] and b[2])
        t_pct = t_wins / (t_wins + t_losses) * 100 if (t_wins + t_losses) > 0 else 0
        print(f"     {tier}: {t_wins}-{t_losses} ({t_pct:.1f}%) | Profit: {t_profit:+.2f}u")

    # By market type
    print(f"\n  📋 BY MARKET TYPE:")
    for mtype in ['SPREAD', 'MONEYLINE', 'TOTAL', 'PROP']:
        m_bets = [b for b in bets if b[6] == mtype]
        if not m_bets:
            continue
        m_wins = sum(1 for b in m_bets if b[0] == 'WIN')
        m_losses = sum(1 for b in m_bets if b[0] == 'LOSS')
        m_profit = sum(b[1] * b[2] for b in m_bets if b[1] and b[2])
        m_pct = m_wins / (m_wins + m_losses) * 100 if (m_wins + m_losses) > 0 else 0
        print(f"     {mtype}: {m_wins}-{m_losses} ({m_pct:.1f}%) | Profit: {m_profit:+.2f}u")

    # By sport
    sports_seen = set(b[7] for b in bets)
    if len(sports_seen) > 1:
        print(f"\n  📋 BY SPORT:")
        for sp in sorted(sports_seen):
            s_bets = [b for b in bets if b[7] == sp]
            s_wins = sum(1 for b in s_bets if b[0] == 'WIN')
            s_losses = sum(1 for b in s_bets if b[0] == 'LOSS')
            s_profit = sum(b[1] * b[2] for b in s_bets if b[1] and b[2])
            s_pct = s_wins / (s_wins + s_losses) * 100 if (s_wins + s_losses) > 0 else 0
            print(f"     {sp}: {s_wins}-{s_losses} ({s_pct:.1f}%) | Profit: {s_profit:+.2f}u")

    # Calibration check
    print(f"\n  🎯 MODEL CALIBRATION:")
    buckets = [(0.50, 0.55), (0.55, 0.60), (0.60, 0.65), (0.65, 0.70), (0.70, 0.80), (0.80, 1.0)]
    for low, high in buckets:
        bucket_bets = [b for b in bets if b[3] and low <= (b[3]/100 + 0.5) < high]  # rough
        if len(bucket_bets) < 5:
            continue
        b_wins = sum(1 for b in bucket_bets if b[0] == 'WIN')
        actual_pct = b_wins / len(bucket_bets) * 100
        expected_pct = (low + high) / 2 * 100
        print(f"     Predicted {low*100:.0f}-{high*100:.0f}%: Actual {actual_pct:.0f}% ({b_wins}/{len(bucket_bets)})")

    conn.close()
    print(f"\n{'='*70}")


def daily_pnl(days=7):
    """Show daily P&L for recent days."""
    conn = sqlite3.connect(DB_PATH)

    print(f"\n  📊 DAILY P&L (last {days} days):")
    print(f"  {'Date':>12} | {'W-L-P':>8} | {'Units':>8} | {'Profit':>8} | {'ROI':>8}")
    print(f"  {'-'*12} | {'-'*8} | {'-'*8} | {'-'*8} | {'-'*8}")

    for i in range(days):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        bets = conn.execute("""
            SELECT result, profit, units FROM bets
            WHERE created_at LIKE ? AND result IS NOT NULL
        """, (f"{date}%",)).fetchall()

        if not bets:
            continue

        w = sum(1 for b in bets if b[0] == 'WIN')
        l = sum(1 for b in bets if b[0] == 'LOSS')
        p = sum(1 for b in bets if b[0] == 'PUSH')
        profit = sum(b[1] * b[2] for b in bets if b[1] and b[2])
        risked = sum(b[2] for b in bets if b[2])
        roi = profit / risked * 100 if risked > 0 else 0

        print(f"  {date:>12} | {w}-{l}-{p:>1} {'':<3} | {risked:>7.1f}u | {profit:>+7.2f}u | {roi:>+6.1f}%")

    conn.close()


if __name__ == '__main__':
    performance_report()
    daily_pnl()
