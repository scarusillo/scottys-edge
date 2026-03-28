"""
Analyze the FULL universe of edges - not just what we bet, but everything
the model evaluated. Find where we are leaving money on the table.
"""
import sqlite3
import os
from math import erf, sqrt

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

SPORT_STD = {
    'basketball_nba': 11.0, 'basketball_ncaab': 10.5,
    'icehockey_nhl': 1.5, 'baseball_ncaa': 3.5, 'baseball_mlb': 3.5,
    'soccer_epl': 1.2, 'soccer_italy_serie_a': 1.2, 'soccer_spain_la_liga': 1.2,
    'soccer_germany_bundesliga': 1.2, 'soccer_france_ligue_one': 1.2,
    'soccer_usa_mls': 1.2, 'soccer_mexico_ligamx': 1.2,
    'soccer_uefa_champs_league': 1.2,
}


def calc_edge(diff, std):
    if std == 0:
        return 0
    z = diff / std
    prob = 0.5 * (1 + erf(z / sqrt(2)))
    return max(0, (prob - 0.5) * 100 * 2)


def analyze():
    conn = sqlite3.connect(DB)

    # All games with model prediction AND completed result
    games = conn.execute("""
        SELECT mc.event_id, mc.sport, mc.home, mc.away,
               mc.model_spread, mc.model_total, mc.consensus_spread, mc.consensus_total,
               mc.best_home_spread, mc.best_away_spread, mc.best_over_total, mc.best_under_total,
               r.home_score, r.away_score, r.actual_total, r.actual_margin,
               mc.commence_time
        FROM market_consensus mc
        JOIN results r ON mc.event_id = r.event_id AND r.completed = 1
        WHERE mc.snapshot_date >= '2026-03-21' AND mc.tag = 'CURRENT'
          AND mc.model_spread IS NOT NULL AND r.home_score IS NOT NULL
        GROUP BY mc.event_id
    """).fetchall()

    bet_events = set(r[0] for r in conn.execute(
        "SELECT DISTINCT event_id FROM bets WHERE created_at >= '2026-03-21'"
    ).fetchall())

    print("=" * 90)
    print("  FULL EDGE UNIVERSE - Games with model prediction + result")
    print("=" * 90)
    print(f"\n  {len(games)} games evaluated | {len(bet_events)} bet on")

    by_edge = {}
    by_sport = {}
    by_market = {}
    passed_winners = []

    for g in games:
        eid, sport, home, away, model_sp, model_tot, con_sp, con_tot, \
        bhs, bas, bot, but, h_score, a_score, actual_tot, actual_margin, commence = g

        was_bet = eid in bet_events
        actual_margin = actual_margin or (h_score - a_score)
        actual_tot = actual_tot or (h_score + a_score)
        std = SPORT_STD.get(sport, 2.0)

        # SPREAD analysis
        if model_sp is not None and con_sp is not None:
            spread_diff = abs(model_sp - con_sp)
            edge = calc_edge(spread_diff, std)

            if edge >= 3:
                if model_sp < con_sp:
                    line = bhs or con_sp
                    won = actual_margin > -line if line else False
                    sel = f"{home} {line:+.1f}" if line else home
                else:
                    line = bas or -con_sp
                    won = actual_margin < line if line else False
                    sel = f"{away} {line:+.1f}" if line else away

                bucket = "bet" if was_bet else "passed"
                eb = "20%+" if edge >= 20 else "15-20%" if edge >= 15 else "10-15%" if edge >= 10 else "5-10%" if edge >= 5 else "3-5%"

                by_edge[(eb, bucket, won)] = by_edge.get((eb, bucket, won), 0) + 1
                by_sport[(sport, bucket, won)] = by_sport.get((sport, bucket, won), 0) + 1
                by_market[("SPREAD", bucket, won)] = by_market.get(("SPREAD", bucket, won), 0) + 1

                if not was_bet and won:
                    passed_winners.append(("SPREAD", sel, sport, edge, actual_margin, commence[:10]))

        # TOTAL analysis
        if model_tot is not None and con_tot is not None:
            tot_diff = abs(model_tot - con_tot)
            edge = calc_edge(tot_diff, std * 1.2)

            if edge >= 3:
                line = bot or con_tot
                if model_tot > con_tot:
                    won = actual_tot > line if line else False
                    sel = f"{away}@{home} O{line}"
                    mkt = "OVER"
                else:
                    won = actual_tot < line if line else False
                    sel = f"{away}@{home} U{line}"
                    mkt = "UNDER"

                bucket = "bet" if was_bet else "passed"
                eb = "20%+" if edge >= 20 else "15-20%" if edge >= 15 else "10-15%" if edge >= 10 else "5-10%" if edge >= 5 else "3-5%"

                by_edge[(eb, bucket, won)] = by_edge.get((eb, bucket, won), 0) + 1
                by_sport[(sport, bucket, won)] = by_sport.get((sport, bucket, won), 0) + 1
                by_market[(mkt, bucket, won)] = by_market.get((mkt, bucket, won), 0) + 1

                if not was_bet and won:
                    passed_winners.append((mkt, sel, sport, edge, actual_tot, commence[:10]))

    # Print results
    print("\n" + "=" * 90)
    print("  BET vs PASSED - Win Rates by Edge Size")
    print("=" * 90)
    print(f"\n  {'Bucket':12s} | {'BET':18s} | {'PASSED':18s} | Missed?")
    print(f"  {'-'*12} | {'-'*18} | {'-'*18} | {'-'*10}")

    for eb in ["3-5%", "5-10%", "10-15%", "15-20%", "20%+"]:
        bw = by_edge.get((eb, "bet", True), 0)
        bl = by_edge.get((eb, "bet", False), 0)
        pw = by_edge.get((eb, "passed", True), 0)
        pl = by_edge.get((eb, "passed", False), 0)
        bt, pt = bw + bl, pw + pl
        bwr = f"{bw}/{bt} ({bw/bt*100:.0f}%)" if bt > 0 else "---"
        pwr = f"{pw}/{pt} ({pw/pt*100:.0f}%)" if pt > 0 else "---"
        flag = "YES" if pt > 0 and pw / pt > 0.52 else ""
        print(f"  {eb:12s} | {bwr:18s} | {pwr:18s} | {flag}")

    print(f"\n  -- By Sport (Passed edges with 3+ games) --")
    all_sp = set(k[0] for k in by_sport if k[1] == "passed")
    for s in sorted(all_sp):
        w = by_sport.get((s, "passed", True), 0)
        l = by_sport.get((s, "passed", False), 0)
        t = w + l
        if t >= 3:
            sp = s.replace("basketball_", "").replace("icehockey_", "").replace("baseball_", "").replace("soccer_", "")
            tag = " << PROFITABLE" if w / t > 0.55 else ""
            print(f"  {sp:28s}: {w:3d}W-{l:3d}L ({w/t*100:.0f}%){tag}")

    print(f"\n  -- By Market Type (Passed Only) --")
    for mkt in ["SPREAD", "OVER", "UNDER"]:
        w = by_market.get((mkt, "passed", True), 0)
        l = by_market.get((mkt, "passed", False), 0)
        t = w + l
        if t > 0:
            print(f"  {mkt:10s}: {w:3d}W-{l:3d}L ({w/t*100:.0f}%)")

    print(f"\n  -- Top 20 Missed Winners (highest edge) --")
    passed_winners.sort(key=lambda x: -x[3])
    for p in passed_winners[:20]:
        mkt, sel, sport, edge, actual, date = p
        sp = sport.replace("basketball_", "").replace("icehockey_", "").replace("baseball_", "").replace("soccer_", "")
        print(f"  {date} | {sel[:45]:45s} | {sp:15s} | edge={edge:5.1f}%")

    conn.close()


if __name__ == "__main__":
    analyze()
