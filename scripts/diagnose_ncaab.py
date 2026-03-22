"""
diagnose_ncaab.py — Show why more NCAAB games aren't making it through.

Runs the same logic as the model but prints every game and why it was
kept or filtered. Run this to see the full pipeline for today's games.

Usage: python diagnose_ncaab.py
"""
import sqlite3, os
from datetime import datetime, timedelta, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')


def diagnose():
    conn = sqlite3.connect(DB_PATH)
    sport = 'basketball_ncaab'

    from model_engine import (
        get_latest_ratings, compute_model_spread, SPORT_CONFIG,
        spread_to_cover_prob, american_to_implied_prob,
    )
    from scottys_edge import (
        scottys_edge_assessment, minimum_play_threshold, calculate_point_value
    )

    try:
        from elo_engine import get_elo_ratings, blended_spread
        HAS_ELO = True
    except:
        HAS_ELO = False

    ratings = get_latest_ratings(conn, sport)
    print(f"  NCAAB rated teams: {len(ratings)}")

    elo_data = {}
    if HAS_ELO:
        elo_data = get_elo_ratings(conn, sport)
        elo_conf = sum(1 for v in elo_data.values() if v.get('confidence') != 'LOW') if elo_data else 0
        print(f"  Elo teams: {elo_conf} with confidence")

    game_count = conn.execute(
        "SELECT COUNT(DISTINCT event_id) FROM market_consensus WHERE sport=?",
        (sport,)).fetchone()[0]
    is_thin = game_count < 30
    min_pv = minimum_play_threshold(sport, is_thin)
    print(f"  Total games in consensus: {game_count} ({'THIN → conservative' if is_thin else 'normal'})")
    print(f"  Threshold: {min_pv}% {'(8% × 1.5 = 12% conservative)' if is_thin else '(8% normal)'}")

    cfg = SPORT_CONFIG[sport]
    max_div = cfg['max_spread_divergence']
    print(f"  Max divergence: {max_div}")

    now_utc = datetime.now(timezone.utc)
    est_midnight = now_utc.replace(hour=5, minute=0, second=0, microsecond=0)
    if now_utc.hour >= 5:
        est_midnight += timedelta(days=1)
    window_start = (now_utc - timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
    window_end = est_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')

    games = conn.execute("""
        SELECT event_id, commence_time, home, away,
               best_home_spread, best_home_spread_odds, best_home_spread_book,
               best_away_spread, best_away_spread_odds, best_away_spread_book
        FROM market_consensus
        WHERE sport=? AND commence_time>=? AND commence_time<=?
        ORDER BY commence_time
    """, (sport, window_start, window_end)).fetchall()

    print(f"  Games today: {len(games)}")
    print(f"\n{'='*80}")
    print(f"  {'Game':<45s} {'Model':>6s} {'Market':>7s} {'Div':>5s} {'Edge':>6s} {'Status'}")
    print(f"{'='*80}")

    kept = 0
    no_rating = 0
    divergence = 0
    below_thresh = 0
    in_progress = 0
    low_prob_edge = 0

    for g in games:
        eid, commence, home, away = g[0], g[1], g[2], g[3]
        mkt_hs, mkt_hs_odds = g[4], g[5]
        mkt_as, mkt_as_odds = g[7], g[8]

        short = f"{away[:18]}@{home[:18]}"

        # Skip in-progress
        if commence:
            try:
                gt = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                if gt < now_utc - timedelta(minutes=30):
                    in_progress += 1
                    continue
            except:
                pass

        ms = compute_model_spread(home, away, ratings, sport)
        if ms is None:
            no_rating += 1
            print(f"  {short:<45s} {'N/A':>6s} {'':>7s} {'':>5s} {'':>6s} ❌ NO RATING")
            continue

        if HAS_ELO and elo_data:
            elo_ms = blended_spread(home, away, elo_data, ratings, sport, conn)
            if elo_ms is not None:
                ms = elo_ms

        if mkt_hs is None:
            print(f"  {short:<45s} {ms:>+6.1f} {'N/A':>7s} {'':>5s} {'':>6s} ❌ NO MARKET LINE")
            continue

        div = abs(ms - mkt_hs)
        if div > max_div:
            divergence += 1
            print(f"  {short:<45s} {ms:>+6.1f} {mkt_hs:>+7.1f} {div:>5.1f} {'':>6s} ❌ DIVERGENCE ({div:.1f} > {max_div})")
            continue

        # Check away spread (the dog side, which usually has the edge)
        if mkt_as is not None and mkt_as_odds is not None:
            wa = scottys_edge_assessment(
                -ms, mkt_as, mkt_as_odds, sport, None, 0, 0)
            pv = wa['point_value_pct']
            
            # Also check actual probability edge
            prob = spread_to_cover_prob(-ms, mkt_as, sport)
            imp = american_to_implied_prob(mkt_as_odds)
            prob_edge = (prob - imp) * 100 if imp else 0

            if pv >= min_pv and wa['is_play'] and prob_edge >= 1.0:
                kept += 1
                print(f"  {short:<45s} {ms:>+6.1f} {mkt_hs:>+7.1f} {div:>5.1f} {pv:>5.1f}% ✅ PLAY ({away} {mkt_as:+.1f})")
            elif pv >= min_pv and prob_edge < 1.0:
                low_prob_edge += 1
                print(f"  {short:<45s} {ms:>+6.1f} {mkt_hs:>+7.1f} {div:>5.1f} {pv:>5.1f}% ⚠️  PV passes but prob_edge={prob_edge:.1f}% too low")
            else:
                below_thresh += 1
                if pv > 3.0:  # Show near-misses
                    print(f"  {short:<45s} {ms:>+6.1f} {mkt_hs:>+7.1f} {div:>5.1f} {pv:>5.1f}% ❌ Below {min_pv}%")
        else:
            print(f"  {short:<45s} {ms:>+6.1f} {mkt_hs:>+7.1f} {'':>5s} {'':>6s} ❌ NO AWAY LINE")

    print(f"\n{'='*80}")
    print(f"  SUMMARY:")
    print(f"    Total games today:    {len(games)}")
    print(f"    In progress/started:  {in_progress}")
    print(f"    No rating:            {no_rating}")
    print(f"    Divergence filtered:  {divergence}")
    print(f"    Below threshold:      {below_thresh}")
    print(f"    Low prob edge:        {low_prob_edge}")
    print(f"    ✅ PLAYS:             {kept}")
    print(f"\n  If 'No rating' is high → need more teams rated")
    print(f"  If 'Divergence' is high → max_spread_divergence too tight")
    print(f"  If 'Below threshold' is high → threshold or model calibration issue")
    print(f"  If 'Low prob edge' is high → PV% inflates but real edge is thin")
    
    conn.close()


if __name__ == '__main__':
    diagnose()
