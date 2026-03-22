"""
bootstrap_ratings.py v4 — Derive power ratings from ALL odds data.

Now pulls from raw odds table (not just market_consensus) to get
maximum game coverage. More games = better ratings.
"""
import sqlite3, math, os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# v12 FIX: Import from config.py — bootstrap was using pre-v12 HCA values
# (EPL 0.25 instead of 0.40, etc.) which skewed initial ratings.
from config import HOME_ADVANTAGE as HCA

from config import MAX_RATING


def american_to_implied_prob(odds):
    if odds is None or odds == 0: return None
    return 100.0/(odds+100.0) if odds > 0 else abs(odds)/(abs(odds)+100.0)


def ml_to_implied_spread(home_ml, away_ml, sport):
    h = american_to_implied_prob(home_ml)
    a = american_to_implied_prob(away_ml)
    if not h or not a: return None
    total = h + a
    hf = h / total
    if hf <= 0.01 or hf >= 0.99: return None
    scale = {'basketball_nba': 6.3, 'basketball_ncaab': 6.3,
             'icehockey_nhl': 0.49, 'soccer_epl': 0.40,
             'soccer_italy_serie_a': 0.40, 'soccer_spain_la_liga': 0.40,
             'soccer_germany_bundesliga': 0.40, 'soccer_france_ligue_one': 0.40,
             'soccer_uefa_champs_league': 0.40, 'soccer_usa_mls': 0.40,
             'baseball_ncaa': 1.8}
    s = scale.get(sport, 4.0)
    return round(-s * math.log(hf / (1 - hf)), 2)


def get_spread_data(conn, sport):
    """Get spread data from market_consensus AND raw odds."""
    spreads = []
    seen = set()

    # Source 1: market_consensus (best spreads already computed)
    rows = conn.execute("""
        SELECT home, away, best_home_spread, event_id
        FROM market_consensus WHERE sport=? AND best_home_spread IS NOT NULL
    """, (sport,)).fetchall()
    for h, a, sp, eid in rows:
        key = f"{h}|{a}"
        if key not in seen:
            seen.add(key)
            spreads.append((h, a, sp))

    # Source 2: raw odds table (spreads)
    rows = conn.execute("""
        SELECT DISTINCT home, away, AVG(line) as avg_line
        FROM odds WHERE sport=? AND market='spreads' AND selection=home AND line IS NOT NULL
        GROUP BY event_id
    """, (sport,)).fetchall()
    for h, a, sp in rows:
        key = f"{h}|{a}"
        if key not in seen:
            seen.add(key)
            spreads.append((h, a, sp))

    # Source 3: raw odds moneylines → implied spreads
    if len(spreads) < 10:
        rows = conn.execute("""
            SELECT o1.home, o1.away, AVG(o1.odds) as home_ml, AVG(o2.odds) as away_ml
            FROM (
                SELECT DISTINCT event_id, home, away, odds
                FROM odds WHERE sport=? AND market='h2h' AND selection=home
            ) o1
            JOIN (
                SELECT DISTINCT event_id, odds
                FROM odds WHERE sport=? AND market='h2h' AND selection=away
            ) o2 ON o1.event_id = o2.event_id
            GROUP BY o1.home, o1.away
        """, (sport, sport)).fetchall()
        for h, a, hml, aml in rows:
            key = f"{h}|{a}"
            if key not in seen:
                impl = ml_to_implied_spread(hml, aml, sport)
                if impl is not None:
                    seen.add(key)
                    spreads.append((h, a, impl))

    return spreads


def bootstrap_sport(conn, sport):
    """Derive ratings for one sport using averaging method."""
    spreads = get_spread_data(conn, sport)
    if len(spreads) < 3:
        print(f"    ⚠ Only {len(spreads)} games — need at least 3")
        return {}

    hca = HCA.get(sport, 2.0)
    teams = set()
    for h, a, _ in spreads:
        teams.add(h)
        teams.add(a)

    # Score each team from each game
    team_scores = {t: [] for t in teams}
    for home, away, mkt_spread in spreads:
        # mkt_spread from home perspective: negative = home favored
        # e.g., spread = -7 → home is favored by 7
        # home_advantage beyond HCA = (-spread) - hca
        raw_strength = (-mkt_spread) - hca  # positive when home is strong
        team_scores[home].append(raw_strength / 2)
        team_scores[away].append(-raw_strength / 2)

    # Average and normalize
    ratings = {}
    for t, scores in team_scores.items():
        ratings[t] = sum(scores) / len(scores) if scores else 0.0
    avg = sum(ratings.values()) / len(ratings)
    for t in ratings:
        ratings[t] -= avg

    # Clamp extremes
    cap = MAX_RATING.get(sport, 10)
    clamped = 0
    for t in ratings:
        if abs(ratings[t]) > cap:
            ratings[t] = cap if ratings[t] > 0 else -cap
            clamped += 1

    # Measure fit
    total_err = sum((mkt - (ratings.get(a, 0) - ratings.get(h, 0) - hca))**2
                     for h, a, mkt in spreads)
    rmse = math.sqrt(total_err / len(spreads))

    print(f"    ✅ {len(ratings)} teams from {len(spreads)} games (RMSE: {rmse:.2f})")
    if clamped:
        print(f"    Clamped {clamped} extreme ratings to ±{cap}")

    # Show top 3 / bottom 3
    ranked = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
    for t, r in ranked[:3]:
        print(f"    🏆 {t}: {r:+.2f}")
    print(f"    ...")
    for t, r in ranked[-3:]:
        print(f"    📉 {t}: {r:+.2f}")

    return {t: round(v, 3) for t, v in ratings.items()}


def save_ratings(conn, sport, ratings):
    now = datetime.now().isoformat()
    hca = HCA.get(sport, 2.0)

    # Remove placeholder/null ratings
    conn.execute("""
        DELETE FROM power_ratings
        WHERE sport=? AND (base_rating IS NULL OR base_rating = 0)
    """, (sport,))

    for team, rating in ratings.items():
        existing = conn.execute("""
            SELECT base_rating FROM power_ratings
            WHERE sport=? AND team=? AND base_rating IS NOT NULL AND base_rating != 0
            ORDER BY run_timestamp DESC LIMIT 1
        """, (sport, team)).fetchone()

        if existing:
            continue  # Don't overwrite existing fitted ratings

        conn.execute("""
            INSERT INTO power_ratings (run_timestamp, sport, team, base_rating,
                home_court, rest_adjust, injury_adjust, situational_adjust,
                manual_override, final_rating, games_used, iterations,
                learning_rate, regularization)
            VALUES (?,?,?,?,?,0,0,0,NULL,?,NULL,1,0,0)
        """, (now, sport, team, rating, hca, rating))
    conn.commit()


def _auto_rate_missing(conn, sport):
    """Auto-rate any team that appears in odds/consensus but has no power rating.
    
    Uses the team's market lines to estimate a quick rating.
    This is critical for NCAAB where 350+ teams exist but we may only
    have bootstrapped ~220 from early-season data.
    """
    # Find teams in recent odds with no rating
    missing = conn.execute("""
        SELECT DISTINCT mc.home FROM market_consensus mc
        LEFT JOIN power_ratings pr ON pr.team = mc.home AND pr.sport = mc.sport
        WHERE mc.sport=? AND (pr.base_rating IS NULL OR pr.team IS NULL)
        UNION
        SELECT DISTINCT mc.away FROM market_consensus mc
        LEFT JOIN power_ratings pr ON pr.team = mc.away AND pr.sport = mc.sport
        WHERE mc.sport=? AND (pr.base_rating IS NULL OR pr.team IS NULL)
    """, (sport, sport)).fetchall()

    if not missing:
        return

    missing_names = [m[0] for m in missing if m[0]]
    if not missing_names:
        return

    # Get avg rating for this sport to use as baseline
    avg = conn.execute("""
        SELECT AVG(base_rating) FROM power_ratings
        WHERE sport=? AND base_rating IS NOT NULL AND base_rating != 0
    """, (sport,)).fetchone()[0] or 0.0

    hca = HCA.get(sport, 2.0)
    now = datetime.now().isoformat()
    added = 0

    for team in missing_names:
        # Try to estimate from this team's spread lines
        lines = conn.execute("""
            SELECT home, away, best_home_spread
            FROM market_consensus
            WHERE sport=? AND (home=? OR away=?) AND best_home_spread IS NOT NULL
            ORDER BY snapshot_date DESC LIMIT 10
        """, (sport, team, team)).fetchall()

        if lines:
            # Estimate rating from market spreads vs known opponents
            scores = []
            for h, a, spread in lines:
                # Get opponent rating
                opp = a if h == team else h
                opp_rat = conn.execute("""
                    SELECT base_rating FROM power_ratings
                    WHERE sport=? AND team=? AND base_rating IS NOT NULL
                    ORDER BY run_timestamp DESC LIMIT 1
                """, (sport, opp)).fetchone()

                if opp_rat:
                    opp_r = opp_rat[0]
                    if h == team:
                        # team is home: spread negative = team favored
                        est = opp_r + (-spread) - hca
                    else:
                        # team is away
                        est = opp_r - (-spread) + hca
                    scores.append(est)

            if scores:
                rating = round(sum(scores) / len(scores), 3)
            else:
                rating = round(avg, 3)  # Fallback to league average
        else:
            rating = round(avg, 3)

        conn.execute("""
            INSERT INTO power_ratings (run_timestamp, sport, team, base_rating,
                home_court, rest_adjust, injury_adjust, situational_adjust,
                manual_override, final_rating, games_used, iterations,
                learning_rate, regularization)
            VALUES (?,?,?,?,?,0,0,0,NULL,?,NULL,1,0,0)
        """, (now, sport, team, rating, hca, rating))
        added += 1

    conn.commit()
    if added:
        print(f"    + Auto-rated {added} missing teams (from market lines)")


def bootstrap_all(conn=None):
    if conn is None:
        conn = sqlite3.connect(DB_PATH)
        close = True
    else:
        close = False

    print("=" * 60)
    print("  BOOTSTRAPPING POWER RATINGS FROM MARKET DATA")
    print("=" * 60)

    sports = list(HCA.keys())
    for sport in sports:
        count = conn.execute("""
            SELECT COUNT(*) FROM power_ratings
            WHERE sport=? AND base_rating IS NOT NULL AND base_rating != 0
        """, (sport,)).fetchone()[0]

        if count >= 10:
            print(f"\n  {sport}: already has {count} ratings — keeping")
            # But check for MISSING teams that appear in today's odds
            _auto_rate_missing(conn, sport)
            continue

        print(f"\n  {sport}:")
        ratings = bootstrap_sport(conn, sport)
        if ratings:
            save_ratings(conn, sport, ratings)

    # Summary
    print("\n" + "=" * 60)
    print("  RATINGS SUMMARY")
    print("=" * 60)
    for sport in sports:
        count = conn.execute("""
            SELECT COUNT(*) FROM power_ratings
            WHERE sport=? AND base_rating IS NOT NULL
        """, (sport,)).fetchone()[0]
        best = conn.execute("""
            SELECT team, base_rating FROM power_ratings
            WHERE sport=? AND base_rating IS NOT NULL
            ORDER BY base_rating DESC LIMIT 1
        """, (sport,)).fetchone()
        worst = conn.execute("""
            SELECT team, base_rating FROM power_ratings
            WHERE sport=? AND base_rating IS NOT NULL
            ORDER BY base_rating ASC LIMIT 1
        """, (sport,)).fetchone()
        s = "✅" if count >= 5 else "⚠"
        b_str = f"{best[0]} ({best[1]:+.2f})" if best else "none"
        w_str = f"{worst[0]} ({worst[1]:+.2f})" if worst else "none"
        print(f"  {s} {sport:30s} | {count:>3} teams | Best: {b_str}")

    if close:
        conn.close()


if __name__ == '__main__':
    bootstrap_all()
