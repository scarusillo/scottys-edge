"""Tennis Elo predictive-accuracy backtest.

Tennis results and odds use different event_id schemes, so we can't join
them for a classic FOLLOW/FADE backtest. Instead we ask the fundamental
question: does our surface-split Elo correctly pick tennis match winners?

If yes (>55% WR), the model has real predictive signal — live firing
can be expected to find edge vs market when markets misprice.
If no (~50% WR), the model is coin-flip — no fires should go live.

Splits: by tour (ATP/WTA), by surface (hard/clay/grass).
"""
import sqlite3
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from elo_engine import get_tennis_elo

DB = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(DB)
c = conn.cursor()


def infer_surface_from_tournament(tournament_key):
    t = tournament_key.lower()
    CLAY = ['french_open', 'roland_garros', 'monte_carlo', 'madrid', 'italian_open',
            'rome', 'barcelona', 'hamburg', 'rio', 'buenos_aires', 'lyon', 'bastad',
            'kitzbuhel', 'umag', 'gstaad', 'geneva', 'marrakech', 'bucharest', 'parma',
            'palermo', 'prague', 'rabat', 'strasbourg', 'lausanne', 'portoroz',
            'bogota', 'istanbul', 'budapest', 'argentina', 'chile', 'tirante']
    GRASS = ['wimbledon', 'queens', 'halle', 'eastbourne', 'berlin', 'bad_homburg',
             'nottingham', 'mallorca', 'birmingham', 'libema']
    if any(kw in t for kw in CLAY): return 'clay'
    if any(kw in t for kw in GRASS): return 'grass'
    return 'hard'


# Pull completed tennis matches with winner known
rows = c.execute("""
    SELECT sport, home, away, home_score, away_score, commence_time
    FROM results
    WHERE sport LIKE 'tennis%' AND completed=1
      AND home_score IS NOT NULL AND away_score IS NOT NULL
      AND commence_time >= date('now','-120 days')
""").fetchall()
print(f'Matches pulled (last 120 days): {len(rows)}')

# Per-match: who was Elo favorite vs actual winner
buckets = defaultdict(lambda: {'n': 0, 'correct': 0, 'total_confidence': 0})
by_surface_tour = defaultdict(lambda: {'n': 0, 'correct': 0})
skipped_no_elo = 0

# Cache Elo lookups per (tour, surface) to avoid 1k DB hits
elo_cache = {}
def get_surface_elo(tour, surface):
    key = (tour, surface)
    if key not in elo_cache:
        sp = f'tennis_{tour}_{surface}'
        # Pretend-tournament key so get_tennis_elo routes correctly
        dummy_tourney = f'tennis_{tour}_fake_{surface}_tournament_key'
        elo, elo_key = get_tennis_elo(conn, dummy_tourney)
        # Override with exact surface if inference went wrong
        if elo_key != sp:
            c2 = conn.execute("SELECT team, elo, games_played, confidence, sos FROM elo_ratings WHERE sport=?", (sp,)).fetchall()
            elo = {r[0]: {'elo': r[1], 'games_played': r[2], 'confidence': r[3] or 'LOW', 'sos': r[4] or 0.5} for r in c2}
        elo_cache[key] = elo
    return elo_cache[key]


# Accept both tennis_atp_X and tennis_wta_X naming
for sport, home, away, hs, as_, commence in rows:
    if '_atp_' in sport:
        tour = 'atp'
    elif '_wta_' in sport:
        tour = 'wta'
    else:
        continue
    # Tournament key → surface
    surface = infer_surface_from_tournament(sport)
    elo = get_surface_elo(tour, surface)
    h = elo.get(home)
    a = elo.get(away)
    if h is None or a is None:
        skipped_no_elo += 1
        continue
    h_elo = h['elo']
    a_elo = a['elo']
    h_conf = h['confidence']
    a_conf = a['confidence']
    # Elo says who favored?
    if h_elo > a_elo:
        elo_pick = home
        elo_diff = h_elo - a_elo
    elif a_elo > h_elo:
        elo_pick = away
        elo_diff = a_elo - h_elo
    else:
        continue  # exact tie, skip
    # Who actually won? (higher score in sets-based tennis)
    if hs > as_: winner = home
    elif as_ > hs: winner = away
    else: continue  # skip if somehow equal

    # Bucket by confidence bucket and elo diff magnitude
    min_conf = 'LOW' if 'LOW' in (h_conf, a_conf) else ('MEDIUM' if 'MEDIUM' in (h_conf, a_conf) else 'HIGH')
    diff_bucket = 'tiny(<20)' if elo_diff < 20 else ('small(20-50)' if elo_diff < 50 else ('medium(50-100)' if elo_diff < 100 else 'large(100+)'))

    key = (tour, surface)
    by_surface_tour[key]['n'] += 1
    if elo_pick == winner:
        by_surface_tour[key]['correct'] += 1

    conf_key = (tour, min_conf)
    buckets[conf_key]['n'] += 1
    if elo_pick == winner:
        buckets[conf_key]['correct'] += 1

    diff_key = (tour, diff_bucket)
    buckets[diff_key]['n'] += 1
    if elo_pick == winner:
        buckets[diff_key]['correct'] += 1

print(f'Skipped (no Elo for one/both players): {skipped_no_elo}')
print()
print('=== Elo winner-prediction accuracy — by TOUR × SURFACE ===')
print(f'{"Tour":<6} {"Surface":<7} {"N":>5} {"Correct":>8} {"WR":>6}')
for (tour, surface), s in sorted(by_surface_tour.items()):
    wr = s['correct'] / s['n'] * 100 if s['n'] else 0
    mark = '✅' if wr >= 55 else ('⚠️ ' if wr >= 52 else '❌')
    print(f'  {tour.upper():<4} {surface:<7} {s["n"]:>5} {s["correct"]:>8} {wr:>5.1f}% {mark}')

print()
print('=== By min-confidence of the two players (weakest-rated side) ===')
for (tour, conf), s in sorted(buckets.items(), key=lambda x: x[0]):
    if conf not in ('LOW', 'MEDIUM', 'HIGH'): continue
    wr = s['correct'] / s['n'] * 100 if s['n'] else 0
    print(f'  {tour.upper()} min_conf={conf:<7} n={s["n"]:<4} correct={s["correct"]:<4} WR={wr:.1f}%')

print()
print('=== By Elo-diff bucket (tennis Elo spreads of 20/50/100+ reflect confidence) ===')
for (tour, bkt), s in sorted(buckets.items(), key=lambda x: (x[0][0], x[0][1])):
    if 'tiny' not in bkt and 'small' not in bkt and 'medium' not in bkt and 'large' not in bkt:
        continue
    wr = s['correct'] / s['n'] * 100 if s['n'] else 0
    ev_per_match = (wr/100 - 0.524) if wr else 0  # Rough EV vs -110 breakeven
    marker = '🔥 BEATS JUICE' if wr >= 55 else ''
    print(f'  {tour.upper()} {bkt:<12} n={s["n"]:<4} correct={s["correct"]:<4} WR={wr:.1f}%  {marker}')

conn.close()
