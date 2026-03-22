"""Debug: trace where picks disappear in the run pipeline"""
import sqlite3, os, sys
sys.path.insert(0, os.path.dirname(__file__))

db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(db)

from model_engine import generate_predictions

ALL_SPORTS = [
    'basketball_nba', 'basketball_ncaab', 'icehockey_nhl', 'baseball_ncaa',
]

# Step 1: What does generate_predictions return?
print("=" * 50)
print("STEP 1: generate_predictions output")
print("=" * 50)
game_picks = []
for sp in ALL_SPORTS:
    picks = generate_predictions(conn, sport=sp)
    if picks:
        print(f"\n  {sp}: {len(picks)} picks")
        for p in picks:
            print(f"    {p['units']:.1f}u  {p['edge_pct']:.1f}%  {p['selection']:40s} timing={p.get('timing','?')}  book={p.get('book','?')}")
    game_picks.extend(picks)

print(f"\nTotal game_picks: {len(game_picks)}")

if not game_picks:
    print("\n*** generate_predictions returned 0 picks — problem is in the model, not the filter ***")
    conn.close()
    exit()

# Step 2: What does _merge_and_select do to them?
print("\n" + "=" * 50)
print("STEP 2: _merge_and_select filter")
print("=" * 50)

from main import _merge_and_select
result = _merge_and_select(game_picks, [])
print(f"\nAfter _merge_and_select: {len(result)} picks")
for p in result:
    print(f"  {p['units']:.1f}u  {p['edge_pct']:.1f}%  {p['selection']}")

# Step 3: Trace each pick through the filter manually
print("\n" + "=" * 50)
print("STEP 3: Manual filter trace")
print("=" * 50)

try:
    from scottys_edge import SOFT_MARKETS, SHARP_MARKETS
except:
    SOFT_MARKETS = {'basketball_ncaab', 'soccer_usa_mls', 'soccer_germany_bundesliga', 
                    'soccer_france_ligue_one', 'soccer_italy_serie_a', 'soccer_uefa_champs_league',
                    'baseball_ncaa'}
    SHARP_MARKETS = {'basketball_nba', 'icehockey_nhl', 'soccer_epl', 'soccer_spain_la_liga'}

EXCLUDED_BOOKS = {'Bovada', 'BetOnline.ag', 'BetUS', 'MyBookie.ag', 'LowVig.ag'}

for p in game_picks:
    sel = p['selection']
    sport = p.get('sport', '')
    mtype = p.get('market_type', 'SPREAD')
    edge = p.get('edge_pct', 0)
    units = p.get('units', 0)
    timing = p.get('timing', 'EARLY')
    book = p.get('book', '')
    line = p.get('line')
    odds = p.get('odds', -110)
    is_soft = sport in SOFT_MARKETS
    is_sharp = sport in SHARP_MARKETS
    has_context = bool(p.get('context', ''))
    
    min_edge = {'TOTAL': 15.0, 'SPREAD': 13.0, 'MONEYLINE': 13.0}.get(mtype, 13.0)
    
    is_favorite = line is not None and line < 0
    early_penalty = False
    if timing == 'EARLY' and 'soccer' not in sport and not is_favorite:
        min_edge += 5.0
        early_penalty = True
    
    reasons = []
    
    if units < 3.0:
        reasons.append(f"units {units:.1f} < 3.0")
    if edge < min_edge:
        reasons.append(f"edge {edge:.1f}% < {min_edge:.1f}% min" + (" (EARLY +5%)" if early_penalty else ""))
    if is_soft and not has_context and edge < 20.0:
        reasons.append(f"soft market, no context, edge {edge:.1f}% < 20%")
    if mtype == 'MONEYLINE' and odds >= 500:
        reasons.append(f"blocked: ML odds +{odds} >= +500")
    if mtype == 'MONEYLINE' and 300 <= odds < 500 and edge < 25.0:
        reasons.append(f"big dog ML +{odds}, edge {edge:.1f}% < 25%")
    if mtype == 'SPREAD' and line and line > 0:
        if line <= 3.5 and not is_sharp and edge < 20.0:
            reasons.append(f"soft small dog, edge {edge:.1f}% < 20%")
        elif line <= 7.5:
            if is_sharp and edge < 15.0:
                reasons.append(f"sharp med dog, edge {edge:.1f}% < 15%")
            elif not is_sharp and edge < 17.0:
                reasons.append(f"soft med dog, edge {edge:.1f}% < 17%")
    if book in EXCLUDED_BOOKS:
        reasons.append(f"excluded book: {book}")
    
    status = "PASS" if not reasons else "FAIL"
    print(f"\n  {status}: {sel}")
    print(f"    {units:.1f}u | {edge:.1f}% edge | {mtype} | {sport} | timing={timing} | book={book} | line={line}")
    if reasons:
        for r in reasons:
            print(f"    ❌ {r}")

conn.close()
