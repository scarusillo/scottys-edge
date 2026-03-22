"""Check every sport's classification and what filters apply"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from config import SOFT_MARKETS, SHARP_MARKETS, PLAY_THRESHOLDS, SPORT_CONFIG

ALL_SPORTS = [
    'basketball_nba', 'basketball_ncaab', 'icehockey_nhl',
    'baseball_ncaa', 'soccer_epl', 'soccer_italy_serie_a',
    'soccer_spain_la_liga', 'soccer_germany_bundesliga',
    'soccer_france_ligue_one', 'soccer_uefa_champs_league', 'soccer_usa_mls',
]

print("=" * 70)
print("  SPORT CLASSIFICATION CHECK")
print("=" * 70)

issues = []

for sport in ALL_SPORTS:
    label = sport.split('_')[-1].upper()
    if 'ncaab' in sport: label = 'NCAAB'
    elif 'nba' in sport: label = 'NBA'
    elif 'nhl' in sport: label = 'NHL'
    elif 'ncaa' in sport and 'baseball' in sport: label = 'BASEBALL'
    
    in_soft = sport in SOFT_MARKETS
    in_sharp = sport in SHARP_MARKETS
    
    if not in_soft and not in_sharp:
        tier = "MISSING"
        issues.append(f"{label}: Not in SOFT or SHARP — will default unpredictably")
    elif in_soft and in_sharp:
        tier = "BOTH"
        issues.append(f"{label}: In BOTH SOFT and SHARP — conflict")
    elif in_soft:
        tier = "SOFT"
    else:
        tier = "SHARP"
    
    threshold = PLAY_THRESHOLDS.get(sport, '???')
    divergence = SPORT_CONFIG.get(sport, {}).get('max_spread_divergence', '???')
    
    # What the merge filter requires
    if tier == "SOFT":
        spread_min = "13% (18% early, 20% small dog no ctx)"
    elif tier == "SHARP":
        spread_min = "13% (18% early)"
    else:
        spread_min = "UNKNOWN"
    
    print(f"\n  {label:15s} | {tier:6s} | threshold: {threshold}% | divergence: {divergence}")
    print(f"  {'':15s} | merge filter: {spread_min}")

if issues:
    print(f"\n  {'='*50}")
    print(f"  ISSUES FOUND:")
    for i in issues:
        print(f"    ! {i}")
else:
    print(f"\n  All sports correctly classified.")

# Show what each tier means in practice
print(f"\n  {'='*50}")
print(f"  WHAT EACH TIER MEANS:")
print(f"    SOFT: Small dogs need 20% edge (no context) or context-confirmed")
print(f"    SOFT: Med dogs (4-7.5) need 17% edge")  
print(f"    SHARP: Small dogs use normal 13% threshold")
print(f"    SHARP: Med dogs need 15% edge")
print(f"    BOTH: Early bets get +5% penalty (EARLY non-fav non-soccer)")
print(f"    BOTH: Max 4 sharp picks per run, max 10 soft picks per run")
