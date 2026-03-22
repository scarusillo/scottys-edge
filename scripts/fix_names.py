"""
fix_names.py — Diagnose and fix ESPN ↔ Odds API team name mismatches

The #1 reason Elo ratings are wrong is name mismatches. ESPN says "LA Clippers"
but the Odds API says "Los Angeles Clippers" — so the Elo engine treats them
as two different teams. This tool:

  1. DIAGNOSE: Shows all unmatched names side-by-side
  2. AUTO-MAP: Fuzzy-matches ESPN names to Odds API names  
  3. HARDCODED: Known mismatches that fuzzy matching can't catch
  4. FILTER: Removes wrong-league teams (e.g. Burnley in EPL data)
  5. FIX: Updates all results in the database
  6. REBUILD: Re-runs Elo engine with clean names

Usage:
    python fix_names.py                  # Full diagnostic + fix + rebuild
    python fix_names.py --diagnose       # Just show mismatches (no changes)
    python fix_names.py --sport nba      # Fix one sport only
"""
import sqlite3, os, sys, re
from difflib import SequenceMatcher
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')

# ══════════════════════════════════════════════════════════════
#  HARDCODED MAPPINGS: ESPN displayName → Odds API name
#  These are cases fuzzy matching CANNOT solve reliably.
#  Add new mappings here as you discover them.
# ══════════════════════════════════════════════════════════════

HARDCODED_MAPS = {
    'basketball_nba': {
        'LA Clippers': 'Los Angeles Clippers',
        'LA Lakers': 'Los Angeles Lakers',
    },
    'icehockey_nhl': {
        'Montréal Canadiens': 'Montreal Canadiens',
        'Montreal Canadiens': 'Montreal Canadiens',
        'Utah Hockey Club': 'Utah Hockey Club',
        'St. Louis Blues': 'St Louis Blues',  # Period vs no period
    },
    'basketball_ncaab': {
        'UConn Huskies': 'Connecticut Huskies',
        'Pitt Panthers': 'Pittsburgh Panthers',
        'USC Trojans': 'Southern California Trojans',
        'UNLV Rebels': 'Nevada-Las Vegas Rebels',
        'LSU Tigers': 'LSU Tigers',
        'UCF Knights': 'Central Florida Knights',
        'UNC Tar Heels': 'North Carolina Tar Heels',
        'UNC Wilmington Seahawks': 'North Carolina Wilmington Seahawks',
        'UNC Greensboro Spartans': 'UNC Greensboro Spartans',
        'UNC Asheville Bulldogs': 'UNC Asheville Bulldogs',
        'SMU Mustangs': 'Southern Methodist Mustangs',
        'UTSA Roadrunners': 'Texas-San Antonio Roadrunners',
        'UT Arlington Mavericks': 'Texas-Arlington Mavericks',
        'UT Martin Skyhawks': 'Tennessee-Martin Skyhawks',
        'UTEP Miners': 'Texas-El Paso Miners',
        'UAB Blazers': 'Alabama-Birmingham Blazers',
        'VCU Rams': 'Virginia Commonwealth Rams',
        'BYU Cougars': 'Brigham Young Cougars',
        'TCU Horned Frogs': 'Texas Christian Horned Frogs',
        'Ole Miss Rebels': 'Mississippi Rebels',
        'ETSU Buccaneers': 'East Tennessee State Buccaneers',
        'SIU Edwardsville Cougars': 'SIU-Edwardsville Cougars',
        'SIU-Edwardsville Cougars': 'SIU-Edwardsville Cougars',
        'UMass Minutemen': 'Massachusetts Minutemen',
        'UMass Lowell River Hawks': 'Massachusetts-Lowell River Hawks',
        'FIU Panthers': 'Florida International Panthers',
        'FDU Knights': 'Fairleigh Dickinson Knights',
        'SIUE Cougars': 'SIU-Edwardsville Cougars',
        'Little Rock Trojans': 'Arkansas-Little Rock Trojans',
        'Omaha Mavericks': 'Nebraska-Omaha Mavericks',
        'LIU Sharks': 'Long Island University Sharks',
        'NIU Huskies': 'Northern Illinois Huskies',
        'IUPUI Jaguars': 'Indiana University-Purdue University Indianapolis Jaguars',
        'Saint Mary\'s Gaels': 'Saint Mary\'s Gaels',
        "St. John's Red Storm": "St. John's (NY) Red Storm",
        'Miami Hurricanes': 'Miami (FL) Hurricanes',
        'Miami (OH) RedHawks': 'Miami (OH) RedHawks',
        # From diagnostic: block bad fuzzy matches
        'Southern Jaguars': 'Southern Jaguars',             # Southern University, NOT South Alabama
        # From diagnostic: additional mappings for unmatched teams
        'Massachusetts Minutemen': 'Massachusetts Minutemen',
        'IU Indianapolis Jaguars': 'IU Indianapolis Jaguars',  # Formerly IUPUI, rebranded
        'Cornell Big Red': 'Cornell Big Red',
        'Duquesne Dukes': 'Duquesne Dukes',
        'Grand Canyon Lopes': 'Grand Canyon Antelopes',
        'Holy Cross Crusaders': 'Holy Cross Crusaders',
        'Howard Bison': 'Howard Bison',
        'Jacksonville Dolphins': 'Jacksonville Dolphins',
        'James Madison Dukes': 'James Madison Dukes',
        'La Salle Explorers': 'La Salle Explorers',
        'Lafayette Leopards': 'Lafayette Leopards',
        'Navy Midshipmen': 'Navy Midshipmen',
        'Nevada Wolf Pack': 'Nevada Wolf Pack',
        'New Mexico Lobos': 'New Mexico Lobos',
        'New Orleans Privateers': 'New Orleans Privateers',
        'Nicholls Colonels': 'Nicholls State Colonels',
        'Ohio Bobcats': 'Ohio Bobcats',
        'Pennsylvania Quakers': 'Pennsylvania Quakers',
        'Quinnipiac Bobcats': 'Quinnipiac Bobcats',
        'Rice Owls': 'Rice Owls',
        'Rutgers Scarlet Knights': 'Rutgers Scarlet Knights',
        'South Carolina Gamecocks': 'South Carolina Gamecocks',
        'South Florida Bulls': 'South Florida Bulls',
        'Southern Miss Golden Eagles': 'Southern Miss Golden Eagles',
        'Stony Brook Seawolves': 'Stony Brook Seawolves',
        'Temple Owls': 'Temple Owls',
        'Tulsa Golden Hurricane': 'Tulsa Golden Hurricane',
        'UAlbany Great Danes': 'Albany Great Danes',
        'Loyola Chicago Ramblers': 'Loyola (Chi) Ramblers',
    },
    'soccer_epl': {
        'Tottenham Hotspur': 'Tottenham Hotspur',
        'Wolverhampton Wanderers': 'Wolverhampton Wanderers',
        'Nottingham Forest': 'Nottingham Forest',
        'Brighton & Hove Albion': 'Brighton and Hove Albion',
        'Brighton and Hove Albion': 'Brighton and Hove Albion',
        'Newcastle United': 'Newcastle United',
        'Leicester City': 'Leicester City',
        # 2025-26 promoted teams — keep as-is unless API uses different name
        'Burnley': 'Burnley',
        'Leeds United': 'Leeds United',
        'Sunderland': 'Sunderland',
    },
    'soccer_italy_serie_a': {
        'Internazionale': 'Inter Milan',
        'Inter Miami CF': None,  # Wrong league — MLS, not Serie A
        'AS Roma': 'AS Roma',
        'AC Milan': 'AC Milan',
        'Hellas Verona': 'Hellas Verona',
        # 2025-26 promoted teams
        'Sassuolo': 'Sassuolo',
        'Cremonese': 'Cremonese',
        'Pisa': 'Pisa',
    },
    'soccer_spain_la_liga': {
        'Atlético Madrid': 'Atletico Madrid',
        'Atlético de Madrid': 'Atletico Madrid',
        'Athletic Club': 'Athletic Bilbao',
        'Athletic Bilbao': 'Athletic Bilbao',
        'Deportivo Alavés': 'Deportivo Alaves',
        'Cádiz CF': 'Cadiz',
        'Real Sociedad': 'Real Sociedad',
        'Rayo Vallecano': 'Rayo Vallecano',
        # 2025-26 promoted teams
        'Levante': 'Levante',
        'Oviedo': 'Oviedo',
        'Elche CF': 'Elche',
        'Elche': 'Elche',
    },
}

# ══════════════════════════════════════════════════════════════
#  WRONG-LEAGUE FILTERS
#  
#  REMOVED in v11.1 — ESPN's league-specific endpoints (eng.1,
#  ita.1, esp.1) already return ONLY teams in that league.
#  If ESPN returns a team under eng.1, that team is in the EPL.
#  Hardcoding promotion/relegation is fragile and wrong.
#
#  The only teams to filter are non-competitive (All-Star, etc.)
# ══════════════════════════════════════════════════════════════

WRONG_LEAGUE_TEAMS = {
    'basketball_nba': [
        # All-Star / Rising Stars game teams
        'Team Stars', 'Team Stripes', 'World', 'USA',
        'Team LeBron', 'Team Durant', 'Team Giannis', 'Team Stephen',
    ],
    'basketball_ncaab': [],
    'icehockey_nhl': [
        'Atlantic', 'Metropolitan', 'Central', 'Pacific',  # All-Star teams
    ],
    'soccer_epl': [],
    'soccer_italy_serie_a': [],
    'soccer_spain_la_liga': [],
}


def _get_api_teams(conn, sport):
    """Get all team names the Odds API uses (from market_consensus + power_ratings)."""
    teams = set()
    for table in ['market_consensus', 'power_ratings']:
        try:
            if table == 'market_consensus':
                rows = conn.execute(f"""
                    SELECT DISTINCT home FROM {table} WHERE sport=?
                    UNION SELECT DISTINCT away FROM {table} WHERE sport=?
                """, (sport, sport)).fetchall()
            else:
                rows = conn.execute(f"""
                    SELECT DISTINCT team FROM {table} WHERE sport=?
                """, (sport,)).fetchall()
            for r in rows:
                if r[0]:
                    teams.add(r[0])
        except:
            pass
    return teams


def _get_espn_teams(conn, sport):
    """Get all team names from ESPN results."""
    rows = conn.execute("""
        SELECT DISTINCT home FROM results WHERE sport=?
        UNION SELECT DISTINCT away FROM results WHERE sport=?
    """, (sport, sport)).fetchall()
    return set(r[0] for r in rows if r[0])


def _fuzzy_match(name, candidates, threshold=0.70):
    """Find best fuzzy match for a name among candidates."""
    best_match = None
    best_score = 0
    
    name_lower = name.lower().strip()
    
    for candidate in candidates:
        cand_lower = candidate.lower().strip()
        
        # Exact match
        if name_lower == cand_lower:
            return candidate, 1.0
        
        # Try full string similarity
        score = SequenceMatcher(None, name_lower, cand_lower).ratio()
        
        # Also try matching just the mascot (last word)
        name_parts = name_lower.split()
        cand_parts = cand_lower.split()
        if name_parts and cand_parts:
            mascot_score = SequenceMatcher(None, name_parts[-1], cand_parts[-1]).ratio()
            # If mascots match perfectly and city is similar, boost score
            if mascot_score > 0.95:
                city_a = ' '.join(name_parts[:-1])
                city_b = ' '.join(cand_parts[:-1])
                city_score = SequenceMatcher(None, city_a, city_b).ratio()
                combined = 0.4 * city_score + 0.6 * mascot_score
                score = max(score, combined)
        
        if score > best_score:
            best_score = score
            best_match = candidate
    
    if best_score >= threshold:
        return best_match, best_score
    return None, best_score


def _is_wrong_league(team_name, sport):
    """Check if a team shouldn't be in this league."""
    wrong_teams = WRONG_LEAGUE_TEAMS.get(sport, [])
    name_lower = team_name.lower()
    for wrong in wrong_teams:
        if wrong.lower() in name_lower or name_lower in wrong.lower():
            return True
    return False


def diagnose_sport(conn, sport, verbose=True):
    """
    Diagnose name mismatches for one sport.
    
    Returns: {
        'matched': {espn_name: api_name},
        'unmatched_espn': [names in results but not in API],
        'unmatched_api': [names in API but not in results],
        'wrong_league': [names that shouldn't be in this league],
        'auto_mapped': {espn_name: (api_name, confidence_score)},
    }
    """
    espn_teams = _get_espn_teams(conn, sport)
    api_teams = _get_api_teams(conn, sport)
    hardcoded = HARDCODED_MAPS.get(sport, {})
    
    if not espn_teams:
        if verbose:
            print(f"  ⚠ No ESPN results for {sport}")
        return None
    
    matched = {}
    wrong_league = []
    auto_mapped = {}
    
    for espn_name in sorted(espn_teams):
        # Check wrong league first
        if _is_wrong_league(espn_name, sport):
            wrong_league.append(espn_name)
            continue
        
        # Check hardcoded mapping
        if espn_name in hardcoded:
            mapped = hardcoded[espn_name]
            if mapped is None:
                wrong_league.append(espn_name)
            else:
                matched[espn_name] = mapped
            continue
        
        # Check exact match
        if espn_name in api_teams:
            matched[espn_name] = espn_name
            continue
        
        # Try fuzzy match
        best, score = _fuzzy_match(espn_name, api_teams)
        if best and score >= 0.75:
            auto_mapped[espn_name] = (best, score)
        # else: unmatched
    
    # Find truly unmatched
    all_mapped_espn = set(matched.keys()) | set(auto_mapped.keys()) | set(wrong_league)
    unmatched_espn = sorted(espn_teams - all_mapped_espn)
    
    all_mapped_api = set(matched.values()) | set(v[0] for v in auto_mapped.values())
    unmatched_api = sorted(api_teams - all_mapped_api)
    
    result = {
        'matched': matched,
        'auto_mapped': auto_mapped,
        'unmatched_espn': unmatched_espn,
        'unmatched_api': unmatched_api,
        'wrong_league': wrong_league,
    }
    
    if verbose:
        _print_diagnosis(sport, result, espn_teams, api_teams)
    
    return result


def _print_diagnosis(sport, diag, espn_teams, api_teams):
    """Pretty-print diagnosis results."""
    print(f"\n  {'═' * 60}")
    print(f"  {sport.upper()}")
    print(f"  {'═' * 60}")
    print(f"  ESPN teams: {len(espn_teams)}  |  API teams: {len(api_teams)}")
    
    # Exact + hardcoded matches
    print(f"\n  ✅ MATCHED ({len(diag['matched'])})")
    non_trivial = {k: v for k, v in diag['matched'].items() if k != v}
    if non_trivial:
        for espn, api in sorted(non_trivial.items()):
            print(f"     {espn:40s} → {api}")
    
    # Auto-mapped (fuzzy)
    if diag['auto_mapped']:
        print(f"\n  🔄 AUTO-MAPPED — fuzzy match ({len(diag['auto_mapped'])})")
        for espn, (api, score) in sorted(diag['auto_mapped'].items()):
            conf = "HIGH" if score >= 0.90 else ("MED" if score >= 0.80 else "LOW")
            flag = " ⚠ CHECK" if conf == "LOW" else ""
            print(f"     {espn:40s} → {api:40s} ({score:.0%} {conf}){flag}")
    
    # Wrong league
    if diag['wrong_league']:
        print(f"\n  🚫 WRONG LEAGUE — will be removed ({len(diag['wrong_league'])})")
        for t in sorted(diag['wrong_league']):
            print(f"     {t}")
    
    # Unmatched ESPN
    if diag['unmatched_espn']:
        print(f"\n  ❌ UNMATCHED ESPN NAMES ({len(diag['unmatched_espn'])})")
        print(f"     These teams have results but can't map to an API name.")
        print(f"     They'll be kept as-is (Elo still works, just won't blend with market).")
        for t in diag['unmatched_espn'][:30]:
            # Try to find closest API match
            best, score = _fuzzy_match(t, diag['unmatched_api'], threshold=0.50)
            hint = f"  (closest: {best} @ {score:.0%})" if best else ""
            print(f"     {t}{hint}")
        if len(diag['unmatched_espn']) > 30:
            print(f"     ... and {len(diag['unmatched_espn']) - 30} more")


def apply_fixes(conn, sport, diag, verbose=True):
    """
    Apply name fixes to the results table.
    
    - Renames ESPN names to API names (from matched + auto_mapped)
    - Removes wrong-league team results
    
    Returns count of rows updated and deleted.
    """
    updated = 0
    deleted = 0
    
    # Build full mapping
    name_map = dict(diag['matched'])  # espn → api
    for espn, (api, score) in diag['auto_mapped'].items():
        if score >= 0.75:  # Only apply confident matches
            name_map[espn] = api
    
    # Apply renames
    for espn_name, api_name in name_map.items():
        if espn_name == api_name:
            continue  # No change needed
        
        # Update home
        cur = conn.execute("""
            UPDATE results SET home=? WHERE sport=? AND home=?
        """, (api_name, sport, espn_name))
        updated += cur.rowcount
        
        # Update away
        cur = conn.execute("""
            UPDATE results SET away=? WHERE sport=? AND away=?
        """, (api_name, sport, espn_name))
        updated += cur.rowcount
        
        # Also update winner field
        conn.execute("""
            UPDATE results SET winner=? WHERE sport=? AND winner=?
        """, (api_name, sport, espn_name))
    
    # Remove wrong-league results
    for team in diag['wrong_league']:
        cur = conn.execute("""
            DELETE FROM results WHERE sport=? AND (home=? OR away=?)
        """, (sport, team, team))
        deleted += cur.rowcount
    
    conn.commit()
    
    if verbose:
        print(f"\n  📝 {sport}: {updated} name updates, {deleted} wrong-league deletions")
    
    return updated, deleted


def save_mappings(conn, sport, diag):
    """Save name mappings to team_aliases table for future use."""
    # Ensure table has espn_name column
    try:
        conn.execute("ALTER TABLE team_aliases ADD COLUMN espn_name TEXT")
    except:
        pass  # Column already exists
    
    name_map = dict(diag['matched'])
    for espn, (api, score) in diag['auto_mapped'].items():
        name_map[espn] = api
    
    for espn_name, api_name in name_map.items():
        if espn_name == api_name:
            continue
        try:
            conn.execute("""
                INSERT OR REPLACE INTO team_aliases (sport, alias, canonical, espn_name)
                VALUES (?, ?, ?, ?)
            """, (sport, espn_name, api_name, espn_name))
        except:
            pass
    
    conn.commit()


def full_fix(sports=None, diagnose_only=False, verbose=True):
    """Run full diagnostic + fix pipeline."""
    conn = sqlite3.connect(DB_PATH)
    
    if sports is None:
        # Get all sports that have results
        sports = [r[0] for r in conn.execute(
            "SELECT DISTINCT sport FROM results"
        ).fetchall()]
    
    if not sports:
        print("  ⚠ No results in database. Run historical_scores.py first.")
        conn.close()
        return
    
    print("=" * 60)
    print("  TEAM NAME DIAGNOSTIC + FIX")
    print("=" * 60)
    
    total_updated = 0
    total_deleted = 0
    
    for sport in sports:
        diag = diagnose_sport(conn, sport, verbose=verbose)
        if not diag:
            continue
        
        if not diagnose_only:
            u, d = apply_fixes(conn, sport, diag, verbose=verbose)
            total_updated += u
            total_deleted += d
            save_mappings(conn, sport, diag)
    
    if not diagnose_only:
        print(f"\n  {'═' * 60}")
        print(f"  TOTAL: {total_updated} renames, {total_deleted} deletions")
        print(f"  {'═' * 60}")
        
        # Show updated counts
        print(f"\n  DATABASE AFTER FIX:")
        for sport in sports:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM results WHERE sport=?", (sport,)
            ).fetchone()[0]
            print(f"    {sport:30s} {cnt:4d} games")
        
        # Clear old Elo ratings so they get rebuilt fresh
        for sport in sports:
            conn.execute("DELETE FROM elo_ratings WHERE sport=?", (sport,))
        conn.commit()
        print(f"\n  🗑️  Cleared old Elo ratings (will rebuild fresh)")
        
        # Rebuild Elo
        print()
        try:
            from elo_engine import build_all_elo
            build_all_elo(sports=sports)
        except Exception as e:
            print(f"  ⚠ Elo rebuild error: {e}")
            print(f"  Run manually: python elo_engine.py")
    
    conn.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Fix ESPN ↔ Odds API team name mismatches')
    parser.add_argument('--diagnose', action='store_true', help='Show mismatches only (no changes)')
    parser.add_argument('--sport', type=str, help='Fix one sport only')
    args = parser.parse_args()
    
    sports = None
    if args.sport:
        # Map short names to full sport keys
        short_map = {
            'nba': 'basketball_nba', 'ncaab': 'basketball_ncaab',
            'nhl': 'icehockey_nhl', 'epl': 'soccer_epl',
            'seriea': 'soccer_italy_serie_a', 'liga': 'soccer_spain_la_liga',
        }
        sports = [short_map.get(args.sport, args.sport)]
    
    full_fix(sports=sports, diagnose_only=args.diagnose)
