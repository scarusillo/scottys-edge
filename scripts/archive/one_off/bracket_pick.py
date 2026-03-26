#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
2026 March Madness Bracket Simulator
Uses Elo ratings from betting_model.db to predict every game.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "betting_model.db")

# ── Team name mappings (bracket name -> DB name) ──────────────────────────
TEAM_MAP = {
    "Duke": "Duke Blue Devils",
    "Siena": "Siena Saints",
    "Ohio St": "Ohio State Buckeyes",
    "TCU": "TCU Horned Frogs",
    "St. John's": "St. John's (NY) Red Storm",
    "Northern Iowa": "Northern Iowa Panthers",
    "Kansas": "Kansas Jayhawks",
    "Cal Baptist": "Cal Baptist Lancers",
    "Louisville": "Louisville Cardinals",
    "South Florida": "South Florida Bulls",
    "Michigan St": "Michigan St Spartans",
    "North Dakota St": "North Dakota St Bison",
    "UCLA": "UCLA Bruins",
    "UCF": "Central Florida Knights",
    "UConn": "Connecticut Huskies",
    "Furman": "Furman Paladins",
    "Florida": "Florida Gators",
    "Prairie View A&M": "Prairie View Panthers",
    "Clemson": "Clemson Tigers",
    "Iowa": "Iowa Hawkeyes",
    "Vanderbilt": "Vanderbilt Commodores",
    "McNeese": "McNeese Cowboys",
    "Nebraska": "Nebraska Cornhuskers",
    "Troy": "Troy Trojans",
    "North Carolina": "North Carolina Tar Heels",
    "VCU": "VCU Rams",
    "Illinois": "Illinois Fighting Illini",
    "Penn": "Pennsylvania Quakers",
    "Saint Mary's": "Saint Mary's Gaels",
    "Texas A&M": "Texas A&M Aggies",
    "Houston": "Houston Cougars",
    "Idaho": "Idaho Vandals",
    "Arizona": "Arizona Wildcats",
    "Long Island": "LIU Sharks",
    "Villanova": "Villanova Wildcats",
    "Utah St": "Utah State Aggies",
    "Wisconsin": "Wisconsin Badgers",
    "High Point": "High Point Panthers",
    "Arkansas": "Arkansas Razorbacks",
    "Hawaii": "Hawai'i Rainbow Warriors",
    "BYU": "BYU Cougars",
    "Texas": "Texas Longhorns",
    "Gonzaga": "Gonzaga Bulldogs",
    "Kennesaw St": "Kennesaw St Owls",
    "Miami FL": "Miami (FL) Hurricanes",
    "Missouri": "Missouri Tigers",
    "Purdue": "Purdue Boilermakers",
    "Queens NC": "Queens University Royals",
    "Michigan": "Michigan Wolverines",
    "Howard": "Howard Bison",
    "Georgia": "Georgia Bulldogs",
    "Saint Louis": "Saint Louis Billikens",
    "Texas Tech": "Texas Tech Red Raiders",
    "Akron": "Akron Zips",
    "Alabama": "Alabama Crimson Tide",
    "Hofstra": "Hofstra Pride",
    "Tennessee": "Tennessee Volunteers",
    "Miami Ohio": "Miami (OH) RedHawks",
    "Virginia": "Virginia Cavaliers",
    "Wright St": "Wright St Raiders",
    "Kentucky": "Kentucky Wildcats",
    "Santa Clara": "Santa Clara Broncos",
    "Iowa St": "Iowa State Cyclones",
    "Tennessee St": "Tennessee St Tigers",
}

# ── Bracket structure: (seed, short_name) ─────────────────────────────────
# Each region is a list of 8 first-round matchups ordered so that winners
# feed into the standard bracket tree.
EAST = [
    [(1, "Duke"),          (16, "Siena")],
    [(8, "Ohio St"),       (9, "TCU")],
    [(5, "St. John's"),    (12, "Northern Iowa")],
    [(4, "Kansas"),        (13, "Cal Baptist")],
    [(6, "Louisville"),    (11, "South Florida")],
    [(3, "Michigan St"),   (14, "North Dakota St")],
    [(7, "UCLA"),          (10, "UCF")],
    [(2, "UConn"),         (15, "Furman")],
]

SOUTH = [
    [(1, "Florida"),       (16, "Prairie View A&M")],
    [(8, "Clemson"),       (9, "Iowa")],
    [(5, "Vanderbilt"),    (12, "McNeese")],
    [(4, "Nebraska"),      (13, "Troy")],
    [(6, "North Carolina"),(11, "VCU")],
    [(3, "Illinois"),      (14, "Penn")],
    [(7, "Saint Mary's"),  (10, "Texas A&M")],
    [(2, "Houston"),       (15, "Idaho")],
]

WEST = [
    [(1, "Arizona"),       (16, "Long Island")],
    [(8, "Villanova"),     (9, "Utah St")],
    [(5, "Wisconsin"),     (12, "High Point")],
    [(4, "Arkansas"),      (13, "Hawaii")],
    [(6, "BYU"),           (11, "Texas")],
    [(3, "Gonzaga"),       (14, "Kennesaw St")],
    [(7, "Miami FL"),      (10, "Missouri")],
    [(2, "Purdue"),        (15, "Queens NC")],
]

MIDWEST = [
    [(1, "Michigan"),      (16, "Howard")],
    [(8, "Georgia"),       (9, "Saint Louis")],
    [(5, "Texas Tech"),    (12, "Akron")],
    [(4, "Alabama"),       (13, "Hofstra")],
    [(6, "Tennessee"),     (11, "Miami Ohio")],
    [(3, "Virginia"),      (14, "Wright St")],
    [(7, "Kentucky"),      (10, "Santa Clara")],
    [(2, "Iowa St"),       (15, "Tennessee St")],
]

ROUND_NAMES = [
    "Round of 64",
    "Round of 32",
    "Sweet 16",
    "Elite 8",
    "Final Four",
    "Championship",
]

# ── Load Elo ratings ──────────────────────────────────────────────────────
def load_elos():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT team, elo, games_played FROM elo_ratings WHERE sport = 'basketball_ncaab'"
    )
    rows = cur.fetchall()
    conn.close()
    elo_dict = {}
    gp_dict = {}
    for team, elo, gp in rows:
        elo_dict[team] = elo
        gp_dict[team] = gp
    return elo_dict, gp_dict


def win_prob(elo_a, elo_b):
    """Elo win probability for team A vs team B on neutral court."""
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def spread(elo_a, elo_b):
    """Approximate point spread (negative = A favored)."""
    return (elo_a - elo_b) / 28.5


def simulate_game(team_a, team_b, elo_dict, round_name, game_log, upset_log, close_log):
    """Simulate a single game. Returns the winner tuple (seed, name)."""
    seed_a, name_a = team_a
    seed_b, name_b = team_b
    db_a = TEAM_MAP[name_a]
    db_b = TEAM_MAP[name_b]
    elo_a = elo_dict.get(db_a, 1500.0)
    elo_b = elo_dict.get(db_b, 1500.0)

    prob_a = win_prob(elo_a, elo_b)
    prob_b = 1.0 - prob_a
    pts = spread(elo_a, elo_b)

    # Determine winner (pick the higher-probability team)
    if prob_a >= 0.5:
        winner = team_a
        loser = team_b
        w_prob = prob_a
    else:
        winner = team_b
        loser = team_a
        w_prob = prob_b

    # Check upset: model picks the higher (worse) seed
    is_upset = winner[0] > loser[0]
    upset_tag = "  ** UPSET **" if is_upset else ""

    line = (
        f"  ({seed_a:>2}) {name_a:<20s} [Elo {elo_a:7.1f}]  vs  "
        f"({seed_b:>2}) {name_b:<20s} [Elo {elo_b:7.1f}]  |  "
        f"Win%: {prob_a*100:5.1f}-{prob_b*100:5.1f}  "
        f"Spread: {pts:+.1f}  -->  "
        f"Winner: ({winner[0]}) {winner[1]}{upset_tag}"
    )
    game_log.append(line)

    # Track upsets where model picks the lower seed
    if is_upset:
        upset_log.append({
            "round": round_name,
            "winner_seed": winner[0],
            "winner_name": winner[1],
            "loser_seed": loser[0],
            "loser_name": loser[1],
            "win_prob": w_prob,
            "spread": abs(pts),
        })

    # Track close games (45-55% range)
    if 0.45 <= prob_a <= 0.55:
        close_log.append({
            "round": round_name,
            "team_a": name_a,
            "seed_a": seed_a,
            "team_b": name_b,
            "seed_b": seed_b,
            "prob_a": prob_a,
            "spread": abs(pts),
        })

    return winner


def simulate_region(region_name, matchups, elo_dict, all_games, upset_log, close_log, bracket_summary):
    """Simulate an entire region from Rd64 through Elite 8. Returns the region winner."""
    teams = matchups  # list of 8 first-round pairs
    round_idx = 0

    current = []  # list of (seed, name) advancing

    while True:
        if round_idx == 0:
            pairs = teams
            rnd = ROUND_NAMES[0]
        else:
            # pair up current list
            pairs = []
            for i in range(0, len(current), 2):
                pairs.append([current[i], current[i + 1]])
            if round_idx == 1:
                rnd = ROUND_NAMES[1]
            elif round_idx == 2:
                rnd = ROUND_NAMES[2]
            elif round_idx == 3:
                rnd = ROUND_NAMES[3]
            else:
                break

        header = f"\n{'='*100}\n  {region_name} - {rnd}\n{'='*100}"
        all_games.append(header)
        bracket_summary.setdefault(rnd, [])

        winners = []
        for pair in pairs:
            w = simulate_game(pair[0], pair[1], elo_dict, rnd, all_games, upset_log, close_log)
            winners.append(w)
            bracket_summary[rnd].append(f"({w[0]}) {w[1]}")

        current = winners
        round_idx += 1

        if len(current) == 1:
            break

    return current[0]


def main():
    print("=" * 100)
    print("  2026 NCAA MARCH MADNESS BRACKET SIMULATOR  (Elo-Based)")
    print("=" * 100)

    elo_dict, gp_dict = load_elos()

    # Show Elo lookup for all bracket teams
    print("\n--- ELO RATINGS FOR BRACKET TEAMS ---\n")
    missing = []
    all_teams_sorted = []
    for short, db_name in sorted(TEAM_MAP.items(), key=lambda x: x[1]):
        elo = elo_dict.get(db_name)
        gp = gp_dict.get(db_name)
        if elo is None:
            missing.append((short, db_name))
            print(f"  {short:<22s} ({db_name:<35s})  Elo: *MISSING* (using 1500.0)")
        else:
            all_teams_sorted.append((short, db_name, elo, gp))
            print(f"  {short:<22s} ({db_name:<35s})  Elo: {elo:7.1f}  GP: {gp}")

    if missing:
        print(f"\n  WARNING: {len(missing)} team(s) not found in DB; defaulting to Elo 1500.0")

    # Sort by Elo descending for a quick power ranking
    all_teams_sorted.sort(key=lambda x: -x[2])
    print("\n--- POWER RANKINGS (by Elo) ---\n")
    for rank, (short, db, elo, gp) in enumerate(all_teams_sorted, 1):
        print(f"  {rank:>2}. {short:<22s}  Elo: {elo:7.1f}  GP: {gp}")

    # Simulate each region
    all_games = []
    upset_log = []
    close_log = []
    bracket_summary = {}

    regions = [
        ("EAST",    EAST),
        ("SOUTH",   SOUTH),
        ("WEST",    WEST),
        ("MIDWEST", MIDWEST),
    ]

    final_four = []
    for rname, matchups in regions:
        winner = simulate_region(rname, matchups, elo_dict, all_games, upset_log, close_log, bracket_summary)
        final_four.append((rname, winner))

    # Print all game-by-game results
    print("\n")
    print("*" * 100)
    print("  FULL GAME-BY-GAME RESULTS")
    print("*" * 100)
    for line in all_games:
        print(line)

    # ── Final Four ─────────────────────────────────────────────────────
    print(f"\n{'='*100}")
    print("  FINAL FOUR")
    print(f"{'='*100}")

    ff_games = []
    bracket_summary["Final Four"] = []

    # East vs South, West vs Midwest
    sem1 = [final_four[0], final_four[1]]  # East vs South
    sem2 = [final_four[2], final_four[3]]  # West vs Midwest

    print(f"\n  Semifinal 1: {sem1[0][0]} champion vs {sem1[1][0]} champion")
    w1 = simulate_game(sem1[0][1], sem1[1][1], elo_dict, "Final Four", ff_games, upset_log, close_log)
    bracket_summary["Final Four"].append(f"({w1[0]}) {w1[1]}")
    for l in ff_games:
        print(l)

    ff_games2 = []
    print(f"\n  Semifinal 2: {sem2[0][0]} champion vs {sem2[1][0]} champion")
    w2 = simulate_game(sem2[0][1], sem2[1][1], elo_dict, "Final Four", ff_games2, upset_log, close_log)
    bracket_summary["Final Four"].append(f"({w2[0]}) {w2[1]}")
    for l in ff_games2:
        print(l)

    # ── Championship ───────────────────────────────────────────────────
    print(f"\n{'='*100}")
    print("  NATIONAL CHAMPIONSHIP")
    print(f"{'='*100}")

    champ_games = []
    bracket_summary["Championship"] = []
    champ = simulate_game(w1, w2, elo_dict, "Championship", champ_games, upset_log, close_log)
    bracket_summary["Championship"].append(f"({champ[0]}) {champ[1]}")
    for l in champ_games:
        print(l)

    # ── Bracket Summary ────────────────────────────────────────────────
    print(f"\n\n{'*'*100}")
    print("  BRACKET SUMMARY BY ROUND")
    print(f"{'*'*100}")

    for rnd in ROUND_NAMES:
        teams = bracket_summary.get(rnd, [])
        if teams:
            print(f"\n  {rnd}  ({len(teams)} advancing)")
            print(f"  {'-'*40}")
            for t in teams:
                print(f"    {t}")

    # ── Final Four & Champion ──────────────────────────────────────────
    print(f"\n\n{'*'*100}")
    print("  FINAL FOUR & CHAMPION")
    print(f"{'*'*100}")
    print(f"\n  Final Four:")
    for rname, (seed, name) in final_four:
        print(f"    {rname:>10s}:  ({seed}) {name}")
    print(f"\n  Championship Matchup:  ({w1[0]}) {w1[1]}  vs  ({w2[0]}) {w2[1]}")
    print(f"\n  *** 2026 NATIONAL CHAMPION:  ({champ[0]}) {champ[1]} ***")

    # ── Top Upset Picks ────────────────────────────────────────────────
    print(f"\n\n{'*'*100}")
    print("  TOP UPSET PICKS (model favors lower seed)")
    print(f"{'*'*100}")

    if upset_log:
        upset_log.sort(key=lambda x: -x["win_prob"])
        for u in upset_log:
            print(
                f"  [{u['round']:<14s}]  ({u['winner_seed']:>2}) {u['winner_name']:<20s} "
                f"over ({u['loser_seed']:>2}) {u['loser_name']:<20s}  "
                f"Win%: {u['win_prob']*100:.1f}%  Spread: {u['spread']:.1f} pts"
            )
    else:
        print("  No upsets predicted by the model.")

    # ── Close Games / Watch Out ────────────────────────────────────────
    print(f"\n\n{'*'*100}")
    print("  'WATCH OUT' GAMES (45-55% win probability)")
    print(f"{'*'*100}")

    if close_log:
        close_log.sort(key=lambda x: abs(x["prob_a"] - 0.5))
        for c in close_log:
            print(
                f"  [{c['round']:<14s}]  ({c['seed_a']:>2}) {c['team_a']:<20s} vs "
                f"({c['seed_b']:>2}) {c['team_b']:<20s}  "
                f"Win%: {c['prob_a']*100:.1f}-{(1-c['prob_a'])*100:.1f}  "
                f"Spread: {c['spread']:.1f} pts"
            )
    else:
        print("  No games in the 45-55% range.")

    print(f"\n{'='*100}")
    print("  Simulation complete.")
    print(f"{'='*100}\n")


if __name__ == "__main__":
    main()
