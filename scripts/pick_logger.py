"""
pick_logger.py — Saves every pick to a running log file.

The user can upload this file to Claude for analysis at any time.
Format: one JSON object per line (JSONL), easy to parse.

Log file location: betting_model/data/picks_log.jsonl
"""
import json, os
from datetime import datetime

LOG_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'picks_log.jsonl')
GRADE_LOG_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'grades_log.jsonl')


def log_picks(picks, run_type='Manual'):
    """Append picks to the log file."""
    now = datetime.now().isoformat()
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        for p in picks:
            entry = {
                'logged_at': now,
                'run_type': run_type,
                'sport': p.get('sport'),
                'event_id': p.get('event_id'),
                'commence': p.get('commence'),
                'home': p.get('home'),
                'away': p.get('away'),
                'market_type': p.get('market_type'),
                'selection': p.get('selection'),
                'book': p.get('book'),
                'line': p.get('line'),
                'odds': p.get('odds'),
                'model_spread': p.get('model_spread'),
                'model_prob': p.get('model_prob'),
                'implied_prob': p.get('implied_prob'),
                'edge_pct': p.get('edge_pct'),
                'star_rating': p.get('star_rating'),
                'units': p.get('units'),
                'confidence': p.get('confidence'),
                'timing': p.get('timing'),
                'notes': p.get('notes'),
            }
            f.write(json.dumps(entry) + '\n')
    print(f"  📝 Logged {len(picks)} picks to picks_log.jsonl")


def log_grades(graded_bets):
    """Append graded results to the grades log."""
    now = datetime.now().isoformat()
    with open(GRADE_LOG_PATH, 'a', encoding='utf-8') as f:
        for g in graded_bets:
            entry = {
                'graded_at': now,
                'selection': g.get('selection'),
                'sport': g.get('sport'),
                'market_type': g.get('market_type'),
                'result': g.get('result'),
                'pnl': g.get('pnl'),
                'edge': g.get('edge'),
                'confidence': g.get('confidence'),
                'units': g.get('units'),
                'odds': g.get('odds'),
            }
            f.write(json.dumps(entry) + '\n')
    print(f"  📝 Logged {len(graded_bets)} grades to grades_log.jsonl")


def get_log_summary():
    """Generate a summary of the log for quick review."""
    if not os.path.exists(LOG_PATH):
        return "No picks logged yet."

    picks = []
    with open(LOG_PATH, 'r') as f:
        for line in f:
            try:
                picks.append(json.loads(line))
            except Exception:
                pass

    if not picks:
        return "No picks logged yet."

    # Summary stats
    total = len(picks)
    by_sport = {}
    by_conf = {}
    by_date = {}
    total_units = 0

    for p in picks:
        sp = p.get('sport', '?')
        by_sport[sp] = by_sport.get(sp, 0) + 1
        conf = p.get('confidence', '?')
        by_conf[conf] = by_conf.get(conf, 0) + 1
        date = p.get('logged_at', '')[:10]
        by_date[date] = by_date.get(date, 0) + 1
        total_units += p.get('units', 0)

    lines = []
    lines.append(f"Total picks logged: {total}")
    lines.append(f"Total units wagered: {total_units:.1f}")
    lines.append(f"Days tracked: {len(by_date)}")
    lines.append(f"\nBy sport: {by_sport}")
    lines.append(f"By confidence: {by_conf}")
    lines.append(f"By date: {by_date}")

    # Grades
    if os.path.exists(GRADE_LOG_PATH):
        grades = []
        with open(GRADE_LOG_PATH, 'r') as f:
            for line in f:
                try:
                    grades.append(json.loads(line))
                except Exception:
                    pass
        if grades:
            wins = sum(1 for g in grades if g.get('result') == 'WIN')
            losses = sum(1 for g in grades if g.get('result') == 'LOSS')
            pnl = sum(g.get('pnl', 0) for g in grades)
            wp = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
            lines.append(f"\nGRADED: {wins}W-{losses}L ({wp:.1f}%) | P/L: {pnl:+.2f} units")

    return '\n'.join(lines)


if __name__ == '__main__':
    print(get_log_summary())
