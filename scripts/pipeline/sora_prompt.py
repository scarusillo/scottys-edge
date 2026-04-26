"""
Sora video prompt generator — builds the 12-second cinematic vertical-video
prompt for daily Sora generation (replaces older Kling workflow per
reference_sora_video.md).

Pulled fresh from yesterday's graded results so the on-screen scoreboards
match what was actually published.

Extracted from main.py in v26.0 Phase 8.

Re-exported from main for back-compat.
"""
import re
from datetime import datetime
import os
from model_engine import CARDS_DIR


def _generate_kling_prompt(conn):
    """Generate a Kling 3.0 video prompt based on today's graded results."""
    bets = conn.execute("""
        SELECT sport, result, pnl_units FROM graded_bets
        WHERE DATE(created_at) = (
            SELECT MAX(DATE(created_at)) FROM graded_bets
            WHERE result IN ('WIN','LOSS') AND units >= 3.5
        )
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchall()

    if not bets:
        return

    # Group by sport — each entry: (display label, athlete description, spotlight color)
    sport_labels = {
        'basketball_nba': ('NBA', 'An NBA guard in a crisp team jersey drives hard to the rim, mid-stride, ball cocked for a finish', 'deep orange'),
        'icehockey_nhl': ('NHL', 'An NHL forward in full pads winds up for a slap shot, ice mist swirling around his skates', 'electric blue'),
        'baseball_ncaa': ('College Baseball', 'A college baseball pitcher mid-windup on a lit mound, leg kicked high, jersey pinstripes sharp', 'crimson'),
        'baseball_mlb': ('MLB', 'An MLB slugger in full uniform frozen at the top of his swing, bat blurred, eyes locked on the ball', 'warm red'),
        'basketball_ncaab': ('NCAAB', 'A college basketball player elevates for a mid-range jumper, wrist flicked, ball spinning off fingertips', 'amber'),
    }
    # Display-label overrides (soccer leagues + tennis tours map to generic labels
    # with their own athlete cinematography — matches the landing-page / card style)
    label_athletes = {
        'Soccer': ('A soccer striker mid-strike, boot connecting with the ball, stadium lights catching the turf', 'golden'),
        'Tennis': ('A tennis player mid-serve, racket fully extended overhead, ball suspended at contact point', 'white'),
    }

    sport_records = {}
    for sp, result, pnl in bets:
        label = sport_labels.get(sp, (sp, 'athlete'))[0]
        if 'soccer' in sp:
            label = 'Soccer'
        elif 'tennis' in sp:
            label = 'Tennis'
        if label not in sport_records:
            sport_records[label] = {'W': 0, 'L': 0}
        if result == 'WIN':
            sport_records[label]['W'] += 1
        else:
            sport_records[label]['L'] += 1

    # Season totals
    season = conn.execute("""
        SELECT SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END),
               SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END)
        FROM graded_bets WHERE DATE(created_at) >= '2026-03-04'
        AND result IN ('WIN','LOSS') AND units >= 3.5
    """).fetchone()
    tw, tl = season[0] or 0, season[1] or 0
    twp = round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0

    # Build athlete scenes — Sora 12s format, scenes 2..N span 3-9s (6s of runtime)
    scenes = []
    scene_num = 2
    n_sports = len(sport_records) or 1
    mid_budget = 6.0  # 3s–9s
    time_per = mid_budget / n_sports
    t = 3.0
    for label, rec in sport_records.items():
        sp_key = [k for k, v in sport_labels.items() if v[0] == label]
        if sp_key:
            _, athlete_desc, spotlight = sport_labels[sp_key[0]]
        elif label in label_athletes:
            athlete_desc, spotlight = label_athletes[label]
        else:
            athlete_desc, spotlight = ('an athlete framed in a sport-specific spotlight', 'white')
        record_str = f"{rec['W']}-{rec['L']}"
        color = 'glowing green' if rec['W'] > rec['L'] else ('glowing red' if rec['L'] > rec['W'] else 'white')
        t_end = t + time_per
        scenes.append(
            f'Scene {scene_num} ({t:.1f}-{t_end:.1f}s): {athlete_desc}, bathed in a {spotlight} spotlight against deep black. '
            f'A sleek LED scoreboard behind reads "{label} {record_str}" in {color} numbers. Subtle camera dolly.'
        )
        scene_num += 1
        t = t_end

    total_w = sum(r['W'] for r in sport_records.values())
    total_l = sum(r['L'] for r in sport_records.values())

    prompt = f"""Cinematic vertical video (9:16), 12 seconds. Dark premium sports broadcast studio with signature green neon lighting. No voiceover — all text is on-screen only.

Scene 1 (0-3s): Camera glides into a dark premium sports studio. A large neon sign on the wall reads "SCOTTY'S EDGE" — "EDGE" in bright green neon, "SCOTTY'S" in crisp white neon tubes. The sign flickers dramatically to life. Green neon light reflects across glossy black floors. On-screen text appears in a clean broadcast font beneath the sign: "WELCOME TO SCOTTY'S EDGE". A smaller LED scoreboard below shows tonight's record "{total_w}-{total_l}" in bold white numerals. Slow cinematic push toward the sign.

{chr(10).join(scenes)}

Scene {scene_num} ({t:.1f}-12.0s): Camera pulls back smoothly to reveal the full studio. The neon "SCOTTY'S EDGE" sign glows steady above. A massive LED scoreboard shows season record "{tw}-{tl}" in large glowing green numerals with "{twp}%" pulsing beneath. Green neon accents across the studio pulse slowly in rhythm. Premium broadcast sign-off, cinematic fade to black.

Style: Dark ESPN SportsCenter aesthetic. Athletes are fully visible and well-lit with sport-specific colored spotlights — NOT silhouettes. Green neon is the signature accent color throughout. All numbers appear on LED scoreboards inside the studio. Smooth, slow camera movements. Premium sports broadcast quality. 9:16 vertical, 12 seconds total."""

    # Save prompt to file and email
    prompt_path = os.path.join(CARDS_DIR, 'kling_prompt.txt')
    with open(prompt_path, 'w', encoding='utf-8') as f:
        f.write(prompt)
    print(f"  Kling prompt saved: {prompt_path}")

    # Email the prompt
    try:
        from emailer import send_email
        send_email(f"Kling Video Prompt — {datetime.now().strftime('%Y-%m-%d')}", prompt)
        print("  Kling prompt emailed")
    except Exception:
        pass


