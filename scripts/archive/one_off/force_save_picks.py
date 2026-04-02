"""
force_save_picks.py — Save picks from cached odds (predict mode) and generate card + email

Use when 'run' fetches fresh odds that kill edges but 'predict' finds real plays.
"""
import sqlite3, os, sys
sys.path.insert(0, os.path.dirname(__file__))

db = os.path.join(os.path.dirname(__file__), '..', 'data', 'betting_model.db')
conn = sqlite3.connect(db)

from model_engine import generate_predictions, save_picks_to_db, print_picks

ALL_SPORTS = [
    'basketball_nba', 'basketball_ncaab', 'icehockey_nhl',
    'baseball_ncaa',
    'soccer_epl', 'soccer_italy_serie_a', 'soccer_spain_la_liga',
    'soccer_germany_bundesliga', 'soccer_france_ligue_one',
    'soccer_uefa_champs_league', 'soccer_usa_mls',
]

print("Generating picks from cached odds...")
all_picks = []
for sp in ALL_SPORTS:
    picks = generate_predictions(conn, sport=sp)
    all_picks.extend(picks)

if not all_picks:
    print("No picks found.")
    conn.close()
    exit()

# Filter to 4.5u+ only
max_plays = [p for p in all_picks if p.get('units', 0) >= 4.5]
print(f"\nFound {len(all_picks)} total picks, {len(max_plays)} MAX PLAYs (4.5u+)")

for p in sorted(all_picks, key=lambda x: x.get('units', 0), reverse=True):
    tier = "MAX" if p['units'] >= 4.5 else "STR" if p['units'] >= 4.0 else "---"
    print(f"  {tier} {p['units']:.1f}u  {p['selection']:40s} {p.get('sport','')}")

# Save ALL picks to DB
save_picks_to_db(conn, all_picks)
print(f"\nSaved {len(all_picks)} picks to database")

# Generate card
try:
    from card_image import generate_card_image, generate_caption
    card_path = generate_card_image(all_picks)
    if card_path:
        print(f"Card: {card_path}")
    
    caption = generate_caption(all_picks)
    if caption:
        print(f"\nINSTAGRAM CAPTION:\n{'='*40}\n{caption}")
except Exception as e:
    print(f"Card error: {e}")

# Email
try:
    from emailer import send_picks_email, send_email
    from model_engine import picks_to_text
    from datetime import datetime
    
    text = picks_to_text(all_picks, "Evening Picks")
    
    # Try to generate HTML card
    html_content = None
    try:
        from card_image import generate_picks_html
        html_content = generate_picks_html(all_picks)
    except Exception:
        pass
    
    send_picks_email(text, "Evening", html_body=html_content, attachment_path=card_path)
    
    # Caption email
    if caption:
        today = datetime.now().strftime('%Y-%m-%d')
        send_email(f"Social Captions - Evening {today}", caption)
    
    print("\nEmails sent!")
except Exception as e:
    print(f"Email error: {e}")

conn.close()
