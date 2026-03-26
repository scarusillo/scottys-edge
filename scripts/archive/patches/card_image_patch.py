"""
card_image_patch.py — Wire PNG card into the email pipeline

Adds:
  1. PNG card generation after HTML card in cmd_run
  2. PNG attached to email so you can save it to camera roll
  3. emailer.py updated to support image attachments

Usage:
    python card_image_patch.py              # Preview
    python card_image_patch.py --apply      # Apply

Requires: pip install Pillow
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_card_image')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# PATCH 1: Generate PNG card after HTML card in cmd_run
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """    # Step 8: Log picks
    try:
        from pick_logger import log_picks
        log_picks(all_picks, run_type)
    except Exception as e:
        print(f"  Logging: {e}")""",
    """    # Step 8: Generate PNG card for Instagram/social
    png_card_path = None
    if all_picks:
        try:
            from card_image import generate_card_image
            png_card_path = generate_card_image(all_picks)
        except Exception as e:
            print(f"  PNG card: {e}")

    # Step 8b: Log picks
    try:
        from pick_logger import log_picks
        log_picks(all_picks, run_type)
    except Exception as e:
        print(f"  Logging: {e}")""",
    "Generate PNG card image in cmd_run pipeline"
))

# ══════════════════════════════════════════════════════════════
# PATCH 2: Attach PNG to email
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """            send_picks_email(full_text, run_type, html_body=combined_html)""",
    """            send_picks_email(full_text, run_type, html_body=combined_html, attachment_path=png_card_path)""",
    "Attach PNG card image to picks email"
))

# ══════════════════════════════════════════════════════════════
# PATCH 3: Update emailer to properly attach images
# ══════════════════════════════════════════════════════════════
# The current send_html_email doesn't support attachments.
# We need send_picks_email to pass the attachment through.

PATCHES.append((
    'emailer.py',
    """def send_picks_email(picks_text, run_type='Morning', attachment_path=None, html_body=None):
    \"\"\"Send today's picks as email. If html_body provided, sends as HTML email.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"🎯 {run_type} Picks — {today}"
    
    plain_body = f\"\"\"Scotty's Edge — {run_type} Picks
{datetime.now().strftime('%I:%M %p EST')}

{picks_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
    """def send_picks_email(picks_text, run_type='Morning', attachment_path=None, html_body=None):
    \"\"\"Send today's picks as email. If html_body provided, sends as HTML email with optional PNG attachment.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"🎯 {run_type} Picks — {today}"
    
    plain_body = f\"\"\"Scotty's Edge — {run_type} Picks
{datetime.now().strftime('%I:%M %p EST')}

{picks_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body, attachment_path=attachment_path)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
    "Pass attachment_path through to send_html_email"
))

PATCHES.append((
    'emailer.py',
    """def send_html_email(subject, plain_body, html_body, to_addr=None):
    \"\"\"Send email with HTML body that renders inline.\"\"\"
    if not GMAIL_PASSWORD:
        print("  ⚠ GMAIL_APP_PASSWORD not set.")
        return False

    if to_addr is None:
        to_addr = GMAIL_ADDRESS

    msg = MIMEMultipart('alternative')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_addr
    msg['Subject'] = subject

    # Plain text fallback
    msg.attach(MIMEText(plain_body, 'plain'))
    # HTML version (renders inline in email client)
    msg.attach(MIMEText(html_body, 'html'))""",
    """def send_html_email(subject, plain_body, html_body, to_addr=None, attachment_path=None):
    \"\"\"Send email with HTML body that renders inline, optional PNG attachment.\"\"\"
    if not GMAIL_PASSWORD:
        print("  ⚠ GMAIL_APP_PASSWORD not set.")
        return False

    if to_addr is None:
        to_addr = GMAIL_ADDRESS

    # Use mixed (not alternative) so attachment shows alongside HTML
    msg = MIMEMultipart('mixed')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_addr
    msg['Subject'] = subject

    # HTML + text as alternative sub-part
    alt_part = MIMEMultipart('alternative')
    alt_part.attach(MIMEText(plain_body, 'plain'))
    alt_part.attach(MIMEText(html_body, 'html'))
    msg.attach(alt_part)

    # Attach PNG card if provided
    if attachment_path and os.path.exists(attachment_path):
        from email.mime.image import MIMEImage
        try:
            with open(attachment_path, 'rb') as f:
                img_data = f.read()
            img_part = MIMEImage(img_data, name=os.path.basename(attachment_path))
            img_part.add_header('Content-Disposition', 'attachment',
                               filename=os.path.basename(attachment_path))
            msg.attach(img_part)
            print(f"  📎 PNG card attached: {os.path.basename(attachment_path)}")
        except Exception as e:
            print(f"  ⚠ PNG attachment failed: {e}")""",
    "Support PNG image attachment in send_html_email"
))


# ══════════════════════════════════════════════════════════════
# EXECUTION
# ══════════════════════════════════════════════════════════════

def preview():
    print("=" * 65)
    print("  PNG CARD EMAIL INTEGRATION (PREVIEW)")
    print("=" * 65)
    
    # Check Pillow
    try:
        from PIL import Image
        print(f"  ✅ Pillow installed")
    except ImportError:
        print(f"  ❌ Pillow not installed — run: pip install Pillow")
    
    # Check card_image.py
    ci_path = os.path.join(SCRIPTS_DIR, 'card_image.py')
    if os.path.exists(ci_path):
        print(f"  ✅ card_image.py found")
    else:
        print(f"  ❌ card_image.py not found — copy it to scripts folder first")
    
    pending = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found")
            continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"  📝 {desc}")
            pending += 1
        elif new_text[:80] in content:
            print(f"  ✅ {desc} — already applied")
        else:
            print(f"  ⚠️  {desc} — text not found")
            print(f"      Looking for: {old_text[:80]}...")
    print(f"\n  {pending} patches to apply.")
    print(f"  Run with --apply to execute.")


def apply():
    print("=" * 65)
    print("  PNG CARD EMAIL INTEGRATION — Applying")
    print("=" * 65)
    
    ci_path = os.path.join(SCRIPTS_DIR, 'card_image.py')
    if not os.path.exists(ci_path):
        print(f"  ❌ card_image.py not found — copy it to scripts folder first")
        return
    
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found")
            continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text not in content:
            if new_text[:80] in content:
                print(f"  ✅ {desc} — already applied")
            else:
                print(f"  ⚠️  {desc} — text mismatch")
                print(f"      Looking for: {old_text[:80]}...")
            continue
        bak = os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak")
        shutil.copy2(filepath, bak)
        new_content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ {desc}")
        success += 1
    
    print(f"\n  Applied {success} patches.")
    print(f"\n  Install Pillow if not already:")
    print(f"    pip install Pillow")
    print(f"\n  Test:")
    print(f"    python card_image.py")
    print(f"    python main.py run --email")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
