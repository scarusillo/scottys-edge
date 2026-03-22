"""
grade_cards_patch.py — Attach results + stats PNG cards to 9am grade email

Patches:
  1. cmd_grade in main.py — generates results + stats cards after grading
  2. send_grading_email in emailer.py — passes attachments through
  3. send_html_email in emailer.py — supports multiple attachments

Usage:
    python grade_cards_patch.py              # Preview
    python grade_cards_patch.py --apply      # Apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_grade_cards')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# PATCH 1: Generate results + stats cards in cmd_grade
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'main.py',
    """    report = daily_grade_and_report(conn)
    conn.close()

    if do_email:
        # Generate results HTML for inline email
        results_html_content = None
        if report:
            try:
                results_path, results_html_content = _generate_results_html(report)
            except Exception as e:
                results_html_content = None
                print(f"  Results HTML: {e}")
        
        from emailer import send_grading_email
        send_grading_email(report, html_body=results_html_content)""",
    """    report = daily_grade_and_report(conn)

    # Generate PNG cards for email attachment (FREE — no API calls)
    card_paths = []
    try:
        from card_image import generate_results_card, generate_stats_card
        results_png = generate_results_card(conn, start_date='2026-03-04')
        if results_png: card_paths.append(results_png)
        stats_png = generate_stats_card(conn, start_date='2026-03-04')
        if stats_png: card_paths.append(stats_png)
    except Exception as e:
        print(f"  PNG cards: {e}")

    conn.close()

    if do_email:
        # Generate results HTML for inline email
        results_html_content = None
        if report:
            try:
                results_path, results_html_content = _generate_results_html(report)
            except Exception as e:
                results_html_content = None
                print(f"  Results HTML: {e}")
        
        from emailer import send_grading_email
        send_grading_email(report, html_body=results_html_content, attachment_paths=card_paths)""",
    "Generate results + stats PNG cards and attach to grade email"
))

# ══════════════════════════════════════════════════════════════
# PATCH 2: send_grading_email passes attachment_paths through
# ══════════════════════════════════════════════════════════════

PATCHES.append((
    'emailer.py',
    """def send_grading_email(report_text, attachment_path=None, html_body=None):
    \"\"\"Send daily performance report.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"📊 Performance Report — {today}"
    
    plain_body = f\"\"\"Scotty's Edge — Daily Performance Report
{datetime.now().strftime('%I:%M %p EST')}

{report_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
    """def send_grading_email(report_text, attachment_path=None, html_body=None, attachment_paths=None):
    \"\"\"Send daily performance report with optional PNG card attachments.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"📊 Performance Report — {today}"
    
    plain_body = f\"\"\"Scotty's Edge — Daily Performance Report
{datetime.now().strftime('%I:%M %p EST')}

{report_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body, attachment_paths=attachment_paths)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
    "send_grading_email supports attachment_paths list"
))

# ══════════════════════════════════════════════════════════════
# PATCH 3: send_html_email supports multiple attachments
# ══════════════════════════════════════════════════════════════
# The card_image_patch already changed send_html_email to support
# one attachment. This adds support for a list of attachments.

PATCHES.append((
    'emailer.py',
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
    """def send_html_email(subject, plain_body, html_body, to_addr=None, attachment_path=None, attachment_paths=None):
    \"\"\"Send email with HTML body, supports single or multiple PNG attachments.\"\"\"
    if not GMAIL_PASSWORD:
        print("  ⚠ GMAIL_APP_PASSWORD not set.")
        return False

    if to_addr is None:
        to_addr = GMAIL_ADDRESS

    # Use mixed (not alternative) so attachments show alongside HTML
    msg = MIMEMultipart('mixed')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_addr
    msg['Subject'] = subject

    # HTML + text as alternative sub-part
    alt_part = MIMEMultipart('alternative')
    alt_part.attach(MIMEText(plain_body, 'plain'))
    alt_part.attach(MIMEText(html_body, 'html'))
    msg.attach(alt_part)

    # Build list of all attachments
    all_paths = []
    if attachment_path and os.path.exists(attachment_path):
        all_paths.append(attachment_path)
    if attachment_paths:
        all_paths.extend([p for p in attachment_paths if p and os.path.exists(p)])

    # Attach all PNGs
    if all_paths:
        from email.mime.image import MIMEImage
        for img_path in all_paths:
            try:
                with open(img_path, 'rb') as f:
                    img_data = f.read()
                img_part = MIMEImage(img_data, name=os.path.basename(img_path))
                img_part.add_header('Content-Disposition', 'attachment',
                                   filename=os.path.basename(img_path))
                msg.attach(img_part)
                print(f"  📎 Attached: {os.path.basename(img_path)}")
            except Exception as e:
                print(f"  ⚠ Attachment failed: {e}")""",
    "send_html_email supports multiple attachments via attachment_paths list"
))


def preview():
    print("=" * 65)
    print("  GRADE EMAIL CARDS INTEGRATION (PREVIEW)")
    print("=" * 65)
    pending = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found"); continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"  📝 {desc}"); pending += 1
        elif new_text[:80] in content:
            print(f"  ✅ {desc} — already applied")
        else:
            print(f"  ⚠️  {desc} — text not found")
            print(f"      Looking for: {old_text[:80]}...")
    print(f"\n  {pending} patches to apply.")
    print(f"  Run with --apply to execute.")


def apply():
    print("=" * 65)
    print("  GRADE EMAIL CARDS — Applying")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  ⚠️  {filename}: not found"); continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text not in content:
            if new_text[:80] in content:
                print(f"  ✅ {desc} — already applied")
            else:
                print(f"  ⚠️  {desc} — text mismatch")
                print(f"      Looking for: {old_text[:80]}...")
            continue
        shutil.copy2(filepath, os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak"))
        new_content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ {desc}"); success += 1
    print(f"\n  Applied {success} patches.")
    print(f"  Test: python main.py grade --email")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
