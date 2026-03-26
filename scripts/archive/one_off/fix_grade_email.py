"""
fix_grade_email.py — Fix the 9am grade email: add cards, captions, auto Elo

This patches cmd_grade in main.py and the emailer to support attachments.

Usage:
    python fix_grade_email.py --apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_grade_fix')

PATCHES = []

# ── PATCH 1: cmd_grade in main.py ──
PATCHES.append(('main.py',
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

    # Auto Elo rebuild
    print("  Rebuilding Elo ratings...")
    try:
        from elo_engine import build_all_elo
        build_all_elo()
        print("  ✅ Elo ratings updated")
    except Exception as e:
        print(f"  Elo rebuild: {e}")

    # Generate PNG cards
    card_paths = []
    try:
        from card_image import generate_results_card, generate_stats_card
        results_pngs = generate_results_card(conn, start_date='2026-03-04')
        if results_pngs:
            if isinstance(results_pngs, list):
                card_paths.extend(results_pngs)
            else:
                card_paths.append(results_pngs)
        stats_png = generate_stats_card(conn, start_date='2026-03-04')
        if stats_png:
            card_paths.append(stats_png)
    except Exception as e:
        print(f"  PNG cards: {e}")

    # Generate captions
    results_caption = ""
    try:
        _yb = conn.execute(
            "SELECT selection, result, pnl_units FROM graded_bets "
            "WHERE DATE(created_at) = (SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING')) "
            "AND result NOT IN ('DUPLICATE','PENDING') ORDER BY pnl_units DESC"
        ).fetchall()
        _all = conn.execute(
            "SELECT result, pnl_units FROM graded_bets "
            "WHERE DATE(created_at) >= '2026-03-04' AND result NOT IN ('DUPLICATE','PENDING')"
        ).fetchall()
        _yw = sum(1 for b in _yb if b[1]=='WIN')
        _yl = sum(1 for b in _yb if b[1]=='LOSS')
        _yp = sum(b[2] or 0 for b in _yb)
        _tw = sum(1 for b in _all if b[0]=='WIN')
        _tl = sum(1 for b in _all if b[0]=='LOSS')
        _tp = sum(b[1] or 0 for b in _all)
        _twp = _tw/(_tw+_tl)*100 if (_tw+_tl)>0 else 0
        _pick_lines = []
        for b in _yb:
            icon = chr(9989) if b[1]=='WIN' else chr(10060)
            _pick_lines.append(f"{icon} {b[0]} | {b[2]:+.1f}u")
        ig = f"Scotty's Edge — {datetime.now().strftime('%A %B %d')} Results\\n\\n"
        ig += f"{_yw}W-{_yl}L | {_yp:+.1f}u\\n\\n"
        ig += "\\n".join(_pick_lines)
        ig += f"\\n\\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u | {_twp:.1f}%"
        ig += "\\nEvery pick tracked & graded"
        ig += "\\n\\nNot gambling advice. 21+. 1-800-GAMBLER"
        ig += "\\n\\n#ScottysEdge #SportsBetting #SportsPicks"
        tw = f"{_yw}W-{_yl}L | {_yp:+.1f}u"
        tw += f"\\n\\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u | {_twp:.1f}%"
        tw += "\\nEvery pick tracked. Every loss shown."
        tw += "\\n\\n#ScottysEdge #SportsBetting"
        results_caption = (
            "\\n\\n" + "="*50 +
            "\\nINSTAGRAM CAPTION (copy below):\\n" + "="*50 +
            "\\n" + ig +
            "\\n\\n" + "="*50 +
            "\\nTWITTER CAPTION (copy below):\\n" + "="*50 +
            "\\n" + tw + "\\n"
        )
    except Exception as e:
        print(f"  Captions: {e}")

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

        # Embed captions in HTML
        if results_html_content and results_caption:
            cap_html = results_caption.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\\n', '<br>')
            results_html_content = results_html_content.replace('</body>',
                f'<div style="max-width:1080px;margin:30px auto;padding:24px 40px;font-family:Consolas,monospace;font-size:13px;color:rgba(255,255,255,0.7);background:#0a0a0a;line-height:1.8;white-space:pre-wrap;">{cap_html}</div></body>')

        from emailer import send_grading_email
        report_with_captions = report + results_caption if isinstance(report, str) else report
        send_grading_email(report_with_captions, html_body=results_html_content, attachment_paths=card_paths)""",
    "cmd_grade: Elo + cards + captions in HTML"))

# ── PATCH 2: send_grading_email accepts attachment_paths ──
PATCHES.append(('emailer.py',
    """def send_grading_email(report_text, attachment_path=None, html_body=None):
    \"\"\"Send daily performance report.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"\U0001f4ca Performance Report \u2014 {today}"
    
    plain_body = f\"\"\"Scotty's Edge \u2014 Daily Performance Report
{datetime.now().strftime('%I:%M %p EST')}

{report_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
    """def send_grading_email(report_text, attachment_path=None, html_body=None, attachment_paths=None):
    \"\"\"Send daily performance report with PNG cards.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"\U0001f4ca Performance Report \u2014 {today}"
    
    plain_body = f\"\"\"Scotty's Edge \u2014 Daily Performance Report
{datetime.now().strftime('%I:%M %p EST')}

{report_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body, attachment_paths=attachment_paths)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
    "send_grading_email: accept attachment_paths"))

# ── PATCH 3: send_html_email supports multiple attachments ──
PATCHES.append(('emailer.py',
    """def send_html_email(subject, plain_body, html_body, to_addr=None):
    \"\"\"Send email with HTML body that renders inline.\"\"\"
    if not GMAIL_PASSWORD:
        print("  \u26a0 GMAIL_APP_PASSWORD not set.")
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
    """def send_html_email(subject, plain_body, html_body, to_addr=None, attachment_path=None, attachment_paths=None):
    \"\"\"Send email with HTML body + PNG attachments.\"\"\"
    if not GMAIL_PASSWORD:
        print("  \u26a0 GMAIL_APP_PASSWORD not set.")
        return False

    if to_addr is None:
        to_addr = GMAIL_ADDRESS

    msg = MIMEMultipart('mixed')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_addr
    msg['Subject'] = subject

    alt_part = MIMEMultipart('alternative')
    alt_part.attach(MIMEText(plain_body, 'plain'))
    alt_part.attach(MIMEText(html_body, 'html'))
    msg.attach(alt_part)

    all_paths = []
    if attachment_path and os.path.exists(attachment_path):
        all_paths.append(attachment_path)
    if attachment_paths:
        all_paths.extend([p for p in attachment_paths if p and os.path.exists(p)])
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
                print(f"  \U0001f4ce Attached: {os.path.basename(img_path)}")
            except Exception as e:
                print(f"  \u26a0 Attachment failed: {e}")""",
    "send_html_email: multiple attachments"))


def apply():
    print("=" * 65)
    print("  FIX GRADE EMAIL — Applying")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old, new, desc in PATCHES:
        fp = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(fp):
            print(f"  \u26a0\ufe0f {filename}: not found"); continue
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        if old in content:
            bak = os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak")
            if not os.path.exists(bak):
                shutil.copy2(fp, bak)
            content = content.replace(old, new, 1)
            with open(fp, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  \u2705 {desc}"); success += 1
        elif new[:80] in content:
            print(f"  \u2705 {desc} — already applied")
        else:
            print(f"  \u26a0\ufe0f {desc} — text not found")
    print(f"\n  Applied {success} patches.")
    print(f"  Test: python main.py grade --email")


def preview():
    print("=" * 65)
    print("  FIX GRADE EMAIL (PREVIEW)")
    print("=" * 65)
    pending = 0
    for filename, old, new, desc in PATCHES:
        fp = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(fp):
            print(f"  \u26a0\ufe0f {filename}: not found"); continue
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        if old in content:
            print(f"  \U0001f4dd {desc}"); pending += 1
        elif new[:80] in content:
            print(f"  \u2705 {desc} — already applied")
        else:
            print(f"  \u26a0\ufe0f {desc} — text not found")
    print(f"\n  {pending} patches to apply. Run with --apply")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
