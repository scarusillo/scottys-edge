"""
master_patch.py — All remaining automation in one shot

Patches:
  1. cmd_grade: PNG cards (wins/losses/stats) + captions + auto Elo rebuild
  2. emailer: send_grading_email + send_html_email support attachments
  3. cmd_run: Picks email includes Instagram/Twitter captions
  4. Scheduler: Adds 1pm snapshot command

Usage:
    python master_patch.py              # Preview
    python master_patch.py --apply      # Apply all
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_master_patch')

PATCHES = []

# ══════════════════════════════════════════════════════════════
# PATCH 1: cmd_grade — PNG cards + captions + auto Elo
# ══════════════════════════════════════════════════════════════

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

    # v12.1: Auto Elo rebuild (keeps ratings fresh daily, no manual step)
    print("  Rebuilding Elo ratings...")
    try:
        from elo_engine import build_all_elo
        build_all_elo()
        print("  ✅ Elo ratings updated")
    except Exception as e:
        print(f"  Elo rebuild: {e}")

    # v12.1: Generate PNG cards for email (FREE — no API calls)
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

    # v12.1: Generate Instagram/Twitter captions
    results_caption = ""
    try:
        _yb = conn.execute(\"\"\"
            SELECT selection, result, pnl_units FROM graded_bets
            WHERE DATE(created_at) = (SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING'))
            AND result NOT IN ('DUPLICATE','PENDING') ORDER BY pnl_units DESC
        \"\"\").fetchall()
        _all = conn.execute(\"\"\"
            SELECT result, pnl_units FROM graded_bets
            WHERE DATE(created_at) >= '2026-03-04' AND result NOT IN ('DUPLICATE','PENDING')
        \"\"\").fetchall()
        _yw = sum(1 for b in _yb if b[1]=='WIN')
        _yl = sum(1 for b in _yb if b[1]=='LOSS')
        _yp = sum(b[2] or 0 for b in _yb)
        _tw = sum(1 for b in _all if b[0]=='WIN')
        _tl = sum(1 for b in _all if b[0]=='LOSS')
        _tp = sum(b[1] or 0 for b in _all)
        _twg = sum(b[2] or 0 for b in _all) if any(len(b) > 2 for b in _all) else 1
        _twp = _tw/(_tw+_tl)*100 if (_tw+_tl)>0 else 0
        _pick_lines = []
        for b in _yb:
            icon = chr(9989) if b[1]=='WIN' else chr(10060)
            _pick_lines.append(f"{icon} {b[0]} | {b[2]:+.1f}u")
        ig = f"Scotty's Edge {chr(8212)} {datetime.now().strftime('%A %B %d')} Results\\n\\n"
        ig += f"{_yw}W-{_yl}L | {_yp:+.1f}u\\n\\n"
        ig += "\\n".join(_pick_lines)
        ig += f"\\n\\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u | {_twp:.1f}%"
        ig += "\\nEvery pick tracked & graded"
        ig += "\\n\\nNot gambling advice. 21+. 1-800-GAMBLER"
        ig += "\\n\\n#ScottysEdge #SportsBetting #SportsPicks"
        tw = f"{_yw}W-{_yl}L | {_yp:+.1f}u"
        if _yp >= 10: tw += " " + chr(128293)
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
        
        from emailer import send_grading_email
        report_with_captions = report + results_caption if isinstance(report, str) else report
        send_grading_email(report_with_captions, html_body=results_html_content, attachment_paths=card_paths)""",
"cmd_grade: PNG cards + captions + auto Elo rebuild"))


# ══════════════════════════════════════════════════════════════
# PATCH 2: send_grading_email — accept attachment_paths
# ══════════════════════════════════════════════════════════════

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
    \"\"\"Send daily performance report with PNG card attachments.\"\"\"
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
"send_grading_email accepts attachment_paths"))


# ══════════════════════════════════════════════════════════════
# PATCH 3: send_html_email — support multiple attachments
# ══════════════════════════════════════════════════════════════

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
    \"\"\"Send email with HTML body + optional PNG attachments.\"\"\"
    if not GMAIL_PASSWORD:
        print("  \u26a0 GMAIL_APP_PASSWORD not set.")
        return False

    if to_addr is None:
        to_addr = GMAIL_ADDRESS

    msg = MIMEMultipart('mixed')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_addr
    msg['Subject'] = subject

    # HTML + text as alternative sub-part
    alt_part = MIMEMultipart('alternative')
    alt_part.attach(MIMEText(plain_body, 'plain'))
    alt_part.attach(MIMEText(html_body, 'html'))
    msg.attach(alt_part)

    # Attach all PNGs
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
"send_html_email supports multiple PNG attachments"))


# ══════════════════════════════════════════════════════════════
# PATCH 4: cmd_run — add captions to picks email
# ══════════════════════════════════════════════════════════════

PATCHES.append(('main.py',
"""    if do_email:
        print("\\n\U0001f4e7 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            social = _social_media_card(all_picks)
            full_text = text + "\\n\\n" + social
            send_picks_email(full_text, run_type, html_body=html_content)""",
"""    # Generate PNG card
    png_card_path = None
    if all_picks:
        try:
            from card_image import generate_card_image
            png_card_path = generate_card_image(all_picks)
        except Exception as e:
            print(f"  PNG card: {e}")

    if do_email:
        print("\\n\U0001f4e7 Step 9: Sending email...")
        if all_picks:
            from emailer import send_picks_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            social = _social_media_card(all_picks)
            # Generate captions
            try:
                from card_image import generate_caption
                ig_caption = generate_caption(all_picks)
            except:
                ig_caption = ""
            caption_block = ""
            if ig_caption:
                tw_caption = ig_caption.split("\\n")[0]
                tw_caption += f"\\n\\n{len([p for p in all_picks if p.get('units',0)>=4.0])} plays locked in. Every pick tracked."
                tw_caption += "\\n\\n#ScottysEdge #SportsBetting"
                caption_block = (
                    "\\n\\n" + "="*50 +
                    "\\nINSTAGRAM CAPTION (copy below):\\n" + "="*50 +
                    "\\n" + ig_caption +
                    "\\n\\n" + "="*50 +
                    "\\nTWITTER CAPTION (copy below):\\n" + "="*50 +
                    "\\n" + tw_caption + "\\n"
                )
            full_text = text + "\\n\\n" + social + caption_block
            send_picks_email(full_text, run_type, html_body=html_content, attachment_path=png_card_path)""",
"cmd_run: PNG card + captions in picks email"))


# ══════════════════════════════════════════════════════════════
# PATCH 5: send_picks_email — pass attachment through
# ══════════════════════════════════════════════════════════════

PATCHES.append(('emailer.py',
"""def send_picks_email(picks_text, run_type='Morning', attachment_path=None, html_body=None):
    \"\"\"Send today's picks as email. If html_body provided, sends as HTML email.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"\U0001f3af {run_type} Picks \u2014 {today}"
    
    plain_body = f\"\"\"Scotty's Edge \u2014 {run_type} Picks
{datetime.now().strftime('%I:%M %p EST')}

{picks_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
"""def send_picks_email(picks_text, run_type='Morning', attachment_path=None, html_body=None):
    \"\"\"Send today's picks as email with optional PNG attachment.\"\"\"
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"\U0001f3af {run_type} Picks \u2014 {today}"
    
    plain_body = f\"\"\"Scotty's Edge \u2014 {run_type} Picks
{datetime.now().strftime('%I:%M %p EST')}

{picks_text}
\"\"\"
    
    if html_body:
        return send_html_email(subject, plain_body, html_body, attachment_path=attachment_path)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)""",
"send_picks_email passes attachment to send_html_email"))


# ══════════════════════════════════════════════════════════════
# PATCH 6: Update scheduler to include 1pm snapshot
# ══════════════════════════════════════════════════════════════

PATCHES.append(('main.py',
"""  STEP 2: Open CMD as Administrator and run these 4 commands:

    schtasks /create /tn "BettingModel_Opener" /tr "cmd /c cd /d {scripts_dir} && python main.py opener --email" /sc daily /st 08:00 /f

    schtasks /create /tn "BettingModel_Morning" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 11:00 /f

    schtasks /create /tn "BettingModel_Afternoon" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 17:30 /f

    schtasks /create /tn "BettingModel_Grade" /tr "cmd /c cd /d {scripts_dir} && python main.py grade --email" /sc daily /st 09:00 /f

  STEP 3: Verify all 4 tasks:
    schtasks /query /tn "BettingModel_Opener"
    schtasks /query /tn "BettingModel_Morning"
    schtasks /query /tn "BettingModel_Afternoon"
    schtasks /query /tn "BettingModel_Grade\"""",
"""  STEP 2: Open CMD as Administrator and run these 5 commands:

    schtasks /create /tn "BettingModel_Opener" /tr "cmd /c cd /d {scripts_dir} && python main.py opener --email" /sc daily /st 08:00 /f

    schtasks /create /tn "BettingModel_Morning" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 11:00 /f

    schtasks /create /tn "BettingModel_Snapshot" /tr "cmd /c cd /d {scripts_dir} && python main.py snapshot" /sc daily /st 13:00 /f

    schtasks /create /tn "BettingModel_Afternoon" /tr "cmd /c cd /d {scripts_dir} && python main.py run --email" /sc daily /st 17:30 /f

    schtasks /create /tn "BettingModel_Grade" /tr "cmd /c cd /d {scripts_dir} && python main.py grade --email" /sc daily /st 09:00 /f

  STEP 3: Verify all 5 tasks:
    schtasks /query /tn "BettingModel_Opener"
    schtasks /query /tn "BettingModel_Morning"
    schtasks /query /tn "BettingModel_Snapshot"
    schtasks /query /tn "BettingModel_Afternoon"
    schtasks /query /tn "BettingModel_Grade\"""",
"Scheduler: add 1pm snapshot for better CLV"))


# ══════════════════════════════════════════════════════════════

def preview():
    print("=" * 65)
    print("  MASTER PATCH — ALL AUTOMATION (PREVIEW)")
    print("=" * 65)
    pending = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  \u26a0\ufe0f  {filename}: not found"); continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"  \U0001f4dd {desc}"); pending += 1
        elif new_text[:80] in content:
            print(f"  \u2705 {desc} — already applied")
        else:
            print(f"  \u26a0\ufe0f  {desc} — text not found")
            first_line = old_text.strip().split('\n')[0].strip()
            for i, line in enumerate(content.split('\n'), 1):
                if first_line[:50] in line:
                    print(f"      Found similar at line {i}")
                    break
    print(f"\n  {pending} patches to apply. Run with --apply")


def apply():
    print("=" * 65)
    print("  MASTER PATCH — Applying")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  \u26a0\ufe0f  {filename}: not found"); continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text not in content:
            if new_text[:80] in content:
                print(f"  \u2705 {desc} — already applied")
            else:
                print(f"  \u26a0\ufe0f  {desc} — text not found")
            continue
        bak = os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak")
        if not os.path.exists(bak):
            shutil.copy2(filepath, bak)
        content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  \u2705 {desc}"); success += 1

    print(f"\n  Applied {success} patches.")
    print(f"\n  To add the 1pm snapshot to Task Scheduler, open CMD as Admin:")
    scripts_dir = SCRIPTS_DIR
    print(f'    schtasks /create /tn "BettingModel_Snapshot" /tr "cmd /c cd /d {scripts_dir} && python main.py snapshot" /sc daily /st 13:00 /f')
    print(f"\n  Test: python main.py grade --email")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
