"""
caption_email_patch.py — Add PNG cards + copyable captions to all emails

Patches:
  1. Grade email (9am) — generates result cards, stats card, adds captions
  2. send_grading_email — accepts attachment_paths
  3. send_html_email — supports multiple attachments
  4. Picks email (11am/5:30pm) — adds captions to body

Usage:
    python caption_email_patch.py              # Preview
    python caption_email_patch.py --apply      # Apply
"""
import os, sys, shutil
from datetime import datetime

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(SCRIPTS_DIR, 'backup_caption_email')

PATCHES = []

PATCHES.append(('main.py',
    '    report = daily_grade_and_report(conn)\n    conn.close()\n\n    if do_email:\n        # Generate results HTML for inline email\n        results_html_content = None\n        if report:\n            try:\n                results_path, results_html_content = _generate_results_html(report)\n            except Exception as e:\n                results_html_content = None\n                print(f"  Results HTML: {e}")\n        \n        from emailer import send_grading_email\n        send_grading_email(report, html_body=results_html_content)',
    '''    report = daily_grade_and_report(conn)

    # Generate PNG cards (FREE — no API calls)
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
        if stats_png: card_paths.append(stats_png)
    except Exception as e:
        print(f"  PNG cards: {e}")

    # Generate results captions
    results_caption = ""
    try:
        _yb = conn.execute("""
            SELECT selection, result, pnl_units FROM graded_bets
            WHERE DATE(created_at) = (SELECT MAX(DATE(created_at)) FROM graded_bets WHERE result NOT IN ('DUPLICATE','PENDING'))
            AND result NOT IN ('DUPLICATE','PENDING') ORDER BY pnl_units DESC
        """).fetchall()
        _all = conn.execute("""
            SELECT result, pnl_units FROM graded_bets
            WHERE DATE(created_at) >= '2026-03-04' AND result NOT IN ('DUPLICATE','PENDING')
        """).fetchall()
        _yw = sum(1 for b in _yb if b[1]=='WIN')
        _yl = sum(1 for b in _yb if b[1]=='LOSS')
        _yp = sum(b[2] or 0 for b in _yb)
        _tw = sum(1 for b in _all if b[0]=='WIN')
        _tl = sum(1 for b in _all if b[0]=='LOSS')
        _tp = sum(b[1] or 0 for b in _all)
        _twg = sum(abs(b[1] or 0) for b in _all)
        _twp = _tw/(_tw+_tl)*100 if (_tw+_tl)>0 else 0
        _tr = (_tp/_twg*100) if _twg>0 else 0
        _pick_lines = []
        for b in _yb:
            icon = chr(9989) if b[1]=='WIN' else chr(10060)
            _pick_lines.append(f"{icon} {b[0]} | {b[2]:+.1f}u")
        ig = f"Yesterday's Results\\n\\n{_yw}W-{_yl}L | {_yp:+.1f}u\\n\\n" + "\\n".join(_pick_lines)
        ig += f"\\n\\nSeason: {_tw}W-{_tl}L | {_tp:+.1f}u | {_twp:.1f}% | ROI {_tr:+.1f}%"
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
        results_html_content = None
        if report:
            try:
                results_path, results_html_content = _generate_results_html(report)
            except Exception as e:
                results_html_content = None
                print(f"  Results HTML: {e}")

        from emailer import send_grading_email
        report_with_captions = report + results_caption if isinstance(report, str) else report
        send_grading_email(report_with_captions, html_body=results_html_content, attachment_paths=card_paths)''',
    "Grade email: PNG cards + captions"))

PATCHES.append(('emailer.py',
    'def send_grading_email(report_text, attachment_path=None, html_body=None):\n    """Send daily performance report."""\n    today = datetime.now().strftime(\'%Y-%m-%d\')\n    subject = f"\\U0001f4ca Performance Report \\u2014 {today}"\n    \n    plain_body = f"""Scotty\'s Edge \\u2014 Daily Performance Report\n{datetime.now().strftime(\'%I:%M %p EST\')}\n\n{report_text}\n"""\n    \n    if html_body:\n        return send_html_email(subject, plain_body, html_body)\n    else:\n        return send_email(subject, plain_body, attachment_path=attachment_path)',
    'def send_grading_email(report_text, attachment_path=None, html_body=None, attachment_paths=None):\n    """Send daily performance report with PNG cards."""\n    today = datetime.now().strftime(\'%Y-%m-%d\')\n    subject = f"\\U0001f4ca Performance Report \\u2014 {today}"\n    \n    plain_body = f"""Scotty\'s Edge \\u2014 Daily Performance Report\n{datetime.now().strftime(\'%I:%M %p EST\')}\n\n{report_text}\n"""\n    \n    if html_body:\n        return send_html_email(subject, plain_body, html_body, attachment_paths=attachment_paths)\n    else:\n        return send_email(subject, plain_body, attachment_path=attachment_path)',
    "send_grading_email accepts attachment_paths"))

PATCHES.append(('main.py',
    '            from emailer import send_picks_email\n            text = picks_to_text(all_picks, f"{run_type} Picks")\n            social = _social_media_card(all_picks)\n            full_text = text + "\\n\\n" + social',
    '''            from emailer import send_picks_email
            text = picks_to_text(all_picks, f"{run_type} Picks")
            social = _social_media_card(all_picks)
            try:
                from card_image import generate_caption
                ig_caption = generate_caption(all_picks)
            except:
                ig_caption = ""
            caption_block = ""
            if ig_caption:
                tw_caption = ig_caption.split("\\n")[0] + f"\\n\\n{len([p for p in all_picks if p.get('units',0)>=4.0])} plays locked in. Every pick tracked.\\n\\n#ScottysEdge #SportsBetting"
                caption_block = (
                    "\\n\\n" + "="*50 +
                    "\\nINSTAGRAM CAPTION (copy below):\\n" + "="*50 +
                    "\\n" + ig_caption +
                    "\\n\\n" + "="*50 +
                    "\\nTWITTER CAPTION (copy below):\\n" + "="*50 +
                    "\\n" + tw_caption + "\\n"
                )
            full_text = text + "\\n\\n" + social + caption_block''',
    "Picks email: add captions to body"))


def preview():
    print("=" * 65)
    print("  CAPTION + CARD EMAIL INTEGRATION (PREVIEW)")
    print("=" * 65)
    pending = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  {filename}: not found"); continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text in content:
            print(f"  READY: {desc}"); pending += 1
        elif new_text[:60] in content:
            print(f"  DONE: {desc}")
        else:
            print(f"  MISMATCH: {desc}")
    print(f"\n  {pending} patches to apply. Run with --apply")


def apply():
    print("=" * 65)
    print("  CAPTION + CARD EMAIL INTEGRATION — Applying")
    print("=" * 65)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    for filename, old_text, new_text, desc in PATCHES:
        filepath = os.path.join(SCRIPTS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  {filename}: not found"); continue
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        if old_text not in content:
            if new_text[:60] in content:
                print(f"  DONE: {desc}")
            else:
                print(f"  MISMATCH: {desc}")
            continue
        shutil.copy2(filepath, os.path.join(BACKUP_DIR, f"{filename}.{ts}.bak"))
        content = content.replace(old_text, new_text, 1)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  APPLIED: {desc}"); success += 1
    print(f"\n  Applied {success} patches.")


if __name__ == '__main__':
    if '--apply' in sys.argv:
        apply()
    else:
        preview()
