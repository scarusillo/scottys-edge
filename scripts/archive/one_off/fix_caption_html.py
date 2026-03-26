"""
fix_caption_html.py — Make captions visible in HTML emails

Problem: Captions go into plain text, but email apps show HTML and ignore plain text.
Fix: Embed captions at the bottom of the HTML body.

Fixes both grade email (9am) and picks email (11am/5:30pm).

Usage:
    python fix_caption_html.py --apply
"""
import os, sys, shutil

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
filepath = os.path.join(SCRIPTS_DIR, 'main.py')

PATCHES = []

# Grade email fix
PATCHES.append((
    """        from emailer import send_grading_email
        report_with_captions = report + results_caption if isinstance(report, str) else report
        send_grading_email(report_with_captions, html_body=results_html_content, attachment_paths=card_paths)""",
    """        # Embed captions in HTML so email clients show them
        if results_html_content and results_caption:
            cap_html = results_caption.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\\n', '<br>')
            results_html_content = results_html_content.replace('</body>',
                f'<div style="max-width:1080px;margin:30px auto;padding:24px 40px;font-family:Consolas,monospace;font-size:13px;color:rgba(255,255,255,0.7);background:#0a0a0a;line-height:1.8;white-space:pre-wrap;">{cap_html}</div></body>')

        from emailer import send_grading_email
        report_with_captions = report + results_caption if isinstance(report, str) else report
        send_grading_email(report_with_captions, html_body=results_html_content, attachment_paths=card_paths)""",
    "Grade email: captions in HTML"))

# Picks email fix
PATCHES.append((
    """            full_text = text + "\\n\\n" + social + caption_block
            send_picks_email(full_text, run_type, html_body=html_content, attachment_path=png_card_path)""",
    """            full_text = text + "\\n\\n" + social + caption_block
            # Embed captions in HTML so email clients show them
            if html_content and caption_block:
                cap_html = caption_block.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\\n', '<br>')
                html_content = html_content.replace('</body>',
                    f'<div style="max-width:1080px;margin:30px auto;padding:24px 40px;font-family:Consolas,monospace;font-size:13px;color:rgba(255,255,255,0.7);background:#0a0a0a;line-height:1.8;white-space:pre-wrap;">{cap_html}</div></body>')
            send_picks_email(full_text, run_type, html_body=html_content, attachment_path=png_card_path)""",
    "Picks email: captions in HTML"))


if '--apply' in sys.argv:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    shutil.copy2(filepath, filepath + '.caption_fix.bak')
    applied = 0
    for old, new, desc in PATCHES:
        if old in content:
            content = content.replace(old, new, 1)
            print(f"  \u2705 {desc}")
            applied += 1
        elif new[:60] in content:
            print(f"  \u2705 {desc} — already applied")
        else:
            print(f"  \u26a0\ufe0f {desc} — text not found")
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"\n  Applied {applied} fixes.")
else:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    for old, new, desc in PATCHES:
        if old in content:
            print(f"  Ready: {desc}")
        elif new[:60] in content:
            print(f"  Done: {desc}")
        else:
            print(f"  Missing: {desc}")
    print("\n  Run with --apply")
