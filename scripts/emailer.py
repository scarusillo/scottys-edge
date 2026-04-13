"""
emailer.py — Send picks and performance reports via Gmail

SETUP (one-time, 30 seconds):
  1. Go to https://myaccount.google.com/apppasswords
  2. Create an App Password for "Mail"
  3. Copy the 16-character password
  4. Set environment variable: set GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx

Your Gmail address: set via GMAIL_ADDRESS env var or default below
"""
import smtplib, os, sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

GMAIL_ADDRESS = os.environ.get('GMAIL_ADDRESS', '')
GMAIL_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', '')

def send_email(subject, body, to_addr=None, attachment_path=None):
    """Send email via Gmail SMTP. Optionally attach a file."""
    if not GMAIL_PASSWORD:
        print("  ⚠ GMAIL_APP_PASSWORD not set.")
        print("  Run: setx GMAIL_APP_PASSWORD \"your16charpassword\"")
        print("  Then close and reopen CMD.")
        return False

    if len(GMAIL_PASSWORD.replace(' ', '')) != 16:
        print(f"  ⚠ GMAIL_APP_PASSWORD looks wrong ({len(GMAIL_PASSWORD)} chars, need 16)")
        print("  Make sure to remove spaces: setx GMAIL_APP_PASSWORD \"abcdefghijklmnop\"")
        return False

    if to_addr is None:
        to_addr = GMAIL_ADDRESS

    msg = MIMEMultipart()
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = to_addr
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    # Attach file if provided
    if attachment_path and os.path.exists(attachment_path):
        from email.mime.base import MIMEBase
        from email import encoders
        try:
            with open(attachment_path, 'rb') as f:
                part = MIMEBase('text', 'html')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                filename = os.path.basename(attachment_path)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)
        except Exception as e:
            print(f"  ⚠ Attachment failed: {e}")

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_PASSWORD.replace(' ', ''))
            server.send_message(msg)
        print(f"  ✅ Email sent: {subject}")
        return True
    except smtplib.SMTPAuthenticationError:
        print("  ❌ Gmail authentication failed!")
        print("  This means your app password is wrong.")
        print("  Go to https://myaccount.google.com/apppasswords")
        print("  Create a NEW app password and try again.")
        print("  Use: setx GMAIL_APP_PASSWORD \"new16charpassword\"")
        return False
    except Exception as e:
        print(f"  ⚠ Email failed: {e}")
        return False


def send_picks_email(picks_text, run_type='Morning', attachment_path=None, attachment_paths=None, html_body=None):
    """Send today's picks as email with optional PNG attachment(s)."""
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"🎯 {run_type} Picks — {today}"

    plain_body = f"""Scotty's Edge — {run_type} Picks
{datetime.now().strftime('%I:%M %p EST')}

{picks_text}
"""

    # Support both single path and list of paths
    paths = attachment_paths or ([attachment_path] if attachment_path else [])

    if html_body:
        return send_html_email(subject, plain_body, html_body, attachment_paths=paths)
    elif len(paths) > 1:
        return _send_multi_attachment(subject, plain_body, paths)
    else:
        return send_email(subject, plain_body, attachment_path=paths[0] if paths else None)


def _send_multi_attachment(subject, body, paths):
    """Send email with multiple file attachments."""
    if not GMAIL_PASSWORD:
        print("  ⚠ GMAIL_APP_PASSWORD not set.")
        return False

    from email.mime.base import MIMEBase
    from email import encoders

    msg = MIMEMultipart()
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = GMAIL_ADDRESS
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    for path in paths:
        if path and os.path.exists(path):
            with open(path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
                msg.attach(part)
            print(f"  📎 Attached: {os.path.basename(path)}")

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_PASSWORD.replace(' ', ''))
            server.send_message(msg)
        print(f"  ✅ Email sent: {subject}")
        return True
    except Exception as e:
        print(f"  ⚠ Email failed: {e}")
        return False


def send_grading_email(report_text, attachment_path=None, html_body=None, attachment_paths=None):
    """Send daily performance report with PNG cards."""
    today = datetime.now().strftime('%Y-%m-%d')
    subject = f"📊 Performance Report — {today}"
    
    plain_body = f"""Scotty's Edge — Daily Performance Report
{datetime.now().strftime('%I:%M %p EST')}

{report_text}
"""
    
    if html_body:
        return send_html_email(subject, plain_body, html_body, attachment_paths=attachment_paths)
    else:
        return send_email(subject, plain_body, attachment_path=attachment_path)


def send_html_email(subject, plain_body, html_body, to_addr=None, attachment_path=None, attachment_paths=None):
    """Send email with HTML body + PNG attachments."""
    if not GMAIL_PASSWORD:
        print("  ⚠ GMAIL_APP_PASSWORD not set.")
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
                print(f"  📎 Attached: {os.path.basename(img_path)}")
            except Exception as e:
                print(f"  ⚠ Attachment failed: {e}")

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_PASSWORD.replace(' ', ''))
            server.send_message(msg)
        print(f"  ✅ Email sent: {subject}")
        return True
    except Exception as e:
        print(f"  ⚠ Email failed: {e}")
        return False


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        print("Testing email...")
        send_email("Test — Betting Model",
                   f"Email works! Sent at {datetime.now().strftime('%I:%M %p')}")
    else:
        print(__doc__)
