import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from Config.email_Config import get_email_details

def send_email(to_email, subject, content):
    try:
        # Email server configuration
        email_details=get_email_details()
        smtp_server = email_details["smtp_server"]
        smtp_port = email_details["smtp_port"]
        sender_email = email_details["sender_email"]
        sender_password = email_details["sender_password"]

        # Create email message
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(content, "html"))

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"Email sent to {to_email}")

    except Exception as e:
        print(f"Failed to send email: {e}")
