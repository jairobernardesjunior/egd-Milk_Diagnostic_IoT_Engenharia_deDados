import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def envia_email(url_imapx, portax, remetentex, passwx, destinatariox, textox, subjectx):
    message = MIMEMultipart("alternative")
    message["Subject"] = subjectx
    message["From"] = remetentex
    message["To"] = destinatariox

    # Create the plain-text and HTML version of your message
    text = textox
    part1 = MIMEText(text, "plain")
    message.attach(part1)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(url_imapx, portax, context=context) as server:
        server.login(remetentex, passwx)
        server.sendmail(remetentex, destinatariox, message.as_string())