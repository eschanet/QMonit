
import smtplib, ssl
from email.mime.text import MIMEText

def send_email(host='smtp.cern.ch', port=587, **kwargs):

    message = kwargs.pop('message')
    sender = kwargs.pop('sender')
    subject = kwargs.pop('subject')
    recipients = kwargs.pop('recipients')
    password = kwargs.pop('password')

    msg = MIMEText(message, 'plain')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    if kwargs:
        raise TypeError('Unexpected kwargs provided: %s' % list(kwargs.keys()))

    context = ssl.create_default_context()

    server = smtplib.SMTP(host, port)
    server.starttls()
    server.login(sender, password)
    server.sendmail(sender, recipients, msg.as_string())

    return True
