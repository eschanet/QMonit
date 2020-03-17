
import smtplib, ssl
from email.mime.text import MIMEText

def send_email(sender='adcmon@cern.ch', host='smtp.cern.ch', port=587, subject='Error occurred', message='Error occurred', recipients=['eric.schanet@cern.ch'], **kwargs):

    password = kwargs.pop('password')

    if kwargs:
        raise TypeError('Unexpected kwargs provided: %s' % list(kwargs.keys()))

    msg = MIMEText(message, 'plain')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    context = ssl.create_default_context()

    server = smtplib.SMTP(host, port)
    server.starttls()
    server.login(sender, password)
    server.sendmail(sender, recipients, msg.as_string())

    return True
