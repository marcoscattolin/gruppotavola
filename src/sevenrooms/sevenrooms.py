import imaplib
import re
import email
import os
import json
from grptavutils.constants import Storage
from grptavutils import write_parquet
import pandas as pd
from io import BytesIO

def login():

    with open("../../secrets/yahoo_sevenrooms.json", "r") as f:
        creds = json.load(f)

    email_account = creds["email_address"]
    password = creds["password"]

    mailbox = imaplib.IMAP4_SSL("imap.mail.yahoo.com")
    mailbox.login(email_account, password)

    return mailbox

def download_attachment(email_id):

    _, data = mailbox.fetch(email_id, '(RFC822)' )
    raw_email = data[0][1]

    # converts byte literal to string removing b''
    raw_email_string = raw_email.decode('utf-8')
    email_message = email.message_from_string(raw_email_string)

    # downloading attachments
    for part in email_message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        attachment = part.get_filename()

        if attachment.startswith("seven_rooms_future_"):
            filename = Storage.future_reservations + "seven_rooms_future.parquet"
        elif attachment.startswith("seven_rooms_yesterday_"):
            filename = Storage.yesterday_reservations + attachment
            filename = re.sub("csv$", "parquet", filename)

        else:
            filename = None

        if filename is not None:
            csv_data = BytesIO(part.get_payload(decode=True))
            df = pd.read_csv(csv_data)
            write_parquet(dataframe=df, container=Storage.bronze, file_path=filename)

def parse_uid(data):
    pattern_uid = re.compile(r"\d+ \(UID (?P<uid>\d+)\)")
    match = pattern_uid.match(data)
    return match.group('uid')


if __name__ == "__main__":

    # login
    mailbox = login()

    # lista email della inbox
    mailbox.select(mailbox="Inbox", readonly=False)
    _, items = mailbox.search(None, 'All')
    email_ids = items[0].split()

    # per ognuna
    for msg in email_ids:

        # scarica allegato
        download_attachment(msg)

        # copy into sevenrooms
        resp, data = mailbox.fetch(msg, "(UID)")
        msg_uid = parse_uid(data[0].decode())
        result = mailbox.uid('COPY', msg_uid, "sevenrooms")

        # delete from inbox
        if result[0] == 'OK':
            mov, data = mailbox.uid('STORE', msg_uid , '+FLAGS', '(\Deleted)')
            mailbox.expunge()

    # logout
    mailbox.logout()

