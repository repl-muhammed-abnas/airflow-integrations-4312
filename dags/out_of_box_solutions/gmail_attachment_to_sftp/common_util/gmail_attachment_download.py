"""
Steps to be done before using the Function

1. Go to `https://console.cloud.google.com/` website and create a project
2. Go to `APIs and Services` -> `Enable APIs and Services` -> `Search GMail` -> `Enable API`
3. Go to OAuth consent screen fill the necessary fields and click next. Define the scope of your app
    to find optimal scope refer `https://developers.google.com/apis-explorer`
4. Go to `Credentials` -> `CREATE CREDENTIALS` -> `OAuth client ID` -> select application type as `Desktop app` -> Create
5. Install the Google client library for Python `pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib`
6. Download the JSON file, save it as `credentials.json` and RUN the code `generate_token_using_oauth.py`. Requires to provide consent
7. Create the airflow variable and add the token details in it. Make sure the variable name contains `token` or `secrete` in it

`SCOPE` are required to specify as it limits the action that can be done
if your APP have permission for modify, but while making connection you didn't specify the
modify SCOPE it will throw `Request had insufficient authentication scopes.` error while performing modifications]
for example,
    for read: ['https://www.googleapis.com/auth/gmail.readonly']
    for modify: ['https://www.googleapis.com/auth/gmail.modify']
"""

# pylint: disable=no-member
import base64
from email.utils import parseaddr
import json
import os
import chardet
from airflow.models import Variable
from airflow.utils.operator_helpers import make_kwargs_callable
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from rail.lib.artifact import new_artifact
import rail

GMAIL_MODIFY_SCOPE = ['https://www.googleapis.com/auth/gmail.modify']
GMAIL_READONLY_SCOPE = ['https://www.googleapis.com/auth/gmail.readonly']

def get_encoding_of_attachment(attachment_data_byte):
    return chardet.detect_all(attachment_data_byte)[0].get('encoding')

def get_credentials_info_from_airflow_variable(variable_name):
    credentials_info = Variable.get(variable_name)
    return json.loads(credentials_info)

def get_credentials(credentials_info, scope):
    if not credentials_info:
        raise Exception("credentials_info not found")

    return Credentials.from_authorized_user_info(info=credentials_info, scopes=scope)

def refresh_token(variable_name):
    creds = get_credentials(get_credentials_info_from_airflow_variable(variable_name), scope= GMAIL_MODIFY_SCOPE)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        print("Token Refreshed")
        # update the airflow variable with refreshed tokens
        Variable.set(variable_name, value=creds.to_json())
        print(f"Token details updated for `{variable_name}` variable")


# Define a function to get the authorized Gmail API client
# `credentials_info` will get from the airflow_variable which needs to be parsed as JSON
def get_gmail_service(variable_name):

    credentials = get_credentials(get_credentials_info_from_airflow_variable(variable_name), scope=GMAIL_MODIFY_SCOPE)
    service = build('gmail', 'v1', credentials=credentials, cache_discovery=False)
    return service


def mark_email_as_unread_batch(service, user_id, msg_ids):
    service.users().messages().batchModify(
        userId=user_id,
        body={
            'ids': msg_ids,
            'addLabelIds': ['UNREAD']
        }
    ).execute()


def mark_email_as_unread(service, user_id, msg_id):
    msg_labels = {'addLabelIds': ['UNREAD']}
    service.users().messages().modify(userId=user_id, id=msg_id, body=msg_labels).execute()


def mark_email_as_read_batch(service, user_id, msg_ids):
    service.users().messages().batchModify(
        userId=user_id,
        body={
            'ids': msg_ids,
            'removeLabelIds': ['UNREAD']
        }
    ).execute()

def mark_mail_as_read(service, user_id, msg_id):
    msg_labels = {'removeLabelIds': ['UNREAD']}
    service.users().messages().modify(userId=user_id, id=msg_id, body=msg_labels).execute()

def create_artifact(data, file_format):
    with new_artifact(mode="w") as attachment:
        attachment.file.write(data)
        attachment.set_attribute(name="type", value=file_format)
        return attachment.name

def get_file_content(service, email_body, msg_id, file_format, user_id):

    if 'attachmentId' in email_body:
        attachment_id = email_body['attachmentId']
        attachment = service.users().messages().attachments().get(
            userId=user_id,
            messageId=msg_id,
            id=attachment_id).execute()

        data = base64.urlsafe_b64decode(attachment['data'])
        return create_artifact(str(data, get_encoding_of_attachment(data)), file_format)

    return None

# pylint: disable=too-many-branches
def extract_attachments_from_gmail(creds_variable_name, query, file_format, gmail_user_id='me'):

    # `creds_variable_name`: type=str : airflow variable name which contains the token details of the OAuth
    # `query`: type=str : query to retrieve data
    # `gmail_user_id`: type= str : by Default set to `me` (token user)
    # `file_format`: type=str : Attachment file format

    if callable(file_format):
        kwargs_callable = make_kwargs_callable(file_format)
        file_format = kwargs_callable()

    if callable(creds_variable_name):
        kwargs_callable = make_kwargs_callable(creds_variable_name)
        creds_variable_name = kwargs_callable()

    service = get_gmail_service(creds_variable_name)

    if not service:
        raise Exception("Not able to connect to GMail API services")

    if callable(query):
        kwargs_callable = make_kwargs_callable(query)
        query = kwargs_callable()

    if not query:
        query="is:unread"

    results = service.users().messages().list(userId=gmail_user_id, q=query).execute()

    email_messages = results.get('messages', [])
    if not email_messages:
        return []

    try:
        emails_info = []
        for message in email_messages:
            # get the message details
            msg = service.users().messages().get(userId='me', id=message['id']).execute()

            # `parts` contains the attachment for reference you can checkout `sample_mail_response.json`
            message_parts = msg['payload'].get('parts')

            if not message_parts:
                email_subject = list(filter(lambda x: x['name'] == 'Subject', msg['payload']['headers']))[-1].get('value', None)
                print(f"Email '{email_subject}' does not have any message parts.")
                mark_mail_as_read(service, gmail_user_id,msg['id'])
                continue

            for part in message_parts:
                if 'filename' in part:
                    filename = part['filename']
                    file_ext = os.path.splitext(filename)[1][1:]
                    if file_ext == file_format:
                        attachment_file_artifact = get_file_content(service, part['body'], msg['id'], file_format, gmail_user_id)
                        emails_info.append(
                            {
                                "msg_id": msg['id'],
                                "from_email_address": parseaddr(rail.find_first_by_attr_and_get_attr(msg['payload']['headers'],'name','From','value'))[1],
                                "file_name": filename,
                                "artifact": attachment_file_artifact,
                                "email_subject": list(filter(lambda x: x['name']=='Subject', msg['payload']['headers']))[-1].get('value', None)
                            })
            mark_mail_as_read(service, gmail_user_id,msg['id'])

    except Exception:
        # mark messages as unread again in case of failure, to be picked again
        mark_email_as_unread_batch(service, gmail_user_id, [msg['id'] for msg in email_messages])
        raise

    return emails_info
