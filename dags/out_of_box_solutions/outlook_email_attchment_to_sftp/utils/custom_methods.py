
from os import path

from out_of_box_solutions.outlook_email_attchment_to_sftp.outlook.OutlookConnection import OutlookConnection

def _get_var_values(key):
    from airflow.models import Variable
    var_value = Variable.get(key, default_var=None, deserialize_json=True)
    if not var_value:
        raise Exception(f"No Data found in the Variable for key: {key}")
    return var_value

def create_artifact(data, file_format):
    from rail.lib.artifact import new_artifact

    with new_artifact(mode="w") as attachment:
        attachment.file.write(data)
        attachment.set_attribute(name="type", value=file_format)
        return attachment.name

def get_encoding_of_attachment(attachment_data_byte):
    import chardet
    return chardet.detect_all(attachment_data_byte)[0].get('encoding')


def create_attachment_artifact(content, file_format):
    from base64 import urlsafe_b64decode
    data = urlsafe_b64decode(content)

    return create_artifact(
        data=str(data, get_encoding_of_attachment(data)),
        file_format=file_format
    )

def extract_attachments_from_outlook_email(dag_run):
    from out_of_box_solutions.outlook_email_attchment_to_sftp.outlook.OutlookEmail import OutlookEmail

    allowed_file_formats = dag_run.conf['allowed_formats']
    conn = OutlookEmail(
        outlook_connection_var_name=dag_run.conf['creds_variable_name'],
        user=dag_run.conf['shared_account_user_name']
    )

    unread_emails = conn.get_emails_from_folder(
        folder=dag_run.conf['folder_name'],
        query=dag_run.conf['outlook_query'],
    )

    if not unread_emails['response']:
        return {
            "status_code": 204,
            "status_message": "No Unread Emails found for the given query",
            "response": unread_emails
        }

    response_success = []
    response_exception = []

    try:
        for email in unread_emails['response']:
            if not email['hasAttachments']:
                response_exception.append({
                    "email_id": email['id'],
                    "subject": email['subject'],
                    "sender_name": email['sender']['emailAddress']['name'],
                    "sender_email": email['sender']['emailAddress']['address'],
                    "date": email['receivedDateTime'],
                    "status_code": 204,
                    "status_message": "No Attachments found in the email",
                    "response": [],
                    "mark_as_read": True
                })
            else:
                attachments = conn.get_email_attachments(
                    email['id']
                )
                if not attachments['status_code'] == 200:
                    response_exception.append({
                        "email_id": email['id'],
                        "subject": email['subject'],
                        "sender_name": email['sender']['emailAddress']['name'],
                        "sender_email": email['sender']['emailAddress']['address'],
                        "date": email['receivedDateTime'],
                        "status_code": attachments['status_code'],
                        "status_message": attachments['status_message'],
                        "response": [],
                        "mark_as_read": False
                    })
                else:
                    for attachment in attachments['response']:
                        file_format = path.splitext(attachment['name'])[1][1:]
                        if file_format not in allowed_file_formats:
                            response_exception.append(
                                {
                                    "email_id": email['id'],
                                    "subject": email['subject'],
                                    "sender_name": email['sender']['emailAddress']['name'],
                                    "sender_email": email['sender']['emailAddress']['address'],
                                    "date": email['receivedDateTime'],
                                    "status_code": 415,
                                    "status_message": f"Attachment processing skipped as `{file_format}` format is not allowed",
                                    "response": {
                                        "attachment_name": attachment['name'],
                                        "attachment_ext": file_format,
                                        "attachment_content_artifact": "NA"

                                    },
                                    "mark_as_read": True
                                }
                            )
                        else:
                            response_success.append(
                                {
                                    "email_id": email['id'],
                                    "subject": email['subject'],
                                    "sender_name": email['sender']['emailAddress']['name'],
                                    "sender_email": email['sender']['emailAddress']['address'],
                                    "date": email['receivedDateTime'],
                                    "status_code": 200,
                                    "status_message": "Attachment fetched successfully",
                                    "response": {
                                        "attachment_name": attachment['name'],
                                        "attachment_id": attachment['id'],
                                        "attachment_ext": file_format,
                                        "attachment_content_artifact": create_attachment_artifact(attachment['contentBytes'], file_format),

                                    },
                                    "mark_as_read": True
                                }
                            )
            conn.mark_email_as_read(email['id'])
    except Exception as e:
        conn.mark_email_as_unread_batch([ email['id'] for email in unread_emails['response'] ], log_message="Failure occurred while storing attachments, marking all fetched emails as unread")
        raise e
    
    return {
            "status_code": 200,
            "status_message": f"Found {len(unread_emails['response'])} Unread Emails found for the given query",
            "response": {
                "response_success": response_success,
                "response_exception": response_exception
            }
         }

