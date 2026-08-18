# pylint: disable=line-too-long
import json
import rail
null = None


def get_first_payload_sendemail2():
    # subject = "Timesheet Reminder- Due Date is Monday, 12:00 p.m. CST"
    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": rail.result('foreach_reminder_entry')['user_uri'],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": [],
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": rail.result('get_email_subject'),
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)


def get_first_payload_sendemail():
    supervisor = []
    if rail.result('foreach_reminder_entry')["supervisor_email"] and "@" in rail.result('foreach_reminder_entry')["supervisor_email"] and rail.result('foreach_reminder_entry')["supervisor_uri"]:
        supervisor = [{
            "user": {
                "uri": rail.result('foreach_reminder_entry')["supervisor_uri"],
                "loginName": null
            },
            "email": null
        }]

    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": rail.result('foreach_reminder_entry')['user_uri'],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": supervisor,
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": rail.result('get_email_subject'),
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)


def get_final_payload_sendemail():
    supervisor = []
    if rail.result('foreach_reminder_entry')["supervisor_email"] and "@" in rail.result('foreach_reminder_entry')["supervisor_email"] and rail.result('foreach_reminder_entry')["supervisor_uri"]:
        supervisor = [{
            "user": {
                "uri": rail.result('foreach_reminder_entry')["supervisor_uri"],
                "loginName": null
            },
            "email": null
        }]

    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": rail.result('foreach_reminder_entry')['user_uri'],
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": supervisor,
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": rail.result('get_email_subject'),
            "htmlBody": rail.result('get_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)


def get_hr_payload_sendemail():

    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": rail.result('get_hr_uri'),
                        "loginName": null
                    },
                    "email": null
                }
            ],
            "cc": [],
            "bcc": [],
            "replyTo": null,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": rail.result('get_hr_email_subject'),
            "htmlBody": rail.result('get_hr_email_body'),
            "textBody": null,
            "attachments": []
        }
    }
    return json.dumps(final_payload)
