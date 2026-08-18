# pylint: disable=line-too-long
import json
import uuid
import rail
null = None


def get_user_payload_sendemail():
    supervisor = []
    if rail.result('get_all_entries')["supervisor_email"] and "@" in rail.result('get_all_entries')["supervisor_email"] and rail.result('get_all_entries')["supervisor_uri"]:
        supervisor = [{
            "user": {
                "uri": rail.result('get_all_entries')["supervisor_uri"],
                "loginName": null
            },
            "email": null
        }]

    final_payload = {
        "email": {
            "to": [
                {
                    "user": {
                        "uri": rail.result('get_all_entries')['user_uri'],
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


def get_approve_timeoff_booking_payload():
    return {
        "timeOffUri": rail.result("publish_timeoff_draft_for_user")["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Attendance/Effort Not Marked"
    }


def get_timeoff_booking_payload(dag_run):
    is_halfday = rail.result("get_booking_details")["is_halfday"]
    book_type = "urn:replicon:time-off-relative-duration:full-day"
    if is_halfday:
        book_type = "urn:replicon:time-off-relative-duration:half-day"
    return {
        "timeOff": {
            "target": {
                "uri": rail.result('createdraft_timeoff_booking_for_user'),
            },
            "owner": {
                "uri": dag_run.conf['user_uri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": rail.result('get_booking_details')['timeoff_uri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": {
                        "year": rail.result('get_booking_date').get('year'),
                        "month": rail.result('get_booking_date').get('month'),
                        "day": rail.result('get_booking_date').get('day'),
                    },
                    "timeOfDay": null,
                    "relativeDuration": book_type,
                    "specificDuration": null
                },
                "timeOffEnd": null
            },
            "userExplicitEntries": [],
            "comments": "Attendance/Effort Not Marked",
            "customFieldValues": []
        }
    }
