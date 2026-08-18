from datetime import datetime, timedelta
import json
import rail
from cie_wipro.ireland_absconding_employee_notification import config

null = None


def get_to_report_filter_uris(config, start_date, end_date):
    data = rail.result("get_to_report_details")[
        'filterConfiguration']['enabledFilters']
    report_uri = rail.result("get_to_report_details")['uri']
    to_period_filter = rail.find_first_by_attr_and_get_attr(
        data, 'displayText', config.TO_DATE_RANGE_FILTER, 'uri')
    to_type_filter = rail.find_first_by_attr_and_get_attr(
        data, 'displayText', config.TO_TYPE_FILTER, 'uri')

    filters = [
        {
            "reportFilterUri": to_period_filter,
            "value": null
        },
        {
            "reportFilterUri": to_period_filter,
            "value": start_date.strftime(config.report_date_format)
        },
        {
            "reportFilterUri": to_period_filter,
            "value": end_date.strftime(config.report_date_format)

        },
        {
            "reportFilterUri": to_type_filter,
            "value": config.annual_leave_filter_value
        },
        {
            "reportFilterUri": to_type_filter,
            "value": config.lwop_leave_filter_value
        }
    ]

    report_input = {
        "reportParameters": [
            {
                "reportUri": report_uri,
                "filterValues": filters,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }]
    }

    return json.dumps(report_input)


def add_recipients_users(key, val, store):
    cc = {
        "user": {
            "uri": val if key == "uri" else None,
            "loginName": val if key == "loginName" else None
        },
        "email": None
    }
    store.append(cc)


def get_employee_payload_sendemail():
    reminder_entry = rail.result('foreach_reminder_entry')

    # Extract common fields once
    reminder_index = reminder_entry.get("reminder_index")
    supervisor_uri = reminder_entry.get("Supervisor URI")
    user_uri = reminder_entry.get("UserUri")

    # supervisor_email = reminder_entry.get("User Supervisor Email address")
    hr_manager_uri = reminder_entry.get("hrManagerUserUri")
    gpo_uri = reminder_entry.get("gpoUserUri")

    # Email lists
    emailTo, emailCC = [], []

    # Handle 'to' recipients
    add_recipients_users(
        "uri", supervisor_uri if reminder_index == "4" else user_uri, emailTo)

    # Handle 'cc' recipients based on reminder index
    if reminder_index in ["2", "3", "4"]:
        if reminder_index in ["2", "3"] and supervisor_uri:
            add_recipients_users("uri", supervisor_uri, emailCC)

        if reminder_index in ["3", "4"] and hr_manager_uri:
            add_recipients_users("uri", hr_manager_uri, emailCC)

        if reminder_index == "4" and gpo_uri:
            add_recipients_users("uri", gpo_uri, emailCC)

    # Final payload
    final_payload = {
        "email": {
            "to": emailTo,
            "cc": emailCC,
            "bcc": [],
            "replyTo": None,
            "fromDisplayName": "Do-Not-Reply@deltek.com",
            "subject": rail.result('get_email_subject'),
            "htmlBody": rail.result('get_email_body'),
            "textBody": None,
            "attachments": []
        }
    }

    return json.dumps(final_payload)


def bulk_get_user2():
    employeeIds = rail.result('gpo_and_hr_manager_empid')
    empIdObj = []
    for empid in employeeIds:
        empIdObj.append(
            {
                "uri": null,
                "loginName": null,
                "employeeId": empid,
                "parameterCorrelationId": null
            }
        )

    final_payload = {
        "users": empIdObj
    }

    return json.dumps(final_payload)


def bulk_get_user_details():
    users = rail.result('get_gpo_and_hr_manager_data')
    final_payload = {
        "userUri": users
    }
    return json.dumps(final_payload)
