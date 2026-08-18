from datetime import datetime as dt, timedelta
import json
import rail

def get_run_report_payload(duration_days):
    get_specific_report_details = rail.result('get_specific_report_details')
    number_of_days = 3 if dt.now().weekday == 0 else 2

    return {
                "reportParameters": [
                {
    "reportUri": get_specific_report_details['uri'],
    "filterValues": [
        {
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"],'displayText', 'DateFilter', 'uri'),
            "value": None
        },
        {
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"],'displayText', 'DateFilter', 'uri'),
            "value": str((dt.now() - timedelta(days=duration_days)).strftime("%m/%d/%Y"))
        },
        {
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"],'displayText', 'DateFilter', 'uri'),
            "value": str((dt.now() - timedelta(days=number_of_days)).strftime("%m/%d/%Y"))

        }
    ],
    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
     ]
    }

def get_run_timesheet_report_payload(duration_days):
    get_specific_timesheet_report_details = rail.result('get_timesheet_specific_report_details')
    number_of_days = 3 if dt.now().weekday == 0 else 2

    return {
                "reportParameters": [
                {
    "reportUri": get_specific_timesheet_report_details['uri'],
    "filterValues": [
        {
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            get_specific_timesheet_report_details["filterConfiguration"]["enabledFilters"],'displayText', 'EntryDateFilter', 'uri'),
            "value": None
        },
        {
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            get_specific_timesheet_report_details["filterConfiguration"]["enabledFilters"],'displayText', 'EntryDateFilter', 'uri'),
            "value": str((dt.now() - timedelta(days=duration_days)).strftime("%m/%d/%Y"))
        },
        {
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(
            get_specific_timesheet_report_details["filterConfiguration"]["enabledFilters"],'displayText', 'EntryDateFilter', 'uri'),
            "value": str((dt.now() - timedelta(days=number_of_days)).strftime("%m/%d/%Y"))

        }
    ],
    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
     ]
    }


def process_process_notofication_for_user_conf(item):
    return {
        'userUri' : item['useruri'],
        'user_last_name' :  item['User_Last_Name'],
        'user_first_name': item['User_First_Name']
    }
def get_final_payload_sendemail(username,useruri, html_body):
    subject_line = f'{username}: Time Entries Due for Submission'
    final_payload = {"email": {
        "to": [
            {
                "user": {
                    "uri": useruri,
                    "loginName": None
                },
                "email": None
            }
        ],
        "cc": [],
        "bcc": [],
        "replyTo": None,
        "fromDisplayName":"Do-Not-Reply@deltek.com",
        "subject": subject_line,
        "htmlBody": html_body,
        "textBody": None,
        "attachments": []
    }}
    return json.dumps(final_payload)
