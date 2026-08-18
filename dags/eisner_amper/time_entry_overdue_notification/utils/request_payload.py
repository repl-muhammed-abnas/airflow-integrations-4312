from datetime import datetime as dt, timedelta
import json
import rail

def get_run_report_payload(duration_days):
    get_specific_report_details = rail.result('get_specific_report_details')
    begin_of_week = dt.utcnow() - timedelta(days=dt.utcnow().weekday())
    def get_specific_filter_uri(filter_name):
        return rail.find_first_by_attr_and_get_attr(
            get_specific_report_details["filterConfiguration"]["enabledFilters"],'displayText', filter_name, 'uri')

    return {
                "reportParameters": [
                {
    "reportUri": get_specific_report_details['uri'],
    "filterValues": [
        {
            "reportFilterUri": get_specific_filter_uri(filter_name = "EntryDateFilter"),
            "value": None
        },
        {
            "reportFilterUri": get_specific_filter_uri(filter_name = "EntryDateFilter"),
            "value": str((begin_of_week - timedelta(days=duration_days)).strftime("%m/%d/%Y"))
        },
        {
            "reportFilterUri": get_specific_filter_uri(filter_name = "EntryDateFilter"),
            "value": str((dt.now() - timedelta(days=2)).strftime("%m/%d/%Y"))

        }
    ],
    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
     ]
    }


def process_process_notofication_for_user_conf(item):
    return {
        'userUri' : item['userUri'],
        'user_name' :  item['User_Name'],
        'user_first_name': item['User_First_Name']
    }
def get_final_payload_sendemail(username,useruri, html_body):
    null = None
    subject_line = f'{username}: Time Entries Overdue for Submission'
    final_payload = {"email": {
        "to": [
            {
                "user": {
                    "uri": useruri,
                    "loginName": null
                },
                "email": null
            }
        ],
        "cc": [],
        "bcc": [],
        "replyTo": null,
        "fromDisplayName":"Do-Not-Reply@deltek.com",
        "subject": subject_line,
        "htmlBody": html_body,
        "textBody": null,
        "attachments": []
    }}
    return json.dumps(final_payload)
