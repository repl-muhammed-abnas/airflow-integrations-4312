from datetime import datetime
import json
import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def parse_list_of_users(data):
    response = rail.result(data)
    australia_company_codes = ['3001', '3124', '1602', '3118', 'AUES']
    filtered_data = list(filter(lambda item: item['count'] < 1 if item['code'] in australia_company_codes else item['count'] < 5,
                                list(map(lambda x: {
                                    'user': x['username'],
                                    'code': x['code'],
                                    'count': x['count']}, response))))
    return filtered_data


def get_shift_details(item):
    today= datetime.now().weekday()
    start_date= item['startdatetocheckforthursday'] if today == 3 else item['startdatetocheckforfriday']
    end_date= item['enddatetocheckforthursday'] if today == 3 else item['endatetocheckforfriday']
    startdate_topass = datetime.strptime(start_date, '%d %B %Y')
    enddate_topass = datetime.strptime(end_date, '%d %B %Y')
    return {
        "search": {
            "userSearch": {
                "includeShiftAssignmentsWithNoUser": "false",
                "specificUserUris": [
                    item['useruri']
                ]
            },
            "shiftSearch": null,
            "shiftJobSearch": null,
            "objectExtensionFieldSearches": []
        },
        "dateRange": {
            "startDate": {
                "year": startdate_topass.year,
                "month": startdate_topass.month,
                "day": startdate_topass.day
            },
            "endDate": {
                "year": enddate_topass.year,
                "month": enddate_topass.month,
                "day": enddate_topass.day
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_date_range(shift_result_task_id):
    data = rail.result(shift_result_task_id)
    filtered_startdate = list(filter(lambda item: datetime.strptime(
        item['startdatetocheck'], '%d %B %Y'), data))
    start_date_list = list(
        map(lambda x: x['startdatetocheck'], filtered_startdate))
    filtered_enddate = list(filter(lambda item: datetime.strptime(
        item['enddatetocheck'], '%d %B %Y'), data))
    end_date_list = list(map(lambda x: x['enddatetocheck'], filtered_enddate))
    start_date_list.sort()
    end_date_list.sort
    start_date = start_date_list[-1]
    end_date = end_date_list[0]
    return str(start_date) + " - " + str(end_date)

def get_final_payload_sendemail(supervisoruri, get_dates, html_body):
    dates = rail.result(get_dates)
    subject_line = f'Supervisor: Shift Assignment to be done for - {dates}'
    final_payload = {"email": {
        "to": [
            {
                "user": {
                    "uri": supervisoruri,
                    "loginName": null
                },
                "email": null
            }
        ],
        "cc": [],
        "bcc": [],
        "replyTo": null,
        "fromDisplayName": null,
        "subject": subject_line,
        "htmlBody": html_body,
        "textBody": null,
        "attachments": []
    }}
    return json.dumps(final_payload)
