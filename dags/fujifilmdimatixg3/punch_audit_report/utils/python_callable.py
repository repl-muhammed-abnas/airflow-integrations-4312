# pylint: disable=too-many-statements line-too-long
from datetime import datetime, timedelta
from pendulum import timezone, now
from rail import find_first_by_attr_and_get_attr, result

def get_start_date():
    current_day = now(timezone("America/New_York"))
    day_diff = {
        "Monday": 9,
        "Tuesday": 10,
        "Wednesday": 11,
        "Thursday": 12,
        "Friday": 13,
        "Saturday": 14
    }.get(current_day.format("dddd"), 15)
    return (current_day - timedelta(days=day_diff)).strftime("%m/%d/%Y")

def get_end_date():
    current_day = now(timezone("America/New_York"))
    day_diff = {
        "Monday": 3,
        "Tuesday": 4,
        "Wednesday": 5,
        "Thursday": 6,
        "Friday": 7,
        "Saturday": 8
    }.get(current_day.format("dddd"), 9)
    return (current_day - timedelta(days=day_diff)).strftime("%m/%d/%Y")

def get_report_params():
    department_uris = result('get_department_details')
    department_filter_uri = find_first_by_attr_and_get_attr(result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'DepartmentFilter', 'uri')
    def get_filter_values():
        return list(map(lambda item: {
            "reportFilterUri" : department_filter_uri,
            "value"           : item.split(":")[-1]
        }, department_uris))
    return {
        "reportParameters": [ {
            "reportUri": result('get_report_details')['uri'],
            "filterValues": get_filter_values(),
            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
        ]
    }


def get_year_month_date(date_str):
    date_str = get_start_date() if date_str == 'start' else get_end_date()
    date = datetime.strptime(date_str, '%m/%d/%Y')
    return {
            "year": date.year,
            "month": date.month,
            "day": date.day
            }

def check_if_length_and_user_uri():
    line_resp = result('for_each_time_punch_audit_details')['auditRecords']
    actual_user_uri = ''
    new_punch_user_uri = ''
    if len(line_resp) == 1:
        actual_user_uri = line_resp[0].get('actualUser', {}).get('uri','') if line_resp[0].get('actualUser', {}) else ''
        new_punch_user_uri = line_resp[0].get('newPunchUser', {}).get('uri','') if line_resp[0].get('newPunchUser', {}) else ''
    return (len(line_resp) == 1 and not actual_user_uri == new_punch_user_uri)

def check_if_length_is_greater():
    return len(result('for_each_time_punch_audit_details')['auditRecords']) > 1

def check_if_edited():
    return result('for_each_audit_records').get('modificationTypeUri','').split(':')[-1].lower() == 'edited'

def check_if_deleted():
    return result('for_each_audit_records').get('modificationTypeUri','').split(':')[-1].lower() == 'deleted'
