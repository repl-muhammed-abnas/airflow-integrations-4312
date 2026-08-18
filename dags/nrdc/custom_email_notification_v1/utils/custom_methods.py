import calendar
from datetime import datetime, timedelta
import rail
import json
import base64
import pendulum

null = None

def get_not_submitted_timesheets(response):
    data = response
    if data and data['rows']:
        return list(map(lambda x: {
            'user': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', 'urn:replicon:object-type:user', 'textValue'),
            'timesheetperiod': rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', 'urn:replicon:list-type:date-range', 'textValue'),
            'timesheeturi': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', 'urn:replicon:object-type:timesheet', 'uri'),
            'status': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', 'urn:replicon:object-type:approval-status', 'textValue'),
            'useruri': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', 'urn:replicon:object-type:user', 'uri'),
            'duedate': rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', 'urn:replicon:list-type:date', 'textValue')
        }, data['rows']))
    return []

def is_today_last_day_of_month(config):
    today = pendulum.now(config.et_timezone)
    last_day = calendar.monthrange(today.year, today.month)[1]
    return today.day == last_day

def get_date_to_consider(dag_run, config):
    if dag_run.conf.get("req_date"):
        return dag_run.conf["req_date"]

    current_date = pendulum.now(config.et_timezone)
    today = current_date.day

    if today in [25, 28, 30]:
        # Reminders 1, 2, 3: Check for 1st of next month (upcoming deadline)
        next_month = current_date.add(months=1)
        date_to_consider = next_month.replace(day=1)
    else:
        # Reminders 4, 5, 6, 7: Check for 1st of current month
        # Day 1: Due today reminders
        # Day 2, 3: Overdue reminders
        date_to_consider = current_date.replace(day=1)

    return rail.get_replicon_date(date_to_consider)


def get_timsheet_button_url(dag_run, config):
    payload = {"authorityUri":null,"resourceUri":"replicon://"+config.company_key+"/timesheet/"+ dag_run.conf['timesheeturi'],"tenant":{"companyKey":config.company_key,"slug":null,"uri":null},"user":{"loginName":null,"uri":dag_run.conf['useruri']}}
    json_str = json.dumps(payload)
    encoded_bytes = base64.b64encode(json_str.encode('utf-8'))
    encoded_str = encoded_bytes.decode('utf-8')
    return config.replicon_go_link_url + encoded_str

def check_schedule(config):
    """
    Check if today is a valid schedule day for due notifications.
    V2.2 Update: Changed from checking last day of month, 1, and 4
    to checking 25, 28, and 30.
    """
    today = pendulum.now(config.et_timezone).day
    return today in [25, 28, 30]