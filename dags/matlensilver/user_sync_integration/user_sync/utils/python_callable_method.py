from datetime import datetime
import json
import ast
import rail
from airflow.models import Variable
import pandas as pd


def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, '%m-%d-%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_policy_to_assign():
    data = rail.result('get_default_time_off_policy_schedule')
    if not data:
        return None
    res = list(map(lambda item: {
        'description': 'effective',
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, data))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))


def get_timeoff_type_list(dag_run, config):

    sick_timeoff_mapper = ast.literal_eval(
        Variable.get(config.sick_time_off_mapper))

    def get_sick_time_off_policy_name():
        if dag_run.conf['worklocation'] == 'Remote':
            timeoffname = rail.find_first_by_attr_and_get_attr(
                sick_timeoff_mapper, 'zipcode', dag_run.conf['homezip'], 'time_off_policy')
            if not timeoffname:
                return 'Not Available'
            return timeoffname
        timeoffname = rail.find_first_by_attr_and_get_attr(
            sick_timeoff_mapper, 'zipcode', dag_run.conf['workzip'], 'time_off_policy')
        if not timeoffname:
            return 'Not Available'
        return timeoffname

    def get_timeoff_name(item):
        if item['timeofftype'] == 'Sick Time Off':
            return get_sick_time_off_policy_name()
        return item['timeofftype']

    data = rail.result('time_off_types_to_assign')
    return list(map(lambda item: {
        'timeofftypename':  get_timeoff_name(item) if item['timeofftype'] == 'Sick Time Off' else item['timeofftype'],
        "timeofftypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types'),
        'displayText', get_timeoff_name(item), 'uri', 'Not Available')
    }, data))


def get_time_off_types_to_assign(dag_run, config):
    time_off_to_assign = []
    user_sync_mapper = ast.literal_eval(Variable.get(config.user_sync_mapper))
    data = filter(lambda x: x['type'] == 'time_off' and x['employeecode'] == dag_run.conf['employeetypecode']
                  and x['employeetype'] == dag_run.conf['employeetype'], user_sync_mapper)
    for time_off_details in data:
        time_off_to_assign.append(
            {'timeofftype': time_off_details['timeoffname']})
    return time_off_to_assign


def get_time_off_type_uris():
    data = rail.result('get_timeoff_type_list')
    return list(map(lambda item: item['timeofftypeuri'], data))


def assigned_time_offs():
    data = rail.result('get_user_time_off_policy_summary')[
        'policiesByTimeOffType']
    return list(filter(lambda x: x['enabled'], map(lambda item: {
        'timeofftypename': item['timeOffType']['displayText'],
        "timeofftypeuri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType']
    }, data)))


def time_off_types_to_be_assigned():
    data = rail.result('get_timeoff_type_list')
    compare_data = rail.result('assigned_time_offs')
    return list(map(lambda item: {
        'timeofftypename': item['timeofftypename'],
        "timeofftypeuri": item['timeofftypeuri'],
        "enabled": rail.find_first_by_attr_and_get_attr(compare_data, 'timeofftypeuri', item['timeofftypeuri'], 'enabled'),
        "status": 'Yes' if rail.find_first_by_attr_and_get_attr(compare_data, 'timeofftypeuri', item['timeofftypeuri'], 'timeofftypename') else 'No'
    }, data))


def time_off_types_to_be_disabled():
    compare_data = rail.result('get_timeoff_type_list')
    data = rail.result('assigned_time_offs')
    return list(filter(lambda x: x['status'] == 'No', map(lambda item: {
        'timeofftypename': item['timeofftypename'],
        "timeofftypeuri": item['timeofftypeuri'],
        "enabled": item['enabled'],
        "status": 'Yes' if rail.find_first_by_attr_and_get_attr(compare_data, 'timeofftypeuri', item['timeofftypeuri'], 'timeofftypename') else 'No'
    }, data)))



def get_user_logs_by_status(task_id):
    data = rail.load_all_records(rail.result(task_id))
    res = list(map(lambda x: x['message'], data))
    return res


def get_policy_to_assign_list(dag_run):
    data = dag_run.conf['policy']
    return list(filter(lambda x: datetime.strptime(get_date_from_replicon_date(x['effectiveDate']).strftime("%m-%d-%Y"), "%m-%d-%Y")
            < datetime.strptime(get_date_from_replicon_date(dag_run.conf['todays_date']).strftime("%m-%d-%Y"), "%m-%d-%Y"), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, data)))


def get_default_policy_set():
    data = rail.result(
        'get_default_timeoff_policy_set_schedule_for_timeofftype')
    return list(map(lambda item: {
        'offsetunit': (item['startOffset']['offsetUnitUri']).split(':')[-1],
        'offsetvalue': item['startOffset']['offsetValue'],
        'policy': item['policySet']
    }, data))


def get_current_policy_set_to_assign(dag_run):
    def get_offset(item):
        if item['offsetunit'] == 'years':
            return pd.DateOffset(years=int(item['offsetvalue']))
        if item['offsetunit'] == 'months':
            return pd.DateOffset(months=int(item['offsetvalue']))
        return pd.DateOffset(days=int(item['offsetvalue']))

    def get_effective_date(item):
        present = get_date_from_replicon_date(
            dag_run.conf['todays_date']).strftime("%m-%d-%Y")
        current_date = str(
            (pd.to_datetime(present) + get_offset(item)).strftime("%m-%d-%Y"))
        return get_replicon_date(current_date)

    data = rail.result('get_default_policy_set')
    return list(map(lambda item: {
        'description': 'Effective on ' + get_date_from_replicon_date(dag_run.conf['todays_date']).strftime("%m-%d-%Y"),
        'effectiveDate': get_effective_date(item),
        'policySet': item['policy']
    }, data))


def get_all_policy_to_assign(dag_run):
    data = (rail.result('get_policy_to_assign_list') + rail.result('get_current_policy_set_to_assign')
            ) if dag_run.conf['policy'] else rail.result('get_current_policy_set_to_assign')
    return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

def get_message_from_log():
    data = rail.load_all_records(rail.result('filter_master_logs'))
    return list(map(lambda item: {
        'message': item['message'],
        'severity': item['severity'],
        'status': item['properties']['status']
    }, data))
