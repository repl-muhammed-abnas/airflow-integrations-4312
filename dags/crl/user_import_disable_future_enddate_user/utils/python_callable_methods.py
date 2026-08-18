from datetime import datetime, timedelta
import itertools
import json
import ast
from operator import itemgetter
import rail

DATE_FORMAT = "%m/%d/%Y"
null = None
DATE_FORMAT = "%m/%d/%Y"
null = None

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])
def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None
def get_historical_policy_to_assign_list(dag_run, action, for_each_loop, config):
    data = rail.result(for_each_loop)['policy']
    if not data:
        return []
    def get_compare_date():
        if action =="update":
            return dag_run.conf['change_effective_date']
        if action =='rehire':
            if rail.result(for_each_loop)['timeoff_type_name'] in config.SPECIAL_ACCRUAL_TIMEOFF_TYPE_NAMES and\
                (dag_run.conf['previous_employee_status'] =="Unpaid Leave" or dag_run.conf['previous_employee_status'] =="Paid Leave"):
                if dag_run.conf['assigned_event_reason_code']=='10' and \
                    list(filter(lambda x: x['event']==dag_run.conf['assigned_event'],config.SPECIAL_TIMEOFF_TYPES_ACCRUALS)):
                    return dag_run.conf['change_effective_date']
            return dag_run.conf['start_date']
        return dag_run.conf['end_date'] if dag_run.conf['end_date'] else dag_run.conf['change_effective_date']

    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(get_compare_date(), DATE_FORMAT).date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))
def get_no_accrual_policy_line(dag_run, action):
    if rail.result("for_each_time_off_type_no_accural")['timeoff_type_name'] == "[USA] Sick" and action =='disable':
        return rail.result("get_termination_policy_for_sick_timeoff_Type")

    effective_date = (datetime.strptime(dag_run.conf['end_date'],DATE_FORMAT)+timedelta(days=1)).strftime(DATE_FORMAT)\
        if action =='disable' and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']
    return [{
        "effectiveDate":get_replicon_date(effective_date),
        "description": "Added By Integration on"+
            f"{dag_run.conf['end_date'] if action =='disable'and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']}",
        "policySet": {
            "timeOffBalanceEventScripts":[{
             "script": {
                "description": "Set initial balance for the first day of a policy",
                "name": "Starting Balance Set To",
                "uri": dag_run.conf['starting_balance_script_uri']
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                "number": rail.result('get_balance_summary_for_user')['timeRemaining']
                }
            }]
            }],
            "timeOffValidationScripts": [{
            "script": {
                "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                "name": "Prevent balance overdraw",
                "uri": dag_run.conf['prevent_balance_overdraw_uri']
            },
            "additionalParameters": [{
                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                "value": {
                "number": "0"
                }
            }],
            }]
        }
        }]
def get_all_policy_to_assign_for_disable_user():
    if rail.result('for_each_time_off_type_no_accural')['policy'] and rail.result('get_no_accrual_policy_line'):
        data =rail.result('get_historical_policy_to_assign_list_disable_user') + rail.result('get_no_accrual_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('for_each_time_off_type_no_accural')['policy'] and rail.result('get_no_accrual_policy_line'):
        data = rail.result('get_no_accrual_policy_line')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if rail.result('for_each_time_off_type_no_accural')['policy'] and not rail.result('get_no_accrual_policy_line'):
        data =rail.result('get_historical_policy_to_assign_list_disable_user')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    return null