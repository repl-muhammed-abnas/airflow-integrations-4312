from datetime import datetime,timedelta
import ast
import json
from dateutil.relativedelta import relativedelta
from pandas import DateOffset, to_datetime
import rail

GROUPS_DELIMITER = '|'
DATE_FORMAT = "%m/%d/%Y"

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
def get_filtered_time_off_types(response):
    return list(map(lambda item: {
        "timeoff_type_name": item['displayText'],
        'timeoff_type_uri': item['uri'],
    }, response))
def assigned_time_offs_types_to_user(response,dag_run, mannual_time_off_types):
    if not response:
        return None
    return list(filter(lambda x: x['timeoff_type_name'] not in mannual_time_off_types
            if dag_run.conf['action']=='update' else x['timeoff_type_name'],map(lambda item: {
        'timeoff_type_name': item['timeOffType']['displayText'],
        "timeoff_type_uri": item['timeOffType']['uri'],
        "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    }, response['policiesByTimeOffType'])))

def get_modified_termination_sick_policy(policy_set):
    for item in policy_set['timeOffBalanceEventScripts']:
        if item['script']['name'] == 'Starting Balance Set To':
            for value in item['additionalParameters']:
                if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                    value['value']={"number": rail.result('get_balance_summary_for_user')['timeRemaining']}
                    break
        if item['script']['name'] == 'Yearly Carry Over with Expiry':
            for value in item['additionalParameters']:
                if value['keyUri']=="urn:replicon:script-key:parameter:carry-up-to-amount":
                    value['value']={"number": rail.result('get_balance_summary_for_user')['timeRemaining']}
                    break

    return policy_set    
def get_termination_policyset_sick_timeoff_type(response,dag_run,action):
    effective_date = (datetime.strptime(dag_run.conf['end_date'],DATE_FORMAT)+timedelta(days=1)).strftime(DATE_FORMAT)\
        if action =='disable' and dag_run.conf['end_date'] else dag_run.conf['change_effective_date']

    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ str(effective_date),
        'effectiveDate': get_replicon_date(effective_date),
        'policySet': get_modified_termination_sick_policy(item['policySet']),
    }, response)))