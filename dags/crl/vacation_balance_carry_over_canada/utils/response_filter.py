from datetime import datetime
import ast
import json
import rail

null = None

def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_required_timeoff_type_uris(response, config):
    return {
        'timeoff_uris_to_pick_balance_from': rail.find_first_by_attr_and_get_attr(response, 'displayText', config.VACATION_TIMEOFF, 'uri'),
        'timeoff_uri_to_transfer_balance_into': {
            'name': config.VACATION_TIMEOFF_CARRY_OVER,
            'uri':rail.find_first_by_attr_and_get_attr(response, 'displayText', config.VACATION_TIMEOFF_CARRY_OVER, 'uri')},
    }

def get_all_time_off_type(response,dag_run):
    data = rail.result("get_user_details")["timeoffpolicies"]
    assigned_timeoff = list(map(lambda x: x['uri'], response))
    if not rail.find_first_by_attr_and_get_attr(data, 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri']) or\
        not rail.find_first_by_attr_and_get_attr(data, 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri'],
        'isTimeOffAllowedAgainstThisTimeOffType', null):
        assigned_timeoff.append(dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri'])
    return assigned_timeoff

def get_all_policy_to_assign_to_carry_over():
    if rail.result('get_historical_timeoff_policy_sets') and rail.result('get_default_time_off_policy_set'):
        data =rail.result('get_historical_timeoff_policy_sets') + rail.result('get_default_time_off_policy_set')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))

    if not rail.result('get_historical_timeoff_policy_sets') and rail.result('get_default_time_off_policy_set'):
        data = rail.result('get_default_time_off_policy_set')
        return json.dumps(ast.literal_eval(str(data).replace("'script'", "'scriptTarget'")))
    return null

def get_modified_policy_update(dag_run, policyset):
    for item in policyset['timeOffBalanceEventScripts']:
        if item['script']['name'] == 'Starting Balance Set To with Expiry':
            for value in item['additionalParameters']:
                if value['keyUri']=="urn:replicon:script-key:parameter:amount":
                    value['value']={"number": dag_run.conf['balance_to_transfer']['balance'] }
                    break

                if value['keyUri']=="urn:replicon:script-key:parameter:expiry-date":
                    value['value']={"date": rail.get_replicon_date(datetime.strptime(
                            dag_run.conf['expiry_date_for_new_policyset'], "%Y-%m-%d")) 
                        }
                    break

    return policyset

def get_policy_to_assign_for_timeoff(response, dag_run):
    return list(filter(lambda x: bool(x['effectiveDate']), map(lambda item: {
        'description': 'effective on '+ str(dag_run.conf['effective_date_for_new_policyset']),
        'effectiveDate': rail.get_replicon_date(datetime.strptime(dag_run.conf['effective_date_for_new_policyset'], "%Y-%m-%d")),
        'policySet': get_modified_policy_update(dag_run,item['policySet'])
    }, response)))

def get_historical_timeoff_policy_set(dag_run):
    if dag_run.conf['remove_historical_policies']=='Yes':
        return None
    data = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_user_details")["timeoffpolicies"], 'timeOffType.uri',
            dag_run.conf['timeoff_type_uri_for_transferring_balance_into']['uri'], 'policySetSchedule',[])
    if not data:
        return []
    return list(filter(lambda x: get_date_from_replicon_date(x['effectiveDate']).date()
            < datetime.strptime(dag_run.conf['effective_date_for_new_policyset'], "%Y-%m-%d").date(), map(lambda item: {
        'description': item['description'],
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    },data )))
