# pylint: disable=unused-variable,too-many-statements,too-many-branches
from datetime import datetime, timedelta
import json
import itertools
import rail
from strayeruniversity.user_sync_v3.mappers.strayer_schedule_mapper import schedule_mapper
from strayeruniversity.user_sync_v3.mappers.strayer_dynamic_timeoff_mapper import dynamic_timeoff_mapper
from strayeruniversity.user_sync_v3.mappers.strayer_static_timeoff_mapper import static_timeoff_mapper

null = None


def get_today():
    return datetime.now().strftime("%m/%d/%Y")


def get_ref_file_name(filepath):
    return filepath + "/" + rail.result('list_reference_files')[filepath][0]['name']


def get_inp_file_name(filepath):
    return filepath + "/" + rail.result('list_input_files')[filepath][0]['name']

def get_inp_file_name_no_changed_records(filepath):
    return filepath + "/" + rail.result('list_input_files_for_no_changed_records')[filepath][0]['name']

def construct_policyschedule():
    policy_set_schedule = rail.result(
        'get_existingpolicy_schedule_for_timeoff')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime.date() < datetime.now().date():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)
    return policy_schedule_entries


def get_userdata_list_for_managername(response, dag_run):
    userdata = response['rows']
    filtered_data = list(filter(
        lambda x: 'textValue' in x['cells'][1] and x['cells'][1]['textValue'] == dag_run.conf['managername'], userdata))
    if filtered_data:
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['managername'], list(map(
            lambda d: {
                'uri': d['cells'][1]['uri'],
                'enabled': d['cells'][0]['textValue'],
                'managerid_txt': d['cells'][1]
            }, userdata))))
    return []


def get_exceptions():
    exceptions = (rail.result('log_supervisor_assign_skipped') if rail.result(
        'log_supervisor_assign_skipped') else '')
    return exceptions


def check_start_date_mismatch(dag_run):
    user_start_date = rail.result('get_user_details_for_update')[
        0]['userDetails']['employmentDateRange']['startDate']
    start_date_value = str(user_start_date['year']) + '-' + str(
        user_start_date['month']) + '-' + str(user_start_date['day'])
    if (not user_start_date['day']) or datetime.strptime(dag_run.conf['hiredate'], '%d-%b-%Y') != datetime.strptime(start_date_value, '%Y-%m-%d'):
        return True
    return False


def get_employeetype_value(dag_run):
    if "Salaried" in dag_run.conf['employeetype']:
        return "Sal"
    if "Hourly" in dag_run.conf['employeetype']:
        return "Hou"
    return "NA"


def get_primaryworkstate_val():
    data = rail.result('get_user_details_for_update')[
        0]['userDetails']['customFieldValues']
    return rail.find_first_by_attr_and_get_attr(
        data, 'customField.displayText', 'Primary Work State', 'text', ''
    )


def get_current_data(arg1, arg2):
    data_dict = {}
    data = rail.result('get_user_details_for_update')[0][arg1]
    if not data:
        return ''

    emplpoyment_daterange_data = rail.result('get_user_details_for_update')[
        0]['userDetails']['employmentDateRange']['startDate']
    for i, p_data in enumerate(data):
        if p_data['effectiveDate']:
            effective_date = str(p_data['effectiveDate']['month']) + "/" + str(p_data['effectiveDate']['day']) \
                + "/" + str(p_data['effectiveDate']['year'])
        else:
            effective_date = str(emplpoyment_daterange_data['month']) + "/" + str(emplpoyment_daterange_data['day']) \
                + "/" + str(emplpoyment_daterange_data['year'])
        date_diff = (datetime.strptime(get_today(), "%m/%d/%Y") -
                     datetime.strptime(effective_date, "%m/%d/%Y")).days
        data_dict[p_data[arg2]['uri']] = date_diff

    uri = min(data_dict.keys(), key=lambda k: data_dict[k])
    return uri


def add_to_payrule_schedule(dag_run):
    payrule_data = rail.result('get_payruleassignementschedule_foruser')
    hiredate = datetime.strptime(dag_run.conf['hiredate'], "%d-%b-%Y")
    if rail.result('get_timesheet_periods_for_user'):
        timesheet_data = rail.result('get_timesheet_periods_for_user')[
            0]['dateRange']['startDate']
        timesheet_start_date = datetime.strptime(
            str(timesheet_data['day']) + '/' + str(timesheet_data['month']) + '/' + str(timesheet_data['year']), "%d/%m/%Y")
    else:
        timesheet_start_date = datetime.strptime(get_today(), "%m/%d/%Y")
    schedule_entries = []
    for item in payrule_data:
        payrulescript_uri = item['payRuleScript']['uri']
        payrulescript_name = item['payRuleScript']['displayText']
        if item['effectiveDate'] and ('day' in item['effectiveDate']):
            schedule_entries.append({
                'payRuleScript': {
                    'uri': payrulescript_uri,
                    'name': payrulescript_name
                },
                'effectiveDate': item['effectiveDate']
            })
        else:
            schedule_entries.append({
                'payRuleScript': {
                    'uri': payrulescript_uri,
                    'name': payrulescript_name
                }
            })
    if rail.result('get_required_payrulescript_name_uri')['uri']:
        if (timesheet_start_date - hiredate).days > 0:
            schedule_entries.append({
                'payRuleScript': {
                    'uri': rail.result('get_required_payrulescript_name_uri')['uri']
                },
                'effectiveDate': {
                    'day': timesheet_start_date.day,
                    'month': timesheet_start_date.month,
                    'year': timesheet_start_date.year
                }
            })
        else:
            hiredate = hiredate + timedelta(days=1)
            schedule_entries.append({
                'payRuleScript': {
                    'uri': rail.result('get_required_payrulescript_name_uri')['uri']
                },
                'effectiveDate': {
                    'day': hiredate.day,
                    'month': hiredate.month,
                    'year': hiredate.year
                }
            })

    return schedule_entries


def get_policyset_val_foruser(dag_run):
    if dag_run.conf['employeetype'] == 'Hourly':
        return rail.result('get_all_policy_sets')['tm_hourly']
    if dag_run.conf['employeetype'] == 'Hourly Exempt':
        return rail.result('get_all_policy_sets')['tm_hourly_exmpt']
    if dag_run.conf['employeetype'] == 'Part-time Hourly':
        return rail.result('get_all_policy_sets')['tm_hourly']
    if dag_run.conf['employeetype'] == 'Salaried':
        return rail.result('get_all_policy_sets')['widget_tm']
    if dag_run.conf['employeetype'] == 'Part-time Salaried':
        return rail.result('get_all_policy_sets')['prtm_sal']
    return False


def get_substitueUserUris(substitute_user, substitute_user_task):
    existing_substitute_users = rail.result(substitute_user_task)
    user_info = list(filter(
        lambda item: item['user'] and item['user']['loginName'] == substitute_user, existing_substitute_users))
    return user_info[0]['user']['uri'] if user_info else None


def get_substitueUser_fromsearch(substitute_user, search_sub):
    searched_substitute_users = rail.result(search_sub)['rows']
    user_info = list(filter(
        lambda item: item['cells'] and item['cells'][0]['textValue'] == substitute_user, searched_substitute_users))
    return user_info[0]['cells'][0]['uri'] if user_info else None


def get_schedulename(dag_run):
    return list(filter(lambda x: x['employeetype'] == dag_run.conf['employeetype'] and
                       x['scheduledhours'] == dag_run.conf['scheduledhours'], schedule_mapper))


def get_timeoff_tobe_assigned(dag_run):
    if rail.result('get_primaryworkstate_var_val'):
        primaryworkstate = rail.result('get_primaryworkstate_var_val')
    else:
        primaryworkstate = rail.result('get_homestate_to_search_val')

    if rail.result('get_employeetype_var_val'):
        employeetype = rail.result('get_employeetype_var_val')
    else:
        employeetype = dag_run.conf['employeetype']

    return list(filter(lambda x: x['division'] == dag_run.conf['division'] and
                       x['employeetype'] == employeetype and x['primaryworkstate'] == primaryworkstate and
                       x['scheduledhours'] == rail.result('get_scheduledhours_var_val'), dynamic_timeoff_mapper))


def get_timeoff_tobe_assigned_for30(dag_run):
    return list(filter(lambda x: x['division'] == dag_run.conf['division'] and
                       x['scheduledhours'] == rail.result('get_scheduledhours_var_val_for30'), static_timeoff_mapper))


def get_statictimeoff_with_scheduled_all(dag_run):
    return list(filter(lambda x: x['division'] == dag_run.conf['division'] and
                       x['scheduledhours'] == 'All', static_timeoff_mapper))


def get_timeoff_policy_uri():
    return list(filter(None, map(lambda x: x['timeoffuri'], rail.result('get_eligibletimeofftypeslist')['value'])))


def get_threshold_and_scheduledhourval(dag_run):
    policySetScheduleEntries = rail.result('get_default_timeoffpolicyschedule_foruser')[
        'policySetScheduleEntries']
    data_policyset = policySetScheduleEntries[0]['policySet']['timeOffBalanceEventScripts']
    additional_param = rail.find_first_by_attr_and_get_attr(
        data_policyset, 'scriptTarget.name', 'Bi-Weekly Custom Accrual', 'additionalParameters', '')
    existing_threshold = rail.find_first_by_attr_and_get_attr(
        additional_param, 'keyUri', 'urn:replicon:script-key:parameter:threshold', 'value.number', '')
    threshold_val = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:threshold", "value": {"number": existing_threshold}})
    scheduledhrs_val = 0
    if dag_run.conf['scheduledhours']:
        scheduledhrs_val = dag_run.conf['scheduledhours']
    threshold_for_scheduledhrs = json.dumps(
        {"keyUri": "urn:replicon:script-key:parameter:threshold", "value": {"number": scheduledhrs_val}})

    return json.loads(json.dumps(policySetScheduleEntries).replace(threshold_val, threshold_for_scheduledhrs))


def get_final_timeoflist():
    data = rail.result('get_eligibletimeofftypeslist')['value']
    return list(filter(None, map(lambda x: x['timeoffuri'], list(filter(lambda x: x['disabled'] == 'No', data)))))


def do_format_logs():

    def get_filtered_records(logs, status):
        return list(filter(lambda log: log['status'].lower() == status, logs))

    def get_record_summary(logs):
        return {
            "success": len(get_filtered_records(logs, 'success')),
            "failed":  len(get_filtered_records(logs, 'error')),
            "exception": len(get_filtered_records(logs, "exception"))
        }

    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"

    master_log = json.loads(rail.result('load_master_log'))

    gather_logs = rail.result('gather_user_logs') if rail.result(
        'gather_user_logs') else []

    for log in gather_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)

    users = list(
        set(map(lambda x: x['properties'].get('username', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for user in users:
        if not user:
            continue
        user_logs = list(
            filter(lambda x: x['properties'].get('username', '') == user and x['properties'].get('details', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'username': user,
                'action': first['properties'].get('action'),
                'status': get_status(user_logs),
                'details': ",".join(list(map(lambda x: x['properties'].get('details'), user_logs))),
                "ecid": first['ecid']
            })

    return {
        "get_record_summary": get_record_summary(logs),
        "final_logs": json.dumps(logs, ensure_ascii=False)
    }


def get_policyschedule_entries(response):
    policy_set_schedule_entries = []
    if response:
        for x in response:
            if x.get('effectiveDate', {}).get('day'):
                policy_set_schedule_entries.append(json.loads(json.dumps(
                    x, ensure_ascii=False).replace('null', '"effective"').replace('"script"', '"scriptTarget"')))
    return {
        'timeOffTypeUri': rail.result('foreach_eligibletimeofftypes')['timeoffuri'],
        'policySetScheduleEntries': policy_set_schedule_entries
    } if policy_set_schedule_entries else ''


def page_handler(request, result_resp):
    if len(result_resp['rows']) > 0:
        request['page'] += 1
        return request
    return null


def get_timeoff_uris(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    return [x['cells'][0]['uri'] for x in flatten_rows] if flatten_rows else []
