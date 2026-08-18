from datetime import datetime, timedelta
import re
import itertools
import json
from dateutil.relativedelta import relativedelta
from rail import result, smartjoin_by_delim, find_first_by_attr_and_get_attr, set_result
from rail.filters import remove_empty


null = None


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_datetime_obj(date_str, fmt='%m/%d/%Y'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }


def get_user_uri_by_empid(response):

    user_uris = [item['cells'][0]['uri'] for item in response['rows']
                 if re.sub('^0+', "", item['cells'][1].get('textValue')) == result(
        'get_employeeid')] if response['rows'] else []
    return smartjoin_by_delim(user_uris) if user_uris else ''


def get_user_uri_by_loginname(response, dag_run):

    user_uris = [item['cells'][0]['uri'] for item in response['rows']
                 if item['cells'][0]['textValue'] == dag_run.conf['loginname']] if response['rows'] else []
    return smartjoin_by_delim(user_uris) if user_uris else ''


def get_supervisor(response, dag_run):
    user_uri = ''
    if response['rows']:
        user_uri = smartjoin_by_delim(
            [x['cells'][0]['uri'] for x in response['rows'] if x['cells'][0]['textValue'] == dag_run.conf['supervisor']])
    return user_uri


def is_assign_supervisorpermission(response):

    supervisor_permission = False
    if response:
        if not find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission


def get_permissions_to_assign_user(response):

    supervisor_permission_uri = find_first_by_attr_and_get_attr(
        response, 'permissionSet.displayText', 'Supervisor', 'permissionSet.uri', '')
    enduser_permission_uri = find_first_by_attr_and_get_attr(
        response, 'permissionSet.displayText', 'End User', 'permissionSet.uri', '')
    return remove_empty([supervisor_permission_uri, enduser_permission_uri])


def page_handler(request, result_resp):
    if len(result_resp['rows']) > 0:
        request['page'] += 1
        return request
    return null


def compare_enddate_duedate(timesheet_date_obj, end_date_str, due_date_obj):
    timesheet_datetime = datetime.strptime(
        f"{timesheet_date_obj['day']}/{timesheet_date_obj['month']}/{timesheet_date_obj['year']}", '%d/%m/%Y')
    due_date_datetime = datetime.strptime(
        f"{due_date_obj['day']}/{due_date_obj['month']}/{due_date_obj['year']}", '%d/%m/%Y') if due_date_obj else ''
    enddate_plus1_datetime = datetime.strptime(
        end_date_str, "%d/%m/%Y") + timedelta(days=1)
    return bool(timesheet_datetime > enddate_plus1_datetime and timesheet_datetime > (
        due_date_datetime if due_date_datetime else enddate_plus1_datetime))


def get_studentworker_dropdown_uri(response, dag_run):
    jobcode = dag_run.conf['jobcode']
    val = 'YES' if jobcode in ('SW', 'FW') else 'NO'
    return find_first_by_attr_and_get_attr(response, 'displayText', val, 'uri', '')


def get_timesheet_uris(response, dag_run):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    timesheet_uris_to_delete = list(filter(lambda y: bool(y['check']), map(lambda item: {
        'uri': item['cells'][0]['uri'],
        'check': compare_enddate_duedate(
            item['cells'][1]['dateValue'], dag_run.conf['enddate'], (result(
                'get_currenttimesheet_details')['dueDate'] if result(
                    'get_currenttimesheet_details') else '')),
        'date': item['cells'][1]['dateValue']
    }, flatten_rows))) if flatten_rows else []

    set_result(timesheet_uris_to_delete, 'timesheet_uris_to_delete')

    return [x['uri'] for x in timesheet_uris_to_delete if x['uri']]


def get_putlocationschedule_user(response):

    schedule_entries = []
    for item in response:
        location_uri = item['location']['uri']
        if item['effectiveDate'] and not item.get('effectiveDate').get('day'):
            schedule_entries.append({
                'location': {
                    'uri': location_uri
                }
            })
        else:
            schedule_entries.append({
                'location': {
                    'uri': location_uri
                },
                'effectiveDate': item['effectiveDate']
            })

    effective_date = result('get_timesheet_details')['dateRange']['startDate'] if result(
        'get_timesheet_details') and result('get_timesheet_details')['dateRange'] else get_today_date()
    schedule_entries.append({
        'location': {
            'uri': result('get_locationuri_to_assign')
        },
        'effectiveDate': effective_date
    })

    return schedule_entries


def get_putpayrulescheduleentries_user(response):
    schedule_entries = []
    for item in response:
        payrulescript_uri = item['payRuleScript']['uri']
        payrulescript_name = item['payRuleScript']['displayText']
        if item['effectiveDate'] and item.get('effectiveDate', {}).get('day'):
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
    if result('get_required_payrulescript_name_uri')['uri']:
        schedule_entries.append({
            'payRuleScript': result('get_required_payrulescript_name_uri'),
            'effectiveDate': get_today_date()
        })

    return schedule_entries


def get_permissions_to_assign_updateuser(response, dag_run):
    permission_set_uris = []
    supervisor_permission_uri = find_first_by_attr_and_get_attr(
        response, 'permissionSet.displayText', 'Supervisor', '')
    if not supervisor_permission_uri:
        for item in response:
            permission_set_uris.append(item['permissionSet']['uri'])
        new_permission_to_add = dag_run.conf['supervisorpermissionuri']
        permission_set_uris.append(new_permission_to_add)
    return permission_set_uris


def get_assigned_timeoff_policy_update(response, dag_run):

    policies_by_timeoff_type = response['policiesByTimeOffType']

    policy_set_schedule = find_first_by_attr_and_get_attr(policies_by_timeoff_type, 'timeOffType.uri', result(
        'get_timeofftype_uris_to_assign', 'sick_timeoffname_uri'), 'policySetSchedule', '')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < datetime.now():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)

    final_sickleave_name = result(
        'get_timeofftype_uris_to_assign', 'sick_timeoff_name')
    date_to_consider = datetime.strptime(
        dag_run.conf['servicedate'], '%m/%d/%Y') + timedelta(days=90) if final_sickleave_name in (
            'Sick Leave - MA', 'Sick Leave - OR', 'Sick Leave') else datetime.now()
    policy_schedule_entries.append({
        "effectiveDate": {
            "year": date_to_consider.year,
            "month": date_to_consider.month,
            "day": date_to_consider.day
        },
        "description": f"Effective on {date_to_consider.month}-{date_to_consider.day}-{date_to_consider.year}",
        **result('get_policy_schedule')['poliset']
    })
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": result('get_timeofftype_uris_to_assign', 'sick_timeoffname_uri')
        },
        "policySetScheduleEntries": policy_schedule_entries
    }


def get_assigned_timeoff_policy_update_paidtimeoff_v40(response, dag_run):

    policies_by_timeoff_type = response['policiesByTimeOffType']

    policy_set_schedule = find_first_by_attr_and_get_attr(policies_by_timeoff_type, 'timeOffType.uri', result(
        'log_time_offurifor_paid_time_off_65'), 'policySetSchedule', '')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < datetime.now():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)

    paid_timeoff_uri = result('log_time_offurifor_paid_time_off_65')
    date_to_consider = datetime.now()
    policy_schedule_entries.append({
        "effectiveDate": {
            "year": date_to_consider.year,
            "month": date_to_consider.month,
            "day": date_to_consider.day
        },
        "description": f"Effective on {date_to_consider.month}-{date_to_consider.day}-{date_to_consider.year}",
        **result('log_policysetmodified_67')
    })
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": paid_timeoff_uri
        },
        "policySetScheduleEntries": policy_schedule_entries
    }


def get_assigned_timeoff_policy_update_v40_existingusers(response, dag_run):

    policies_by_timeoff_type = response['policiesByTimeOffType']

    policy_set_schedule = find_first_by_attr_and_get_attr(policies_by_timeoff_type, 'timeOffType.uri', result(
        'get_timeofftype_uris_to_assign', 'sick_timeoffname_uri'), 'policySetSchedule', '')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < datetime.now():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)

    date_to_consider = datetime.now()
    policy_schedule_entries.append({
        "effectiveDate": {
            "year": date_to_consider.year,
            "month": date_to_consider.month,
            "day": date_to_consider.day
        },
        "description": f"Effective on {date_to_consider.month}-{date_to_consider.day}-{date_to_consider.year}",
        **result('get_policy_schedule_existingusers')['poliset']
    })
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": result('get_timeofftype_uris_to_assign', 'sick_timeoffname_uri')
        },
        "policySetScheduleEntries": policy_schedule_entries
    }


def get_user_uri_reports_central_queue(response):
    user_uris = [item['cells'][0]['uri'] for item in response['rows']
                 if item['cells'][1].get('textValue') == 'central.queue'] if response['rows'] else []
    return smartjoin_by_delim(user_uris) if user_uris else ''


def get_assigned_timeoff_policy_update_v40_rehireusers(response, dag_run):

    policies_by_timeoff_type = response['policiesByTimeOffType']

    policy_set_schedule = find_first_by_attr_and_get_attr(policies_by_timeoff_type, 'timeOffType.uri', result(
        'get_timeofftype_uris_to_assign', 'sick_timeoffname_uri'), 'policySetSchedule', '')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < datetime.now():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)

    date_to_consider = datetime.strptime(
        dag_run.conf['rehiredate'], '%m/%d/%Y')
    policy_schedule_entries.append({
        "effectiveDate": {
            "year": date_to_consider.year,
            "month": date_to_consider.month,
            "day": date_to_consider.day
        },
        "description": f"Effective on {date_to_consider.month}-{date_to_consider.day}-{date_to_consider.year}",
        **result('get_policy_schedule_existingusers')['poliset']
    })
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": result('get_timeofftype_uris_to_assign', 'sick_timeoffname_uri')
        },
        "policySetScheduleEntries": policy_schedule_entries
    }


def get_previous_vacationtimeoff_policies(response):

    policies_by_timeoff_type = response['policiesByTimeOffType']
    vacation_timeoff_uri = find_first_by_attr_and_get_attr(result(
        'get_alltimeoff_types'), 'displayText', 'Vacation', 'uri', '')
    policy_set_schedule = find_first_by_attr_and_get_attr(
        policies_by_timeoff_type, 'timeOffType.uri', vacation_timeoff_uri, 'policySetSchedule', '')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < datetime.now():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)

    return policy_schedule_entries


def get_assigned_timeoffuris(response):

    timeoff_uris = []
    for item1 in response:
        timeoff_types = item1.get(
            'timeOffTypeAssignmentsDetails', {}).get('timeOffTypes')
        if timeoff_types:
            for item2 in timeoff_types:
                timeoff_uris.append(item2['uri'])

    return timeoff_uris


def get_required_activityuris(response):
    activities = result('get_activities_from_mapper').split('|')
    activity_uris = []
    for item in activities:
        activity_uri = find_first_by_attr_and_get_attr(
            response, 'displayText', item, 'uri', '')
        if activity_uri:
            activity_uris.append(activity_uri)
    return activity_uris


def get_activities_to_remove(response):

    existing_activity_uris = []
    if response and response[0]['uri']:
        existing_activity_uris = [x['uri'] for x in response if x['uri']]
    return existing_activity_uris


def get_final_policysets(response, dag_run):
    final_policy_sets = result('past_policyset_schedule') if result(
        'past_policyset_schedule') else []
    if response:
        user_tenure = result('get_usertenure_servicedate')
        service_date_datetime = datetime.strptime(
            dag_run.conf['servicedate'], '%m/%d/%Y')

        policy_set_less_1 = []
        for item in response:
            offset_val_difference = int(
                item['startOffset']['offsetValue']) - user_tenure
            if offset_val_difference < 1:
                description = f"Effective On {service_date_datetime.month}-{service_date_datetime.day}-{service_date_datetime.year}"
                policy_set_less_1.append({
                    'effectiveDate': f"{service_date_datetime.month}/{service_date_datetime.day}/{service_date_datetime.year}",
                    'description': description,
                    'policySet': json.loads(json.dumps(
                        item['policySet'], ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"')),
                    'daydiff': offset_val_difference
                })
        if policy_set_less_1:
            max_offset_val_difference = max(x['daydiff']
                                            for x in policy_set_less_1 if x['effectiveDate'])
            for item2 in policy_set_less_1:
                if item2['daydiff'] == max_offset_val_difference:
                    final_policy_sets.append({
                        'description': item2['description'],
                        'effectiveDate': get_datetime_obj(item2['effectiveDate']),
                        'policySet': json.loads(json.dumps(
                            item2['policySet'], ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                                '"script"', '"scriptTarget"'))
                    })
        for item3 in response:
            offset_val_difference2 = int(
                item3['startOffset']['offsetValue']) - user_tenure
            if offset_val_difference2 > 0:
                required_no_of_days = offset_val_difference2 * 365
                datetime_to_consider = service_date_datetime + \
                    timedelta(days=required_no_of_days)
                description2 = f"Effective On {datetime_to_consider.month}-{datetime_to_consider.day}-{datetime_to_consider.year}"
                final_policy_sets.append({
                    'description': description2,
                    'effectiveDate': {
                        'day': datetime_to_consider.day,
                        'month': datetime_to_consider.month,
                        'year': datetime_to_consider.year
                    },
                    'policySet': json.loads(json.dumps(
                        item3['policySet'], ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                })

    return final_policy_sets


def get_timeoff_uris(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    return [x['cells'][0]['uri'] for x in flatten_rows] if flatten_rows else []


def get_usertimeoff_policy_by_effectivedate(response, dag_run):

    policies_by_timeoff_type = response['policiesByTimeOffType']

    policy_schedule_entries = []
    for item in policies_by_timeoff_type:
        if item['isTimeOffAllowedAgainstThisTimeOffType'] and item['uri'] == dag_run.conf['timeoffuri']:
            for item2 in item['policySetSchedule']:
                effective_datetime = datetime.strptime(
                    f"{item2['effectiveDate']['day']}/{item2['effectiveDate']['month']}/{item2['effectiveDate']['year']}",
                    '%d/%m/%Y') if item2.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < datetime.strptime(
                        dag_run.conf['rehiredate'], '%m/%d/%Y'):
                    policy_schedule_entries.append(item2)
    return policy_schedule_entries


def get_final_policysets2(response, dag_run):
    final_policy_sets = result('get_user_timeoff_policy') if result(
        'get_user_timeoff_policy') else []
    if response:
        rehire_date = get_datetime_obj(
            dag_run.conf['rehiredate']) if 'Rehire' in dag_run.conf['type'] else get_today_date()
        for item in response:
            if int(item['startOffset']['offsetValue']) == 0:
                description = f"Effective On {rehire_date['month']}-{rehire_date['day']}-{rehire_date['year']}"
                final_policy_sets.append({
                    'effectiveDate': rehire_date,
                    'description': description,
                    'policySet': item['policySet']
                })
            if int(item['startOffset']['offsetValue']) == 1:
                rehire_date_datetime = datetime.strptime(
                    dag_run.conf['rehiredate'], '%m/%d/%Y') + timedelta(days=1)
                description = f"Effective On {rehire_date_datetime.month}-{rehire_date_datetime.day}-{rehire_date_datetime.year}"
                final_policy_sets.append({
                    'effectiveDate': {
                        'day': rehire_date_datetime.day,
                        'month': rehire_date_datetime.month,
                        'year': rehire_date_datetime.year
                    },
                    'description': description,
                    'policySet': item['policySet']
                })

    return final_policy_sets


def get_timeoffs_to_assign(response):
    timeoffs_to_assign = []
    pto_buyup_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'PTO Buy Up', 'uri', '')
    if pto_buyup_uri:
        timeoffs_to_assign.append(pto_buyup_uri)
    fto_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'FTO', 'uri', '')
    if fto_uri:
        timeoffs_to_assign.append(fto_uri)
    return timeoffs_to_assign


def get_usertimeoff_policy_by_effectivedate_plus_tenure(response, dag_run):

    policies_by_timeoff_type = response['policiesByTimeOffType']

    date_to_consider = datetime.strptime(dag_run.conf['rehiredate'], '%m/%d/%Y') + timedelta(
        months=int(result('get_usertenure_servicedate')) * 12)

    policy_schedule_entries = []
    for item in policies_by_timeoff_type:
        if item['isTimeOffAllowedAgainstThisTimeOffType'] and item['uri'] == dag_run.conf['timeoffuri']:
            for item2 in item['policySetSchedule']:
                effective_datetime = datetime.strptime(
                    f"{item2['effectiveDate']['day']}/{item2['effectiveDate']['month']}/{item2['effectiveDate']['year']}",
                    '%d/%m/%Y') if item2.get('effectiveDate') else ''
                if effective_datetime and effective_datetime < date_to_consider:
                    policy_schedule_entries.append(item2)
    return policy_schedule_entries


def get_reset_day_month(day):
    day_suffix = {1: f'{day}st', 2: f'{day}nd',
                  3: f'{day}rd'}.get(day % 20, f'{day}th')
    return f'urn:replicon:monthly-frequency-start-day-option:{day_suffix}'


# pylint:disable=too-many-nested-blocks
def get_final_policysets_anniversary_update_rehire(response, dag_run):

    final_policy_sets = result('get_user_timeoff_policy') if result(
        'get_user_timeoff_policy') else []
    if response:
        policy_set_to_assign = ''
        for item in response:
            if int(item['startOffset']['offsetValue']) >= int(result('get_usertenure_servicedate')):
                effective_date_datetime = datetime.strptime(
                    dag_run.conf['servicedate'], '%m/%d/%Y') + relativedelta(months=+int(item['startOffset']['offsetValue'])*12)
                required_month = {
                    "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                    "value": {
                        "uri": f"urn:replicon:month:{effective_date_datetime.strftime('%B').lower()}"
                    }
                }
                reset_day_of_month = get_reset_day_month(
                    effective_date_datetime.day)
                required_date = {
                    "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                    "value": {
                        "uri": reset_day_of_month
                    }
                }

                timeoff_balance_event_scripts = item['policySet']['timeOffBalanceEventScripts']

                for counter, x in enumerate(timeoff_balance_event_scripts):
                    if x["script"]["name"] == 'Yearly Reset':
                        yearly_reset_policy_set = x
                        if yearly_reset_policy_set.get('additionalParameters', {}):
                            for i, item2 in enumerate(yearly_reset_policy_set['additionalParameters']):
                                if item2['keyUri'] == 'urn:replicon:script-key:parameter:reset-on-month':
                                    yearly_reset_policy_set['additionalParameters'][i] = required_month
                                elif item2['keyUri'] == 'urn:replicon:script-key:parameter:reset-on-day-of-month':
                                    yearly_reset_policy_set['additionalParameters'][i] = required_date
                        item['policySet']['timeOffBalanceEventScripts'][counter] = yearly_reset_policy_set
                policy_set_to_assign = item['policySet']

                description = f"Effective On {effective_date_datetime.month}-{effective_date_datetime.day}-{effective_date_datetime.year}"
                final_policy_sets.append({
                    'effectiveDate': {
                        'day': effective_date_datetime.day,
                        'month': effective_date_datetime.month,
                        'year': effective_date_datetime.year
                    },
                    'description': description,
                    'policySet': policy_set_to_assign
                })

    return json.loads(json.dumps(
        final_policy_sets, ensure_ascii=False).replace(
        '"script"', '"scriptTarget"'))


def get_final_policysets_anniversary_add(response, dag_run):

    final_policy_sets = []
    if response:
        policy_set_to_assign = ''
        for item in response:
            effective_date_datetime = datetime.strptime(
                dag_run.conf['servicedate'], '%m/%d/%Y') + relativedelta(months=+int(item['startOffset']['offsetValue'])*12)
            required_month = {
                "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                "value": {
                    "uri": f"urn:replicon:month:{effective_date_datetime.strftime('%B').lower()}"
                }
            }
            reset_day_of_month = get_reset_day_month(
                effective_date_datetime.day)
            required_date = {
                "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                "value": {
                    "uri": reset_day_of_month
                }
            }
            timeoff_balance_event_scripts = item['policySet']['timeOffBalanceEventScripts']

            for counter, x in enumerate(timeoff_balance_event_scripts):
                if x["script"]["name"] == 'Yearly Reset':
                    yearly_reset_policy_set = x
                    if yearly_reset_policy_set.get('additionalParameters', {}):
                        for i, item2 in enumerate(yearly_reset_policy_set['additionalParameters']):
                            if item2['keyUri'] == 'urn:replicon:script-key:parameter:reset-on-month':
                                yearly_reset_policy_set['additionalParameters'][i] = required_month
                            elif item2['keyUri'] == 'urn:replicon:script-key:parameter:reset-on-day-of-month':
                                yearly_reset_policy_set['additionalParameters'][i] = required_date
                    item['policySet']['timeOffBalanceEventScripts'][counter] = yearly_reset_policy_set
            policy_set_to_assign = item['policySet']

            description = f"Effective On {effective_date_datetime.month}-{effective_date_datetime.day}-{effective_date_datetime.year}"
            final_policy_sets.append({
                'effectiveDate': {
                    'day': effective_date_datetime.day,
                    'month': effective_date_datetime.month,
                    'year': effective_date_datetime.year
                },
                'description': description,
                'policySet': policy_set_to_assign
            })

    return json.loads(json.dumps(
        final_policy_sets, ensure_ascii=False).replace(
        '"script"', '"scriptTarget"'))


def map_impersonate_and_create_interactive_session(response):
    auth_token = list(
        filter(lambda x: x['name'] == 'AUTHTOKEN', response['sessionCookies']))[0]['value']
    tenant = list(
        filter(lambda x: x['name'] == 'TENANT', response['sessionCookies']))[0]['value']
    return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}


def get_policyschedule_entries(response, item):
    policy_set_schedule_entries = []
    if response:
        for x in response:
            if x.get('effectiveDate', {}).get('day'):
                policy_set_schedule_entries.append(json.loads(json.dumps(
                    x, ensure_ascii=False).replace('null', '"effective"').replace('"script"', '"scriptTarget"')))
    return {
        'timeOffTypeUri': item,
        'policySetScheduleEntries': policy_set_schedule_entries
    } if policy_set_schedule_entries else ''


def map_supervisor_listdata(response):
    return list(map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'employeeid': item['cells'][2].get('textValue'),
        'status': item['cells'][3]['textValue']
    }, response['rows'])) if response['rows'] else []
