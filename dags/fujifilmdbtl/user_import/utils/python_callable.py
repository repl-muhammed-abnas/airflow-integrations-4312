from datetime import datetime
from pendulum import now
import rail
import json

null = None


def get_split_date(date, date_format="%m/%d/%Y"):
    if isinstance(date, str):
        date = datetime.strptime(date, date_format)
    return {
        'year': date.strftime("%Y"),
        'month': date.strftime("%m"),
        'day': date.strftime("%d")
    }


def to_datetime(date, date_format="%m/%d/%Y"):
    if isinstance(date, dict):
        return datetime(day=date['day'], month=date['month'], year=date['year'])
    elif isinstance(date, str):
        return datetime.strptime(date, date_format)
    return date


def get_timeoff_type_uris(timeoff_entries):
    return [item['uri'] for item in timeoff_entries if item['uri']]


def get_department_list_output(rows):
    return [{
        "name": item['cells'][0]['textValue'],
        "code": item['cells'][1]['textValue'],
        "parent": item['cells'][3]['textValue'],
        "uri": item['cells'][0]['uri'],
        "enabled": item['cells'][2]['textValue']
    }for item in rows]


def get_search_user_details(search_result_rows):
    return [{
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'email': item['cells'][3]['textValue'] if item['cells'][3]['dataType'] != "urn:replicon:list-type:null" else "",
        'employeeid': item['cells'][2]['textValue'] if item['cells'][2]['dataType'] == "urn:replicon:list-type:string" else "",
        'enabled': item['cells'][4]['textValue']
    }for item in search_result_rows]


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key):
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:

            if not (item['effectiveDate']):
                initial_value = item
                continue

            daydiff = (
                now().date() - to_datetime(item['effectiveDate'], "%m/%d/%Y").date())

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return current_value[scrpit_name][required_key] if current_value else (initial_value[scrpit_name][required_key] if initial_value else '')


def get_final_supervisor_assignment_entry_status(existing_user_log):

    if existing_user_log['properties']['status'] == 'Error':
        return 'Error'

    if rail.result('log_errorfor_supervisorand_userslogin_nameissame_31') or rail.result('log_erroras_supervisorisnotavailable_29') or rail.result(
            'log_errorwhensupervisorisdisabled_27'):
        return 'Exception'

    return existing_user_log['properties']['status']


def get_final_supervisor_assignment_entry_details(existing_user_log):
    details = [existing_user_log['properties']['details']] if existing_user_log['properties']['details'] else []

    if rail.result('log_errorfor_supervisorand_userslogin_nameissame_31'):
        details.append(rail.result(
            'log_errorfor_supervisorand_userslogin_nameissame_31'))

    if rail.result('log_errorwhensupervisorisdisabled_27'):
        details.append(rail.result(
            'log_errorwhensupervisorisdisabled_27'))

    if rail.result('log_erroras_supervisorisnotavailable_29'):
        details.append(rail.result(
            'log_erroras_supervisorisnotavailable_29'))

    return ";".join(details)


def get_relevant_historical_policies(existing_timeoff_policysetschedule, effective_date_derived):
    if bool(existing_timeoff_policysetschedule and existing_timeoff_policysetschedule[0] and existing_timeoff_policysetschedule[0]['description']):
        count = 0
        for item in existing_timeoff_policysetschedule:
            if to_datetime(item['effectiveDate']) < to_datetime(effective_date_derived, "%m/%d/%Y"):
                count += 1

        relevant_historical_policies = json.loads(json.dumps(existing_timeoff_policysetschedule[0:count]).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        return relevant_historical_policies

    return []


def create_new_policyset_schedule_with_historical_policies(relevant_historical_policies):
    new_policyset_schedule = []
    if bool(relevant_historical_policies):
        for item in relevant_historical_policies:
            new_policyset_schedule.append({
                'description': item['description'],
                'effectiveDate': item['effectiveDate'],
                'policySet': item['policySet']
            })
    return new_policyset_schedule


def get_final_policy_with_remaining_balance_policy_line(remaining_balance, new_policyset_schedule_with_historical,
                                                        effective_date, date_format, starting_balance_script_uri, prevent_balance_overdraw_script_uri):

    new_policyset_schedule_with_historical.append({
        "description": f"Added by Integration on {effective_date}",
        "effectiveDate": rail.parse_date(effective_date, date_format),
        "policySet": {
            "timeOffBalanceEventScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": remaining_balance
                    }
                }],
                "scriptTarget": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": starting_balance_script_uri
                }
            }],
            "timeOffValidationScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                    "value": {
                        "number": "0"
                    }
                }],
                "scriptTarget": {
                    "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                    "name": "Prevent balance overdraw",
                    "uri": prevent_balance_overdraw_script_uri
                }
            }]
        }
    })

    return new_policyset_schedule_with_historical


def dict_date_to_datetime(dict_date):
    return datetime.strptime(str(dict_date['month']) + "/" + str(dict_date['day']) + "/" + str(dict_date['year']), "%m/%d/%Y").date()


def get_modified_payrule_list(user_payrulescript_schedule, default_payrulescript_uri):
    schedule_entries = []

    for script in user_payrulescript_schedule:
        if script['effectiveDate']:
            if dict_date_to_datetime(script['effectiveDate']) <= datetime.now().date():
                schedule_entries.append({
                    "payRuleScript": {
                        "uri": script['payRuleScript']['uri'],
                        "name": null
                    },
                    "effectiveDate": script['effectiveDate']
                })

        elif not script['effectiveDate']:
            schedule_entries.append({
                "payRuleScript": {
                    "uri": script['payRuleScript']['uri'],
                    "name": null
                },
                "effectiveDate": null
            })

    if not schedule_entries:
        if default_payrulescript_uri:
            schedule_entries.append({
                "payRuleScript": {
                    "uri": default_payrulescript_uri,
                    "name": null
                },
                "effectiveDate": null
            })

    return schedule_entries
