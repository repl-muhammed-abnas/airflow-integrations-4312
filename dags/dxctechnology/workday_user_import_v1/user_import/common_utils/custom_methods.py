import itertools
from decimal import Decimal
import rail
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, date, timezone
from json import dumps, loads
from airflow.exceptions import AirflowException
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_minus_specified_days_date_in_json
from dxctechnology.workday_user_import_v1.user_import.common_utils.region_fields_config import get_excluded_fields

null  = None
TIMEZONE = 'America/Los_Angeles'
INPUT_DATE_FORMAT = "%Y-%d-%m"
OPEN_BRACKETS = "{{"
CLOSE_BRACKETS = "}}"

def exp_to_decimal_best(exp_str):
    try:
        decimal_num = Decimal(str(exp_str))
        result = format(decimal_num.quantize(Decimal('1.00')), '.2f')
        return result
    except Exception as e:
        return f"Error: {str(e)}"


def get_json_date_from_date(_date):
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):
    _date = datetime.strptime(date_str, _date_format)
    if return_format == "date":
        return _date
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_end_date_to_use(dag_run):
    timeoff_details = rail.result("get_timeoff_details")[0]
    location = rail.result("get_users_effective_group_membership")['locations'][0] \
        if rail.result("get_users_effective_group_membership")['locations'] else {}

    _date = datetime.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)

    if timeoff_details['name'].startswith("[AUS]"):
        _date += timedelta(days=1)
    elif not location.get('location', {}).get('parent', {}):
        return _date
    elif location.get('location', {}).get('parent', {}).get('location', {}).get('displayText', "") == "Australia":
        _date += timedelta(days=1)
    else:
        pass  # initialized the value

    return _date

def convert_date_to_string_date(_date, _format=INPUT_DATE_FORMAT):
    return _date.strftime(_format)

def get_user_uri(dag_run, task_id='create_user'):
    if dag_run.conf.get('user_uri'):
        return dag_run.conf.get('user_uri')
    return rail.result(task_id)['uri']

def convert_json_date_to_date(json_date):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year'])

def convert_json_date_to_string_date(json_date, _format= INPUT_DATE_FORMAT):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year']).strftime(_format)

def get_tenure_value(date_1, date_2):
    tenure = (min(float(((date_1-date_2).days)/365), 0))
    return 0 if tenure == float(0) else tenure * (-1)


def should_trigger_delete_time_and_timeoff_for_disabled_user(dag_run):
    # Check if user is disabled (profile_status != "enabled")
    profile_status = dag_run.conf.get('file_data', {}).get('status', {})
    
    if not profile_status or profile_status.lower() != "0":
        return False
    
    # Check if term_date exists and is in the past
    term_date_str = dag_run.conf.get('file_data', {}).get('term_date')

    if not term_date_str:
        return False
    
    try:
        # Parse term_date and compare with today
        term_date = datetime.strptime(term_date_str, INPUT_DATE_FORMAT).date()
        today = datetime.now().date()
        
        # Return True if term_date is in the past
        return term_date < today
    except (ValueError, TypeError):
        return False

def get_day_diff_between_two_dates(date_1, date_2: date):
    return int(min(float(((date_1-date_2).days)), 0))*(-1)

def get_specified_json_date_minus_specified_days_months_years_date_in_json(_date_to_use: dict, days_in_number:int=0, months_in_number:int=0, years_in_number:int=0, return_type="json"):
    _date_to_use:date = convert_json_date_to_date(_date_to_use)
    _date_to_return = _date_to_use - relativedelta(
        days=days_in_number,
        months=months_in_number,
        years=years_in_number
    )
    if return_type == "date":
        return _date_to_return
    return {
        "day": _date_to_return.day,
        "month": _date_to_return.month,
        "year": _date_to_return.year
    }


def add_new_policy_line(dag_run):
    _date = get_end_date_to_use(dag_run)
    return {
        "effectiveDate": {
            "day": _date.day,
            "month": _date.month,
            "year": _date.year
        },
        "description": f"Added by Integration on {_date.strftime('%d-%m-%Y')}",
        "policySet": {
            "timeOffBalanceEventScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": exp_to_decimal_best(str(rail.result('get_user_timeoff_balance_summary')["timeRemaining"]))
                    }
                }],
                "script": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": dag_run.conf['starting_balance_set_to_uri']
                }
            }],
            "timeOffValidationScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                    "value": {
                        "number": "0"
                    }
                }],
                "script": {
                    "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                    "name": "Prevent balance overdraw",
                    "uri": dag_run.conf['prevent_balance_overdraw_uri']
                }
            }]
        }
    }


def get_timeoff_polices_to_assign_callable(dag_run):
    policies = loads(dag_run.conf['policy_set'])
    user_end_date: datetime = convert_json_date_to_date(
        dag_run.conf['user_end_date_json'])
    existing_policy = list(filter(
        lambda policy: convert_json_date_to_date(
            policy['effectiveDate']) < user_end_date,
        policies
    ))

    existing_policy.append(add_new_policy_line(dag_run))

    return existing_policy


def format_timeoff_polices_to_assign_callable():
    return dumps(rail.result("get_timeoff_polices_to_assign")
                ).replace("/null/", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ).replace('":{"additionalParameters', '":[{"additionalParameters'
                ).replace(':{"keyUri"', ':[{"keyUri"'
                ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                ).replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts'
                ).replace('}}},"description', '}}]},"description')


def is_caller_add_update_rehire(dag_run, caller_value_to_compare:str):
    return dag_run.conf['caller'] == caller_value_to_compare

def get_date_from_json_date(json_formatted_date:dict):

    if not isinstance(json_formatted_date, dict):
        raise AirflowException("Expected format {'day': xx, 'month': xx, 'year': xxxx}, got %s", json_formatted_date)
    
    if not json_formatted_date:
        raise AirflowException("value is not present.")
    
    return datetime(year=json_formatted_date['year'],
                month=json_formatted_date['month'],
                day=json_formatted_date['day']
            )

def compare_two_dates(date1, date2, operator='=='):
    if operator == '<':
        return date1 < date2
    if operator == '>':
        return date1 > date2
    if operator == '!=':
        return date1 != date2
    return date1 == date2


def do_format_logs(dag_run):
    def get_status(user_logs):
        available_status = list(
            map(lambda log: log['properties']['Status'], user_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        return "Success"

    master_log = []
    if dag_run.conf['exception_log']:
        master_log.extend(rail.load_all_records(dag_run.conf['exception_log']))

    for log in dag_run.conf['logs']:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)
    users = list(
        set(map(lambda x: x['properties']['Userid'], master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for employeeid in users:
        user_logs = list(
            filter(lambda x: x['properties']['Userid'] == employeeid and x['properties']['Details'], master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                'employee_id': employeeid,
                'login_name': first['properties']['Email'],
                'status': get_status(user_logs),
                'action': first['properties']['Action'],
                'details': ";".join(list(set(map(lambda x: x['properties']['Details'], user_logs)))),
                'jobid': first['ecid'],
            })
    skipped_record_count = len(list(filter(lambda log: log['status'].lower() == 'skipped', logs)))
    rail.set_result(key="new_users_record_count", val=len(list(filter(lambda log: log['action'].lower() in ['added', 'add'], logs))))
    rail.set_result(key="update_users_record_count", val=len(list(filter(lambda log: log['action'].lower() in ['updated', 'update'], logs))))
    rail.set_result(key="skipped", val=len(list(filter(lambda log: log['status'].lower() == 'skipped', logs))))
    rail.set_result(key="success", val=len(list(filter(lambda log: log['status'].lower() == 'success', logs))))
    rail.set_result(key="error", val=len(list(filter(lambda log: log['status'].lower() == 'error', logs))))
    rail.set_result(key="exception", val=len(list(filter(lambda log: log['status'].lower() == 'exception', logs))))
    rail.set_result(key="processed", val=(dag_run.conf['total_record_count'] - (skipped_record_count + dag_run.conf['skipped_in_validation'])))

    return dumps(logs, ensure_ascii=False)

def get_all_run_ids_callable(trigger_id, parallel_count):
    results = []
    for x in range(parallel_count):
        result = rail.result(f'{trigger_id}_{x+1}')
        if result is not None:
            results.append(result)
    return list(itertools.chain(*results))

def get_work_week_based_effective_date(work_week):
    today = datetime.now(timezone.utc)
    # workato: Sunday = 0, Monday = 1
    # python is Monday = 0, Sunday = 6
    today_weekday = today.weekday() + 1 
    work_week_startswith_saturday = work_week.lower().split(" ")[0] == "saturday"
    days_to_reduce_mapper = {
        7 : [1,7], # workato: 0 (Sunday)
        1 : [2,0], # workato: 1 (Monday)
        2 : [3,1], # workato: 2 (Tuesday)
        3 : [4,2], # workato: 3 (Wednesday)
        4 : [5,3], # workato: 4 (Thursday)
        5 : [6,4], # workato: 5 (Friday)
        6 : [0,5]  # workato: 0 (Saturday)
    }

    return today - timedelta(days=days_to_reduce_mapper[today_weekday][0 if work_week_startswith_saturday else 1])

def get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload, current_custom_fields_values, update_txt_udf:callable, update_date_udf: callable):
    is_ia = dag_run.conf['file_data']['is_ia']
    ia_start_date = dag_run.conf['json_formatted_dates']['ia_start_date']
    ia_end_date = dag_run.conf['json_formatted_dates']['ia_end_date']
    today_minus_five_days = convert_json_date_to_date(get_todays_minus_specified_days_date_in_json(5)) 
    effective_date = null
    if (is_ia != rail.find_first_by_attr_and_get_attr(
                current_custom_fields_values, 'customField.displayText', 'International Assignee', 'text')):
        
        update_txt_udf(dag_run, 'is_ia', 'International Assignee', 'international_assignee', custom_fields_payload, current_custom_fields_values)
        
        if not ia_start_date and is_ia in [1, '1']:
            return False, "User processing skipped as IAStart date not available for IA=1", effective_date

        if not ia_end_date and is_ia in [0, '0']:
            return False, "User processing skipped as IAEnd date not available for IA=0", effective_date

        if is_ia in [1,'1'] and (convert_json_date_to_date(ia_start_date) < today_minus_five_days):
            return False, "User processing skipped as IAStart date in past for IA=1", effective_date

        if is_ia in [0,'0'] and (convert_json_date_to_date(ia_end_date) < today_minus_five_days):
            return False, "User processing skipped as IAEnd date in past for IA=0", effective_date

        if is_ia in [1,'1']:
            rail.set_result(key="effective_date", val=ia_start_date)
            effective_date = ia_start_date
            update_date_udf(dag_run, 'ia_start_date',
                            'ia_start_date', 'International assignee start date',
                            'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)

        if is_ia in [0,'0']:
            # For IA=0 (Home/Host Pay), effective date is ia_end_date + 1
            # This is the start date for new policy assignments after IA ends
            ia_end_date_plus_one = get_json_date_from_date(convert_json_date_to_date(ia_end_date) + timedelta(days=1))
            rail.set_result(key="effective_date", val=ia_end_date_plus_one)
            # Store ia_end_date for end date calculations (to end existing policies)
            rail.set_result(key="ia_end_date_for_end", val=ia_end_date)
            update_date_udf(dag_run, 'ia_end_date',
                            'ia_end_date', 'International assignee end date',
                            'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)
            effective_date = ia_end_date_plus_one

        return True, "", effective_date

    else:
        update_date_udf(dag_run, 'ia_start_date', 'ia_start_date', 'International assignee start date', 'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)
        update_date_udf(dag_run, 'ia_end_date', 'ia_end_date', 'International assignee end date', 'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)

        return False, "", effective_date

def get_date_to_use_for_no_accrual(dag_run, default_return="", return_type='json'):
    if dag_run.conf['ia_updated'] in [True, 'true', 'True']:
        if dag_run.conf['is_ia'] in [1,'1']:
            ia_start_date = dag_run.conf['ia_start_date']
            if isinstance(ia_start_date, str):
                ia_start_date = get_replicon_date(ia_start_date)
            if return_type == "str":
                return f"{ia_start_date['year']}-{ia_start_date['day']}-{ia_start_date['month']}"
            return ia_start_date

        if dag_run.conf['is_ia'] in [0,'0']:
            end_date = dag_run.conf['ia_end_date']
            if isinstance(end_date, str):
                end_date = get_replicon_date(end_date)
            _end_date = convert_json_date_to_date(end_date) + timedelta(days=1)
            if return_type == "str":
                return f"{_end_date.year}-{_end_date.day}-{_end_date.month}"
            return get_json_date_from_date(_end_date)
    
    if isinstance(dag_run.conf['start_date'], str):
        if default_return == "":
            if return_type == "str":
                return dag_run.conf['start_date']
            return get_replicon_date(dag_run.conf['start_date'])
        else:
            return default_return
    return dag_run.conf['start_date'] if default_return == "" else default_return


def compare_if_two_json_dates_are_same(date_1, date_2):
    if not date_1:
        return False
    if not date_2:
        return True
    return convert_json_date_to_date(date_1) != convert_json_date_to_date(date_2)


def get_excluded_udf_clear_payloads(region: str, current_custom_fields_values: list, udfs_config: dict) -> list:
    # Get fields that should NOT exist for this region
    excluded_fields = get_excluded_fields(region)

    if not excluded_fields:
        return []

    clear_payloads = []

    # Build a reverse lookup: displayText -> udf_key
    display_text_to_key = {}
    for udf_key, udf_info in udfs_config.items():
        if isinstance(udf_info, dict) and 'name' in udf_info:
            display_text_to_key[udf_info['name']] = udf_key

    # Check each current UDF value
    for udf_value in current_custom_fields_values:
        display_text = udf_value.get('customField', {}).get('displayText', '')

        # Skip if this field is not in the excluded list
        if display_text not in excluded_fields:
            continue

        # Check if the field has a value that needs clearing
        has_value = (
            udf_value.get('text') or
            udf_value.get('date') or
            udf_value.get('dropDownOption')
        )

        if not has_value:
            continue

        # Get the URI for this field
        udf_uri = udf_value.get('customField', {}).get('uri')
        if not udf_uri:
            continue

        # Create payload to clear this UDF
        clear_payloads.append({
            "customField": {"uri": udf_uri},
            "text": null,
            "date": null,
            "dropDownOption": null,
            "number": null
        })

    return clear_payloads


def get_excluded_oef_clear_payloads(region: str, current_oef_values: list) -> list:
    from dxctechnology.workday_user_import_v1.user_import.common_utils.region_fields_config import _OEF_CONFIG

    # Get OEF fields that should NOT exist for this region
    excluded_oefs = [
        field for field, regions in _OEF_CONFIG.items()
        if region.lower() not in regions
    ]

    if not excluded_oefs:
        return []

    clear_payloads = []

    for oef_value in current_oef_values:
        display_text = oef_value.get('definition', {}).get('displayText', '')

        if display_text not in excluded_oefs:
            continue

        # Check if has value
        has_value = (
            oef_value.get('textValue') or
            oef_value.get('numericValue') or
            oef_value.get('tag')
        )

        if not has_value:
            continue

        definition_uri = oef_value.get('definition', {}).get('uri')
        if not definition_uri:
            continue

        clear_payloads.append({
            "definition": {"uri": definition_uri},
            "textValue": null,
            "numericValue": null,
            "tag": null
        })

    return clear_payloads
