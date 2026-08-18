from datetime import datetime
from pendulum import now
import rail
import operator
import json
import itertools

null = None

# This is defined in config.py as well, if any change is required, please update there as well
DATE_FORMAT = "%Y-%m-%d"


def get_validation_exception_for_group(item, replicon_location_group_data, replicon_department_group_data):
    details = []
    if not (rail.find_first_by_attr_and_get_attr(
            replicon_location_group_data, 'full_path_code', item['LOCATION_GROUP_PARENT_HIERARCHY'], 'uri', null)):
        details.append(
            "Location Level 1/Level 2 or both not present in Replicon")
    if not (rail.find_first_by_attr_and_get_attr(
            replicon_department_group_data, 'full_path_code', item['DEPARTMENT_GROUP_HIERARCHY'], 'uri', null)):
        details.append(
            f'Department with fullpath {item["DEPARTMENT_GROUP_HIERARCHY"]} not present in Replicon')

    return 'User not processed due to - ' + ';'.join(details)


def get_process_each_user_payload_dag_ids(parallel_count_active_users, parallel_count_disable_users):
    # Get supervisor DAG runs
    supervisors_with_subordinates_in_feed = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_supervisors_with_subordinates_in_feed_{x+1}') if rail.result(
            f'process_supervisors_with_subordinates_in_feed_{x+1}') else []), range(parallel_count_active_users)))))

    # Get non-supervisor DAG runs
    other_active_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_remaining_active_users_{x+1}') if rail.result(
            f'process_remaining_active_users_{x+1}') else []), range(parallel_count_active_users)))))

    # Get disable users DAG runs
    disable_users = list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'process_disable_users_{x+1}') if rail.result(
            f'process_disable_users_{x+1}') else []), range(parallel_count_disable_users)))))

    return supervisors_with_subordinates_in_feed + other_active_users + disable_users


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_replicon_date(date_str, date_format=DATE_FORMAT):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, date_format)
    except ValueError:
        return None
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


def to_datetime(date, date_format=DATE_FORMAT):
    if isinstance(date, dict):
        return datetime(day=date['day'], month=date['month'], year=date['year'])
    elif isinstance(date, str):
        return datetime.strptime(date, date_format)
    return date


def compare_dates(date_1, op, date_2, date_format=DATE_FORMAT):
    date_1 = to_datetime(date_1, date_format)
    date_2 = to_datetime(date_2, date_format)

    ops = {
        '>': operator.gt,
        '<': operator.lt,
        '=': operator.eq,
        '==': operator.eq,
        '>=': operator.ge,
        '<=': operator.le
    }

    return ops[op](date_1, date_2)


def test_and_log_valid_fields(dag_run):
    # pylint: disable=too-many-return-statements
    log = []
    test_result = True
    startdate = get_replicon_date(dag_run.conf['Hire_Date'])
    effective_date = get_replicon_date(dag_run.conf['Effective_Date'])
    if not startdate:
        log.append('Invalid format for Hire Date')
        test_result = False
    if dag_run.conf['Termination_Date']:
        enddate = get_replicon_date(dag_run.conf['Termination_Date'])
        if not enddate:
            log.append('Invalid format for Termination Date')
            test_result = False
    if not effective_date:
        log.append('Invalid format for Effective Date')
        test_result = False
    return {
        'log': rail.smartjoin_by_delim(log, ";") if log else None,
        'test_result': test_result,
    }


def get_parent_uri_from_created_departments():
    created_departments = rail.load_all_records(rail.result(
        'check_newly_created_departments_for_parent'))
    if not created_departments:
        return None
    return created_departments[0]['properties']['uri_if_created']


def parse_time_off_types_from_csv(time_off_types_str):
    """
    Parse comma-separated time off types and look up their URIs.

    Args:
        time_off_types_str: Comma-separated string like "Sick,Bereavement,Accrued Vacation PT25"

    Returns:
        List of dicts with 'name' and 'uri' for each time off type (uri is blank if not found)
    """
    if not time_off_types_str:
        return []

    # Split by comma and strip whitespace
    time_off_names = [name.strip()
                      for name in time_off_types_str.split(',') if name.strip()]

    # Look up URIs for each time off type
    time_off_list = []
    for name in time_off_names:
        uri = rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_time_off_types'), 'name', name, 'uri')
        time_off_list.append({
            'name': name,
            'uri': uri if uri else ''  # Keep blank if not found
        })

    return time_off_list


def validate_replicon_field_names_uris(dag_run):
    """
    Validates that all required Replicon field names/groups have valid URIs.
    Returns a dict with 'is_valid' boolean and 'missing_fields' list of error messages.
    """
    missing_fields = []

    # Validate Groups
    if not (dag_run.conf["location_to_apply_uri"]):
        missing_fields.append(
            "Required Location does not exist in Replicon")

    if not (dag_run.conf["department_to_apply_uri"]):
        missing_fields.append(
            "Required Department does not exist in Replicon")

    if not (dag_run.conf["employeetype_group_to_apply_uri"]):
        missing_fields.append(
            "Required Employee Type group not found in Replicon")

    # Validate Timesheet Template
    if dag_run.conf.get('Timesheet_Template') and not (dag_run.conf.get('timesheet_template_uri')):
        missing_fields.append(
            f"Timesheet Template '{dag_run.conf['Timesheet_Template']}' not found in Replicon")

    # Validate Punch Entry Policy
    if dag_run.conf.get('Punch_Entry_Policy') and not (dag_run.conf.get('punch_entry_policy_uri')):
        missing_fields.append(
            f"Punch Entry Policy '{dag_run.conf['Punch_Entry_Policy']}' not found in Replicon")

    # Validate Holiday Calendar
    if dag_run.conf.get('Holiday_Calendar') and not (dag_run.conf.get('holiday_calendar_uri')):
        missing_fields.append(
            f"Holiday Calendar '{dag_run.conf['Holiday_Calendar']}' not found in Replicon")

    # Validate Pay Rule
    if dag_run.conf.get('Pay_Rule') and not (dag_run.conf.get('pay_rule_uri')):
        missing_fields.append(
            f"Pay Rule '{dag_run.conf['Pay_Rule']}' not found in Replicon")

    # Validate Office Schedule
    if dag_run.conf.get('Office_Schedule') and not (dag_run.conf.get('office_schedule_uri')):
        missing_fields.append(
            f"Office Schedule '{dag_run.conf['Office_Schedule']}' not found in Replicon")

    # Validate Time Off Types
    if dag_run.conf.get('eligible_timeoffs_for_user'):
        for timeoff in dag_run.conf['eligible_timeoffs_for_user']:
            if timeoff.get('name') and not (timeoff.get('uri')):
                missing_fields.append(
                    f"Time Off Type '{timeoff['name']}' not found in Replicon")

    # Validate Timesheet Period
    if dag_run.conf.get('Timesheet_Period') and not (dag_run.conf.get('timesheet_period_uri')):
        missing_fields.append(
            f"Timesheet Period '{dag_run.conf['Timesheet_Period']}' not found in Replicon")

    # Validate Timesheet Approval Path
    if dag_run.conf.get('Timesheet_Approval_Path') and not (dag_run.conf.get('timesheet_approval_path_uri')):
        missing_fields.append(
            f"Timesheet Approval Path '{dag_run.conf['Timesheet_Approval_Path']}' not found in Replicon")

    # Validate Time Off Template
    if dag_run.conf.get('Time_Off_Template') and not (dag_run.conf.get('time_off_template_uri')):
        missing_fields.append(
            f"Time Off Template '{dag_run.conf['Time_Off_Template']}' not found in Replicon")

    # Validate Work Week
    if dag_run.conf.get('Work_Week') and dag_run.conf["Work_Week"].split(" ")[0].lower() not in (
            ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']):
        missing_fields.append(
            f"Work Week start day - '{dag_run.conf['Work_Week'].split(' ')[0]}' is not a valid day of the week")

    # Validate Supervisor Assignment
    if rail.result('get_final_result_from_supervisor_assignment_workflow')['supervisor_validation_failed_check']:
        missing_fields.append(
            rail.result('get_final_result_from_supervisor_assignment_workflow')['supervisor_validation_error_detail'])

    return {
        'is_valid': len(missing_fields) == 0,
        'missing_fields': missing_fields
    }


def get_new_eligible_timeoffs_for_update_user(dag_run):
    # Get eligible timeoffs, ensure it's a list
    eligible_timeoffs = dag_run.conf.get('eligible_timeoffs_for_user', [])
    if not eligible_timeoffs:
        return {
            'new_eligible_timeoffs': [],
            'timeoff_for_stopping_accrual': []
        }

    # Extract URIs, filtering out any None values
    eligible_timeoff_uris_from_payload = [
        timeoff['uri'] for timeoff in eligible_timeoffs
        if timeoff.get('uri')]

    existing_timeoff_types_uris = [timeoff_details['timeOffType']['uri']
                                   for timeoff_details in rail.result('log_existing_timeoff_policies_for_user')]

    new_eligible_timeoffs = [timeoffs for timeoffs in eligible_timeoffs
                             if timeoffs.get('uri') and timeoffs['uri'] not in existing_timeoff_types_uris]

    existing_non_eligible_timeoffs = [
        timeoff_uri for timeoff_uri in existing_timeoff_types_uris
        if timeoff_uri not in eligible_timeoff_uris_from_payload]

    return {
        'new_eligible_timeoffs': new_eligible_timeoffs,
        'timeoff_for_stopping_accrual': existing_non_eligible_timeoffs
    }


def get_relevant_historical_policies(existing_timeoff_policysetschedule, effective_date_derived):
    if bool(existing_timeoff_policysetschedule and existing_timeoff_policysetschedule[0] and existing_timeoff_policysetschedule[0]['description']):
        count = 0
        for item in existing_timeoff_policysetschedule:
            if to_datetime(item['effectiveDate']) < to_datetime(effective_date_derived):
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


def final_result_from_sup_assignment_workflow():
    validation_error = (
        rail.result('log_supervisor_not_present') or
        rail.result('log_supervisor_end_date_in_past') or
        rail.result('log_supervisor_is_disabled') or
        rail.result('log_error_in_get_missing_supervisor_permissions')
    )
    return {
        'supervisor_validation_failed_check': bool(validation_error),
        'supervisor_validation_error_detail': validation_error,
        'supervisor_to_assign_uri': (rail.result('search_supervisor_in_replicon')['uri'] if not (bool(rail.result(
            'same_supervisor_already_assigned'))) else '') if not (validation_error) else ''
    }


def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf['userlogs']
    otherlogs = dag_run.conf['otherlogs']

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'jobid': log['ecid']
        },
        **log['properties'],
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['status'] == 'Exception', final_log_records))))
    rail.set_result(key="total_record_count",
                    val=dag_run.conf['total_records'])

    return final_log_records


def get_email_details_callable(dag_run, time_zone):
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"Log_{dag_run.conf['input_file_name'].replace('.csv' , '')}_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }
