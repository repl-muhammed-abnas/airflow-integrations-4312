from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
from rail import find_first_by_attr_and_get_attr, get_current_context, result, set_result, load_all_records


def get_task_state(task_id):
    return get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_dag_run_conf():
    return get_current_context()['dag_run'].conf


def get_datetime_obj(effectiveDate):
    year = effectiveDate['year']
    month = effectiveDate['month']
    day = effectiveDate['day']
    return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')


def get_replicon_date(date_str, fmt='%m/%d/%Y'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_archivefilename(filename):
    is_pgp = result('is_pgp')
    upload_processed_file = get_task_state('upload_processed_file')
    if is_pgp == 'send_bad_file_format_email':
        return f'unprocessed_{filename}.pgp'
    if upload_processed_file == 'success':
        return f'{filename}.pgp'
    return ''


def get_varreplicon_feedfile_enabledusers():
    enabled_user_len = float(len(result('get_enabledusers')))
    raw_input_coll_len = float(
        result('create_rawinputdata_collection', 'length'))
    return round(((enabled_user_len - raw_input_coll_len) / enabled_user_len) * 100, 2)


def get_all_userlogs():
    logs = []

    create_unchanged_records_log = result(
        'create_unchanged_records_log')
    if create_unchanged_records_log:
        logs.append(create_unchanged_records_log)

    gather_child_logs = result(
        'gather_child_logs')
    if gather_child_logs:
        logs.extend(gather_child_logs)

    gather_child_logs_disabled = result(
        'gather_child_logs_disabled')
    if gather_child_logs_disabled:
        logs.extend(gather_child_logs_disabled)

    return logs


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():
    dag_run = get_current_context()['dag_run']

    log_artifacts = dag_run.conf['user_logs']

    log_records = []

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **dict(x['properties'].items()),
        **{
            'jobid': x['ecid']
        }}, log_records))


def get_downstreamtasks_error(error_message):
    return {
        'error': error_message
    }


def construct_policyschedule():
    policy_set_schedule = result('get_existingpolicy_schedule_for_timeoff')
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


def addlogs_to_error(mandatoryfield_config):
    errors = []
    for field_name in mandatoryfield_config:
        custom_message = mandatoryfield_config[field_name]
        if custom_message:
            errors.append(custom_message)
    return errors


def get_user_field_exception(dag_run):

    dag_run_conf = dag_run.conf
    mandatoryfield_config = {
        'employeenumber': 'Login name not present' if not dag_run_conf['employeenumber'] else False,
        'firstname': 'First_Name not present' if not dag_run_conf['firstname'] else False,
        'employeeorgcode': 'Employee_Org_Code not present' if not dag_run_conf['employee_org_code'] else False,
        'lastname': 'Last_Name not present' if not dag_run_conf['lastname'] else False,
        'startdate': 'Start_Date not present' if not dag_run_conf['startdate'] else False,
        'emailaddress': 'Email_Address not present' if not dag_run_conf['emailaddress'] else False,
        'principalstatus': 'Principal_Status not present' if not dag_run_conf['principalstatus'] else False,
        'department': 'Department not present' if not dag_run_conf['department'] else False,
        'timezone_code': 'Timezone_Code not present' if not dag_run_conf['timezone_code'] else False,
        'employee_location_state': 'Employee_Location_State not present' if not dag_run_conf['employee_location_state'] else False,
        'chargeability': 'Chargeability_% not present' if not dag_run_conf['chargeability'] else False,
        'full_time_availability': 'Full_Time_Availability not present' if not dag_run_conf['full_time_availability'] else False,
        'job_title': 'Job_Tile not present' if not dag_run_conf['job_title'] else False,
        'assignment_status': 'Assignment_Status not present' if not dag_run_conf['assignment_status'] else False,
        'assignment_category': 'Assignment_Category not present' if not dag_run_conf['assignment_category'] else False,
        'assignment_category_effective_date': 'Assignment_Category Effective Date not present' if not dag_run_conf[
            'assignment_category_effective_date'] else False,
        'assignment_status_effective_date': 'Assignment_Status Effective Date not present' if not dag_run_conf['assignment_status_effective_date'] else False,
        'timesheettemplate': 'Timesheet_Template not present' if not dag_run_conf['timesheettemplate'] else False
    }

    errors = addlogs_to_error(mandatoryfield_config)
    if errors:
        return ', '.join(errors)

    return ''


def get_required_policysets(response, dag_run):
    policy_sets = []

    timesheet_template = dag_run.conf['timesheettemplate']
    timesheet_template_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', timesheet_template, 'uri', '')
    if timesheet_template_uri:
        policy_sets.append(timesheet_template_uri)
    else:
        set_result(
            "Timesheet Template not assigned, since Timesheet Template doesn\'t exist in Replicon",
            'exception_message')

    timeoff_template_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'Time Off', 'uri', '')
    if timeoff_template_uri:
        policy_sets.append(timeoff_template_uri)
    return policy_sets


def get_locationschedule_assignments(response):

    location_schedule_list = []
    if response:
        for location in response:
            uri = location['location']['uri']
            if not (location.get('effectiveDate') and location['effectiveDate'].get('year')):
                location_schedule_list.append({
                    'location': {
                        'uri': uri
                    }
                })
            else:
                effective_date = get_datetime_obj(location['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    location_schedule_list.append({
                        'location': {
                            'uri': uri
                        },
                        'effectiveDate': location['effectiveDate']
                    })
    if result('get_required_locationname')['required_locationuri']:
        location_schedule_list.append({
            'location': {
                'uri': result('get_required_locationname')['required_locationuri']
            },
            'effectiveDate': get_today_date()
        })

    return location_schedule_list if location_schedule_list else ''


def get_costcenterschedule_assignments(response, dag_run):

    costcenter_schedule_list = []
    if response:
        for costcenter in response:
            uri = costcenter['costCenter']['uri']
            if not (costcenter.get('effectiveDate') and costcenter['effectiveDate'].get('year')):
                costcenter_schedule_list.append({
                    'costCenter': {
                        'uri': uri
                    }
                })
            else:
                effective_date = get_datetime_obj(costcenter['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    costcenter_schedule_list.append({
                        'costCenter': {
                            'uri': uri
                        },
                        'effectiveDate': costcenter['effectiveDate']
                    })
    assignment_status_date = get_replicon_date(
        dag_run.conf['assignment_status_effective_date'])
    costcenter_schedule_list.append({
        'costCenter': {
            'uri': result('get_required_costcenter')
        },
        'effectiveDate': assignment_status_date
    })

    return costcenter_schedule_list if costcenter_schedule_list else ''


def get_divisionschedule_assignments(response):

    division_schedule_list = []
    if response:
        for division in response:
            uri = division['division']['uri']
            if not (division.get('effectiveDate') and division['effectiveDate'].get('year')):
                division_schedule_list.append({
                    'division': {
                        'uri': uri
                    }
                })
            else:
                timesheet_startdate = get_datetime_obj(
                    result('get_timesheet_startdate'))
                effective_date = get_datetime_obj(division['effectiveDate'])
                if effective_date.date() < timesheet_startdate.date():
                    division_schedule_list.append({
                        'division': {
                            'uri': uri
                        },
                        'effectiveDate': division['effectiveDate']
                    })
    division_schedule_list.append({
        'division': {
            'uri': result('get_required_divisionname')['required_divisionuri']
        },
        'effectiveDate': result('get_timesheet_startdate')
    })

    return division_schedule_list if division_schedule_list else ''


def get_servicecenterschedule_assignments(response, dag_run):

    servicecenter_schedule_list = []
    if response:
        for servicecenter in response:
            uri = servicecenter['serviceCenter']['uri']
            if not (servicecenter.get('effectiveDate') and servicecenter['effectiveDate'].get('year')):
                servicecenter_schedule_list.append({
                    'serviceCenter': {
                        'uri': uri
                    }
                })
            else:
                effective_date = get_datetime_obj(
                    servicecenter['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    servicecenter_schedule_list.append({
                        'serviceCenter': {
                            'uri': uri
                        },
                        'effectiveDate': servicecenter['effectiveDate']
                    })
    assignment_category_date = get_replicon_date(
        dag_run.conf['assignment_category_effective_date'])
    servicecenter_schedule_list.append({
        'serviceCenter': {
            'uri': result('get_required_servicecenter')
        },
        'effectiveDate': assignment_category_date
    })

    return servicecenter_schedule_list if servicecenter_schedule_list else ''


def get_employeetypeschedule_assignments(response):

    employeetype_schedule_list = []
    if response:
        for employeetype in response:
            uri = employeetype['employeeTypeGroup']['uri']
            if not (employeetype.get('effectiveDate') and employeetype['effectiveDate'].get('year')):
                employeetype_schedule_list.append({
                    'employeeTypeGroup': {
                        'uri': uri
                    }
                })
            else:
                timesheet_startdate = get_datetime_obj(
                    result('get_timesheet_startdate'))
                effective_date = get_datetime_obj(
                    employeetype['effectiveDate'])
                if effective_date.date() < timesheet_startdate.date():
                    employeetype_schedule_list.append({
                        'employeeTypeGroup': {
                            'uri': uri
                        },
                        'effectiveDate': employeetype['effectiveDate']
                    })
    employeetype_schedule_list.append({
        'employeeTypeGroup': {
            'uri': result('get_required_employeetype')
        },
        'effectiveDate': result('get_timesheet_startdate')
    })

    return employeetype_schedule_list if employeetype_schedule_list else ''


def get_payruleschedule_assignments(response):

    payrulescript_schedule_list = []
    if response:
        for payrulescript in response:
            uri = payrulescript['payRuleScript']['uri']
            if not (payrulescript.get('effectiveDate') and payrulescript['effectiveDate'].get('year')):
                payrulescript_schedule_list.append({
                    'payRuleScript': {
                        'uri': uri
                    }
                })
            else:
                timesheet_startdate = get_datetime_obj(
                    result('get_timesheet_startdate'))
                effective_date = get_datetime_obj(
                    payrulescript['effectiveDate'])
                if effective_date.date() < timesheet_startdate.date():
                    display_text = payrulescript['payRuleScript']['displayText']
                    payrulescript_schedule_list.append({
                        'payRuleScript': {
                            'uri': uri,
                            'displayText': display_text
                        },
                        'effectiveDate': payrulescript['effectiveDate']
                    })
    payrulescript_schedule_list.append({
        'payRuleScript': {
            'uri': result('get_required_payrule')['uri'],
            'name': result('get_required_payrule')['name']
        },
        'effectiveDate': result('get_timesheet_startdate')
    })

    return payrulescript_schedule_list if payrulescript_schedule_list else ''


def get_departmentgroupschedule_assignments(response, dag_run):

    departmentgroup_schedule_list = []
    if response:
        for departmentgroup in response:
            uri = departmentgroup['departmentGroup']['uri']
            if not (departmentgroup.get('effectiveDate') and departmentgroup['effectiveDate'].get('year')):
                departmentgroup_schedule_list.append({
                    'departmentGroup': {
                        'uri': uri
                    }
                })
            else:
                effective_date = get_datetime_obj(
                    departmentgroup['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    departmentgroup_schedule_list.append({
                        'departmentGroup': {
                            'uri': uri
                        },
                        'effectiveDate': departmentgroup['effectiveDate']
                    })
    departmentgroup_schedule_list.append({
        'departmentGroup': {
            'uri': dag_run.conf['departmentgroupuri']
        },
        'effectiveDate': get_today_date()
    })

    return departmentgroup_schedule_list if departmentgroup_schedule_list else ''


def get_schedulepolicyschedule_assignments(response):

    office_schedule_list = []
    if response:
        for officeschedule in response:
            uri = officeschedule['officeSchedule']['uri']
            schedule_type_uri = officeschedule['scheduleTypeUri']
            if not (officeschedule.get('effectiveDate') and officeschedule['effectiveDate'].get('year')):
                office_schedule_list.append({
                    'schedulePolicy': {
                        'officeScheduleUri': uri,
                        'scheduleTypeUri': schedule_type_uri
                    }
                })
            else:
                effective_date = get_datetime_obj(
                    officeschedule['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    office_schedule_list.append({
                        'schedulePolicy': {
                            'officeScheduleUri': uri,
                            'scheduleTypeUri': schedule_type_uri
                        },
                        'effectiveDate': officeschedule['effectiveDate']
                    })
    office_schedule_list.append({
        'schedulePolicy': {
            'officeScheduleUri': result('get_required_office_schedule') if result(
                'get_required_office_schedule') else result('publish_officeschedule')['uri'],
            'scheduleTypeUri': 'urn:replicon:schedule-type:office-schedule'
        },
        'effectiveDate': get_today_date()
    })

    return office_schedule_list if office_schedule_list else ''


def get_required_updateuser_vars():

    dag_run_conf = get_dag_run_conf()

    timeofftrigger = all_timeoff_type = floating_holiday_var = 'no'

    assignment_status = dag_run_conf['assignment_status']
    current_assignment_status = result('parse_csv_user_data')[
        'Assignment Status (Current)']

    assignment_category = dag_run_conf['assignment_category']
    current_assignment_category = result('parse_csv_user_data')[
        'Assignment Type (Current)']

    required_divisionname = result('get_required_divisionname')[
        'required_divisionname']
    current_divisionname = result('parse_csv_user_data')[
        'Principal Status (Current)']

    full_time_availability = round(
        float(dag_run_conf['full_time_availability']), 2)
    current_full_time_availability = round(float(result(
        'parse_csv_user_data')['Full Time Availability']), 2) if result(
        'parse_csv_user_data')['Full Time Availability'] else ''

    hourly_salaried_code = dag_run_conf['hourly_salaried_code']
    current_employeetype = result('parse_csv_user_data')[
        'Employee Type (Current)']

    floating_holiday = dag_run_conf['floating_holiday']
    current_floating_holiday = result('parse_csv_user_data')[
        'Floating Holiday']
    if assignment_status == 'Active Assignment' and current_assignment_status != 'Active Assignment':
        timeofftrigger = 'yes'
        all_timeoff_type = 'yes'
    if full_time_availability != current_full_time_availability and assignment_category == 'Regular, <Full Time':
        timeofftrigger = 'yes'
        all_timeoff_type = 'yes'
    if required_divisionname != current_divisionname:
        timeofftrigger = 'yes'
        all_timeoff_type = 'yes'
    if hourly_salaried_code != current_employeetype:
        timeofftrigger = 'yes'
        all_timeoff_type = 'yes'
    if current_assignment_category != assignment_category:
        timeofftrigger = 'yes'
        all_timeoff_type = 'yes'
    if (floating_holiday == 'N' and current_floating_holiday == 'Y') or (
            floating_holiday == 'Y' and current_floating_holiday == 'N'):
        timeofftrigger = 'yes'
        floating_holiday_var = 'yes'
    return {
        'timeofftrigger': timeofftrigger,
        'all_timeoff_type': all_timeoff_type,
        'floating_holiday_var': floating_holiday_var
    }


def filter_records(records):
    dag_run_conf = get_dag_run_conf()
    assignment_category = dag_run_conf['assignment_category']
    return list(filter(lambda x: x["type"] == "Timeoff type" and x["assignment_status"] == assignment_category, records))


def get_exceptionmessage_updateuser():
    dag_run_conf = get_dag_run_conf()
    exception_messages = []

    if result('should_update_supervisor') == 'get_supervisor_useruri_status':
        if result('get_supervisor_useruri_status')['uri'] and result('get_supervisor_useruri_status')['status'] != 'true':
            exception_messages.append(
                'Supervisor not updated since the supervisor is disabled')
    if not dag_run_conf['supervisoremployeeid'] and 'Y' in dag_run_conf['supervisor_required']:
        exception_messages.append(
            'Supervisor not assigned, Spv_emp_number is blank')
    if result('get_required_locationname') and not result('get_required_locationname')['required_locationuri']:
        exception_messages.append(
            f"Location not updated, no location found for the code {dag_run_conf['employee_location_state']} in Replicon.")
    if result('is_assignmentstatus_present') == 'get_required_costcenter' and not result('get_required_costcenter'):
        exception_messages.append(
            f"Assignment status not updated, no Assignment status found for - {dag_run_conf['assignment_status']} in Replicon.")
    if result('get_required_locationname') and not result('get_required_divisionname')['required_divisionuri']:
        exception_messages.append(
            f"Principal status not assigned, since principal status with the code {dag_run_conf['principalstatus']} is not available in Replicon.")
    if result('is_assignment_category_present') == 'get_required_servicecenter' and not result('get_required_servicecenter'):
        exception_messages.append(
            f"Assignment Category not assigned, since Assignment Category - {dag_run_conf['assignment_category']} is not available in Replicon.")
    if result('is_hourly_salariedcode_present') == 'get_required_employeetype' and not result('get_required_employeetype'):
        exception_messages.append(
            f"Hourly Salaried code not assigned, since Hourly Salaried code - {dag_run_conf['hourly_salaried_code']} is not available in Replicon.")
    return ', '.join(exception_messages) if exception_messages else ''


def write_updateuser_log_props(dag_run):
    is_rehired = bool(result('enable_login'))
    exception_message = get_exceptionmessage_updateuser()

    return {
        "loginname": dag_run.conf['employeenumber'],
        "uri": dag_run.conf['useruri'],
        "action": "Rehire" if is_rehired else "Update",
        "status": "Exception" if exception_message else "Success",
        "reason": exception_message
    }


def construct_policyschedule_list():
    dag_run_conf = get_dag_run_conf()
    policy_set_schedule = result(
        'get_user_timeoff_policy_summary')
    policy_schedule_entries = []
    if policy_set_schedule:
        for item1 in policy_set_schedule:
            if item1:
                effective_datetime = datetime.strptime(
                    f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                    '%d/%m/%Y') if item1.get('effectiveDate') else ''
                compare_datetime = datetime.strptime(dag_run_conf['rehiredate'], '%m/%d/%Y') if dag_run_conf[
                    'isarehire'] == 'Yes' else datetime.now()
                if effective_datetime and effective_datetime.date() < compare_datetime.date():
                    parsed_item1 = json.loads(json.dumps(
                        item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"'))
                    policy_schedule_entries.append(parsed_item1)
    return policy_schedule_entries if policy_schedule_entries else ''


def final_policy_to_assign_update():

    today = datetime.now()
    global_policyset = result(
        'get_defaultpolicy_from_global_level')['response']
    if today.month < 9 and today.date() > datetime(today.year, 1, 1).date():
        for i1, item1 in enumerate(global_policyset):
            timeoff_balanceevent_scripts = item1['policySet']['timeOffBalanceEventScripts']
            for i2, item2 in enumerate(timeoff_balanceevent_scripts):
                for i3, item3 in enumerate(item2['additionalParameters']):
                    if item3['keyUri'] == 'urn:replicon:script-key:parameter:amount' and item3['value']['number'] == 0:
                        global_policyset[i1]['policySet'][
                            'timeOffBalanceEventScripts'][i2][
                                'additionalParameters'][i3] = {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": 1
                            }
                        }
    return json.loads(json.dumps(
        [x['policySet'] for x in global_policyset], ensure_ascii=False).replace(
            '"null"', '"effectiveDate"').replace(
            '"script"', '"scriptTarget"'))[0] if 'urn' in json.dumps(
        [x['policySet'] for x in global_policyset]) else ''


# pylint:disable=too-many-branches, too-many-nested-blocks, too-many-statements, line-too-long
def get_final_paidtimeoff_policysets_updateuser():

    dag_run_conf = get_dag_run_conf()
    principalstatus = int(dag_run_conf['principalstatus'])
    effective_date_today = get_today_date()

    global_policyset = result(
        'get_defaultpolicy_from_global_level')['response']
    if principalstatus in (4, 2, 6):
        for item in global_policyset:
            if item['startOffset']['offsetValue'] == 5:
                return [{
                    "effectiveDate": effective_date_today,
                    "description": f"Effective On {effective_date_today['month']}/{effective_date_today['day']}/{effective_date_today['year']}",
                    "policySet": json.loads(json.dumps(
                        item['policySet'], ensure_ascii=False).replace(
                        '"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                        item['policySet']) else ''
                }]

    date_to_consider = dag_run_conf['service_date'].replace(
        '-', '/') if dag_run_conf['service_date'] else dag_run_conf['startdate']
    difference_datetime = datetime.now() - datetime.strptime(date_to_consider, '%m/%d/%Y')
    employee_tenure = int((difference_datetime.total_seconds() / 86400) / 365)
    assignmentcategory = dag_run_conf['assignment_category']
    if assignmentcategory == 'Regular, <Full Time':
        global_policyset2 = result(
            'get_defaultpolicy_from_global_level')['response']
        fulltimeavailability = float(dag_run_conf['full_time_availability'])
        accrual_0_years = fulltimeavailability * 0.01 * 120.12
        accrual_5_years = fulltimeavailability * 0.01 * 160.16
        if employee_tenure < 5:
            for i1, item1 in enumerate(global_policyset2):
                policy_set = item1['policySet']
                if item1['startOffset']['offsetValue'] == 0 and policy_set[
                        'timeOffBalanceEventScripts'][0]['script'][
                        'description'] == 'Accrues time once per week.':
                    existing_accrual_0_years = policy_set['timeOffBalanceEventScripts'][0]['additionalParameters']
                    for i2, item2 in enumerate(existing_accrual_0_years):
                        if item2['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                            global_policyset2[i1]['policySet']['timeOffBalanceEventScripts'][0]['additionalParameters'][i2] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_0_years
                                }
                            }
                if item1['startOffset']['offsetValue'] == 5 and policy_set[
                        'timeOffBalanceEventScripts'][0]['script'][
                        'description'] == 'Accrues time once per week.':
                    existing_accrual_0_years = policy_set['timeOffBalanceEventScripts'][0]['additionalParameters']
                    for i2, item2 in enumerate(existing_accrual_0_years):
                        if item2['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                            global_policyset2[i1]['policySet']['timeOffBalanceEventScripts'][0]['additionalParameters'][i2] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_5_years
                                }
                            }
            parsed_policyset = json.loads(json.dumps(
                global_policyset2, ensure_ascii=False).replace(
                '"null"', '"effectiveDate"').replace(
                    '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                global_policyset2) else ''
            policy_sets = []
            effective_date = get_replicon_date(dag_run_conf['rehiredate']) if dag_run_conf['isarehire'] == 'Yes' else \
                dag_run_conf['effectivedate_toconsider']
            for item in parsed_policyset:
                effective_date_consider = datetime.strptime(
                    f"{effective_date['month']}/{effective_date['day']}/{effective_date['year']}",
                    '%m/%d/%Y') if item['startOffset']['offsetValue'] == 0 else datetime.strptime(
                    dag_run_conf['service_date'].replace('-', '/'), '%m/%d/%Y')
                effective_date_consider_multiply_12_months = effective_date_consider + relativedelta(
                    months=+(item['startOffset']['offsetValue']*12))
                policy_sets.append({
                    'description': f"Effective On {effective_date_consider_multiply_12_months.day}/{effective_date_consider_multiply_12_months.month}/{effective_date_consider_multiply_12_months.year}",
                    'effectiveDate': {
                        'day': effective_date_consider_multiply_12_months.day,
                        'month': effective_date_consider_multiply_12_months.month,
                        'year': effective_date_consider_multiply_12_months.year
                    },
                    'policySet': item['policySet']
                })
            return policy_sets
        for i3, item3 in enumerate(global_policyset2):
            if item3['startOffset']['offsetValue'] == 5:
                existing_accrual_5_years2 = item3['policySet']['timeOffBalanceEventScripts']
                for i4, item4 in enumerate(existing_accrual_5_years2):
                    additional_params = item4['additionalParameters']
                    for i5, item5 in enumerate(additional_params):
                        if item5['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                            global_policyset2[i3]['policySet']['timeOffBalanceEventScripts'][i4]['additionalParameters'][i5] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_5_years
                                }
                            }
        parsed_policyset = json.loads(json.dumps(
            global_policyset2, ensure_ascii=False).replace(
            '"null"', '"effectiveDate"').replace(
                '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
            global_policyset2) else ''
        return [{
            "effectiveDate": effective_date_today,
            "description": f"Effective On {effective_date_today['month']}/{effective_date_today['day']}/{effective_date_today['year']}",
            "policySet": find_first_by_attr_and_get_attr(parsed_policyset, 'startOffset.offsetValue', 5, 'policySet', '')
        }]
    if employee_tenure < 5:
        parsed_policyset = json.loads(json.dumps(
            global_policyset, ensure_ascii=False).replace(
            '"null"', '"effectiveDate"').replace(
                '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
            global_policyset) else ''
        policy_sets = []
        effective_date = get_replicon_date(dag_run_conf['rehiredate']) if dag_run_conf['isarehire'] == 'Yes' else \
            dag_run_conf['effectivedate_toconsider']
        for item in parsed_policyset:
            effective_date_consider = datetime.strptime(
                f"{effective_date['month']}/{effective_date['day']}/{effective_date['year']}",
                '%m/%d/%Y') if item['startOffset']['offsetValue'] == 0 else datetime.strptime(
                dag_run_conf['service_date'].replace('-', '/'), '%m/%d/%Y')
            effective_date_consider_multiply_12_months = effective_date_consider + relativedelta(
                months=+(item['startOffset']['offsetValue']*12))
            policy_sets.append({
                'description': f"Effective On {effective_date_consider_multiply_12_months.day}/{effective_date_consider_multiply_12_months.month}/{effective_date_consider_multiply_12_months.year}",
                'effectiveDate': {
                    'day': effective_date_consider_multiply_12_months.day,
                    'month': effective_date_consider_multiply_12_months.month,
                    'year': effective_date_consider_multiply_12_months.year
                },
                'policySet': item['policySet']
            })
        return policy_sets

    for item6 in global_policyset:
        if item6['startOffset']['offsetValue'] == 5:
            parsed_policyset = json.loads(json.dumps(
                item6['policySet'], ensure_ascii=False).replace(
                '"null"', '"effectiveDate"').replace(
                    '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                item6['policySet']) else ''
            return [{
                "effectiveDate": effective_date_today,
                "description": f"Effective On {effective_date_today['month']}/{effective_date_today['day']}/{effective_date_today['year']}",
                "policySet": parsed_policyset if parsed_policyset else ''
            }]
    return ''


def write_employeetype_exception_log(dag_run):
    required_employeetype = result('get_required_employeetype')
    departmentgroupuri = dag_run.conf['departmentgroupuri']
    exception_message = []
    if not required_employeetype:
        exception_message.append(
            'User not created, since Employee type group doesn\'t exist in Replicon')
    if not departmentgroupuri:
        exception_message.append(
            'User not created, since Department (Employee Org Code) doesn\'t exist in Replicon')

    return {
        "loginname": dag_run.conf['employeenumber'],
        "uri": "NA",
        "action": "Add",
        "status": "Exception",
        "reason": ';'.join(exception_message) if exception_message else ''
    }


def get_exceptionmessage_adduser():
    dag_run_conf = get_dag_run_conf()
    exception_messages = []

    if result('get_required_policysets_to_assign', 'exception_message'):
        exception_messages.append(
            result('get_required_policysets_to_assign', 'exception_message'))
    if result('should_update_supervisor') == 'get_supervisor_useruri_status':
        if result('get_supervisor_useruri_status')['uri'] and result('get_supervisor_useruri_status')['status'] != 'true':
            exception_messages.append(
                'Supervisor not updated since the supervisor is disabled')
    if not dag_run_conf['supervisoremployeeid'] and 'Y' in dag_run_conf['supervisor_required']:
        exception_messages.append(
            'Supervisor not assigned, Spv_emp_number is blank')
    if result('get_required_locationname') and not result('get_required_locationname')['required_locationuri']:
        exception_messages.append(
            f"Location not updated, no location found for the code {dag_run_conf['employee_location_state']} in Replicon.")
    if not result('get_required_costcenter'):
        exception_messages.append(
            f"Assignment status not updated, no Assignment status found for - {dag_run_conf['assignment_status']} in Replicon.")
    if result('get_required_locationname') and not result('get_required_divisionname')['required_divisionuri']:
        exception_messages.append(
            f"Principal status not assigned, since principal status with the code {dag_run_conf['principalstatus']} is not available in Replicon.")
    if not result('get_required_servicecenter'):
        exception_messages.append(
            f"Assignment Category not assigned, since Assignment Category - {dag_run_conf['assignment_category']} is not available in Replicon.")
    return ', '.join(exception_messages) if exception_messages else ''


def write_adduser_log_props(dag_run):
    exception_message = get_exceptionmessage_adduser()

    return {
        "loginname": dag_run.conf['employeenumber'],
        "uri": result('create_user')['uri'],
        "action": "Add",
        "status": "Exception" if exception_message else "Success",
        "reason": exception_message
    }


def get_effective_date_derived():
    dag_run_conf = get_dag_run_conf()
    start_date = dag_run_conf['startdate']
    service_date = dag_run_conf['service_date']
    start_date_dt_obj = datetime.strptime(start_date, '%m/%d/%Y')
    service_date_dt_obj = datetime.strptime(service_date, '%m/%d/%Y')
    difference_datetime = start_date_dt_obj - service_date_dt_obj
    return get_replicon_date(service_date) if \
        int((difference_datetime.total_seconds() / 86400) / 365) < 0 else get_replicon_date(start_date)


# pylint:disable=too-many-statements, line-too-long
def get_final_paidtimeoff_policysets_adduser():

    dag_run_conf = get_dag_run_conf()
    principalstatus = int(dag_run_conf['principalstatus'])
    effective_date_today = get_today_date()

    global_policyset = result(
        'get_defaultpolicy_from_global_level')['response']
    if principalstatus in (4, 2, 6):
        for item in global_policyset:
            if item['startOffset']['offsetValue'] == 5:
                return [{
                    "effectiveDate": effective_date_today,
                    "description": f"Effective On {effective_date_today['month']}/{effective_date_today['day']}/{effective_date_today['year']}",
                    "policySet": json.loads(json.dumps(
                        item['policySet'], ensure_ascii=False).replace(
                        '"null"', '"effectiveDate"').replace(
                        '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                        item['policySet']) else ''
                }]

    date_to_consider = dag_run_conf['service_date'].replace(
        '-', '/') if dag_run_conf['service_date'] else dag_run_conf['startdate']
    difference_datetime = (
        datetime.strptime(dag_run_conf['startdate'], '%m/%d/%Y') - datetime.strptime(
            date_to_consider, '%m/%d/%Y'))
    employee_tenure = int((difference_datetime.total_seconds() / 86400) / 365)
    assignmentcategory = dag_run_conf['assignment_category']
    if assignmentcategory == 'Regular, <Full Time':
        global_policyset2 = result(
            'get_defaultpolicy_from_global_level')['response']
        fulltimeavailability = float(dag_run_conf['full_time_availability'])
        accrual_0_years = fulltimeavailability * 0.01 * 120.12
        accrual_5_years = fulltimeavailability * 0.01 * 160.16
        if employee_tenure < 5:
            for i1, item1 in enumerate(global_policyset2):
                policy_set = item1['policySet']
                if item1['startOffset']['offsetValue'] == 0 and policy_set[
                        'timeOffBalanceEventScripts'][0]['script'][
                        'description'] == 'Accrues time once per week.':
                    existing_accrual_0_years = policy_set['timeOffBalanceEventScripts'][0]['additionalParameters']
                    for i2, item2 in enumerate(existing_accrual_0_years):
                        if item2['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                            global_policyset2[i1]['policySet']['timeOffBalanceEventScripts'][0]['additionalParameters'][i2] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_0_years
                                }
                            }
                if item1['startOffset']['offsetValue'] == 5 and policy_set[
                        'timeOffBalanceEventScripts'][0]['script'][
                        'description'] == 'Accrues time once per week.':
                    existing_accrual_0_years = policy_set['timeOffBalanceEventScripts'][0]['additionalParameters']
                    for i2, item2 in enumerate(existing_accrual_0_years):
                        if item2['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                            global_policyset2[i1]['policySet']['timeOffBalanceEventScripts'][0]['additionalParameters'][i2] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_5_years
                                }
                            }
            parsed_policyset = json.loads(json.dumps(
                global_policyset2, ensure_ascii=False).replace(
                '"null"', '"effectiveDate"').replace(
                    '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                global_policyset2) else ''
            policy_sets = []
            effective_date = result('derive_effective_date')
            for item in parsed_policyset:
                effective_date_consider = datetime.strptime(
                    f"{effective_date['month']}/{effective_date['day']}/{effective_date['year']}",
                    '%m/%d/%Y') if item['startOffset']['offsetValue'] == 0 else datetime.strptime(
                    dag_run_conf['service_date'].replace('-', '/'), '%m/%d/%Y')
                effective_date_consider_multiply_12_months = effective_date_consider + relativedelta(
                    months=+(item['startOffset']['offsetValue']*12))
                policy_sets.append({
                    'description': f"Effective On {effective_date_consider_multiply_12_months.day}/{effective_date_consider_multiply_12_months.month}/{effective_date_consider_multiply_12_months.year}",
                    'effectiveDate': {
                        'day': effective_date_consider_multiply_12_months.day,
                        'month': effective_date_consider_multiply_12_months.month,
                        'year': effective_date_consider_multiply_12_months.year
                    },
                    'policySet': item['policySet']
                })
            return policy_sets
        for i3, item3 in enumerate(global_policyset2):
            if item3['startOffset']['offsetValue'] == 5:
                existing_accrual_5_years2 = item3['policySet']['timeOffBalanceEventScripts']
                for i4, item4 in enumerate(existing_accrual_5_years2):
                    additional_params = item4['additionalParameters']
                    for i5, item5 in enumerate(additional_params):
                        if item5['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount':
                            global_policyset2[i3]['policySet']['timeOffBalanceEventScripts'][i4]['additionalParameters'][i5] = {
                                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                "value": {
                                    "number": accrual_5_years
                                }
                            }
        parsed_policyset = json.loads(json.dumps(
            global_policyset2, ensure_ascii=False).replace(
            '"null"', '"effectiveDate"').replace(
                '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
            global_policyset2) else ''
        return [{
            "effectiveDate": effective_date_today,
            "description": f"Effective On {effective_date_today['month']}/{effective_date_today['day']}/{effective_date_today['year']}",
            "policySet": find_first_by_attr_and_get_attr(parsed_policyset, 'startOffset.offsetValue', 5, 'policySet', '')
        }]

    if employee_tenure < 5:
        parsed_policyset = json.loads(json.dumps(
            global_policyset, ensure_ascii=False).replace(
            '"null"', '"effectiveDate"').replace(
                '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
            global_policyset) else ''
        policy_sets = []
        effective_date = result('derive_effective_date')
        for item in parsed_policyset:
            effective_date_consider = datetime.strptime(
                f"{effective_date['month']}/{effective_date['day']}/{effective_date['year']}",
                '%m/%d/%Y') if item['startOffset']['offsetValue'] == 0 else datetime.strptime(
                dag_run_conf['service_date'].replace('-', '/'), '%m/%d/%Y')
            effective_date_consider_multiply_12_months = effective_date_consider + relativedelta(
                months=+(item['startOffset']['offsetValue']*12))
            policy_sets.append({
                'description': f"Effective On {effective_date_consider_multiply_12_months.day}/{effective_date_consider_multiply_12_months.month}/{effective_date_consider_multiply_12_months.year}",
                'effectiveDate': {
                    'day': effective_date_consider_multiply_12_months.day,
                    'month': effective_date_consider_multiply_12_months.month,
                    'year': effective_date_consider_multiply_12_months.year
                },
                'policySet': item['policySet']
            })
        return policy_sets

    for item6 in global_policyset:
        if item6['startOffset']['offsetValue'] == 5:
            parsed_policyset = json.loads(json.dumps(
                item6['policySet'], ensure_ascii=False).replace(
                '"null"', '"effectiveDate"').replace(
                    '"script"', '"scriptTarget"')) if 'urn' in json.dumps(
                item6['policySet']) else ''
            return [{
                "effectiveDate": effective_date_today,
                "description": f"Effective On {effective_date_today['month']}/{effective_date_today['day']}/{effective_date_today['year']}",
                "policySet": parsed_policyset if parsed_policyset else ''
            }]
    return ''


def final_policy_to_assign_add():

    default_timeofftype_policy = result(
        'get_default_timeofftype_policyschedule_user')['response']
    effective_date = result('derive_effective_date')
    if effective_date['month'] < 9 and datetime.now().date() > datetime(effective_date['year'], 1, 1).date():
        for i1, item1 in enumerate(default_timeofftype_policy):
            timeoff_balanceevent_scripts = item1['policySet']['timeOffBalanceEventScripts']
            for i2, item2 in enumerate(timeoff_balanceevent_scripts):
                for i3, item3 in enumerate(item2['additionalParameters']):
                    if item3['keyUri'] == 'urn:replicon:script-key:parameter:amount' and item3['value']['number'] == 0:
                        default_timeofftype_policy[i1]['policySet'][
                            'timeOffBalanceEventScripts'][i2][
                                'additionalParameters'][i3] = {
                            "keyUri": "urn:replicon:script-key:parameter:amount",
                            "value": {
                                "number": 1
                            }
                        }
    return json.loads(json.dumps(
        [x['policySet'] for x in default_timeofftype_policy], ensure_ascii=False).replace(
            '"null"', '"effectiveDate"').replace(
            '"script"', '"scriptTarget"'))[0] if 'urn' in json.dumps(
        [x['policySet'] for x in default_timeofftype_policy]) else ''
