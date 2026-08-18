from datetime import datetime, timezone
import re
from rail import get_current_context, result, find_first_by_attr_and_get_attr, load_all_records, set_result, smartjoin_by_delim


null = None
email_regex = re.compile(
    r'^([a-zA-Z0-9_.+-])+\@(([a-zA-Z0-9-])+\.)+([a-zA-Z0-9]{2,4})+$')


def get_dag_run_conf():
    return get_current_context()['dag_run'].conf


def get_datetime_obj(effectiveDate):
    year = effectiveDate['year']
    month = effectiveDate['month']
    day = effectiveDate['day']
    return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')


def get_query_for_raw_group(group):
    if group == 'costcenter':
        return """SELECT costcentername AS costcenter, costcentercode FROM inputfile
        WHERE NULLIF(costcentername, '') IS NOT NULL"""
    if group == 'department':
        return """SELECT departmentgroup, code1, code2, code3, code4 FROM inputfile
        WHERE NULLIF(departmentgroup, '') IS NOT NULL"""
    if group == 'servicecenter':
        return """SELECT servicecenter FROM inputfile
        WHERE NULLIF(servicecenter, '') IS NOT NULL"""
    if group == 'location':
        return """SELECT location FROM inputfile
        WHERE NULLIF(location, '') IS NOT NULL"""
    return """SELECT legalentityname AS division, legalentityid AS code FROM inputfile
        WHERE NULLIF(legalentityname, '') IS NOT NULL"""


def get_query_for_group_to_create(group):
    if group == 'costcenter':
        return """SELECT DISTINCT costcenter, costcentercode FROM costcenterrawdata
            WHERE LOWER(costcenter) NOT IN (SELECT DISTINCT LOWER(fullpath) FROM costcenterdata)
            AND costcenter IS NOT NULL"""
    if group == 'department':
        return """SELECT DISTINCT departmentgroup, code1, code2, code3, code4 FROM departmentrawdata
        WHERE LOWER(departmentgroup) NOT IN (SELECT DISTINCT LOWER(fullpath) FROM departmentdata)
        AND departmentgroup IS NOT NULL"""
    if group == 'servicecenter':
        return """SELECT DISTINCT servicecenter FROM servicecenterrawdata
        WHERE LOWER(servicecenter) NOT IN (SELECT DISTINCT LOWER(fullpath) FROM servicecenterdata)
        AND servicecenter IS NOT NULL"""
    if group == 'location':
        return """SELECT DISTINCT location FROM locationrawdata
        WHERE LOWER(location) NOT IN (SELECT DISTINCT LOWER(fullpath) FROM locationdata)
        AND location IS NOT NULL"""
    return """SELECT DISTINCT division, code FROM divisionrawdata
        WHERE LOWER(division) NOT IN (SELECT DISTINCT LOWER(fullpath) FROM divisiondata)
        AND division IS NOT NULL"""


def get_department_params(departmentgroup, code1, code2, code3, code4):
    child_departments = departmentgroup.replace(
        '|MPC - Advertising|MikrosMPC', '|MikrosMPC') if '|MPC - Advertising|MikrosMPC' in departmentgroup else departmentgroup.replace('Technicolor|', '')
    return {
        'required_level': len(child_departments.split('|')),
        'codes_to_be_added': [code1, code2, code3, code4],
        'required_name': child_departments,
        'required_name_array': departmentgroup.replace('Technicolor|', '').split('|')
    }


def do_filter_departmentlog(log):
    dag_run_conf = get_dag_run_conf()
    return log['properties']['fullpath'] == dag_run_conf[
        'required_department_fullpath'] and log['properties']['type'] == 'department'


def do_filter_parent_departmentlog(log):
    dag_run_conf = get_dag_run_conf()
    return log['properties']['fullpath'] == dag_run_conf[
        'parent_department_fullpath'] and log['properties']['type'] == 'department'


def get_downstreamtasks_error(error_message):
    return {
        'error': error_message
    }


def get_servicecenter_or_location_params(service_center):
    return {
        'required_level': len(service_center.split('|'))
    }


def get_group_parameters(group):
    dag_run_conf = get_dag_run_conf()
    if group == 'department':
        return get_department_params(dag_run_conf['departmentgroup'],
                                     dag_run_conf['code1'], dag_run_conf['code2'],
                                     dag_run_conf['code3'], dag_run_conf['code4'])
    if group == 'servicecenter':
        return get_servicecenter_or_location_params(dag_run_conf['servicecenter'])

    return get_servicecenter_or_location_params(dag_run_conf['location'])


def get_groupuri_from_mapper(filtered_log):
    parent_group_log_entries = load_all_records(filtered_log)
    parent_groupuri = parent_group_log_entries[
        0]['properties']['uri'] if parent_group_log_entries else ''

    return parent_groupuri


def get_group_error_message(gather_costcenter_error, gather_department_error, gather_servicecenter_error, gather_location_error, gather_division_error):
    error_message = []

    if result(gather_costcenter_error):
        error_message.append('Error creating cost center')
    if result(gather_department_error):
        error_message.append('Error creating business group (department)')
    if result(gather_location_error):
        error_message.append('Error creating location')
    if result(gather_servicecenter_error):
        error_message.append('Error creating reference job (servicecenter)')
    if result(gather_division_error):
        error_message.append('Error creating legal entity (division)')
    return ';'.join(error_message) if error_message else ''


def write_skipped_user(item):

    def get_exception_message(globalid, employeestatus):
        message = []
        if not globalid:
            message.append('Global ID is not present')
        if not employeestatus:
            message.append('Employee status is not present')
        elif employeestatus.lower() != 'active':
            message.append('Employee status not set as active')
        return ','.join(message) if message else ''

    return {
        'globalid': item['globalid'],
        'action': 'Validation',
        'status': 'Skipped',
        'details': get_exception_message(item['globalid'], item['employeestatus']),
        'username': f"{item['firstname']} {item['lastname']}",
        'new_location': 'No',
        'location': ''
    }


def get_archive_file_name(archive_disableuser_file, send_disableuser_exception_email):

    if archive_disableuser_file == 'success' or send_disableuser_exception_email == 'success':
        return f"Skipped_{datetime.now(timezone.utc).strftime('%d%m%Y%H%M%S')}"

    return datetime.now(timezone.utc).strftime('%d%m%Y%H%M%S')


def addlogs_to_error(mandatoryfield_config):
    errors = []
    for field_name in mandatoryfield_config:
        custom_message = mandatoryfield_config[field_name]
        if custom_message:
            errors.append(custom_message)
    return errors


def get_adduser_field_exception(dag_run):

    error_log = {
        'error': '',
        'location': ''
    }
    dag_run_conf = dag_run.conf
    mandatoryfield_config = {
        'firstname': 'First name is not present' if not dag_run_conf['firstname'] else False,
        'lastname': 'Last name is not present' if not dag_run_conf['lastname'] else False,
        'country': 'Country is not present' if not dag_run_conf['country'] else False,
        'worklocation': 'Work location is not present' if not dag_run_conf['worklocation'] else False,
        'email': 'Email id is not present' if not dag_run_conf['email']
        else 'Email id is not valid' if dag_run_conf['email'] and not re.fullmatch(
            email_regex, dag_run_conf['email']) else False
    }

    errors = addlogs_to_error(mandatoryfield_config)
    if errors:
        error_log['error'] = ', '.join(errors)
        return error_log

    error_log['error'] = f"Creative/Non Creative is set as {dag_run_conf['creativenoncreative']}" if (
        dag_run_conf['creativenoncreative'] and dag_run_conf['creativenoncreative'] not in (
            'Non Creative', 'Creative')) else ''

    if not error_log['error']:

        error_log['error'] = f"User {dag_run_conf['username']} has not been created as requested" if dag_run_conf[
            'globalid'] == 225955 else ''

        error_log['location'] = dag_run_conf['location']

    return error_log


def get_mandatoryfield_skipped_log():

    dag_run_conf = get_dag_run_conf()
    mandatoryfield_skipped_config = {
        'firstname': 'Employee First Name not present' if not dag_run_conf['firstname'] else False,
        'lastname': 'Employee Last Name not present' if not dag_run_conf['lastname'] else False,
        'country': 'Employee email not present' if not dag_run_conf['country'] else False,
        'worklocation': 'Work location is not present' if not dag_run_conf['worklocation'] else False,
        'department': 'Department UDF value not present' if not dag_run_conf['department'] else False,
        'fte': 'FTE UDF value not present' if not dag_run_conf['fte'] else False
    }

    errors = addlogs_to_error(mandatoryfield_skipped_config)
    return ';'.join(errors) if errors else ''


def get_usermapper_entries(user_master_mapper, country, worklocation, businessunitname):

    if country:
        if worklocation:
            return [x for x in user_master_mapper if x['country'] == country and x['identifier1(worklocation)'] == worklocation]

        return [x for x in user_master_mapper if x['country'] == country]

    return [x for x in user_master_mapper if x['identifier1(worklocation)'] == businessunitname]


def compose_supervisor_details(manager_id, is_update_user, user_uri):
    supervisor = list(filter(lambda x: x['employeeid'] == manager_id, result(
        'get_data_for_supervisor'))) if result('get_data_for_supervisor') else []

    return {
        'loginname': supervisor[0]['loginname'] if supervisor else '',
        'name': supervisor[0]['name'] if supervisor else '',
        'uri': supervisor[0]['uri'] if supervisor else '',
        'status': supervisor[0]['status'].lower() if supervisor else '',
        'userdetails_uri': user_uri if is_update_user else result(
            'createuser_in_replicon')['uri']
    }


def get_number(val):
    number = null
    if val.isdigit():
        return int(val)
    try:
        number = float(val)
    except ValueError:
        number = null
    return number


def get_customfields_to_adduser():
    dag_run_conf = get_dag_run_conf()
    useruri = result('createuser_in_replicon')['uri']

    dropdown_udf_payloads = []
    if dag_run_conf['referencejobcode_uri'] and dag_run_conf['referencejobcodevalue_uri']:
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': dag_run_conf['referencejobcode_uri'],
            'customFieldDropDownOptionUri': dag_run_conf['referencejobcodevalue_uri']
        })
    if dag_run_conf['referencejobtitle_uri'] and dag_run_conf['referencejobtitlevalue_uri']:
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': dag_run_conf['referencejobtitle_uri'],
            'customFieldDropDownOptionUri': dag_run_conf['referencejobtitlevalue_uri']
        })
    if dag_run_conf['jobcategoryudf_uri'] and dag_run_conf['jobcategory_uri']:
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': dag_run_conf['jobcategoryudf_uri'],
            'customFieldDropDownOptionUri': dag_run_conf['jobcategory_uri']
        })

    numeric_udf_payloads = []
    standardweeklyhours = get_number(
        dag_run_conf['standardweeklyhours']) if dag_run_conf['standardweeklyhours'] else null
    if dag_run_conf['standardweeklyhours_uri'] and standardweeklyhours:
        numeric_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': dag_run_conf['standardweeklyhours_uri'],
            'value': standardweeklyhours
        })
    fte = get_number(dag_run_conf['fte']) if dag_run_conf['fte'] else null
    if fte and dag_run_conf['fte_uri']:
        numeric_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': dag_run_conf['fte_uri'],
            'value': fte
        })

    return {
        'dropdownudf_payloads': dropdown_udf_payloads,
        'numeric_udf_payloads': numeric_udf_payloads
    }


def get_adduser_exception_logs(should_update_supervisor, is_single_supervisor):
    exception_messages = []

    dag_run_conf = get_dag_run_conf()

    success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == 'success',
                                                         get_current_context()['dag_run'].get_task_instances())))

    if should_update_supervisor in success_tasks and result(should_update_supervisor) == 'finish_supervisor_assignment':
        exception_messages.append(
            'Supervisor not assigned since the user and manager IDs are same')

    if is_single_supervisor in success_tasks and result(is_single_supervisor) == 'finish_supervisor_assignment':
        manager_id = dag_run_conf['manager_id']
        exception_messages.append(
            f'Supervisor is not assigned as multiple users have the same employee id as "{manager_id}" in Replicon')

    if dag_run_conf['standardweeklyhours'] and not dag_run_conf['standardweeklyhours_uri']:
        exception_messages.append('Standard Weekly Hours udf is not available')

    if not dag_run_conf['fte_uri']:
        exception_messages.append('FTE udf is not available')

    return ','.join(exception_messages) if exception_messages else ''


def get_timeoff_types_to_assign(get_all_timeofftypes, get_timeoff_from_mapper):

    timeoff_types_to_assign = list(map(lambda x: {
        'name': x['value'],
        'uri': find_first_by_attr_and_get_attr(result(get_all_timeofftypes), 'name', x['value'].strip(), 'uri')
    }, result(get_timeoff_from_mapper)))

    return list({x['uri'] for x in timeoff_types_to_assign if bool(x['uri'])}) if timeoff_types_to_assign else ''


def get_dropdowns_to_updateuser(dag_run_conf, customfield_values, useruri):

    dropdown_udf_payloads = []
    job_category = dag_run_conf['jobcategory']
    referencejobcode = dag_run_conf['referencejobcode']
    referencejobtitle = dag_run_conf['jobtitle']
    department = dag_run_conf['department']

    job_category_values = [
        x['text'] for x in customfield_values if x['customField']['displayText'] == 'Job Category' and x['text']]
    user_job_category = ''.join(
        job_category_values) if job_category_values else ''
    if job_category and job_category.lower() != user_job_category.lower():
        jobcategoryudf_uri = dag_run_conf['jobcategoryudf_uri']
        jobcategory_uri = dag_run_conf['jobcategory_uri']
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': jobcategoryudf_uri,
            'customFieldDropDownOptionUri': jobcategory_uri
        })
        set_result('yes', 'jobcategorydropdownupdated')
        set_result('yes', 'timeoffapprovalpathchange')
        set_result('yes', 'punchentrychange')
        set_result('yes', 'timesheetchange')

    referencejob_code_values = [
        x['text'] for x in customfield_values if x['customField']['displayText'] == 'Reference Job code' and x['text']]
    user_referencejob_code = ''.join(
        referencejob_code_values) if referencejob_code_values else ''
    if referencejobcode and referencejobcode.lower() != user_referencejob_code.lower():
        referencejobcode_uri = dag_run_conf['referencejobcode_uri']
        referencejobcodevalue_uri = dag_run_conf['referencejobcodevalue_uri']
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': referencejobcode_uri,
            'customFieldDropDownOptionUri': referencejobcodevalue_uri
        })
        set_result('yes', 'referencejobcodedropdownupdated')

    referencejob_title_values = [
        x['text'] for x in customfield_values if x['customField']['displayText'] == 'Reference Job Title' and x['text']]
    user_referencejob_title = ''.join(
        referencejob_title_values) if referencejob_title_values else ''
    if referencejobtitle and referencejobtitle.lower() != user_referencejob_title.lower():
        referencejobtitle_uri = dag_run_conf['referencejobtitle_uri']
        referencejobtitlevalue_uri = dag_run_conf['referencejobtitlevalue_uri']
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': referencejobtitle_uri,
            'customFieldDropDownOptionUri': referencejobtitlevalue_uri
        })
        set_result('yes', 'referencejobtitledropdownupdated')

    department_values = [
        x['text'] for x in customfield_values if x['customField']['displayText'] == 'Department' and x['text']]
    user_department = ''.join(department_values) if department_values else ''
    if department and department.lower() != user_department.lower():
        departmentudf_uri = dag_run_conf['departmentudf_uri']
        departmentvalue_uri = dag_run_conf['departmentvalue_uri']
        dropdown_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': departmentudf_uri,
            'customFieldDropDownOptionUri': departmentvalue_uri
        })

        set_result('yes', 'departmentdropdownupdated')

    return dropdown_udf_payloads


def get_numericvalues_to_updateuser(dag_run_conf, customfield_values, useruri):

    numeric_udf_payloads = []
    standard_weeklyhours = get_number(
        dag_run_conf['standardweeklyhours']) if dag_run_conf['standardweeklyhours'] else null
    fte = get_number(dag_run_conf['fte']) if dag_run_conf['fte'] else null

    standardweeklyhours_values = [
        x['number'] for x in customfield_values if x['customField']['displayText'] == 'Standard Weekly Hours' and (x.get('number') or x.get('number') == 0)]
    user_standardweeklyhours = standardweeklyhours_values[0] if standardweeklyhours_values else null
    if isinstance(standard_weeklyhours, (float, int)) and standard_weeklyhours != user_standardweeklyhours:
        standardweeklyhours_uri = dag_run_conf['standardweeklyhours_uri']
        numeric_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': standardweeklyhours_uri,
            'value': standard_weeklyhours
        })
        set_result('yes', 'standardweeklyhoursupdated')

    fte_values = [x['number']
                  for x in customfield_values if x['customField']['displayText'] == 'FTE' and (x.get('number') or x.get('number') == 0)]
    user_fte = fte_values[0] if fte_values else null
    if isinstance(fte, int) and fte != user_fte:
        fte_uri = dag_run_conf['fte_uri']
        numeric_udf_payloads.append({
            'objectUri': useruri,
            'customFieldUri': fte_uri,
            'value': fte
        })
        set_result('yes', 'fteupdated')

    return numeric_udf_payloads


def get_customfields_to_updateuser(dag_run):

    dag_run_conf = dag_run.conf
    useruri = dag_run_conf['useruri']
    customfield_values = result('bulk_getuser3')[
        'userDetails']['customFieldValues']

    dropdown_udf_payloads = get_dropdowns_to_updateuser(
        dag_run_conf, customfield_values, useruri)

    numeric_udf_payloads = get_numericvalues_to_updateuser(
        dag_run_conf, customfield_values, useruri)

    return {
        'dropdownudf_payloads': dropdown_udf_payloads,
        'numeric_udf_payloads': numeric_udf_payloads
    }


def get_final_timeoff_types_update():

    return list(filter(lambda y: y['status'] == 'No', map(lambda x: {
        'name': find_first_by_attr_and_get_attr(result('get_all_timeofftypes'), 'uri', x, 'name'),
        'enabled': bool(find_first_by_attr_and_get_attr(result('get_user_timeoff_policy_summary'), 'uri', x, 'enabled')),
        'uri': x,
        'status': 'Yes' if find_first_by_attr_and_get_attr(result('get_user_timeoff_policy_summary'), 'uri', x, 'name') else 'No'
    }, result('timeoff_types_to_assign'))))


def get_supervisor_exception_message(should_update_supervisor, is_supervisor_present, is_single_supervisor, is_supervisor_disabled, supervisorloginname):

    exception_messages = []
    exception_task = 'get_supervisor_exceptions_error'

    if result(should_update_supervisor) and result(should_update_supervisor) == exception_task:
        exception_messages.append(
            'Supervisor is not assigned/updated as the "Login name" for user and supervisor is same on the input file')

    if result(is_supervisor_disabled) and result(is_supervisor_disabled) == exception_task:
        exception_messages.append(
            f'Supervisor assignment/update is not done as "{supervisorloginname}" is disabled')

    if result('catch_and_log_error'):
        exception_messages.append(result('catch_and_log_error'))

    if result(is_supervisor_present) and result(is_supervisor_present) == exception_task:
        exception_messages.append(
            f'Supervisor is not assigned/updated as "{supervisorloginname}" is not available in Replicon')

    if result(is_single_supervisor) and result(is_single_supervisor) == exception_task:
        exception_messages.append(
            f'Supervisor is not assigned/updated as multiple users have the same employee id as "{supervisorloginname}" in Replicon')

    return {
        'exception_message': ';'.join(exception_messages) if exception_messages else '',
        'error_message': result('catch_and_log_error') if result('catch_and_log_error') else ''
    }


def get_employeetype_name_list():

    current_employeetype_name = null
    employee_type_schedule = []
    employee_typelist = []

    user_employee_type_schedule = result('bulk_getuser3')[
        'employeeTypeGroupSchedule']
    employee_startdate = result('bulk_getuser3')[
        'userDetails']['employmentDateRange']['startDate']

    if user_employee_type_schedule:
        for employeetype in user_employee_type_schedule:
            uri = employeetype['employeeTypeGroup']['uri']
            name = employeetype['employeeTypeGroup']['displayText']
            if not (employeetype.get('effectiveDate') and employeetype['effectiveDate'].get('day')):
                employee_startdatetime = get_datetime_obj(employee_startdate)
                employee_type_schedule.append({
                    'uri': uri,
                    'effective_date': employee_startdatetime,
                    'name': name
                })

                employee_typelist.append({
                    'employeeTypeGroup': {
                        'uri': uri
                    }
                })

            else:
                effective_date = get_datetime_obj(
                    employeetype['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    employee_type_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    employee_typelist.append({
                        'employeeTypeGroup': {
                            'uri': uri
                        },
                        'effectiveDate': employeetype['effectiveDate']
                    })
    if employee_type_schedule:
        max_effective_date = max(x['effective_date']
                                 for x in employee_type_schedule)
        current_employeetype_list = list(
            filter(lambda x: x['effective_date'].date() == max_effective_date.date(), employee_type_schedule))
        current_employeetype_name = smartjoin_by_delim(
            [x['name'] for x in current_employeetype_list])

    return {
        'employee_typelist': employee_typelist,
        'current_employeetype_name': current_employeetype_name
    }


def update_timesheet_vars_and_get_uri(timesheetapprovalpathchange, timesheetperiodchange, creativenoncreative):
    set_result('yes', timesheetapprovalpathchange)
    set_result('yes', timesheetperiodchange)

    timesheet_template = null
    timesheet_template_uri = null

    get_mapper_entries_from_businessunitname = result(
        'get_mapper_entries_from_businessunitname')
    get_mapper_entries_from_country_location = result(
        'get_mapper_entries_from_country_location')

    if get_mapper_entries_from_businessunitname:
        timesheet_template = find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Timesheet Template', 'value')
    else:
        timesheet_template_entries = [x['value'] for x in get_mapper_entries_from_country_location if x['type'] ==
                                      'Timesheet Template' and x['identifier2(employeetype_businessunit_type)'] ==
                                      creativenoncreative] if get_mapper_entries_from_country_location else null
        timesheet_template = timesheet_template_entries[0] if timesheet_template_entries else null

    if timesheet_template and timesheet_template != result('bulk_getuser3')['timesheetTemplate']['name']:
        timesheet_template_uri = find_first_by_attr_and_get_attr(
            result('get_all_policysets'), 'name', timesheet_template, 'uri')

    return {
        'name': timesheet_template,
        'uri': timesheet_template_uri
    }


def get_timesheetperiod_name_list(creativenoncreative):

    required_timesheetperiod_name = null
    current_timesheetperiod_name = null
    timesheet_periodlist = []

    get_mapper_entries_from_businessunitname = result(
        'get_mapper_entries_from_businessunitname')
    get_mapper_entries_from_country = result(
        'get_mapper_entries_from_country')

    if get_mapper_entries_from_businessunitname:
        required_timesheetperiod_name = find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Timesheet Period', 'value')
    else:
        timesheet_period_entries = [x['value'] for x in get_mapper_entries_from_country if x['type'] ==
                                    'Timesheet Period' and x['identifier2(employeetype_businessunit_type)'] ==
                                    creativenoncreative] if get_mapper_entries_from_country else null
        required_timesheetperiod_name = timesheet_period_entries[
            0] if timesheet_period_entries else null

    if required_timesheetperiod_name:

        timesheet_period_schedule = []

        user_timesheet_period_schedule = result('bulk_getuser3')[
            'timesheetPeriodSchedule']

        if user_timesheet_period_schedule:
            for timesheetperiod in user_timesheet_period_schedule:
                uri = timesheetperiod['timesheetPeriod']['uri']
                name = timesheetperiod['timesheetPeriod']['displayText']
                effective_date = get_datetime_obj(
                    timesheetperiod['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    timesheet_period_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    timesheet_periodlist.append({
                        'timesheetPeriod': {
                            'uri': uri
                        },
                        'effectiveDate': timesheetperiod['effectiveDate']
                    })
        if timesheet_period_schedule:
            max_effective_date = max(x['effective_date']
                                     for x in timesheet_period_schedule)
            current_timesheetperiod_list = list(
                filter(lambda x: x['effective_date'].date() == max_effective_date.date(), timesheet_period_schedule))
            current_timesheetperiod_name = smartjoin_by_delim(
                [x['name'] for x in current_timesheetperiod_list])

    return {
        'required_timesheetperiod_name': required_timesheetperiod_name,
        'timesheet_periodlist': timesheet_periodlist,
        'current_timesheetperiod_name': current_timesheetperiod_name
    }


def get_servicecenter_name_list():

    service_center_schedule = []
    service_centerlist = []
    current_servicecenter_name = null
    current_servicecenter_uri = null

    user_service_center_schedule = result('bulk_getuser3')[
        'serviceCenterSchedule']

    employee_startdate = result('bulk_getuser3')[
        'userDetails']['employmentDateRange']['startDate']

    if user_service_center_schedule:
        for servicecenter in user_service_center_schedule:
            uri = servicecenter['serviceCenter']['uri']
            name = servicecenter['serviceCenter']['displayText']
            if not (servicecenter.get('effectiveDate') and servicecenter['effectiveDate'].get('day')):
                employee_startdatetime = get_datetime_obj(employee_startdate)
                service_center_schedule.append({
                    'uri': uri,
                    'effective_date': employee_startdatetime,
                    'name': name
                })

                service_centerlist.append({
                    'serviceCenter': {
                        'uri': uri
                    }
                })

            else:
                effective_date = get_datetime_obj(
                    servicecenter['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    service_center_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    service_centerlist.append({
                        'serviceCenter': {
                            'uri': uri
                        },
                        'effectiveDate': servicecenter['effectiveDate']
                    })
    if service_center_schedule:
        max_effective_date = max(x['effective_date']
                                 for x in service_center_schedule)
        current_servicecenter_list = list(
            filter(lambda x: x['effective_date'].date() == max_effective_date.date(), service_center_schedule))
        current_servicecenter_name = smartjoin_by_delim(
            [x['name'] for x in current_servicecenter_list]).lower()
        current_servicecenter_uri = smartjoin_by_delim(
            [x['uri'] for x in current_servicecenter_list]).lower()

    return {
        'service_centerlist': service_centerlist,
        'current_servicecenter_name': current_servicecenter_name,
        'current_servicecenter_uri': current_servicecenter_uri
    }


def get_department_name_list():

    department_schedule = []
    departmentlist = []
    current_department_name = null
    current_department_uri = null

    user_department_group_schedule = result('bulk_getuser3')[
        'departmentGroupSchedule']

    employee_startdate = result('bulk_getuser3')[
        'userDetails']['employmentDateRange']['startDate']

    if user_department_group_schedule:
        for departmentgroup in user_department_group_schedule:
            uri = departmentgroup['departmentGroup']['uri']
            name = departmentgroup['departmentGroup']['displayText']
            if not (departmentgroup.get('effectiveDate') and departmentgroup['effectiveDate'].get('day')):
                employee_startdatetime = get_datetime_obj(employee_startdate)
                department_schedule.append({
                    'uri': uri,
                    'effective_date': employee_startdatetime,
                    'name': name
                })

                departmentlist.append({
                    'departmentGroup': {
                        'uri': uri
                    }
                })

            else:
                effective_date = get_datetime_obj(
                    departmentgroup['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    department_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    departmentlist.append({
                        'departmentGroup': {
                            'uri': uri
                        },
                        'effectiveDate': departmentgroup['effectiveDate']
                    })
    if department_schedule:
        max_effective_date = max(x['effective_date']
                                 for x in department_schedule)
        current_department_list = list(
            filter(lambda x: x['effective_date'].date() == max_effective_date.date(), department_schedule))
        current_department_name = smartjoin_by_delim(
            [x['name'] for x in current_department_list]).lower()
        current_department_uri = smartjoin_by_delim(
            [x['uri'] for x in current_department_list]).lower()

    return {
        'departmentlist': departmentlist,
        'current_department_name': current_department_name,
        'current_department_uri': current_department_uri
    }


def get_location_name_list():

    location_schedule = []
    locationlist = []

    current_location_name = null
    current_location_uri = null

    user_location_schedule = result('bulk_getuser3')[
        'locationSchedule']

    employee_startdate = result('bulk_getuser3')[
        'userDetails']['employmentDateRange']['startDate']

    if user_location_schedule:
        for location in user_location_schedule:
            uri = location['location']['uri']
            name = location['location']['displayText']
            if not (location.get('effectiveDate') and location['effectiveDate'].get('day')):
                employee_startdatetime = get_datetime_obj(employee_startdate)
                location_schedule.append({
                    'uri': uri,
                    'effective_date': employee_startdatetime,
                    'name': name
                })

                locationlist.append({
                    'location': {
                        'uri': uri
                    }
                })

            else:
                effective_date = get_datetime_obj(location['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    location_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    locationlist.append({
                        'location': {
                            'uri': uri
                        },
                        'effectiveDate': location['effectiveDate']
                    })
    if location_schedule:
        max_effective_date = max(x['effective_date']
                                 for x in location_schedule)
        current_location_list = list(
            filter(lambda x: x['effective_date'].date() == max_effective_date.date(), location_schedule))
        current_location_name = smartjoin_by_delim(
            [x['name'] for x in current_location_list]).lower()
        current_location_uri = smartjoin_by_delim(
            [x['uri'] for x in current_location_list])

    return {
        'locationlist': locationlist,
        'current_location_name': current_location_name,
        'current_location_uri': current_location_uri
    }


def get_timezoneuri_to_update(work_location):

    timezone_entries = [x['defaulturi'] for x in result(
        'get_mapper_entries_from_country') if
        x['type'] == 'Time Zone' and x[
        'identifier1(worklocation)'] == work_location]

    return timezone_entries[0] if timezone_entries else ''


def get_punchentry_policy(jobcategory):
    punch_entries_mapper = [x['value'] for x in result(
        'get_mapper_entries_from_country_location') if x['type'] == 'Punch Entry Policy' and x[
            'identifier2(employeetype_businessunit_type)'] == jobcategory]
    return punch_entries_mapper[0] if punch_entries_mapper else ''


def get_timeoff_approvalpath(creativenoncreative, businessunitname):
    timeoff_approvalpath_mapper = [x['value'] for x in result(
        'get_mapper_entries_from_country_location') if x['type'] == 'Timeoff Approval Path' and x[
            'identifier2(employeetype_businessunit_type)'] == creativenoncreative and x[
                'identifier3(department)'] == businessunitname]
    return timeoff_approvalpath_mapper[0] if timeoff_approvalpath_mapper else ''


def get_division_name_list():

    division_schedule = []
    divisionlist = []

    current_division_name = null
    current_division_uri = null

    user_division_schedule = result('bulk_getuser3')[
        'divisionSchedule']

    employee_startdate = result('bulk_getuser3')[
        'userDetails']['employmentDateRange']['startDate']

    if user_division_schedule:
        for division in user_division_schedule:
            uri = division['division']['uri']
            name = division['division']['displayText']
            if not (division.get('effectiveDate') and division['effectiveDate'].get('day')):
                employee_startdatetime = get_datetime_obj(employee_startdate)
                division_schedule.append({
                    'uri': uri,
                    'effective_date': employee_startdatetime,
                    'name': name
                })

                divisionlist.append({
                    'division': {
                        'uri': uri
                    }
                })

            else:
                effective_date = get_datetime_obj(division['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    division_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    divisionlist.append({
                        'division': {
                            'uri': uri
                        },
                        'effectiveDate': division['effectiveDate']
                    })
    if division_schedule:
        max_effective_date = max(x['effective_date']
                                 for x in division_schedule)
        current_division_list = list(
            filter(lambda x: x['effective_date'].date() == max_effective_date.date(), division_schedule))
        current_division_name = smartjoin_by_delim(
            [x['name'] for x in current_division_list]).lower()
        current_division_uri = smartjoin_by_delim(
            [x['uri'] for x in current_division_list])

    return {
        'divisionlist': divisionlist,
        'current_division_name': current_division_name,
        'current_division_uri': current_division_uri
    }


def get_costcenter_name_list():

    costcenter_schedule = []
    costcenterlist = []

    current_costcenter_name = null
    current_costcenter_uri = null

    user_costcenter_schedule = result('bulk_getuser3')[
        'costCenterSchedule']

    employee_startdate = result('bulk_getuser3')[
        'userDetails']['employmentDateRange']['startDate']

    if user_costcenter_schedule:
        for costcenter in user_costcenter_schedule:
            uri = costcenter['costCenter']['uri']
            name = costcenter['costCenter']['displayText']
            if not (costcenter.get('effectiveDate') and costcenter['effectiveDate'].get('day')):
                employee_startdatetime = get_datetime_obj(employee_startdate)
                costcenter_schedule.append({
                    'uri': uri,
                    'effective_date': employee_startdatetime,
                    'name': name
                })

                costcenterlist.append({
                    'costCenter': {
                        'uri': uri
                    }
                })

            else:
                effective_date = get_datetime_obj(costcenter['effectiveDate'])
                if effective_date.date() < datetime.utcnow().date():
                    costcenter_schedule.append({
                        'uri': uri,
                        'effective_date': effective_date,
                        'name': name
                    })
                if effective_date.date() != datetime.utcnow().date():
                    costcenterlist.append({
                        'costCenter': {
                            'uri': uri
                        },
                        'effectiveDate': costcenter['effectiveDate']
                    })
    if costcenter_schedule:
        max_effective_date = max(x['effective_date']
                                 for x in costcenter_schedule)
        current_costcenter_list = list(
            filter(lambda x: x['effective_date'].date() == max_effective_date.date(), costcenter_schedule))
        current_costcenter_name = smartjoin_by_delim(
            [x['name'] for x in current_costcenter_list]).lower()
        current_costcenter_uri = smartjoin_by_delim(
            [x['uri'] for x in current_costcenter_list]).lower()

    return {
        'costcenterlist': costcenterlist,
        'current_costcenter_name': current_costcenter_name,
        'current_costcenter_uri': current_costcenter_uri
    }


def get_payrule_to_assign(jobcategory):

    payrule_name = null
    current_payrule = null
    get_mapper_entries_from_businessunitname = result(
        'get_mapper_entries_from_businessunitname')
    get_mapper_entries_from_country_location = result(
        'get_mapper_entries_from_country_location')

    if get_mapper_entries_from_businessunitname:
        payrule_name = find_first_by_attr_and_get_attr(
            get_mapper_entries_from_businessunitname, 'type', 'Payrule', 'value')
    else:
        payrule_entries = [x['value'] for x in get_mapper_entries_from_country_location if x['type'] ==
                           'Payrule' and x['identifier2(employeetype_businessunit_type)'] ==
                           jobcategory] if get_mapper_entries_from_country_location else null
        payrule_name = payrule_entries[0] if payrule_entries else null

    if result('bulk_getuser3')['payRuleScriptSchedule']:
        user_startdate = result('bulk_getuser3')[
            'userDetails']['employmentDateRange']['startDate']

        def get_effective_date(effectiveDate, user_startdate):
            return get_datetime_obj(effectiveDate) if effectiveDate and effectiveDate.get('day') else get_datetime_obj(user_startdate)

        userpayrule_entries = list(map(lambda x: {
            'effective_date': get_effective_date(x['effectiveDate'], user_startdate),
            'display_text': x['payRuleScript']['displayText'],
            'uri': x['payRuleScript']['uri'],
            'daydiff': (get_effective_date(x['effectiveDate'], user_startdate) - datetime.now()).days
        }, result('bulk_getuser3')['payRuleScriptSchedule']))

        min_day_diff = min(x['daydiff'] for x in userpayrule_entries)

        current_payrule = list(
            filter(lambda x: x['daydiff'] == min_day_diff, userpayrule_entries))[0]

    return payrule_name if payrule_name and payrule_name != (current_payrule['display_text'] if current_payrule else null) else ''


def do_map_logs(success_tasks):
    logs_map = {
        'update_loginenabled_employment_daterange': 'User enabled',
        'update_firstname': 'First name updated',
        'update_lastname': 'Last name updated',
        'update_email': 'Email updated',
        'update_loginname': 'Login name updated',
        'update_supervisor_over_date_range': 'Supervisor updated',
        'put_employeetype_group_schedule': 'Employee Type updated',
        'assign_timesheet_policy_set': 'Timesheet template updated',
        'put_timesheet_period_group_schedule': 'Department group updated',
        'put_service_center_group_schedule': 'Job Domain & Family updated',
        'put_department_group_schedule': 'Business group unit updated',
        'put_location_schedule': 'Work Location updated',
        'update_timezone': 'Time zone updated',
        'remove_timeofftemplate': 'Timeoff template removed',
        'update_timeofftemplate': 'Timeoff template updated',
        'put_product_assignments': 'Product license updated',
        'update_punchentry': 'Punch entry policy updated',
        'remove_punchentry_policy': 'Punch entry removed',
        'update_timeoffapprovalpath': 'Timeoff approval path updated',
        'update_timesheetapprovalpath': 'Timesheet approval path updated',
        'update_timesheettemplate_for_user': 'Timesheet template updated',
        'put_division_schedule': 'Legal entity updated',
        'put_costcenter_schedule': 'Cost center updated'
    }
    logs = []

    if any(item in success_tasks for item in ['update_usercustomfields_dropdown', 'update_usercustomfields_numericvalues']):
        customfield_map = {
            'jobcategorydropdownupdated': 'Job Category updated',
            'referencejobcodedropdownupdated': 'Reference Job code updated',
            'referencejobtitledropdownupdated': 'Reference Job Title updated',
            'departmentdropdownupdated': 'Department updated',
            'standardweeklyhoursupdated': 'Standard Weekly Hours updated',
            'fteupdated': 'FTE updated'
        }
        for customfieldkey in [*customfield_map]:
            if result('get_customfields_to_update', key=customfieldkey) == 'yes':
                logs.append(customfield_map[customfieldkey])

    for key in [*logs_map]:
        if key in success_tasks:
            logs.append(logs_map[key])
    return logs


def get_updateuser_exception_logs():
    exception_messages = []
    success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == 'success',
                                                         get_current_context()['dag_run'].get_task_instances())))
    logs = do_map_logs(success_tasks)

    dag_run_conf = get_dag_run_conf()

    if 'should_update_supervisor' in success_tasks and result('should_update_supervisor') == 'finish_supervisor_assignment':
        exception_messages.append(
            'Supervisor not assigned since the user and manager IDs are same')

    if 'is_single_supervisor' in success_tasks and result('is_single_supervisor') == 'finish_supervisor_assignment':
        manager_id = dag_run_conf['manager_id']
        exception_messages.append(
            f'Supervisor is not assigned as multiple users have the same employee id as "{manager_id}" in Replicon')

    if 'is_employeetypegroup_present' in success_tasks and result('is_employeetypegroup_present') == 'set_timeoffapprovalpath':
        creativenoncreative = dag_run_conf['creativenoncreative']
        exception_messages.append(
            f'Employee type "{creativenoncreative}" not available in Replicon')

    if 'is_required_timesheettemplate_uri_present' in success_tasks and (result(
        'is_required_timesheettemplate_uri_present') == 'set_timeoffapprovalpath') and result(
            'get_timesheet_template_name_uri')['name']:
        timesheet_template = result('get_timesheet_template_name_uri')['name']
        exception_messages.append(
            f'Timesheet template "{timesheet_template}" not available in Replicon')

    if 'is_update_timeofftemplate' in success_tasks and (result('is_update_timeofftemplate') == 'required_productlicenses') and result(
            'get_required_timeofftemplate'):
        timeoff_template = result('get_required_timeofftemplate')
        exception_messages.append(
            f'Timeoff template "{timeoff_template}" not available in Replicon')

    if 'is_update_punchentry' in success_tasks and (result('is_update_punchentry') == 'process_remove_punchentry_policy') and result(
            'get_required_punchentry_policy'):
        punch_entry_template = result('get_required_punchentry_policy')
        exception_messages.append(
            f'Punch entry policy "{punch_entry_template}" not available in Replicon')

    if 'is_update_timeoffapprovalpath' in success_tasks and (result('is_update_timeoffapprovalpath') == 'process_timesheetapprovalpathchange') and result(
            'get_required_timeoffapprovalpath'):
        timeoffapprovalpath = result('get_required_timeoffapprovalpath')
        exception_messages.append(
            f'Timeoff approval path "{timeoffapprovalpath}" not available in Replicon')

    if 'is_update_timesheetapprovalpath' in success_tasks and (result('is_update_timesheetapprovalpath') == 'process_timesheetchange') and result(
            'get_required_timesheetapprovalpath'):
        timesheetapprovalpath = result('get_required_timesheetapprovalpath')
        exception_messages.append(
            f'Timesheet approval path "{timesheetapprovalpath}" not available in Replicon')

    if 'get_required_timesheettemplate_uri' in success_tasks and not result('get_required_timesheettemplate_uri')['uri'] and result(
            'get_required_timesheettemplate_uri')['name']:
        timesheet_template = result(
            'get_required_timesheettemplate_uri')['name']
        exception_messages.append(
            f'Timesheet template "{timesheet_template}" not available in Replicon')

    return {
        'exception': ';'.join(exception_messages) if exception_messages else '',
        'logs': ','.join(logs) if logs else ''
    }


def get_all_userlogs():
    logs = []

    create_skippeduser_log = result(
        'create_skippeduser_log')
    if create_skippeduser_log:
        logs.append(create_skippeduser_log)

    gather_disableuser_child_logs = result(
        'gather_disableuser_child_logs')
    if gather_disableuser_child_logs:
        logs.extend(gather_disableuser_child_logs)

    gather_adduser_child_logs = result(
        'gather_adduser_child_logs')
    if gather_adduser_child_logs:
        logs.extend(gather_adduser_child_logs)

    create_unchangedrecords_log = result(
        'create_unchangedrecords_log')
    if create_unchangedrecords_log:
        logs.append(create_unchangedrecords_log)

    gather_updateuser_child_logs = result(
        'gather_updateuser_child_logs')
    if gather_updateuser_child_logs:
        logs.extend(gather_updateuser_child_logs)

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
