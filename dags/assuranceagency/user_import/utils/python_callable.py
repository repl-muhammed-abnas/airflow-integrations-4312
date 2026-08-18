# pylint: disable=unused-variable,too-many-statements,too-many-branches
from datetime import datetime
import rail

def get_current_date_time():
    return datetime.now().strftime("%d%m%YT%H%M%S")

def get_blank_fields_conf(item):
    details = []
    if item['loginname'] == "":
        details.append("Login Name is blank in feed file")
    if item['employeeid'] == "":
        details.append("Employee ID is blank in feed file")
    if item['startdate'] == "":
        details.append("Start- Date is blank in feed file")
    return [
        str(item['firstname']) + " " + str(item['lastname']),
        item['loginname'],
        item['employeeid'],
        'validation',
        'Skipped',
        ','.join(details)
    ]

def get_disabled_skip_conf(item):
    return [
        str(item['firstname']) + " " + str(item['lastname']),
        item['loginname'],
        item['employeeid'],
        'disable',
        'Skipped',
        'Required user already disabled in Replicon'
    ]

def get_disabled_skip_for_no_enddate_conf(item):
    return [
        str(item['firstname']) + " " + str(item['lastname']),
        item['loginname'],
        item['employeeid'],
        'disable',
        'Skipped',
        'End date is not available in feed file'
    ]

def get_add_skip_for_new_user_conf(item):
    details = []
    if item['enabled'].lower() == "no":
        details.append("Enabled column is set to 'No' for new user")
    if item['firstname'] == "":
        details.append("First name is blank for new user")
    if item['lastname'] == "":
        details.append("Last name is blank for new user")
    return [
        str(item['firstname']) + " " + str(item['lastname']),
        item['loginname'],
        item['employeeid'],
        'add',
        'Skipped',
        ','.join(details)
    ]

def split_startdate(dag_run):
    return{
        "day" : dag_run.conf['startdate'].split('/')[0],
        "month" : dag_run.conf['startdate'].split('/')[1],
        "year" : dag_run.conf['startdate'].split('/')[2]
    }

def get_timeoff_type(dag_run):
    return {
        "userUri": rail.result('add_user')['uri'],
        "timeOffTypeUris": dag_run.conf['timeoffuri']
        }

def get_daterange_data(dag_run):
    return (datetime.strptime(dag_run.conf['enddate'], "%d/%m/%Y") - datetime.strptime(dag_run.conf['startdate'], "%d/%m/%Y")).days

def get_ref_file_name(filepath):
    return filepath + "/" + rail.result('list_reference_files')[filepath][0]['name']

def get_unchanged_data_conf(item):
    return [
        str(item['firstname']) + " " + str(item['lastname']),
        item['loginname'],
        item['employeeid'],
        'update',
        'Skipped',
        'No change in the user record'
    ]

def get_today():
    return datetime.now().strftime("%m/%d/%Y")

def get_current_data(arg1,arg2):
    data_dict = {}
    data = rail.result('get_user_data')[0][arg1]
    emplpoyment_daterange_data = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    for i, p_data in enumerate(data):
        if p_data['effectiveDate']:
            effective_date = str(p_data['effectiveDate']['month']) + "/" + str(p_data['effectiveDate']['day']) \
                + "/" + str(p_data['effectiveDate']['year'])
        else:
            effective_date = str(emplpoyment_daterange_data['month']) + "/" + str(emplpoyment_daterange_data['day']) \
                + "/" + str(emplpoyment_daterange_data['year'])
        date_diff = (datetime.strptime(get_today(), "%m/%d/%Y") - datetime.strptime(effective_date, "%m/%d/%Y")).days
        data_dict[p_data[arg2]['uri']] = date_diff

    uri = min(data_dict.keys(), key = lambda k: data_dict[k])
    return uri

def get_current_officeschedule_uri():
    officeschedule_data_dict = {}
    officeschedule_data = rail.result('get_user_data')[0]['schedulePolicies']
    emplpoyment_daterange_data = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    for i, ofc_data in enumerate(officeschedule_data):
        if ofc_data['effectiveDate']:
            effective_date = str(ofc_data['effectiveDate']['month']) + "/" + str(ofc_data['effectiveDate']['day']) \
                + "/" + str(ofc_data['effectiveDate']['year'])
        else:
            effective_date = str(emplpoyment_daterange_data['month']) + "/" + str(emplpoyment_daterange_data['day']) \
                + "/" + str(emplpoyment_daterange_data['year'])
        date_diff = (datetime.strptime(get_today(), "%m/%d/%Y") - datetime.strptime(effective_date, "%m/%d/%Y")).days
        officeschedule_data_dict[ofc_data['uri']] = date_diff

    uri = min(officeschedule_data_dict.keys(), key= lambda k: officeschedule_data_dict[k])
    return uri

def get_supervisor_uri_by_loginname(response, dag_run):
    user_uris = [item['cells'][0]['uri'] for item in response['rows']
                 if item['cells'][0]['textValue'] == dag_run.conf['supervisorloginname']] if response['rows'] else []
    return rail.smartjoin_by_delim(user_uris) if user_uris else ''

def get_subject_line():
    import_completion_message = "completed succesfully"
    has_error_message = rail.render_template('{{result("get_logged_errors", key="length") > 0}}')
    has_exception_message = rail.render_template('{{result("get_logged_exception", key="length") > 0}}')
    if has_error_message == 'True':
        import_completion_message = "completed with errors"
    elif has_exception_message == 'True':
        import_completion_message = "completed with exceptions"
    return import_completion_message

def get_email_body():
    body = ''
    error_message = rail.result('get_logged_errors')
    if error_message:
        body = '''<br />For any queries, \
        please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''
    else:
        body = '''<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''
    return body

def get_status_and_details_for_update(dag_run):
    message = "Success"
    details = "Updated Successfully"
    has_exception_message = ','.join(list(map(lambda v: v['value'] , rail.load_all_records(rail.result('write_log_user_import')))))
    if has_exception_message:
        message = "Exception"
        details = "Partially Updated" + ' ' + has_exception_message
    return {
        "username" : dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "login_name": dag_run.conf['loginname'],
        "emplid" : dag_run.conf['employeeid'],
        "action" : "update",
        "status": message,
        "details": details
    }

def get_status_and_details_for_add(dag_run):
    message = "Success"
    details = "User created successfully"
    if rail.result('get_exception_log')['exc_present'] is True:
        message = 'Exception'
        details = 'User created partially -' + rail.result('get_exception_log')['exc_value']
    return {
        "username" : dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "login_name": dag_run.conf['loginname'],
        "emplid" : dag_run.conf['employeeid'],
        "action" : dag_run.conf['type'],
        "status": message,
        "details": details
    }

def get_exception_logs(dag_run):
    exception_list = []
    if dag_run.conf['department']:
        if not dag_run.conf['departmenturi']:
            exception_list.append('Department not assigned as it is not available in Replicon')
    else:
        exception_list.append('Department not assigned as it is blank in feedfile')

    if dag_run.conf['location']:
        if not dag_run.conf['locationuri']:
            exception_list.append('Location not assigned as it is not available in Replicon')
    else:
        exception_list.append('Location not assigned as it is blank in feedfile')

    if dag_run.conf['permissionsets']:
        if not dag_run.conf['enduseruri']:
            exception_list.append('Permission not assigned as it is not available in Replicon')
    else:
        exception_list.append('Permission not assigned as it is blank in feedfile')

    if dag_run.conf['timesheettemplate']:
        if not dag_run.conf['timesheettemplateuri']:
            exception_list.append('Timesheet template not assigned as it is not available in Replicon')
    else:
        exception_list.append('Timesheet template not assigned as it is blank in feedfile')

    if dag_run.conf['timesheetperiodtype']:
        if not dag_run.conf['timesheetperioduri']:
            exception_list.append('Timesheet period type not assigned as it is not available in Replicon')
    else:
        exception_list.append('Timesheet period type not assigned as it is blank in feedfile')

    if dag_run.conf['timesheetapprovalpath']:
        if not dag_run.conf['timesheetapprovalpathuri']:
            exception_list.append('Global timesheet approval path assigned as it is not available in Replicon')
    else:
        exception_list.append('Global timesheet approval path assigned as it is blank in feedfile')

    if dag_run.conf['timezone']:
        if not dag_run.conf['timezoneuri']:
            exception_list.append('Global timezone assigned as it is not available in Replicon')
    else:
        exception_list.append('Global timezone assigned as it is blank in feedfile')

    if dag_run.conf['holidaycalendar']:
        if not dag_run.conf['holidaycalendaruri']:
            exception_list.append('Global holiday calendar assigned as it is not available in Replicon')
    else:
        exception_list.append('Global holiday calendar assigned as it is blank in feedfile')

    if dag_run.conf['workweek']:
        if not dag_run.conf['workweekuri']:
            exception_list.append('Global work week assigned as it is not available in Replicon')
    else:
        exception_list.append('Global work week assigned as it is blank in feedfile')

    if dag_run.conf['initialschedulename']:
        if not dag_run.conf['officescheduleuri']:
            exception_list.append('Default schedule assigned as it is not available in Replicon')
    else:
        exception_list.append('Default schedule assigned as it is blank in feedfile')

    if dag_run.conf['timeofftemplate']:
        if not dag_run.conf['timeofftemplateuri']:
            exception_list.append('Timeoff template not assigned as it is not available in Replicon')
    else:
        exception_list.append('Timeoff template not assigned as it is blank in feedfile')

    if dag_run.conf['timeoffapprovalpath']:
        if not dag_run.conf['timeoffapprovalpathuri']:
            exception_list.append('Global timeoff approval path assigned as it is not available in Replicon')
    else:
        exception_list.append('Global timeoff approval path assigned as it is blank in feedfile')

    if dag_run.conf['initialpayrulename']:
        if not dag_run.conf['payruleuri']:
            exception_list.append('Payrule not assigned as it is not available in Replicon')
    else:
        exception_list.append('Payrule not assigned as it is blank in feedfile')

    if not dag_run.conf['employeetypeuri']:
        exception_list.append('Employee type not assigned as it is not defined in mapper \
            for the required employee type and worker category combination')

    if not dag_run.conf['workdayid']:
        exception_list.append('Workday ID  not updated as it is blank in feedfile')

    if not dag_run.conf['position']:
        exception_list.append('Position not updated as it is blank in feedfile')

    if not dag_run.conf['manager']:
        exception_list.append('Manager not updated as it is blank in feedfile')

    if len(exception_list) > 0 :
        return {
            'exc_present' : True,
            'exc_value' : ','.join(exception_list)
        }
    return {
        'exc_present' : False,
        'exc_value' : ''
    }

def get_exceptions():
    exceptions = (rail.result('log_same_user_and_supervisor_exception') if rail.result(
        'log_same_user_and_supervisor_exception') else '') + (rail.result('log_supervisor_not_available') if rail.result(
            'log_supervisor_not_available') else '') + (rail.result('log_supervisor_is_disabled') if rail.result(
                'log_supervisor_is_disabled') else '') + (rail.result('log_no_action_found') if rail.result(
                    'log_no_action_found') else '')
    return exceptions
