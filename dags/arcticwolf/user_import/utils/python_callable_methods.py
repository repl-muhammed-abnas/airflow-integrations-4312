from datetime import datetime
import rail

MANDATORY_FIELDS = {
    'firstname': 'First Name',
    'lastname': 'Last Name',
    'email': 'Email',
    'employeeid': 'Employee ID',
    'startdate': 'Start Date',
    'loginname': 'Login Name',
    'supervisor': 'Supervisor',
    'departmentlevel2': 'Department Level 2',
    'departmentlevel3': 'Department Level 3',
    'employeetype': 'Employee Type',
    'jobcode': 'Job Code',
    'pos_title_code': 'Position Title Code',
    'login_status': 'Login Status',
    'status': 'Status',
    'location_level_1': 'Location Level 1',
    'location_level_2': 'Location Level 2'
}


def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the input")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_add_user_conf(item):
    departmentfullpath = rail.smartjoin_by_delim(
        ("Arctic Wolf" + "|" + str(item['departmentlevel2']) + "|" + str(item['departmentlevel3'])).split("|"), '/')
    locationfullpath = rail.smartjoin_by_delim(
        (str(item['location_level_1']) + "|" + str(item['location_level_2'])).split("|"), '/')
    return {
        "firstname": item['firstname'],
        "lastname": item['lastname'],
        "emailaddress": item['email'],
        "employeeid": item['employeeid'],
        "status": item['status'],
        "startdate": rail.get_replicon_date(datetime.strptime(item['startdate'], '%Y-%m-%d')),
        "userstartdate": item['startdate'],
        "loginname": item['loginname'],
        "supervisor": item['supervisor'],
        "supervisor_email":  item['supervisor_email'],
        "departmentlevel2": item['departmentlevel2'] + ".pre",
        "departmentlevel3": item['departmentlevel3'],
        "departmentfullpath": departmentfullpath,
        "employeetype": item['employeetype'],
        "departmentgroupuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_department_group_details'), 'fullpath', departmentfullpath, 'uri', ''),
        "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_permission_sets'), 'displayText', "Supervisor", 'uri', ''),
        "rundate": get_today_date(),
        "location_level_1": item['location_level_1'],
        "location_level_2": item['location_level_2'],
        "locationuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_location_details'), 'fullpath', locationfullpath, 'uri', ''),
        "jobcode": item['jobcode'],
        "division": item['division'],
        "cost_center": item['cost_center'],
        "position_title": item['pos_title'],
        "position_title_code": item['pos_title_code'],
        "fte": item['fte'],
        "exemption_status": item['exemption_status'],
        "type_worker": item['type_worker'],
        "last_hire_date": rail.get_replicon_date(datetime.strptime(item['startdate'], '%Y-%m-%d')),
        "userimportlogslookup": rail.result('create_user_import_logs_lookuptable'),
        "supervisorlookup": rail.result('create_supervisor_assignment_lookup'),
        "callerjobid": rail.render_template("{{dag_run_ecid()}}")
    }


def get_update_user_conf(item):
    departmentfullpath = rail.smartjoin_by_delim(
        ("Arctic Wolf" + "|" + str(item['departmentlevel2']) + "|" + str(item['departmentlevel3'])).split("|"), '/')
    locationfullpath = rail.smartjoin_by_delim(
        (str(item['location_level_1']) + "|" + str(item['location_level_2'])).split("|"), '/')
    return {
        "firstname": item['firstname'],
        "lastname": item['lastname'],
        "emailaddress": item['email'],
        "employeeid": item['employeeid'],
        "status": item['status'],
        "startdate": rail.get_replicon_date(datetime.strptime(item['startdate'], '%Y-%m-%d')),
        "enddate": rail.get_replicon_date(datetime.strptime(item['enddate'], '%Y-%m-%d')) if item['enddate'] else '',
        "loginname": item['loginname'],
        "supervisor": item['supervisor'],
        "supervisor_email":  item['supervisor_email'],
        "departmentlevel2": item['departmentlevel2'],
        "departmentlevel3": item['departmentlevel3'],
        "departmentfullpath": departmentfullpath,
        "employeetype": item['employeetype'],
        "departmentgroupuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_department_group_details'), 'fullpath', departmentfullpath, 'uri', ''),
        "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_all_permission_sets'), 'displayText', "Supervisor", 'uri', ''),
        "useruri": rail.find_first_by_attr_and_get_attr(rail.load_all_records(
            rail.result('create_collection_user_list_replicon')), 'employeeid', item['employeeid'], 'useruri', ''),
        "rundate": get_today_date(),
        "userstartdate": item['startdate'],
        "location_level_1": item['location_level_1'],
        "location_level_2": item['location_level_2'],
        "locationuri": rail.find_first_by_attr_and_get_attr(rail.result(
            'get_location_details'), 'fullpath', locationfullpath, 'uri', ''),
        "jobcode": item['jobcode'],
        "division": item['division'],
        "cost_center": item['cost_center'],
        "position_title": item['pos_title'],
        "position_title_code": item['pos_title_code'],
        "fte": item['fte'],
        "exemption_status": item['exemption_status'],
        "type_worker": item['type_worker'],
        "last_hire_date": rail.get_replicon_date(datetime.strptime(item['startdate'], '%Y-%m-%d')),
        "userimportlogslookup": rail.result('create_user_import_logs_lookuptable'),
        "callerjobid": rail.render_template("{{dag_run_ecid()}}"),
        "supervisorlookup": rail.result('create_supervisor_assignment_lookup')
    }


def get_error_and_email_subject():
    logs = rail.load_all_records(rail.result(
        'user_import_logs_search_entries'))
    iserrorpresent = rail.find_first_by_attr_and_get_attr(
        logs, 'properties.status', 'Error', 'properties.status', '')
    isexceptionpresent = rail.find_first_by_attr_and_get_attr(
        logs, 'properties.status', 'Exception', 'properties.status', '')
    return {
        "errorcheck": iserrorpresent,
        "exceptioncheck": isexceptionpresent,
        "subject": 'completed with errors' if iserrorpresent else ('completed with exceptions' if isexceptionpresent else 'completed successfully'),
        # pylint: disable = line-too-long
        "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if iserrorpresent else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
    }


def get_exception_message():
    return rail.smartjoin_by_delim(((rail.result('log_error_supervisor_and_user_is_same') if rail.result(
        'log_error_supervisor_and_user_is_same') else '') + ',' + (rail.result('log_error_supervisor_is_disabled') if rail.result(
            'log_error_supervisor_is_disabled') else '') + ',' + (rail.result('log_error_supervisor_not_available') if rail.result(
                'log_error_supervisor_not_available') else '') + ',' + (rail.result(
                    'log_error_multiple_same_user') if rail.result(
                    'log_error_multiple_same_user') else '')).split(','), ',')

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def is_create_user_failed_and_already_exists():
    if get_task_state('create_user') == 'failed':
        reason  = rail.result('create_user', key="error").get('response').get('json').get('error').get('reason')
        if reason == 'The specified user already exists.':
            return True
        return False
    return False