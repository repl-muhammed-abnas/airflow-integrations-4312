from datetime import datetime
import pendulum
from rail import get_current_context, result, load_all_records, find_first_by_attr_and_get_attr
from dxctechnology.cwf_user_profile.user_profile_sync.mapper.dxc_cwf_user_mapper import cwf_user_mapper


def get_blank_mandatory_field_log(item):
    logs = []
    field_map = {
        'hpid': 'HPID not available',
        'firstname': 'First Name not available',
        'lastname': 'Last Name not available',
        'emailaddress': 'Email Address not available',
        'manageremail': 'Manager Email not available',
        'managerid': 'Manager HPID not available',
        'contractstartdate': 'Contract Begin Date not available',
        'workertype': 'Worker Type not available',
        'financesystem': 'Finance System not available',
        'timetracking': 'Time Tracking Required not available',
        'companycode': 'Company Code not available'
    }

    for key in [*field_map]:
        if not item[key]:
            logs.extend(field_map[key]) if isinstance(
                field_map[key], list) else logs.append(field_map[key])
        elif key == 'financesystem' and (item['financesystem'] != 'C1' or item['financesystem'] != 'ES'):
            logs.append('Finance system is not allowed')

    return ','.join(logs)


def get_cwf_user_integration_mapper_data():
    return list(filter(lambda x: x['function'] == 'CWF User Integration', cwf_user_mapper))


def get_effective_group_date_payload(financesystem):
    es_date_mapper = {
        0: 6,
        1: 0,
        2: 1,
        3: 2,
        4: 3,
        5: 4,
        6: 5
    }
    c1_date_mapper = {
        0: 1,
        1: 2,
        2: 3,
        3: 4,
        4: 5,
        5: 6,
        6: 0
    }
    america_time_now = pendulum.now('America/Los_Angeles')

    effective_group = america_time_now.subtract(
        days=es_date_mapper[america_time_now.day_of_week]) if financesystem == 'ES' else america_time_now.subtract(
        days=c1_date_mapper[america_time_now.day_of_week])

    return {
        'year': effective_group.year,
        'month': effective_group.month,
        'day': effective_group.day
    }


def get_contract_dates(dag_run):
    null = None
    start_date = datetime.fromisoformat(dag_run.conf['contractstartdate'])
    end_date = datetime.fromisoformat(
        dag_run.conf['contractenddate']) if dag_run.conf['contractenddate'] else null
    return {
        'start_date': {
            'year': start_date.year,
            'month': start_date.month,
            'day': start_date.day
        },
        'end_date': {
            'year': end_date.year,
            'month': end_date.month,
            'day': end_date.day
        } if end_date else null
    }


def get_activities_to_update(dag_run):
    activities = dag_run.conf['activities'].split(',')
    employee_type = dag_run.conf['employee_type']
    financesystem = dag_run.conf['financesystem']
    user_assigned_activities = result('bulk_get_user')['assignedActivities']

    if (activities and 'Agency Contractor' not in employee_type) or (
            activities and 'Agency Contractor' in employee_type and financesystem != 'ES'):
        activities = list(set(activities))
        activities_to_assign = list(filter(lambda x: not x['status'], list(map(
            lambda item: {
                'name': item,
                'status': bool(
                    find_first_by_attr_and_get_attr(
                        user_assigned_activities, 'displayText', item, 'uri')) if user_assigned_activities else False
            }, activities))))
        if activities_to_assign:
            return list(map(lambda x: {
                'name': x
            }, activities))
    return []


def log_supervisor(user_uri, action):

    dag_run_conf = get_current_context()['dag_run'].conf
    return {
        'child_log': dag_run_conf['log'],
        'user_loginname': dag_run_conf['emailaddress'],
        'user_uri': user_uri,
        'user_name': f"{dag_run_conf['firstname']}|{dag_run_conf['lastname']}",
        'supervisor_loginname': f"{dag_run_conf['manageremail']}|{dag_run_conf['managerid']}",
        'action': action,
        'emp_id': dag_run_conf['hpid'],
        'status': 'pending'
    }


def get_exception_log_message(
        enddate_with_userstartdate, enddate_with_userstartdate_disableuser, enddate_with_userstartdate_enddate_update, should_update_supervisor):

    exception_messages = []

    if result(enddate_with_userstartdate) == 'enable_user_for_rehire' or result(
            enddate_with_userstartdate_disableuser) == 'get_effective_group_membership' or result(
                enddate_with_userstartdate_enddate_update) == 'should_disable_user':
        exception_messages.append('End date is before the contract start date')

    if result(should_update_supervisor) == 'get_update_activities_payload':

        exception_messages.append(
            'Supervisor not updated  - Supervisor is same as User')

    return ';'.join(exception_messages) if exception_messages else ''


def compose_supervisor_details(manager_email, manager_id):

    supervisor = list(filter(lambda x: x['loginname'] == manager_email and x['employeeid'] == manager_id, result(
        'get_data_for_supervisor'))) if result('get_data_for_supervisor') else []

    return {
        'name': supervisor[0]['name'] if supervisor else '',
        'uri': supervisor[0]['uri'] if supervisor else '',
        'status': supervisor[0]['status'] if supervisor else ''
    }


def add_supervisorcheck_user_log(dag_run, item):
    properties = item['properties']
    exception_message = f"Supervisor is not updated as the supervisor with login name {dag_run.conf['supervisor_loginname']} is not available"

    status = 'Error' if properties['status'] == 'Error' else (
        'Exception' if result('is_supervisor_present') == 'is_child_log_entries_present' else properties['status'])
    details = f"{properties['details']};{exception_message}" if result(
        'is_supervisor_present') == 'is_child_log_entries_present' else properties['details']

    return {
        **{k: v for k, v in properties.items() if k in ('userid', 'email', 'action')},
        **{
            'status': status,
            'details': details
        }
    }


def load_records(log_artifact):
    try:
        logs = load_all_records(log_artifact)
        return logs
    except:  # pylint: disable=bare-except
        return []


def do_format_logs():

    dag_run = get_current_context()['dag_run']

    log_artifacts = []
    if dag_run.conf['create_blankmandatory_log']:
        log_artifacts.append(dag_run.conf['create_blankmandatory_log'])

    if dag_run.conf['child_log']:
        log_artifacts.extend(dag_run.conf['child_log'])

    log_records = []

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    return list(map(lambda x: {
        **{k: v for k, v in x['properties'].items() if k != 'email'},
        **{
            'jobid': x['ecid']
        }}, log_records))
