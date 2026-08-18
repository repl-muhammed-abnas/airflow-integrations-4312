from datetime import datetime
import pendulum
import rail
from rail import get_current_context, result, load_all_records, find_first_by_attr_and_get_attr
from rail.lib.ecid import get_dagrun_ecid
from dxctechnology.psa_user_profile_gsap.user_profile_sync.mapper.dxc_psa_user_mapper_prd_v1 import psa_user_mapper as production_mapper
from dxctechnology.psa_user_profile_gsap.user_profile_sync.mapper.dxc_psa_user_mapper_sb import psa_user_mapper as sandbox_mapper


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_blank_mandatory_field_log(item):
    logs = []
    field_map = {
        'contractorpern': 'ContractorPERN not available',
        'workertype': 'Worker Type not available',
        'firstname': 'First Name not available',
        'lastname': 'Last Name not available',
        'email': 'Email Address not available',
        'costcenter': 'Cost Center not available',
        'companycode': 'Company Code not available',
        'orgunitcode': 'Organizational Code not available',
        'contractstartdate': 'Start Date not available',
        'contractenddate': 'End Date not available',
        'erp': 'ERP not available',
        'source': 'Source not available'
    }

    for key in [*field_map]:
        if not item[key]:
            logs.extend(field_map[key]) if isinstance(
                field_map[key], list) else logs.append(field_map[key])
        elif key == 'erp' and (item['erp'] != 'C1' or item['erp'] != 'ES' or item['erp'] != 'GSAP'):
            logs.append('ERP is not allowed')

    return ','.join(logs)


def compose_blankmandatory_field_log(item):
    dag_run = get_current_context()['dag_run']
    details = get_blank_mandatory_field_log(item)
    return [item['contractorpern'], 'Validation', 'Skipped', details, get_dagrun_ecid(dag_run)]


def get_mapper_to_use(config):
    if config.instance.lower() == "sandbox":
        return sandbox_mapper
    return production_mapper


def get_psa_user_integration_mapper_data(config):

    return list(filter(lambda x: x['function'] == 'CWF User Integration', get_mapper_to_use(config)))


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
    financesystem = dag_run.conf['erp']
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
        'user_loginname': dag_run_conf['email'],
        'user_uri': user_uri,
        'user_name': f"{dag_run_conf['firstname']}|{dag_run_conf['lastname']}",
        'supervisor_loginname': f"{dag_run_conf['managerid']}",
        'action': action,
        'emp_id': dag_run_conf['contractorpern'],
        'status': 'pending'
    }


def get_user_exception_log_message(should_update_supervisor, check_supervisor_enddate):

    exception_messages = []

    if result(should_update_supervisor) == 'get_exception_logs':
        exception_messages.append(
            'Supervisor not updated - Supervisor is same as User')

    if result(check_supervisor_enddate) == 'get_exception_logs':

        exception_messages.append(
            'Supervisor is not updated as the supervisor is disabled and the endate is less than todays date')

    conf = get_dag_run_conf()
    if not conf['company_parent_uri']:
        exception_messages.append(
            'Comapany Code is not Available in Replicon')

    return ';'.join(exception_messages) if exception_messages else ''


def get_exception_log_message(
        enddate_with_userstartdate, enddate_with_userstartdate_disableuser, should_update_supervisor, check_supervisor_enddate):

    exception_messages = []

    if result(enddate_with_userstartdate) == 'disable_user_login' or result(
            enddate_with_userstartdate_disableuser) == 'get_effective_group_membership':
        exception_messages.append('End date is before the contract start date')

    if result(should_update_supervisor) == 'get_update_activities_payload':

        exception_messages.append(
            'Supervisor not updated  - Supervisor is same as User')

    if result(check_supervisor_enddate) == 'get_update_activities_payload':

        exception_messages.append(
            'Supervisor is not updated as the supervisor is disabled and the endate is less than todays date')

    conf = get_dag_run_conf()
    if not conf['company_parent_uri']:
        exception_messages.append(
            'Comapany Code is not Available in Replicon')

    return ';'.join(exception_messages) if exception_messages else ''


def compose_supervisor_details(manager_id):

    supervisor = list(filter(lambda x: x['employeeid'] == manager_id, result(
        'get_data_for_supervisor'))) if result('get_data_for_supervisor') else []

    return {
        'name': supervisor[0]['name'] if supervisor else '',
        'uri': supervisor[0]['uri'] if supervisor else '',
        'status': supervisor[0]['status'] if supervisor else '',
        'enddate': supervisor[0]['enddate'] if supervisor else '',
        'startdate': supervisor[0]['startdate'] if supervisor else '',
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


def check_company_code(dag_run):
    gsap_codes = ['3001', '3124', '1602', '3118']
    recieved_code = dag_run.conf['companycode']
    erp = dag_run.conf['erp']

    if erp == "GSAP":
        if recieved_code in gsap_codes:
            return True
        return False
    return True
