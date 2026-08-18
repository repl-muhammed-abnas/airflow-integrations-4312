from datetime import date, datetime
import rail
from dxctechnology.compass_iwo_details.utils import custom_methods

null = None


def get_query_required_users(item):
    return {k: v if v is not null else "" for k, v in item.items()} if item else []


def unique_users_from_feed():
    data = rail.result('get_records_from_xml')
    employeeids_list = []
    for item in data:
        if item['assignments']:
            for value in item['assignments']:
                employeeids_list.append(str(value['compasspersonnelnumber']))
    employeeids = list(set((employeeids_list)))

    return "'"+"','".join(employeeids)+"'"  # returns like-> 'abc','123'


def get_oef_object(uri, textvalue=null, numericvalue=null):
    return {
        "definition": {
            "uri": uri,
            "name": null
        },
        "tag": {
            "uri": null,
            "slug": null,
            "tagName": {
                "name": null,
                "tagDefinitionUri": null
            }
        },
        "numericValue": numericvalue,
        "textValue": textvalue,
        "fileValue": null
    }


def get_all_oef_payload(dag_run):
    oef_list = []
    if rail.result('build_oef_for_parentcompanycode'):
        oef_list.append(rail.result('build_oef_for_parentcompanycode'))

    if rail.result('is_parent_details_present') == 'build_parent_details_oef':
        if dag_run.conf['parentproject']:
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentwbsuri'],
                textvalue=dag_run.conf['parentproject']))
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentprojecturi'],
                numericvalue=dag_run.conf['parentproject']))

        if dag_run.conf['parentwbs']:
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentwbsuri'],
                textvalue=dag_run.conf['parentwbs']))

        if dag_run.conf['parentserviceorder']:
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentwbsuri'],
                textvalue=dag_run.conf['parentserviceorder']))
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentserviceorderuri'],
                textvalue=dag_run.conf['parentserviceorder']))

    return oef_list


def check_userend_date(userenddate):
    enddate = custom_methods.get_replicon_date(userenddate, '%d %B %Y')
    date_now = datetime.now()
    return date(enddate['year'], enddate['month'], enddate['day']) < date(date_now.year, date_now.month, date_now.day)


def get_valid_tasks_list():
    exception_messages = []
    conf = custom_methods.get_dag_conf()
    tasks = conf['taskdetails']
    valid_tasks = []
    for task in tasks:
        if not task['useruri']:
            exception_messages.append(
                'user with ' + (task['employeeid'] if task['employeeid'] else '') + ' not available in Replicon')
        else:
            if (task['userenddate'] and check_userend_date(task['userenddate'])) or task['userstatus'] == 'Disabled':
                exception_messages.append(
                    'Employee ' + (task['employeeid'] if task['employeeid'] else '') +
                    ' not assigned as the user is in disabled status or the end date is in the past.')
            else:
                valid_tasks.append(task)
    return {
        'valid_tasks': valid_tasks,
        'exception_messages': exception_messages
    }


def check_start_end_assignment_date(task):
    if not task['assignmentstartdate'] and not task['assignmentenddate']:
        return False
    user_found = False
    if task['userstatus'] == 'Enabled':
        data = rail.result('get_all_project_team_assignment_after_update')
        for item in data:
            if task['useruri'] == item['uri']:
                user_found = True
                task_startdate = custom_methods.get_string_date(
                    item['assignmentstartdate'])
                task_enddate = custom_methods.get_string_date(
                    item['assignmentenddate'])
                if not task_startdate or not task_enddate or task_startdate != task['assignmentstartdate'] \
                        or task_enddate != task['assignmentenddate']:
                    return True
    return not user_found


def get_assignment_date():
    valid_tasks = rail.result('create_valid_task_list')['valid_tasks']
    assignment_list = []

    for task in valid_tasks:
        if check_start_end_assignment_date(task):
            assignment_list.append({
                'useruri': task['useruri'],
                'startdate': custom_methods.get_replicon_date(task['assignmentstartdate']),
                'enddate': custom_methods.get_replicon_date(task['assignmentenddate'])
            })
    return assignment_list


def get_iwo_wbs_element_fields(dag_run):
    iwo_wbs_element_value = rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
        'extensionFieldValues'], 'definition.displayText', 'IWO WBS Element', 'textValue') if rail.result('get_parent_project_details')[
        'extensionFieldValues'] else null

    if iwo_wbs_element_value:
        iwo_wbs_element_list = iwo_wbs_element_value.split('|')
        return {
            'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
                'extensionFieldValues'], 'definition.displayText', 'IWO WBS Element', 'definition.uri'),
            'text': iwo_wbs_element_value if dag_run.conf[
                'wbs'] in iwo_wbs_element_list else iwo_wbs_element_value + '|' + dag_run.conf['wbs']
        }

    return {
        'uri': dag_run.conf['iwowbselement'],
        'text': dag_run.conf['wbs']
    }


def get_exception_messages():
    all_exception_messages = []
    if rail.result('add_company_code_is_blank'):
        all_exception_messages.append(rail.result('add_company_code_is_blank'))
    if rail.result('add_parent_project_not_present'):
        all_exception_messages.append(rail.result(
            'add_parent_project_not_present'))
    if rail.result('create_valid_task_list') and rail.result('create_valid_task_list')['exception_messages']:
        all_exception_messages.extend(rail.result(
            'create_valid_task_list')['exception_messages'])
    return ';'.join(all_exception_messages)


def get_process_iwo_wbs_update_conf(dag_run):
    ignored_keys = ('_ancestry', '_ecid', '_replication_position')
    return {'dag_conf': {k: v for k, v in dag_run.conf.items() if k not in ignored_keys}}
