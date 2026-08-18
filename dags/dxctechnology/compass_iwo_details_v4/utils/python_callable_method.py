from datetime import date, datetime
import rail
from dxctechnology.compass_iwo_details_v4.utils import custom_methods

null = None


def get_query_required_users(item):

    return {k: v if v is not null else "" for k, v in item.items()} if item else []


def get_unique_parent_wbs_with_all_child_py():
    input_records = rail.result('get_records_from_xml')
    unique_parent_wbs = list(set(map(lambda m: m["parentwbs"] or m['parentserviceorder'] or m['parentproject'],
                                     filter(lambda f: bool(f['parentwbs'] or f['parentserviceorder'] or f['parentproject']),
                                            input_records))))

    def get_parent(x):
        if x['parentwbs']:
            return x['parentwbs']

        if x['parentserviceorder']:
            return x['parentserviceorder']

        if x['parentproject']:
            return x['parentproject']

        return ""

    def get_all_child(wbs):
        data = list(
            filter(lambda x: wbs == get_parent(x), input_records))
        return list(map(lambda m: m['wbs'], data))

    parent_child_list = (list(map(lambda x: {
        "parent": x,
        "child": get_all_child(x)
    }, unique_parent_wbs)))

    return {
        "parent_list": unique_parent_wbs,
        "parent_child_list": parent_child_list
    }


def unique_users_from_feed():
    data = rail.result('get_records_from_xml')
    employeeids_list = []
    for item in data:
        if item['assignments']:
            for value in item['assignments']:
                employeeids_list.append(str(value['compasspersonnelnumber']))
    employeeids = list(set((employeeids_list)))

    return "'"+"','".join(employeeids)+"'"  # returns like-> 'abc','123'

# pylint: disable=too-many-arguments
def get_oef_object(uri, textvalue=null, numericvalue=null, tag_uri=null, to_remove=False, tagname= null):
    return {
        "definition": {
            "uri": uri,
            "name": null
        },
        "tag": {
            "uri": tag_uri,
            "slug": null,
            "tagName": {
                "name": tagname,
                "tagDefinitionUri": null
            }
        } if not to_remove else null,
        "numericValue": numericvalue,
        "textValue": textvalue,
        "fileValue": null
    }


def get_all_oef_payload(dag_run):
    oef_list = []
    project_type_oef_value = rail.result('get_companycode_list')[0]['name'] if rail.result(
        "get_companycode_list") else None

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

        if project_type_oef_value == 'GSAP':
            oef_list.append(get_oef_object(
                uri=dag_run.conf['projecttypeuri'],
                tagname='GS'))

            oef_list.append(get_oef_object(
                    uri=dag_run.conf['iwoindicatoruri'],
                    to_remove=True))

        if dag_run.conf['parentserviceorder']:
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentwbsuri'],
                textvalue=dag_run.conf['parentserviceorder']))
            oef_list.append(get_oef_object(
                uri=dag_run.conf['parentserviceorderuri'],
                textvalue=dag_run.conf['parentserviceorder']))
        oefs = ['GSAP Task Required', 'Reference Mandatory',
                'Comments Mandatory', 'PSA Flag']

        if not rail.result("get_parent_project_details"):
            return oef_list
        parent_oefs = rail.result("get_parent_project_details")[
            'extensionFieldValues']
        for oef in oefs:
            value = rail.find_first_by_attr_and_get_attr(
                parent_oefs, 'definition.displayText', oef)
            if not value:
                oef_list.append(get_oef_object(
                    uri=dag_run.conf["_".join(oef.split(" ")).lower()], to_remove=True))
            if value:
                oef_list.append(get_oef_object(
                    uri=value['definition']['uri'], tag_uri=value['tag']['uri']))
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


def get_updated_text_field(dag_run, current_values, previous_values_present=True):
    if not previous_values_present:
        return "|".join(dag_run.conf['child_list'])

    for child_wbs in dag_run.conf['child_list']:
        if child_wbs not in current_values:
            current_values += '|' + child_wbs
    return current_values


def get_iwo_wbs_element_fields(dag_run):
    iwo_wbs_element_value = rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
        'extensionFieldValues'], 'definition.displayText', 'IWO WBS Element', 'textValue') if rail.result('get_parent_project_details')[
        'extensionFieldValues'] else null

    if iwo_wbs_element_value:
        updated_values = get_updated_text_field(dag_run, iwo_wbs_element_value)
        return {
            'uri': rail.find_first_by_attr_and_get_attr(rail.result('get_parent_project_details')[
                'extensionFieldValues'], 'definition.displayText', 'IWO WBS Element', 'definition.uri'),
            'text': updated_values
        }
    updated_values = get_updated_text_field(
        dag_run, iwo_wbs_element_value, False)
    return {
        'uri': dag_run.conf['iwowbselement'],
        'text': updated_values
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

def prepare_task_payloads_with_parents(dag_run):
    """
    Prepare final task payloads with parent URIs and all necessary data.
    This function combines the logic from the child DAG into a single preparation step.
    """
    prepared_tasks = rail.result('prepare_tasks_for_creation')
    parent_wbs_mapping = rail.result('build_parent_wbs_mapping')
    parent_task_mapping = rail.result('build_parent_task_mapping') if rail.result('build_parent_task_mapping') else {}

    # Get resources - transform to correct format
    resources = []
    if dag_run.conf.get('resource_list'):
        # Transform the format if needed
        resource_list = dag_run.conf['resource_list']
        if resource_list and isinstance(resource_list, list):
            # Check if transformation is needed
            if resource_list and isinstance(resource_list[0], dict):
                if 'useruri' in resource_list[0]:
                    # Transform from [{'useruri': ...}] to [{'user': {'uri': ...}}]
                    transformed_list = [{'user': {'uri': item.get('useruri')}}
                                      for item in resource_list
                                      if item.get('useruri')]
                    resources = custom_methods.get_resource_uri(transformed_list)
                elif 'user' in resource_list[0]:
                    # Already in correct format
                    resources = custom_methods.get_resource_uri(resource_list)

    final_payloads = []

    for task in prepared_tasks:
        # Get parent WBS task details
        parent_wbs_task = parent_wbs_mapping.get(task['parent_wbs_task_uri'], {})

        # Get parent task URI for non-level-1 tasks
        parent_task_uri = None
        if task['level'] != '1' and task.get('parent'):
            parent_task_uri = parent_task_mapping.get(task['parent'])

        # Build the payload
        payload = build_put_task_payload(
            task=task,
            parent_wbs_task=parent_wbs_task,
            parent_task_uri=parent_task_uri,
            resources=resources,
            dag_run=dag_run
        )

        final_payloads.append({
            'task_info': task,
            'payload': payload
        })

    return final_payloads


def build_put_task_payload(task, parent_wbs_task, parent_task_uri, resources, dag_run):
    """
    Build the payload for PutTask API call.
    This replicates the logic from request_payload.get_put_task_payload
    """

    # Build custom fields
    custom_fields = []
    if parent_wbs_task and parent_wbs_task.get('customFields') and dag_run.conf.get('task_type'):
        # Find the Task Type dropdown value
        dropdown_value = None
        for field in parent_wbs_task['customFields']:
            if field.get('customField', {}).get('displayText') == 'Task Type':
                dropdown_value = field.get('text')
                break

        if dropdown_value or dag_run.conf.get('task_type'):
            custom_fields = [{
                "customField": {
                    "uri": dag_run.conf['task_type']
                },
                "dropDownOption": {
                    "name": dropdown_value if dropdown_value else dag_run.conf['task_type']
                }
            }]

    # Build time entry date range
    time_entry_date_range = None
    if task.get('start_date') or task.get('end_date'):
        time_entry_date_range = {
            "startDate": custom_methods.get_replicon_date(task['start_date'], "%d %B %Y")
                        if task.get('start_date') else None,
            "endDate": custom_methods.get_replicon_date(task['end_date'], "%d %B %Y")
                      if task.get('end_date') else None,
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }

    # Build task target
    task_target = {
        "name": task['taskname']
    }

    # Add parent reference for non-level-1 tasks
    if task['level'] != '1' and parent_task_uri:
        task_target['parent'] = {
            "uri": parent_task_uri
        }

    # Build complete payload
    payload = {
        "project": {
            "uri": task['processing_wbs_uri']
        },
        "task": {
            "target": task_target,
            "name": task['taskname'],
            "code": None if task.get('code') in ['None', None, ''] else task.get('code'),
            "description": None,
            "percentCompleted": "0",
            "timeEntryDateRange": time_entry_date_range,
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "customFieldValues": custom_fields,
            "assignedResources": resources
        }
    }

    # Add timeAndExpenseEntryType if available
    if parent_wbs_task and parent_wbs_task.get('timeAndExpenseEntryType'):
        payload['task']['timeAndExpenseEntryTypeUri'] = parent_wbs_task['timeAndExpenseEntryType'].get('uri')

    return payload
