from datetime import datetime
from functools import lru_cache
from itertools import chain
from collections import defaultdict
from ast import literal_eval
import rail
from alvarezandmarsalholdings.customer_project_import_v1.utils.request_payload import MANDATORY_FIELDS

null = None
EXISTING_DATE_FORMAT = "%m/%d/%Y"
PAYLOAD_DATE_FORMAT = "%Y-%m-%d"
TASK_ADD_UPDATE_BATCH_SIZE = 200

def can_update_task(replicon_task_details, payload_task_details):
    parent = payload_task_details['parent'] or {}
    non_bill_wp = parent.get('non_bill_wp', payload_task_details.get('non_bill_wp'))
    billing_resp = parent.get('billing_responsible', payload_task_details.get('billing_responsible'))
    wp_func_blocked = parent.get('work_package_func_is_blocked', payload_task_details.get('work_package_func_is_blocked'))

    task_startdate = replicon_task_details.get('startdate')
    task_enddate = replicon_task_details.get('enddate')
    payload_startdate = parent.get('startdate', payload_task_details.get('startdate'))
    payload_enddate = parent.get('enddate', payload_task_details.get('enddate'))

    billing_cntrl_catg = 'NON_BILL' if non_bill_wp == 'X' else ''

    # Compare start or end dates if only one exists
    if (not task_startdate and payload_startdate) or (not task_enddate and payload_enddate):
        return True

    # Compare both start dates
    if task_startdate and payload_startdate:
        task_date = datetime.strptime(task_startdate, EXISTING_DATE_FORMAT).date()
        payload_date = datetime.strptime(payload_startdate, PAYLOAD_DATE_FORMAT).date()
        if task_date != payload_date:
            return True

    # Compare both end dates
    if task_enddate and payload_enddate:
        task_date = datetime.strptime(task_enddate, EXISTING_DATE_FORMAT).date()
        payload_date = datetime.strptime(payload_enddate, PAYLOAD_DATE_FORMAT).date()
        if task_date != payload_date:
            return True

    # Compare billing category
    if replicon_task_details.get('billing_control_catg') != billing_cntrl_catg:
        return True

    # Check billing type consistency
    time_expense = replicon_task_details.get('time_and_expense')
    if (time_expense == 'Billable Only' and non_bill_wp == 'X') or \
       (time_expense == 'Non-Billable' and not non_bill_wp):
        return True

    # Compare billing responsibility
    if replicon_task_details.get('billing_resp') != billing_resp:
        return True

    # Check closed state vs. blocked flag
    is_closed = replicon_task_details.get('isclosed')
    if (not is_closed and wp_func_blocked == 'X') or (is_closed and wp_func_blocked != 'X'):
        return True

    # Check task name
    return replicon_task_details.get('task_name') != payload_task_details.get('taskname')

def get_formatted_payload_tasks(dag_run, instance):
    work_packages = dag_run.conf['WorkpackageSet'].get('WorkPackage', []) if dag_run.conf.get('WorkpackageSet') else []

    task_level1 = []
    task_level2 = []
    missing_fields = []
    resources = defaultdict(list)
    cost_center_resource = defaultdict(list)
    billing_responsibles = set()
    task_code_and_name = dict()

    def get_missing_mandatory_fields(item, task_level, name, message):
        skip = False
        for key, value in MANDATORY_FIELDS[task_level].items():
            if not item[key]:
                message.append(f"{name} {value} is not present in payload")
                skip = True
        return skip

    def add_child_taks(workitemSet, StaffingDataSet, StaffingCostCenter):
        work_items = workitemSet['Workitem'] if workitemSet else []
        staffing = StaffingDataSet['Staffing'] if StaffingDataSet else []
        costcenterentry = StaffingCostCenter['CostCenterEntry'] if StaffingCostCenter else {}
        # Only collect resources/cost centers for workitems, not workpackages
        workitem_resources = []
        workitem_cost_centers = []

        if costcenterentry:
            if costcenterentry['CostCenter']:
                workitem_cost_centers.append(costcenterentry['CostCenter'])

        for staff in staffing:
            if staff['StaffedEmployee']:
                workitem_resources.append(staff['StaffedEmployee'])

        for child_task in work_items:
            temp_dict = {}
            parent_task_details = rail.find_first_by_attr_and_get_attr(
                task_level1,
                'taskcode',
                child_task['WorkpackageID']
            )
            parent_task_details = parent_task_details if parent_task_details else {}
            
            message = []
            skip = get_missing_mandatory_fields(child_task, 'task_level2_fields', 'Workitem', message)
            if skip:
                missing_fields.append({
                    'taskname': child_task['Workitemname'],
                    'taskcode': child_task['Workitem'],
                    'parent_taskcode': child_task['WorkpackageID'],
                    'message': rail.smartjoin_by_delim(message, ";")
                })
                continue
            temp_dict['taskname'] = child_task['Workitemname']
            temp_dict['taskcode'] = child_task['Workitem']
            temp_dict['parent_taskcode'] = child_task['WorkpackageID']
            temp_dict['parent'] = parent_task_details
            task_level2.append(temp_dict)

            # Only keep workitem resources/cost centers
            if workitem_resources:
                resources[child_task['Workitem']] = list(set(workitem_resources))
            if workitem_cost_centers:
                cost_center_resource[child_task['Workitem']] = list(set(workitem_cost_centers))

    for item in work_packages:
        temp_dict = {}
        temp_dict['taskname'] = item['WorkPackageName']
        temp_dict['taskcode'] = item['WorkPackageID']
        temp_dict['startdate'] = item['WPStartDate']
        temp_dict['enddate'] = item['WPEndDate']
        temp_dict['non_bill_wp'] = item['NonBillableWP']
        temp_dict['billing_responsible'] = item['BillingResponsible']
        temp_dict['work_package_func_is_blocked'] = item['WorkPackageFunctionIsBlocked']
        temp_dict['parent'] = False
        message = []
        task_code_and_name[item['WorkPackageID']] = item['WorkPackageName']
        if item['BillingResponsible']:
            billing_responsibles.add((item['BillingResponsible'], item['WorkPackageName'], item['WorkPackageID']))
        skip = get_missing_mandatory_fields(item, 'task_level1_fields', 'WorkPackage', message)
        if skip:
            missing_fields.append({
                'taskname': item['WorkPackageName'],
                'taskcode': item['WorkPackageID'],
                'message': rail.smartjoin_by_delim(message, ";")
            })
            continue
        task_level1.append(temp_dict)

    # Iterating it twice because we need task_level1 list to be prepared before preparing the task_level2 to consume task_level1 data
    for item in work_packages:
        add_child_taks(item['WorkitemSet'], item['StaffingDataSet'], item['StaffingCostCenter'])
    return {
        'task_level1': task_level1,
        'task_level2': task_level2,
        'resources': resources,
        'cost_center_resource': cost_center_resource,
        'missing_mandatory_fields': missing_fields,
        'billing_responsibles': list(billing_responsibles),
        'task_code_and_name': task_code_and_name,
    }

def get_batch_records(records, batch_size=200):
    
    batches = []
    for i in range(0, len(records), batch_size):
        batches.append(records[i:i + batch_size])
    return batches

def get_task_to_add_update_skip(current_task_in_project, task_to_process):
    
    if not task_to_process or not current_task_in_project:
        return {
        'tasks_to_add': get_batch_records(task_to_process, TASK_ADD_UPDATE_BATCH_SIZE) if not current_task_in_project else [],
        'tasks_to_update': [],
        'task_to_skip': []
    }

    task_to_add = []
    task_to_update = []
    task_to_skip= []
    for task in task_to_process:
        taskcode = task['taskcode']
        task_name = task['taskname']
        
        # In case of child task first check if parent task is present
        if task.get('parent_taskcode'):
            parent_taskcode = task['parent_taskcode']
            parent_task_details = rail.find_first_by_attr_and_get_attr(
                current_task_in_project, "task_code", parent_taskcode)
            if not parent_task_details:
                task_to_skip.append({
                    "task": task, "message": f"WorkPackageID {parent_taskcode} is not present for Workitem {taskcode}"})
                continue
            task_name = f"{parent_task_details['task_name']}|{task_name}"
        # For the task level1 or level2 first check with task_code
        task_details = rail.find_first_by_attr_and_get_attr(
            current_task_in_project, "task_code", taskcode)
        
        # This is an additional check if full_path is present
        # if not task_details:
        #     task_details = rail.find_first_by_attr_and_get_attr(
        #         current_task_in_project, "full_task_name", task_name)
        if task_details:
            if can_update_task(task_details, task):
                task['uri'] = task_details['uri']
                task_to_update.append(task)
                continue
            task_to_skip.append({
                "task": task, "message": "No change is received for the task"})
            continue
        task_to_add.append(task)

    return {
        'tasks_to_add': get_batch_records(task_to_add, TASK_ADD_UPDATE_BATCH_SIZE),
        'tasks_to_update': get_batch_records(task_to_update, TASK_ADD_UPDATE_BATCH_SIZE),
        'task_to_skip': task_to_skip
    }

def map_task_success_error(all_task_name, task_id, action, _type):
    task_add_update_result = rail.result(task_id)
    task_list = rail.result(all_task_name)[_type]
    task_list = list(chain.from_iterable(task_list))
    res = []
    for idx, task_res in enumerate(task_add_update_result):
        task_detail = task_list[idx]
        status = "Success"
        msg = f"Task {action}ed Successfully"
        if task_res.get("error"):
            msg = ";".join(list(set([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])))
            if msg in ('A task with this name already exists.', 'The specified Task already exists.'):
                status = "Exception"
                msg = "Task was skipped since the specified Task name already exists with the different task code."
            else:
                status = "Error"
        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_list[idx])
    return res

@lru_cache(maxsize=16)
def get_all_data_from_json_artifact(artifact_name):
    return rail.load_all_records(artifact_name)

def get_project_team_members_uris(dag_run):
    resp = {
        'resource_uris' : [],
        'log_messages': []
    }
    payload_data = rail.result('format_payload_tasks')
    
    message = []

    # filter distinct costcenters
    costcenters = []
    for _, val in payload_data['cost_center_resource'].items():
        costcenters.extend(val)
    costcenters = list(set(costcenters))

    # below task steps is added to tackle the logging part when logging that cencenter is not present 
    # at that time need task information against which task it is not found
    costcenter_mapping = {}

    for task_code, cost_centers in payload_data['cost_center_resource'].items():
        if task_code in payload_data['task_code_and_name']:
            for cost_center in cost_centers:
                costcenter_mapping[cost_center] = task_code

    for cc in costcenters:
        uri = rail.find_first_by_attr_and_get_attr(
            get_all_data_from_json_artifact(dag_run.conf['get_all_costcenters']), 'code', cc, 'uri'
        )
        if not uri:
            if cc in costcenter_mapping:
                task_code = costcenter_mapping[cc]
                message.append({
                    'task_name': payload_data['task_code_and_name'].get(task_code),
                    'task_code': task_code,
                    'message': f"CostCenter {cc} is not present in Replicon"
                })
            continue
        resp['resource_uris'].append(uri)

    # filter distinct employees
    employee_ids = []
    for _, val in payload_data['resources'].items():
        employee_ids.extend(val)
    employee_ids = list(set(employee_ids))

    # below task steps is added to tackle the logging part when logging that cencenter is not present 
    # at that time need task information against which task it is not found
    empl_mapping = {}

    for task_code, users in payload_data['resources'].items():
        if task_code in payload_data['task_code_and_name']:
            for users in users:
                empl_mapping[users] = task_code

    for empl_id in employee_ids:
        uri = rail.find_first_by_attr_and_get_attr(
            get_all_data_from_json_artifact(dag_run.conf['get_all_users_data']), 'employeeid', empl_id, 'uri'
        )
        if not uri:
            if empl_id in empl_mapping:
                task_code = empl_mapping[empl_id]
                message.append({
                    'task_name': payload_data['task_code_and_name'].get(task_code),
                    'task_code': task_code,
                    'message': f"StaffedEmployee {empl_id} is disabled or not present in Replicon"
                })
            continue
        resp['resource_uris'].append(uri)
    if message:
        resp['log_messages'] = message

    return resp

@lru_cache(maxsize=8)
def get_user_costcenter_to_add(dag_run, empl_id='', costcenter_code=''):
    if empl_id:
        return rail.find_first_by_attr_and_get_attr(
            get_all_data_from_json_artifact(dag_run.conf['get_all_users_data']), 'employeeid', empl_id, 'uri'
        )
    if costcenter_code:
        return rail.find_first_by_attr_and_get_attr(
            get_all_data_from_json_artifact(dag_run.conf['get_all_costcenters']), 'code', costcenter_code, 'uri'
        )
    return ''

def get_add_remove_resource(dag_run):
    response = {
        "resource_to_add": [],
        "resource_to_remove": []
    }

    payload = rail.result('format_payload_tasks')
    assigned_tasks = rail.result('resource_assigned')

    # Convert payload to lookup maps for quick access
    received_users_map = {
        task: set(users) for task, users in payload["resources"].items()
    }
    received_cc_map = {
        task: set(ccs) for task, ccs in payload["cost_center_resource"].items()
    }

    # Build assigned lookup map for quicker access in add checks
    assigned_lookup = {task['taskcode']: task for task in assigned_tasks}

    for task_code, task in assigned_lookup.items():
        received_users = received_users_map.get(task_code, set())
        received_ccs = received_cc_map.get(task_code, set())
        uris = []

        # Remove users
        for user in task.get("users", []):
            if user["employeeid"] not in received_users:
                uris.append(user['uri'])

        # Remove cost centers
        for cc in task.get("costcenters", []):
            if cc["code"] not in received_ccs:
                uris.append(cc['uri'])
        if uris:
            response["resource_to_remove"].append({
                'task_uri': task['taskuri'],
                'uris': uris
            })

    resources_to_add = defaultdict(list)
    # Add users
    for task_code, received_users in received_users_map.items():
        uris = []
        assigned_users = {
            u["employeeid"]
            for u in assigned_lookup.get(task_code, {}).get("users", [])
        }
        for emp_id in received_users - assigned_users:
            uri = get_user_costcenter_to_add(dag_run, empl_id=emp_id)
            if uri:
                uris.append(uri)
        task_uri = assigned_lookup.get(task_code, {}).get("taskuri", "")
        if uris and task_uri:
            resources_to_add[task_uri].extend(uris)

    # Add cost centers
    for task_code, received_ccs in received_cc_map.items():
        uris = []
        assigned_ccs = {
            cc["code"]
            for cc in assigned_lookup.get(task_code, {}).get("costcenters", [])
        }
        for cc_code in received_ccs - assigned_ccs:
            uri = get_user_costcenter_to_add(dag_run, costcenter_code=cc_code)
            if uri:
                uris.append(uri)
        task_uri = assigned_lookup.get(task_code, {}).get("taskuri", "")
        if uris and task_uri:
            resources_to_add[task_uri].extend(uris)

    for taskuri, resource_uris in resources_to_add.items():
        response['resource_to_add'].append({
            'task_uri': taskuri,
            'uris': resource_uris
        })

    return response


def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def get_status(item, logstatus):
    status = 'status' if item.get('status') else 'Status'
    return item[status].lower() == logstatus

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    projectlogs = dag_run.conf['projectlogs']
    otherlogs = dag_run.conf['otherlogs']

    if projectlogs:
        if isinstance(projectlogs, list):
            log_artifacts.extend(projectlogs)
        elif isinstance(projectlogs, str) and projectlogs[0] == '[':
            projectlogs = literal_eval(projectlogs)
            log_artifacts.extend(projectlogs)
        else:
            log_artifacts.append(projectlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        elif isinstance(otherlogs, str) and otherlogs[0] == '[':
            otherlogs = literal_eval(otherlogs)
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    def get_log_status(project_logs):
        available_status = list(
            map(lambda log: log['properties']['status'], project_logs))
        if "Error" in available_status:
            return "Error"
        if "Exception" in available_status:
            return "Exception"
        if "Skipped" in available_status:
            return "Skipped"
        return "Success"

    final_log_records = []

    project_task_codes = list(map(lambda x: {
        'project_task_code': f"{x['properties'].get('projectcode', '')}|{x['properties'].get('taskcode', '')}"
        }, log_records))

    final_data = list({f"{value['project_task_code']}": value for value in project_task_codes}.values())

    #pylint: disable=cell-var-from-loop
    for item in final_data:
        project_logs = list(
            filter(lambda x: 
                   (x['properties'].get('projectcode', '') == item['project_task_code'].split('|')[0]) and 
                   (x['properties'].get('taskcode', '') == item['project_task_code'].split('|')[1]), log_records))
        if len(project_logs) > 0:
            first = project_logs[0]
            final_log_records.append({
                'projectcode': first['properties']['projectcode'],
                'projectname': first['properties']['projectname'],
                'taskcode': first['properties']['taskcode'],
                'taskname': first['properties']['taskname'],
                'action': first['properties']['action'],
                'status': get_log_status(project_logs),
                "details":  '; '.join(list(set(map(lambda x: x['properties'].get('details'), project_logs)))),
                'ecid': first['ecid'],
            })

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: get_status(x, 'error'), final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: get_status(x, 'success'), final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: get_status(x, 'exception'), final_log_records ))))
    rail.set_result(key="skipped_record_count",val= len(list(filter(lambda x: get_status(x, 'skipped'), final_log_records ))))

    return  final_log_records

def get_permissions_to_assign(dag_run, config):
    billing_users = rail.result('format_payload_tasks')['billing_responsibles']
    users_data = rail.result('get_billing_responsible_users_data')
    message = []
    permissions_to_add = []

    for user_detail in billing_users:
        user = rail.find_first_by_attr_and_get_attr(
            users_data, 'employeeid', user_detail[0]
        )
        if not user:
            message.append({
                'task_name': user_detail[1],
                'task_code': user_detail[2],
                'message': f"BillingResponsible user {user_detail[0]} is disabled or not present in Replicon"
            })
            continue
        if not bool(rail.find_first_by_attr_and_get_attr(user['permission_sets'],
            "uri", dag_run.conf['supervisor_permissionuri'])):
            permissions_to_add.append({
                "userUri": user['uri'],
                "permissionSetUri": dag_run.conf['supervisor_permissionuri']
            })
        if not bool(rail.find_first_by_attr_and_get_attr(user['permission_sets'],
            "uri", dag_run.conf['end_user_with_report_edit_permissionuri'])):
            permissions_to_add.append({
                "userUri": user['uri'],
                "permissionSetUri": dag_run.conf['end_user_with_report_edit_permissionuri']
            })
    return {
        'permissions_to_add': permissions_to_add,
        'log_details': message
    }

def get_project_manager_permission_to_assign(dag_run):
    permission_sets = rail.result('get_user_info_on_empid')['permission_sets']
    user_uri = rail.result('get_user_info_on_empid')['uri']
    permissions_to_add = []
    if not bool(rail.find_first_by_attr_and_get_attr(permission_sets,
            "uri", dag_run.conf['project_manager_permissionuri'])):
            permissions_to_add.append({
                "userUri": user_uri,
                "permissionSetUri": dag_run.conf['project_manager_permissionuri']
            })
    if not bool(rail.find_first_by_attr_and_get_attr(permission_sets,
            "uri", dag_run.conf['end_user_with_report_edit_permissionuri'])):
            permissions_to_add.append({
                "userUri": user_uri,
                "permissionSetUri": dag_run.conf['end_user_with_report_edit_permissionuri']
            })
    return permissions_to_add
    
def get_payload_with_record_id(dag_run):
    response = []
    for indx, rec in enumerate(dag_run.conf['payload']['CommercialProject']):
        rec['record_id'] = indx
        response.append(rec)
    return rail.write_json_artifact(response) if response else response
    