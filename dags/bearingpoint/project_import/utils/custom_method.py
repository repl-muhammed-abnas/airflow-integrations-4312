import rail
from bearingpoint.project_import.utils.request_payload import does_wbs_exist

mandatory_fields = {
    "project_fields": {
        "RestrictTimePosting": "RestrictTimePosting",
        "ProjectID": "ProjectID",
        "ProjectName": "ProjectName",
        "StageDesc": "StageDesc",
        "StartDate": "StartDate",
        "EndDate": "EndDate",
        "CostCenter": "CostCenter",
        "CostCenterName": "CostCenterName",
        "OrgID": "OrgID",
        "OrgDesc": "OrgDesc",
        "ProjectCategory": "ProjectCategory",
        "SkipProjectManagerApproval": "SkipProjectManagerApproval",
        "WorkPackagename": "WorkPackagename",
        "WorkPackageID": "WorkPackageID",
        "WPAllowTimeEntry": "WPAllowTimeEntry"
    }
}

def get_missing_field(dag_run):
    not_present_fields = []
    for field in mandatory_fields['project_fields']:
        if dag_run.conf[field] in [None, '']:
            not_present_fields.append(field)
    not_present_fields = list(filter(None, not_present_fields))
    return {
        'fields': ";".join(not_present_fields),
        'valid_project': bool(not_present_fields)
    }

def check_project_fileds(dag_run):
    return {
        "projectcode": dag_run.conf['WorkPackageID'],
        "projectname": dag_run.conf['WorkPackagename'],
        "clientcode": dag_run.conf['Customer'],
        "taskname": '',
        "parenttaskname": '',
        'action': 'Validation',
        "details": get_missing_field(dag_run)['fields'] + " not present in the payload",
        "status": 'Skipped'
    }

def get_all_resources(dag_run):
    resource_ids = []

    project_resources = [{
        "employee_id": resource["ResourceID"],
        "role": resource["Role"],
        "role_name": resource["Rolename"],
        "resource_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_user_details")['all_users'], "employee_id", resource["ResourceID"],'uri')
        } for resource in dag_run.conf["Resource"]]
    
    task_resources = []

    for i in dag_run.conf["PurchaseOrder"]:
        for j in i["PurchaseOrderitem"]:
            for k in j["Resource"]:
                if "ResourceID" in k:
                    task_resources.append({
                        "employee_id": k["ResourceID"],
                        "role": k["Role"],
                        "role_name": k["Rolename"],
                        "resource_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_user_details")['all_users'], "employee_id", k["ResourceID"],'uri')
                    })

    resource_ids.extend(project_resources)
    resource_ids.extend(task_resources)

    return {
        'all_resources': [dict(t) for t in {tuple(d.items()) for d in resource_ids}],
        'task_resources': [dict(t) for t in {tuple(d.items()) for d in task_resources}] if dag_run.conf[
            "RestrictTimePosting"].lower() == 'n' else [dict(t) for t in {tuple(d.items()) for d in resource_ids if d['employee_id']}],
        'project_resources':  [dict(t) for t in {tuple(d.items()) for d in project_resources}]
    }

def get_users_permission():
    permissions_map = {}
    for item in rail.result("get_user_permission_details"):
        permissions_map.setdefault(item['uri'], []).append(item['permission'])

    combined_list = [{
        'employee_id': item['employee_id'],
        'uri': item['uri'],
        'permission': ','.join(permissions_map.get(item['uri'], []))
    } for item in rail.result("get_user_details")['project_users']]

    return combined_list

def get_child_conf(dag_run):
    return {
        ** dag_run.conf,
        "log": rail.result('create_log'),
        "project_id_oef_uri": rail.result("get_all_project_oef_details")['project_id_oef_uri'],
        "project_name_oef_uri": rail.result("get_all_project_oef_details")['project_name_oef_uri'],
        "project_category_oef_uri": rail.result("get_all_project_oef_details")['project_category_oef_uri'],
        "controlling_area_oef_uri": rail.result("get_all_project_oef_details")['controlling_area_oef_uri'],
        "service_center_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            "get_all_service_center_details"), "displayText", dag_run.conf['OrgDesc'], 'uri'),
        "cost_center_uri": rail.find_first_by_attr_and_get_attr(rail.result(
            "get_all_cost_center_details"), "displayText", dag_run.conf['CostCenterName'], 'uri'),
        "manager_uri": (rail.find_first_by_attr_and_get_attr(rail.result(
            "get_user_details")['all_users'],"employee_id", dag_run.conf['ProjManagerId'], 'uri')) if dag_run.conf['ProjManagerId'] else None,
        "co_manager_uri": (rail.find_first_by_attr_and_get_attr(rail.result(
            "get_user_details")['all_users'],"employee_id", dag_run.conf['ProjPartnerId'], 'uri')) if dag_run.conf['ProjPartnerId'] else None,
        "client_manager": (rail.find_first_by_attr_and_get_attr(rail.result(
            "get_user_details")['all_users'],"employee_id", dag_run.conf['ClientRepresentative'], 'uri')) if dag_run.conf['ClientRepresentative'] else None,
        "manager_permission_set": (rail.find_first_by_attr_and_get_attr(rail.result(
            "map_users_with_permission"),"employee_id", dag_run.conf['ProjManagerId'], 'permission')) if dag_run.conf['ProjManagerId'] else None,
        "co_manager_permission_set": (rail.find_first_by_attr_and_get_attr(rail.result(
            "map_users_with_permission"),"employee_id", dag_run.conf['ProjPartnerId'], 'permission')) if dag_run.conf['ProjPartnerId'] else None,
        "task_resource_list": [record for record in get_all_resources(dag_run)['task_resources'] if record['resource_uri']],
        "all_resources_list": [record for record in get_all_resources(dag_run)['all_resources']],
        "manager_permission_uri": rail.result("get_all_permission_sets")['manager_permission_uri'],
        "co_manager_permission_uri": rail.result("get_all_permission_sets")['co_manager_permission_uri'],
        "billing_rates": rail.result("get_all_billing_rates"),
        "project_resource_list": [record for record in get_all_resources(dag_run)['project_resources']]
    }

def format_logs_callable(dag_run):
    log_records = []
    log_records.extend(rail.load_all_records(dag_run.conf['logs']))

    final_log_records = list(map(lambda log: {
        **log['properties'],
        "ecid": log['ecid']
        }, log_records))

    rail.set_result(key="success_record_count", val=len(list(filter(lambda item: item['status'].lower() == 'success', final_log_records))))
    rail.set_result(key="error_record_count", val=len(list(filter(lambda item: item['status'].lower() == 'error', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(list(filter(lambda item: item['status'].lower() == 'exception', final_log_records))))

    return rail.write_json_artifact(final_log_records)

def get_tasks_to_process_data(dag_run):
    current_task_in_project = rail.result("get_all_tasks_for_project") or []
    task_to_process = dag_run.conf["PurchaseOrder"]
    #pylint:disable = too-many-arguments
    def get_task_payload(action, task_name, existing_tasks, item=None, parent_task_name=None, parent_task_uri=None,
                         status=None, resource=None, efforts=None, allow_time_entry=None, work_package = False):
        uri = None if status else existing_tasks.get(
            task_name + (' - ' + parent_task_name if parent_task_name else ''), {}).get('uri')

        if action == 'parent':
            return {
                "task_name": task_name,
                "uri": uri,
                "parent_task_name": '',
                "parent_task_uri": None,
                "resource_uri": resource,
                "start_date": None,
                "end_date": None,
                "efforts": efforts,
                "allow_time_entry": allow_time_entry,
                "status": "Update" if not status else "Add",
                "work_package": work_package
            }

        resource_uri = [uri for uri in [rail.find_first_by_attr_and_get_attr(dag_run.conf['task_resource_list'], 'employee_id',
                                                                              items['ResourceID'], 'resource_uri') for items in item['Resource']] if uri]
        return {
            "task_name": task_name,
            "uri": uri,
            "parent_task_name": parent_task_name or '',
            "parent_task_uri": parent_task_uri,
            "resource_uri": resource_uri,
            "start_date": item['POStartDate'],
            "end_date": item['POEndDate'],
            "efforts": item['POEffort'],
            "allow_time_entry": item['POAllowTimeEntry'],
            "status": "Update" if not status else "Add",
            "work_package": work_package
        }

    def create_tasks_to_add(existing_tasks, task_to_process, project_resource_uris):
        tasks_to_add = []

        # Add parent task for the work package
        tasks_to_add.append(get_task_payload('parent', dag_run.conf['WorkPackageID'], existing_tasks, status="Add",
                                             resource=project_resource_uris, efforts=dag_run.conf['Effort'],
                                             allow_time_entry=dag_run.conf['WPAllowTimeEntry'],work_package = True))

        # Add parent tasks for all purchase orders
        for order in task_to_process:
            tasks_to_add.append(get_task_payload('parent', order['PurchaseorderID'], existing_tasks, status="Add"))

            # Add child tasks for all purchase order items
            for item in order['PurchaseOrderitem']:
                tasks_to_add.append(get_task_payload('child', item['POItemID'], existing_tasks, item,
                                                     order['PurchaseorderID'], None, status="Add"))
        return tasks_to_add

    def create_tasks_to_update(existing_tasks, task_to_process, project_resource_uris):
        tasks_to_add = []
        tasks_to_update = []

        # Update parent task for the work package
        tasks_to_update.append(get_task_payload('parent', dag_run.conf['WorkPackageID'], existing_tasks,
                                                 resource=project_resource_uris, efforts=dag_run.conf['Effort'],
                                                 allow_time_entry=dag_run.conf['WPAllowTimeEntry'], work_package = True))

        # Update or add parent tasks for purchase orders
        for order in task_to_process:
            purchase_order_id = order['PurchaseorderID']
            if purchase_order_id in existing_tasks and not existing_tasks[purchase_order_id]['parent_task_name']:
                tasks_to_update.append(get_task_payload('parent', purchase_order_id, existing_tasks))
            else:
                tasks_to_add.append(get_task_payload('parent', purchase_order_id, existing_tasks, status="Add"))

            # Add or update child tasks for purchase order items
            for item in order['PurchaseOrderitem']:
                task_name = item['POItemID']
                parent_task_name = purchase_order_id
                parent_task_uri = existing_tasks.get(purchase_order_id, {}).get('uri', None)
                task_key = task_name + ' - ' + parent_task_name

                if task_key in existing_tasks and purchase_order_id in existing_tasks:
                    tasks_to_update.append(get_task_payload('child', task_name, existing_tasks, item,
                                                            parent_task_name, parent_task_uri))
                else:
                    tasks_to_add.append(get_task_payload('child', task_name, existing_tasks, item,
                                                         parent_task_name, parent_task_uri, status="Add"))

        return tasks_to_add, tasks_to_update

    existing_tasks = {task['task_name'] + (' - ' + task['parent_task_name'] if task['parent_task_name'] else ''): task
                      for task in current_task_in_project}

    project_resource_uris = [uri for uri in [
        rail.find_first_by_attr_and_get_attr(dag_run.conf['project_resource_list'], 'employee_id', resource['ResourceID'],
                                             'resource_uri') for resource in dag_run.conf['Resource'] if resource['ResourceID']] if uri]

    if not existing_tasks:
        tasks_to_add = create_tasks_to_add(existing_tasks, task_to_process, project_resource_uris)
        return {'tasks_to_add': tasks_to_add, 'tasks_to_update': []}

    tasks_to_add, tasks_to_update = create_tasks_to_update(existing_tasks, task_to_process, project_resource_uris)
    return {'tasks_to_add': tasks_to_add, 'tasks_to_update': tasks_to_update}

def map_task_success_error(task_id, action, _type):
    task_add_update_result = rail.result(task_id)
    task_list = rail.result("get_all_task_to_add_update")[_type]
    res = []
    for idx, task_res in enumerate(task_add_update_result):
        task_detail = task_list[idx]
        status = "Success"
        msg = f"Task {action} Successfully"
        if task_res.get("error"):
            msg = ";".join([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])
            status = "Error"
        task_detail['status'] = status
        task_detail['details'] = msg
        res.append(task_list[idx])
    return res

def get_project_log_message(dag_run):
    message = ["Project Updated Successfully" if does_wbs_exist() else "Project Added Successfully"]
    if not dag_run.conf['CustomerName'] or not dag_run.conf['Customer']:
        message.append('client is not synced with the project since client details are not present in the payload')
    if not dag_run.conf['manager_uri']:
        message.append('manager is not assinged to project since manager is not available in replicon')
    if not dag_run.conf['co_manager_uri']:
        message.append('co_manager is not assinged to project since co_manager is not available in replicon')
    if not dag_run.conf['service_center_uri']:
        message.append('service center is not assinged to project since service center is not available in replicon')
    if not dag_run.conf['cost_center_uri']:
        message.append('cost center is not assinged to project since cost center is not available in replicon')
    return {
        "projectcode": dag_run.conf["WorkPackageID"],
        "projectname": dag_run.conf["WorkPackagename"],
        "clientcode": dag_run.conf["Customer"],
        "taskcode": '',
        "taskname": '',
        "parenttaskname": '',
        "action": "Update" if does_wbs_exist() else "Add",
        "status": "Success",
        "details": ','.join(message),
    }
