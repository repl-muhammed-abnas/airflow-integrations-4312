import rail
from mercury_systems_inc.project_import_v1.utils.request_payload import get_project_data, does_wbs_exist


def format_logs_callable():
    final_log_records = rail.load_all_records(rail.result("create_log"))
    rail.set_result(key="total_record_count", val=len(final_log_records))
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['Status'].lower() == 'exception', final_log_records))))
    return rail.write_json_artifact(final_log_records)


def get_task_to_add_update_skip():
    current_task_in_project = rail.result('get_all_tasks_for_project') if bool(
        rail.result('get_all_tasks_for_project')) else []
    task_to_process = rail.load_all_records(
        rail.result("get_project_data_from_query"))

    for task in task_to_process:
        if "child_tasks" in task:
            task_parts = task["child_tasks"].split(".")
            task["task_level"] = len(task_parts) - 1

    if not task_to_process or not current_task_in_project:
        sorted_tasks = sorted(task_to_process, key=lambda t: t.get(
            "task_level", 1)) if task_to_process else []
        return {
            'tasks_to_add': sorted_tasks,
            'tasks_to_update': []
        }

    task_to_add = []
    task_to_update = []

    for task in task_to_process:
        task_details = rail.find_first_by_attr_and_get_attr(
            current_task_in_project, "task_name", task['task_code'])

        if task_details:
            task['uri'] = task_details['uri']
            task_to_update.append(task)
            continue

        task_to_add.append(task)

    task_to_add = sorted(task_to_add, key=lambda t: t.get("task_level", 1))
    task_to_update = sorted(
        task_to_update, key=lambda t: t.get("task_level", 1))

    return {
        'tasks_to_add': task_to_add if task_to_add else task_to_add,
        'tasks_to_update': task_to_update if task_to_update else task_to_update
    }


def map_task_success_error(task_id, action, _type):
    task_add_update_result = rail.result(task_id)
    task_list = rail.result("get_all_task_to_add_update")[_type]
    res = []
    for idx, task_res in enumerate(task_add_update_result):
        task_detail = task_list[idx]
        status = "Success"
        msg = "Task added Successfully" if action == 'add' else "Task Updated Successfully"
        checker = False
        if task_res.get("error"):
            msg = ";".join([error.get('displayText')
                           for error in task_res.get("error").get('notifications')])
            status, checker = ("Exception", True) if msg in ('A task with this name already exists.', 'The specified Task already exists.') else ("Error", False)
        task_detail['status'] = status
        task_detail['details'] = msg if not checker else "Task was skipped since the specified Task name already exists with the different task code."
        res.append(task_list[idx])
    return res


def get_all_required_department_uris(dag_run, dept_mapper):
    # Fetch project metadata (team departments and program)
    project_data = get_project_data()
    team_departments = set(project_data['team_departments'].split(","))
    program = project_data['program']

    # Load the full department hierarchy structure from the DAG run configuration
    department_data = rail.load_json_artifact(
        dag_run.conf['depaprtment_details'])

    result = []  # Final list to hold all required child department URIs

    def collect_children(children):
        return [
            {
                "name": child.get("department", {}).get("name"),
                "uri": child.get("department", {}).get("uri"),
                "code": child.get("department", {}).get("code")
            }
            for child in children
        ]

    # Traverse the department hierarchy: Level 1 -> Level 2 -> Level 3
    for level1 in department_data.get("childDepartments", []):
        level1_code = level1.get("department", {}).get("code")

        # Skip if level1 is not part of the specified team_departments
        if level1_code not in team_departments:
            continue

        for level2 in level1.get("childDepartments", []):
            for level3 in level2.get("childDepartments", []):
                l3_dept = level3.get("department", {})
                l3_code = l3_dept.get("code")

                if program == 'OVERHEAD':
                    # For OVERHEAD, collect all level4 children except those under a level3 with code "NONE"
                    if l3_code != "None":
                        result.extend(collect_children(
                            level3.get("childDepartments", [])))
                else:
                    # For other programs, collect level4 children only if level3 code is in the valid list
                    valid_l3_codes = dept_mapper.get(program, [])
                    if l3_code in valid_l3_codes:
                        result.extend(collect_children(
                            level3.get("childDepartments", [])))

    return result


def get_exception_message(program_mapper):
    exception_list = []
    exception_list.append("Project Updated Successfully") if does_wbs_exist(
    ) else exception_list.append("Project Added Successfully")
    if rail.result("is_project_manager_available") == "is_new_project":
        exception_list.append("Project Manager is not available in replicon")
    if get_project_data()['program'] not in program_mapper:
        exception_list.append(
            "Team Member assignment is skipped, since the received program is not valid")

    return "; ".join(exception_list)
