import functools
import rail

null = None

def filter_required_department(response):
    field_data = rail.result("get_human_readable_data_from_hibob")
    all_departments = list(map(lambda rows: {
        "full_path": "/".join(list(map(lambda cell_collection: cell_collection["textValue"],
        rows["cells"][0]["cellCollection"]))),
        "uri": rows["cells"][0]["cellCollection"][-1]["uri"]
    }, response["rows"]))
    user_department_full_path = "/".join(list(filter(lambda employment: employment, ['Data Intellect Services Limited', field_data["department"],
        field_data["cost_center"], field_data["team"]])))
    for department_full_path in all_departments:
        if department_full_path["full_path"] == user_department_full_path:
            return {
                "department_full_path": department_full_path["full_path"],
                "uri": department_full_path["uri"]
            }
    return null

def filter_required_employee_type(response, user_details):
    field_data = rail.result("get_human_readable_data_from_hibob")
    all_employee_types = list(map(lambda rows: {
        "full_path": "/".join(list(map(lambda cell_collection: cell_collection["textValue"],
        rows["cells"][0]["cellCollection"]))),
        "uri": rows["cells"][0]["cellCollection"][-1]["uri"]
    }, response["rows"]))
    if user_details["action"] == "Update":
        employee_contract_list = [field_data["contract"], field_data["emp_type"]]
    elif user_details["action"] == "Create":
        employee_contract_list = [user_details["contract"], user_details["emp_type"]]
    else:
        return null
    employee_type_full_path = "/".join(list(filter(lambda employment: employment, employee_contract_list)))
    for employee_type in all_employee_types:
        if employee_type["full_path"] == employee_type_full_path:
            return {
                "employee_type_full_path": employee_type["full_path"],
                "uri": employee_type["uri"]
            }
    return null

def get_specific_field_data(field, required_human_readable_data, config):
    field_data = list(map(lambda data: data["values"], filter(lambda data: config.non_readable_columns.get(field)==data["name"],
        required_human_readable_data)))
    return field_data[0] if field_data else []

def get_attribute_value(dataset, field):
    required_value = list(map(lambda data: data["value"], filter(lambda data: str(data["id"]) == field, dataset)))
    return required_value[0] if required_value else null

functools.lru_cache(maxsize=128)
def get_human_readable_data_from_api(response, config):
    required_keys = (config.non_readable_columns).values()
    required_human_readable_data = [response.get(key) for key in required_keys]
    return {
        "title_data": get_specific_field_data("title", required_human_readable_data, config),
        "primary_role_data": get_specific_field_data("primary_role", required_human_readable_data, config),
        "team_data": get_specific_field_data("team", required_human_readable_data, config),
        "cost_center_data": get_specific_field_data("workstream", required_human_readable_data, config),
        "department_data": get_specific_field_data("department", required_human_readable_data, config),
        "location_data": get_specific_field_data("site", required_human_readable_data, config),
        "emp_contract_data": get_specific_field_data("contract", required_human_readable_data, config),
        "emp_type_data": get_specific_field_data("emp_type", required_human_readable_data, config)
    }

def filter_human_readable_data(config):
    dag_run = rail.get_current_context()['dag_run']
    readable_data = rail.result("get_named_lists_data_from_hibob")
    user_details = dag_run.conf["user_details"]
    return {
        "title": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["title_data"],
            user_details["title"]),
        "primary_role": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["primary_role_data"],
            user_details["primary_role"]),
        "team": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["team_data"], user_details["team"]),
        "cost_center": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["cost_center_data"],
            user_details["workstream"]),
        "department": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["department_data"],
            user_details["department"]),
        "location": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["location_data"], user_details["site"])
    }

def filter_human_readable_data_for_update(config):
    dag_run = rail.get_current_context()['dag_run']
    readable_data = rail.result("get_named_lists_data_from_hibob")
    emp_work_created_or_updated_fields = dag_run.conf["user_details"]
    return {
        "title": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["title_data"],
            emp_work_created_or_updated_fields["title"]),
        "primary_role": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["primary_role_data"],
            emp_work_created_or_updated_fields["primary_role"]),
        "team": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["team_data"],
            emp_work_created_or_updated_fields["team"]),
        "cost_center": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["cost_center_data"],
            emp_work_created_or_updated_fields["workstream"]),
        "department": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["department_data"],
            emp_work_created_or_updated_fields["department"]),
        "supervisor": rail.result("get_supervisor_details_from_hibob")["work"]["employeeIdInCompany"]
            if emp_work_created_or_updated_fields["supervisor"] else null,
        "location": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["location_data"],
            emp_work_created_or_updated_fields["site"]),
        "contract": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["emp_contract_data"],
            emp_work_created_or_updated_fields["contract"]),
        "emp_type": get_attribute_value(get_human_readable_data_from_api(readable_data, config)["emp_type_data"],
            emp_work_created_or_updated_fields["emp_type"]),
    }

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {}) if data[0][key] else {}

def get_effective_user_groupmembership_filter(response):
    group_list = ['location', 'department', 'costCenter', 'employeeType']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))
