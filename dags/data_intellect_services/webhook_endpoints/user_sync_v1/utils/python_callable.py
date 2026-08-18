from data_intellect_services.user_sync_v1.mapper.update_fields_mapper_hibob import employee_details_update_fields
from data_intellect_services.user_sync_v1.mapper.update_fields_mapper_hibob import employee_work_update_fields
from data_intellect_services.user_sync_v1.mapper.update_fields_mapper_hibob import employee_contract
from data_intellect_services.user_sync_v1.mapper.non_readable_columns import fields
import rail

null = None

def get_updated_basic_fields(dag_run):
    updated_fields = dag_run.conf["webhook"]["data"]
    employee_basic_updates = {}
    for update_path in employee_details_update_fields:
        for field_name in updated_fields["data"]["fieldUpdates"]:
            if update_path["path"] == field_name["path"]:
                employee_basic_updates[update_path["value"]] = field_name["newValue"]
                break
            employee_basic_updates[update_path["value"]] = null
    if any(field in employee_basic_updates and employee_basic_updates[field]
        for field in list(map(lambda data: data["value"], employee_details_update_fields))):
        return {
            "id": updated_fields["employee"]["id"],
            "employee_id": null,
            "action": "Update",
            "type": "Employee Update",
            "firstname": employee_basic_updates["firstname"],
            "lastname": employee_basic_updates["lastname"],
            "displayname": employee_basic_updates["displayname"],
            "email": null,
            "startdate": employee_basic_updates["startdate"],
            "enddate": employee_basic_updates["enddate"],
            "status": employee_basic_updates["status"],
            "is_manager": null,
            "effective_date": null,
            "team": null,
            "workstream": null,
            "site": null,
            "supervisor": null,
            "supervisor_emp_id": null,
            "title": null,
            "primary_role": null,
            "resource_pool": null,
            "department": null,
            "contract": null,
            "emp_type": null,
            "effectivedateemptype": null,
            "timestamp": updated_fields["creationDate"]
        }
    return null

def get_emp_work_created_or_updated_fields(dag_run):
    updated_fields = dag_run.conf["webhook"]["data"]
    required_keys = employee_work_update_fields
    employee_update_fields = {}
    for key in required_keys:
        if key in updated_fields.keys():
            if key == "effectiveDate":
                employee_update_fields[key] = updated_fields[key]
        if key in updated_fields["data"].keys():
            if key == "reportsTo":
                employee_update_fields[key] = updated_fields["data"][key]
            if key == "customColumns":
                if fields["team"].split(".")[1] in updated_fields["data"][key].keys():
                    employee_update_fields["team"] = updated_fields["data"][key][fields["team"].split(".")[1]]
                if fields["workstream"].split(".")[1] in updated_fields["data"][key].keys():
                    employee_update_fields["workstream"] = updated_fields["data"][key][fields["workstream"].split(".")[1]]
                if fields["primary_role"].split(".")[1] in updated_fields["data"][key].keys():
                    employee_update_fields["primary_role"] = updated_fields["data"][key][fields["primary_role"].split(".")[1]]
            employee_update_fields[key] = updated_fields["data"][key]
        if "contract" in updated_fields["data"].keys() and "type" in updated_fields["data"].keys():
            employee_update_fields["contract"] = updated_fields["data"]["contract"]
            employee_update_fields["type"] = updated_fields["data"]["type"]
    if all(field in employee_update_fields for field in required_keys):
        return {
            "id": updated_fields["employeeId"],
            "employee_id": null,
            "action": "Update",
            "type": "Work Create Update",
            "firstname": null,
            "lastname": null,
            "displayname": null,
            "email": null,
            "startdate": null,
            "enddate": null,
            "status": null,
            "is_manager": null,
            "effective_date": employee_update_fields["effectiveDate"] if employee_update_fields["effectiveDate"] else null,
            "team": employee_update_fields["team"] if employee_update_fields["customColumns"] else null,
            "workstream": employee_update_fields["workstream"] if employee_update_fields["customColumns"] else null,
            "site": employee_update_fields["siteId"] if employee_update_fields["siteId"] else null,
            "supervisor": employee_update_fields["reportsTo"]["id"] if employee_update_fields["reportsTo"] else null,
            "supervisor_emp_id": null,
            "title": employee_update_fields["title"] if employee_update_fields["title"] else null,
            "primary_role": employee_update_fields["primary_role"] if employee_update_fields["customColumns"] else null,
            "resource_pool": employee_update_fields["primary_role"] if employee_update_fields["customColumns"] else null,
            "department": employee_update_fields["department"] if employee_update_fields["department"] else null,
            "contract": null,
            "emp_type": null,
            "effectivedateemptype": null,
            "timestamp": updated_fields["creationDate"]
        }
    if all(field in employee_update_fields for field in employee_contract):
        return {
            "id": updated_fields["employeeId"],
            "employee_id": null,
            "action": "Update",
            "type": "Contract Create Update",
            "firstname": null,
            "lastname": null,
            "displayname": null,
            "email": null,
            "startdate": null,
            "enddate": null,
            "status": null,
            "is_manager": null,
            "effective_date": null,
            "team": null,
            "workstream": null,
            "site": null,
            "supervisor": null,
            "supervisor_emp_id": null,
            "title": null,
            "primary_role": null,
            "resource_pool": null,
            "department": null,
            "contract": employee_update_fields["contract"] if employee_update_fields["contract"] else null,
            "emp_type": employee_update_fields["type"] if employee_update_fields["type"] else null,
            "effectivedateemptype": employee_update_fields["effectiveDate"] if employee_update_fields["effectiveDate"] else null,
            "timestamp": updated_fields["creationDate"]
        }
    return null

def log_update_payload(dag_run):
    updated_fields = dag_run.conf["webhook"]["data"]
    if updated_fields["type"] in ["employee.updated", "table.entry.created", "table.entry.updated"]:
        if updated_fields["type"] == "employee.updated":
            return get_updated_basic_fields(dag_run)
        if updated_fields["type"] in ["table.entry.created", "table.entry.updated"]:
            return get_emp_work_created_or_updated_fields(dag_run)
    return null

def log_create_payload(dag_run):
    updated_fields = dag_run.conf["webhook"]["data"]
    hibob_user_details = rail.result("get_user_details_from_hibob")
    is_custom_column_not_blank = updated_fields["data"]["work"].get("customColumns") is not null
    return {
        "id": updated_fields["employee"]["id"],
        "employee_id": updated_fields["data"]["work"]["employeeIdInCompany"],
        "action": "Create",
        "type": "Employee Create",
        "firstname": updated_fields["data"]["firstName"],
        "lastname": updated_fields["data"]["surname"],
        "displayname": updated_fields["data"]["displayName"],
        "email": updated_fields["data"]["email"],
        "startdate": updated_fields["data"]["work"].get("startDate"),
        "enddate": null,
        "status": null,
        "is_manager": updated_fields["data"]["work"].get("isManager"),
        "effective_date": updated_fields["data"]["work"].get("activeEffectiveDate"),
        "team": updated_fields["data"]["work"].get("customColumns", {}).get(fields["team"].split(".")[1])
            if is_custom_column_not_blank else null,
        "workstream": updated_fields["data"]["work"].get("customColumns", {}).get(fields["workstream"].split(".")[1])
            if is_custom_column_not_blank else null,
        "site": updated_fields["data"]["work"].get("siteId"),
        "supervisor": updated_fields["data"]["work"].get("reportsTo", {}).get("id"),
        "supervisor_emp_id": updated_fields["data"]["work"].get("reportsToIdInCompany"),
        "title": updated_fields["data"]["work"].get("title"),
        "primary_role": updated_fields["data"]["work"].get("customColumns", {}).get(fields["primary_role"].split(".")[1])
            if is_custom_column_not_blank else null,
        "resource_pool": updated_fields["data"]["work"].get("customColumns", {}).get(fields["primary_role"].split(".")[1])
            if is_custom_column_not_blank else null,
        "department": updated_fields["data"]["work"].get("department"),
        "contract": hibob_user_details["payroll"]["employment"].get("contract"),
        "emp_type": hibob_user_details["payroll"]["employment"].get("type"),
        "effectivedateemptype": null,
        "timestamp": updated_fields["creationDate"]
    }
