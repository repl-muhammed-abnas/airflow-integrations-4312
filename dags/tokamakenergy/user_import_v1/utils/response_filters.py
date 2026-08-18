from datetime import datetime
from airflow.exceptions import AirflowException
from functools import lru_cache
import rail

null = None
EFFECTIVE_DATE_FORMAT_BAMBOOHR = '%Y-%m-%d'

# Cached rail.result() wrapper functions
@lru_cache(maxsize=128)
def get_bamboohr_all_employees_data():
    return rail.load_all_records(rail.result("bamboohr_all_employees_data"))

@lru_cache(maxsize=128)
def get_bamboohr_get_employee_datasets_fields():
    return rail.result("filter_required_employee_fields")

@lru_cache(maxsize=128)
def get_enabled_department_groups_cached():
    return rail.result("get_enabled_department_groups")

@lru_cache(maxsize=128)
def get_enabled_employee_type_groups_cached():
    return rail.result("get_enabled_employee_type_groups")

@lru_cache(maxsize=128)
def get_jobgrade_table_records_cached():
    return rail.result("get_jobgrade_table_records")

@lru_cache(maxsize=128)
def get_job_table_records_cached():
    return rail.result("get_job_table_records")

@lru_cache(maxsize=128)
def get_employment_table_records_cached():
    return rail.result("get_employment_table_records")

@lru_cache(maxsize=128)
def get_all_user_oefs_cached():
    return rail.result("get_all_user_oefs")

def filtered_groups_data(dag_run, groupdata, groupname):
    result = []
    previous_group = null
    group_data = dag_run.conf["user_details"][groupdata]
    for record in group_data:
        if record[groupname] != previous_group:
            result.append(record)
            previous_group = record[groupname]
    return result


def get_final_group_payload(records, groupname, grouplist_from_replicon):
    result = []
    if not records or not records["rows"]:
        return []
    group_rows = records["rows"]
    group_rows.sort(key=lambda x: datetime.strptime(x['date'], EFFECTIVE_DATE_FORMAT_BAMBOOHR))

    all_employees_data = get_bamboohr_all_employees_data()

    for record in group_rows:
        if groupname == "reportsTo":
            if record["reportsTo"] != null and record["reportsTo"] != "":
                result.append({
                    "supervisor_name": record["reportsTo"],
                    "supervisor_empid": rail.find_first_by_attr_and_get_attr(all_employees_data,
                        "displayname", record["reportsTo"], "employeenumber"),
                    "date": record["date"]
                })
        else:
            if record[groupname] != null and record[groupname] != "":
                result.append({
                    groupname: record[groupname],
                    "date": record["date"],
                    "uri": rail.find_first_by_attr_and_get_attr(grouplist_from_replicon, "displayText", record[groupname], "uri", "")
                })
    return result

def get_required_employee_datasets_fields(response, required_employee_fields):
    # Create a mapping using both label and parent name for precise field lookup
    response_fields = response.get("fields", [])
    
    # Iterate through mapper and find matching fields in the response
    matched_fields = []
    missing_fields = []
    
    for field_map in required_employee_fields:
        bamboohr_name = field_map["bamboohr_name"]
        parent_name = field_map["parent_name"]
        
        # Find field that matches both label and parent name
        matching_field = next((field for field in response_fields 
                              if field.get("label") == bamboohr_name 
                              and field.get("parentName") == parent_name), None)
        
        if matching_field:
            matched_fields.append({
                "bamboohr_name": bamboohr_name,
                "bamboohr_field": matching_field.get("name"),
                "field_attr": field_map["field_attr"]
            })
        else:
            missing_fields.append(f"{bamboohr_name} (parent: {parent_name})")
    
    # Raise exception if any required fields are missing
    if missing_fields:
        missing_fields_str = ", ".join(missing_fields)
        raise AirflowException(f"Required employee fields missing from BambooHR response: {missing_fields_str}")
    
    return matched_fields

def get_filtered_employees_details(response, data_type, jobgrade_effective_date_field):
    required_employee_fields = get_bamboohr_get_employee_datasets_fields()

    def map_employee_item(item):
        employee_data = {}

        if data_type == "All":
            # Only process specific fields for "All" data type
            all_required_fields = ["id", "firstname", "lastname", "employeenumber", "startdate", "workemail", "status"]
            for field_map in required_employee_fields:
                bamboo_field = field_map["bamboohr_field"]
                field_attr = field_map["field_attr"]

                if field_attr in all_required_fields and bamboo_field in item:
                    employee_data[field_attr] = item[bamboo_field]

            employee_data["displayname"] = f'{employee_data.get("firstname", "")} {employee_data.get("lastname", "")}'.strip()
        else:
            # Process all fields for other data types
            for field_map in required_employee_fields:
                bamboo_field = field_map["bamboohr_field"]
                field_attr = field_map["field_attr"]

                if bamboo_field in item:
                    if field_attr == "hourlyrate" and item[bamboo_field]:
                        rate_parts = item[bamboo_field].split(" ")
                        employee_data["hourlyrate"] = rate_parts[0] if rate_parts else ""
                        employee_data["hourlyratecurrency"] = rate_parts[-1] if len(rate_parts) > 1 else ""
                    elif field_attr == "department" and item[bamboo_field]:
                        employee_data[field_attr] = item[bamboo_field]
                        employee_data["department_uri"] = rail.find_first_by_attr_and_get_attr(
                            get_enabled_department_groups_cached(), "displayText", item[bamboo_field], "uri", "")
                    elif field_attr == "employmentstatus" and item[bamboo_field]:
                        employee_data[field_attr] = item[bamboo_field]
                        employee_data["employmentstatus_uri"] = rail.find_first_by_attr_and_get_attr(
                            get_enabled_employee_type_groups_cached(), "displayText", item[bamboo_field], "uri", "")
                    elif field_attr == "reportsto" and item[bamboo_field]:
                        employee_data[field_attr] = item[bamboo_field]
                        all_employees_data = get_bamboohr_all_employees_data()
                        employee_data["supervisor_empid"] = rail.find_first_by_attr_and_get_attr(
                            all_employees_data, "displayname", item[bamboo_field], "employeenumber")
                    else:
                        employee_data[field_attr] = item[bamboo_field]

            jobgrade_records = get_jobgrade_table_records_cached()
            jobgrade_employees = jobgrade_records.get("employees", {})
            jobgrade_employee_data = jobgrade_employees.get(str(employee_data["id"])) if jobgrade_employees else None
            jobgradedata = jobgrade_employee_data["rows"] if jobgrade_employee_data else []
            if jobgradedata:
                jobgradedata.sort(key=lambda x: datetime.strptime(x[jobgrade_effective_date_field], EFFECTIVE_DATE_FORMAT_BAMBOOHR) if x[jobgrade_effective_date_field] else datetime.min)

            job_table_result = get_job_table_records_cached()
            job_table_employees = job_table_result.get("employees", {})
            job_table_records = job_table_employees if job_table_employees else {}
            employment_table_result = get_employment_table_records_cached()
            employment_table_employees = employment_table_result.get("employees", {})
            employment_table_records = employment_table_employees if employment_table_employees else {}
            
            employee_data.update({
                "departmentgroupdata": (get_final_group_payload(job_table_records.get(str(employee_data["id"])), "department",
                    get_enabled_department_groups_cached()) if job_table_records.get(str(employee_data["id"])) else []),
                "employmenttypedata": (get_final_group_payload(employment_table_records.get(str(employee_data["id"])), "employmentStatus",
                    get_enabled_employee_type_groups_cached()) if employment_table_records.get(str(employee_data["id"])) else []),
                "supervisorsdata": (get_final_group_payload(job_table_records.get(str(employee_data["id"])), "reportsTo", "")
                    if job_table_records.get(str(employee_data["id"])) else []),
                "costratedata": list(map(lambda job_grade: {
                    "hourlyrate": job_grade.get("customHourlyRateCard", "").split(" ")[0] if job_grade.get("customHourlyRateCard") else "",
                    "hourlyratecurrency": job_grade.get("customHourlyRateCard", "").split(" ")[-1] if job_grade.get("customHourlyRateCard") else "",
                    "date": job_grade.get(jobgrade_effective_date_field)
                }, filter(lambda job_grade: job_grade["customHourlyRateCard"] is not null and job_grade["customHourlyRateCard"] != "", jobgradedata)))
            })

        return employee_data

    mapped_employees = list(map(map_employee_item, response['data'])) if response and response['data'] else []

    return mapped_employees if data_type == "All" else list(filter(lambda x: x.get('timesheetuser') == "Yes", mapped_employees))

def get_enabled_departments(response):
    return list(map(lambda department_details: {
        "displayText": department_details["cells"][0]["textValue"],
        "uri": department_details["cells"][0]["uri"]
    }, filter(lambda department_details: department_details["cells"][1]["boolValue"], response["rows"])))

def get_oef_dropdown_value_uri(response, oefname, oefurikey, bamboohr_field):
    oef_tags = list(map(lambda oef_values: {
            "value": oef_values["displayText"],
            "uri": oef_values["uri"]
        }, filter(lambda oef_values: oef_values["definition"]["uri"] == get_all_user_oefs_cached()[oefurikey], response)))
    return {
        "oefname": oefname,
        "oefuri": get_all_user_oefs_cached()[oefurikey],
        "bamboohr_field": bamboohr_field,
        "oeftags": oef_tags
    }

def get_required_permission_set_uris(response, supervisor_permission_set):
    return list(map(lambda permission_set: permission_set["uri"],
        filter(lambda permission_set: permission_set["displayText"] in supervisor_permission_set, response)))

def get_group_value(data, key):
    if not data:
        return {}
    return data[0].get(key, {}).get(key, {}) if data[0][key] else {}

def get_effective_user_groupmembership_filter(response):
    group_list = ['department', 'employeeType']
    for group in group_list:
        rail.set_result(key=group.lower(), val=get_group_value(
            response.get(f'{group}s'), group))
