from airflow.exceptions import AirflowException

def get_updated_employees_details(response, required_employee_fields, all_mapper_fields=None):
    result = []
    for item in response.get('data', []):
        employee = {}

        # First, process fields from BambooHR response
        for field_data in required_employee_fields:
            # Use bamboohr_field from the matched fields (from fields API)
            value = item.get(field_data.get("bamboohr_field", ""), "")

            # Handle special case for payroll number
            if field_data["field_attr"] == "payrollnumber_oef" and not value:
                value = "000000"

            # Handle date fields - convert MySQL default date '0000-00-00' to empty string
            if field_data["type"] == "date" and value == "0000-00-00":
                value = ""

            # Convert boolean values to lowercase string "true"/"false" for SQL compatibility
            if field_data["type"] == "boolean" and isinstance(value, bool):
                value = "true" if value else "false"

            employee[field_data["field_attr"]] = value

        # If all_mapper_fields provided, add any missing fields with empty values
        if all_mapper_fields:
            bamboohr_field_attrs = {field["field_attr"] for field in required_employee_fields}
            for field_data in all_mapper_fields:
                if field_data["field_attr"] not in bamboohr_field_attrs:
                    # Add missing field with empty value
                    employee[field_data["field_attr"]] = ""

        result.append(employee)
    return result

def get_all_project_roles(response):
    return [{
        "project_role_name": roles_data["displayText"],
        "uri": roles_data["uri"]
    } for roles_data in response]

def get_all_oef_tags(response):
    return [{
        "oef_tag": oef_data["name"],
        "code": oef_data["code"],
        "description": oef_data["description"],
        "is_enabled": oef_data["isEnabled"],
        "uri": oef_data["uri"]
    } for oef_data in response["tags"]]


def get_required_permission_set_uris(response, supervisor_permission_set):
    return list(map(lambda permission_set: permission_set["uri"],
        filter(lambda permission_set: permission_set["displayText"] in supervisor_permission_set, response)))

def get_current_effective_groups(response):
    def get_group_data(group_type, singular_key):
        data = response.get(group_type)
        if not data:
            return None, None
        group = (data[0].get(singular_key) or {}).get(singular_key) or {}
        return group.get('uri'), group.get('displayText')

    groups = ['location', 'serviceCenter', 'department', 'costCenter', 'employeeType', 'division']

    result = {}
    for group in groups:
        uri, name = get_group_data(f"{group}s", group)
        key_prefix = f"existing{group.lower()}"
        result[f"{key_prefix}uri"] = uri
        result[f"{key_prefix}name"] = name

    return result

def get_required_employee_datasets_fields(response, required_employee_fields):
    """
    Match BambooHR fields API response with required fields mapper.
    Returns list of matched fields with bamboohr_field (API field name) added.
    """
    response_fields = response.get("fields", [])

    matched_fields = []
    missing_mandatory_fields = []

    for field_map in required_employee_fields:
        bamboohr_name = field_map["bamboohr_name"]
        parent_name = field_map["parent_name"]

        # Try to find field with exact parent match
        matching_field = next((field for field in response_fields
                              if field.get("label") == bamboohr_name
                              and field.get("parentName") == parent_name), None)

        # If not found and parent is "Custom", try to find with any parent
        if not matching_field and parent_name == "Custom":
            matching_field = next((field for field in response_fields
                                  if field.get("label") == bamboohr_name), None)

        if matching_field:
            matched_fields.append({
                **field_map,
                "bamboohr_field": matching_field["name"]
            })
        elif field_map["mandatory"]:
            # Only track missing mandatory fields
            missing_mandatory_fields.append(f"{bamboohr_name} (parent: {parent_name})")

    # Only raise exception if mandatory fields are missing
    if missing_mandatory_fields:
        missing_fields_str = ", ".join(missing_mandatory_fields)
        raise AirflowException(f"Required mandatory employee fields missing from BambooHR response: {missing_fields_str}")

    return matched_fields
