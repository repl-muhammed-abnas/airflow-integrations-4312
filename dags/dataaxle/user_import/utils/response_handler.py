import rail


def get_custom_fields(response):
    custom_fields = list(map(lambda i: {
        "displayText": i["displayText"],
        "uri": i["uri"]
    }, response))
    return {
        "payroll_department_number_uri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Payroll Dept #",
            "uri"
        ),
        "payroll_department_uri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Payroll Department",
            "uri"
        ),
        "executive_level_uri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Executive level",
            "uri"
        ),
        "user_supervisor_name_uri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "User's Supervisor Name",
            "uri"
        )
    }

def get_employee_type_group(response):
    rows = response.get("rows", [])
    extracted_data = []
    for row in rows:
        extracted_data.append({
            "code": row["cells"][0]["textValue"],
            "text_value": row["cells"][1]["textValue"],
            "uri": row["cells"][1]["uri"]
        })
    return extracted_data