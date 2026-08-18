from hashlib import md5
import rail


null = None


def add_encoding(item):
    if not item:
        return []
    return {**item, "encoded": md5((",".join(str(v) for v in item.values() if v is not None)).encode()).hexdigest()}


def check_for_project_updates(dag_run):
    project_details = rail.result("get_project_details")
    for i in project_details:
        if i != "uri" and i in dag_run.conf and project_details[i] != dag_run.conf[i]:
            return True


def check_for_task_updates(dag_run):
    return list(
        set(dag_run.conf["task_list"]) - set(rail.result("get_project_task_details"))
    )


def parse_project_response(response):
    """Safely parse project details response with null checks"""
    if not response or not response[0].get("projectDetails"):
        return null

    project = response[0]["projectDetails"]

    # Safe custom fields parsing
    custom_fields = {
        "categorization": "",
        "deliverydate": "",
        "estimatedengineeringcost": "",
        "estimatedengineeringhours": "",
        "estimatedpmcost": "",
        "estimatedpmhours": "",
        "projectvalue": "",
        "type": "",
        "underwarranty": "",
    }
    for field in response[0].get("customFields", []):
        if field and field.get("customField") and field.get("customField").get("name"):
            field_name = field["customField"]["name"].lower().replace(" ", "")
            custom_fields[field_name] = field.get("text", "")

    # Safe nested object access
    def safe_nested_get(obj, *keys):
        """Navigate nested dict safely"""
        for key in keys:
            if obj and isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return None
        return obj
    return {
        "uri": project.get("uri"),
        "startdate":project.get("timeEntryDateRange",{}).get("startDate"),
        "enddate":project.get("timeEntryDateRange",{}).get("endDate"),
        **custom_fields,
        "budgethours": safe_nested_get(project, "budgetedHours", "hours"),
        "budgetcost": safe_nested_get(project, "budgetedCost", "amount"),
        "projectmanageruri": safe_nested_get(project, "projectLeader", "uri"),
        "clients":project["clients"][0]["client"]["name"] if project.get("clients") else null
    }

def get_project_manager_data_handler(response, dag_run):
    res = response.get("rows", [])
    if not res:
        return None

    user_list = list(filter(lambda item: dag_run.conf.get("projectmanager") == item['name'],
        map(
            lambda user: {
                "uri": user["cells"][0]["uri"],
                "name": " ".join(
                    part.strip() for part in reversed(user["cells"][0]["textValue"].split(","))
                ),
            },
            res,
        )
    ))
    return user_list[0]['uri'] if user_list else None
