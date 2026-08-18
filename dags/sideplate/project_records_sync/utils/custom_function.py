import rail
from datetime import datetime, timedelta, timezone

def last_sync_time(last_sync_var):
    sync_time = (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    return rail.get_lastsync_time_variable(
        variable_name= last_sync_var,
        date_format='%Y-%m-%dT%H:%M:%SZ',
        initial_sync_time= sync_time,
        reset_after_threshold=False
        )

def update_last_sync(update_sync_time):
    return rail.set_lastsync_time_variable(
            variable_name= update_sync_time,
            value_to_set= rail.result('get_last_sync_time')['current_time']
        )

def get_formatted_date(input_date):
    if input_date:
        dt = datetime.strptime(input_date, "%Y-%m-%d")

        return {
            "date": {
                "year": dt.year,
                "month": dt.month,
                "day": dt.day
            }
        }

def validate_all_child_dags_succeeded(child_dag_runs):
    """
    Validate that all child DAG runs completed successfully.

    Args:
        child_dag_runs: List of child DAG run information from WaitForDagRunsSensor

    Returns:
        True if all succeeded

    Raises:
        Exception if any child DAG failed
    """
    if not child_dag_runs:
        return True

    failed_runs = []
    for dag_run in child_dag_runs:
        state = dag_run.get('state', 'unknown')
        if state not in ['success', 'skipped']:
            failed_runs.append({
                'dag_id': dag_run.get('dag_id'),
                'run_id': dag_run.get('run_id'),
                'state': state
            })

    if failed_runs:
        raise Exception(f"Child DAG runs failed: {failed_runs}")

    return True


def check_if_rows_are_present(client_data):
    datatype = []
    if client_data['rows']:
        datatype.append(client_data['rows'][0])
        return datatype
    else:
        return datatype 

def create_accumulate_items_to_searchoutput_list(client_data):
    searchoutput = []
    if client_data['rows'][0]['cells']:
        output_data = {
            "name": client_data['rows'][0]['cells'][0]['textValue'] if client_data['rows'][0]['cells'][0]['textValue'] else None,
            "uri": client_data['rows'][0]['cells'][0]['uri'] if client_data['rows'][0]['cells'][0]['uri'] else None,
            "code": client_data['rows'][0]['cells'][1]['textValue'] if client_data['rows'][0]['cells'][1]['textValue'] else None
        }
        searchoutput.append(output_data)
    return searchoutput
        
def get_client_uri(client_data):
    client_list = []
    if client_data[0]["uri"]:
        client_list.append(client_data[0]["uri"])
        return client_list
    else:
        return client_list

def create_accumulate_items_to_project_resource_list(client_data):
    if client_data['rows'][0]['cells']:
        output_data = [
            {"uri": row["cells"][0]["uri"], "name": row["cells"][0]["textValue"]}
            for row in client_data["rows"]
            if row["cells"] and "textValue" in row["cells"][0]
        ]

    return output_data

def collect_resource_uris(resource_details):
    if not resource_details:
        return []
    return [item["uri"] for item in resource_details if item.get("uri")]

def accumulate_items_to_subtasks_list(task_hierarchy):
    task_uris = []
    if not task_hierarchy:
        return task_uris

    def collect(nodes):
        for node in nodes:
            task_uri = node.get("task", {}).get("uri")
            if task_uri:
                task_uris.append(task_uri)
            collect(node.get("childTasks") or [])

    collect(task_hierarchy)
    return task_uris

def create_accumulate_items_to_project_search_output_list(project_data):
    searchoutput = []
    if project_data['rows'][0]['cells']:
        output_data = {
            "name": project_data['rows'][0]['cells'][0]['textValue'] if project_data['rows'][0]['cells'][0]['textValue'] else None,
            "uri": project_data['rows'][0]['cells'][0]['uri'] if project_data['rows'][0]['cells'][0]['uri'] else None,
            "code": project_data['rows'][0]['cells'][1]['textValue'] if project_data['rows'][0]['cells'][1]['textValue'] else None
        }
        searchoutput.append(output_data)
    return searchoutput

def check_if_argument_is_present(project_data, argument=None):
    output_list = []
    if not project_data.get(f"{argument}"):
        return False
    return bool(project_data.get(argument))
            
def get_an_eligible_project_leader(leader_data, project_data):
    email = project_data.get("Fee_Req_Resp_Email__c")
    if not email:
        return None
    match = next((u for u in leader_data if u.get("user", {}).get("loginName") == email),None)
    return match["uri"] if match else None

def project_status_check(project_data):
    status = project_data.get("MPM4_BASE_Status__c")
    if not status:
        return False
    return bool(status)

def extract_client_uri_from_dag_run():
    log_search_client_code = rail.result("log_search_client_code")
    if log_search_client_code:
        return rail.result("search_client_code")["rows"][0]["cells"][0]["uri"]
    
    create_client2_result = rail.result("create_client2")
    if create_client2_result is not None:
        return create_client2_result.get("uri")

    create_client_result = rail.result("create_client")
    if create_client_result is not None:
        return create_client_result.get("uri")

    search_output = rail.result("accumulate_items_to_searchoutput_list")
    if search_output:
        return search_output[0].get("uri")

    return None

def extract_new_project_uri3():
    create_project_result = rail.result("create_project")
    if create_project_result is not None:
        return create_project_result.get("uri")

    create_project2_result = rail.result("create_project2")
    if create_project2_result is not None:
        return create_project2_result.get("uri")

    return None

def extract_new_project_uri():
    create_project_result = rail.result("create_project")
    if create_project_result is not None:
        return create_project_result.get("uri")
    return None

def extract_new_project_uri2():
    create_project2_result = rail.result("create_project2")
    if create_project2_result is not None:
        return create_project2_result.get("uri")

    return None

def get_project_status_uri_by_display_text(display_text):
    status_data = rail.result("get_project_status")
    match = next((s for s in status_data if s.get("displayText") == display_text), None)
    if match is None:
        raise ValueError(f"No project status found with displayText '{display_text}'")
    return match["uri"]

def billing_type_fixed_bid_check(project_details):
    billing_type_uri = project_details["billingType"]["displayText"]
    if billing_type_uri == "Fixed Bid":
        return bool(billing_type_uri)
    return False
    
def check_if_uri_not_equals_task_based(project_details):
    if project_details and project_details['estimationMode']:
        if project_details['estimationMode']["uri"] != "urn:replicon:project-estimation-mode:task-based":
            return True
        else:
            return False

def check_code_equals_account(client_detail, project_detail):
    client_code = client_detail["code"]
    account_code = project_detail["Account__c"]
    if client_code != account_code:
        return True
    else:
        return False
    
def check_opp_billing_type_contains_fixed_bid(opportunity_data):
    if opportunity_data and opportunity_data["Opp_Billing_Type__c"] and opportunity_data["Opp_Billing_Type__c"] == "Fixed Bid":
        return True
    else:
        return False   

def check_opp_billing_type_contains_hourly(opportunity_data):     
    if opportunity_data and opportunity_data["Opp_Billing_Type__c"] and opportunity_data["Opp_Billing_Type__c"] == "Hourly":
        return True
    else:
        return False 
    
def check_if_argument_exists(project_data, argument=None):
    if project_data and project_data['records']:
        if project_data['records'][0].get(f"{argument}"):
            return bool(project_data['records'][0].get(f"{argument}"))
    else:
        return False
    
def check_if_field_uri_matches(object_extensioin, project_details, argument=None):
    tags = object_extensioin.get("tags", []) if isinstance(object_extensioin, dict) else object_extensioin
    target_name = argument
    target_uri = next(
            (item.get("uri") for item in tags if item.get("name") == target_name),
            None
        )
    if not target_uri:
        return False
    else:
        return bool(target_uri)

def get_field_uri(object_extension, project_details, argument=None):
    field_uri = object_extension
    tag_value = None

    if isinstance(field_uri, dict):
        if "updated" in field_uri.keys():
            tag_value = field_uri["updated"][0]["uri"]
        elif "tags" in field_uri.keys():
            tag_list = field_uri["tags"]
            tag_value = next(
                (item["uri"] for item in tag_list if item["name"] == argument),
                None
            )
    elif field_uri:
        tag_value = field_uri

    return tag_value


def get_list_for_object_extension(tag_defination):
    tags = tag_defination.get("tags", []) if isinstance(tag_defination, dict) else tag_defination
    tag_defination_list = []

    for item in tags:
        tag_defination_list.append({
            "target": {"uri": item.get("uri"), "slug": None, "tagName": None},
            "code": item.get("code"),
            "name": item.get("name"),
            "description": None,
            "isEnabled": "true"
        })

    return tag_defination_list

def check_if_user_exists(object_extension, full_name):
    tags = object_extension["tags"]
    user_uri = rail.find_first_by_attr_and_get_attr(tags, 'name', full_name, 'uri')
    return bool(user_uri)

def get_uri_if_user_exists(object_extension, full_name):
    tags = object_extension["tags"]
    user_uri = rail.find_first_by_attr_and_get_attr(tags, 'name', full_name, 'uri')
    return user_uri