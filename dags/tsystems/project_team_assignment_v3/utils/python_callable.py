import json
import rail
from datetime import datetime
from rail import get_current_context
from functools import reduce

def get_formatted_event_data(dag_run):
    create_response = rail.result('get_create_event_data')
    update_response = rail.result('get_update_event_data')

    # Combine create + update responses (newline separated JSON lines)
    combined_response = ""
    if create_response:
        combined_response += create_response.strip()
    if update_response:
        if combined_response:
            combined_response += "\n"
        combined_response += update_response.strip()

    # Parse JSON lines
    if combined_response:
        try:
            objects = list(map(json.loads, combined_response.split("\n")))
        except json.JSONDecodeError as e:
            rail.set_result(key="log", val={
                "severity": "Exception",
                "message": f"Failed to parse JSON response: {str(e)}",
                "missing_field": "valid_json_format"
            })
            return json.dumps([])
    else:
        objects = []

    ### --- For QA purpose only, to simulate data in trial instance --- ###
    if dag_run and dag_run.conf and dag_run.conf.get("instance") == "trial":
        objects = dag_run.conf.get("data")
    ### ----------------------------------------------------------------###

    def validate_and_transform_object(obj, index):
        missing_fields = []

        resource_item = obj.get("data", {}).get("resourceReservationItem")
        if not resource_item:
            if obj.get("data") is None:
                missing_fields.append("data")
            missing_fields.append("No data in payload")
            return None, missing_fields

        related_entity = resource_item.get("relatedEntity")
        related_party = resource_item.get("relatedParty")
        valid_for = resource_item.get("validFor")

        if not isinstance(related_entity, list):
            missing_fields.append("Project data missing in payload")
        if not isinstance(related_party, list):
            missing_fields.append("Employee data missing in payload")
        if not valid_for:
            missing_fields.append("Datetime field data missing in payload")

        if valid_for:
            missing_fields.extend([
                f"Datetime field data missing in payload for {field}"
                for field in ["startDateTime", "endDateTime"]
                if field not in valid_for or not valid_for.get(field) or valid_for.get(field).strip() == ""
            ])

        cost_object_id = individual_id = project_id = None

        if isinstance(related_entity, list):
            entities = {
                e.get("@referredType", "").lower(): e.get("id")
                for e in related_entity if isinstance(e, dict)
            }
            cost_object_id = entities.get("costobject")
            project_id = entities.get("project")

        if isinstance(related_party, list):
            individual_id = next(
                (p.get("id") for p in related_party
                 if isinstance(p, dict) and p.get("@referredType", "").lower() == "individual"),
                None
            )

        if not cost_object_id or (isinstance(cost_object_id, str) and cost_object_id.strip() == ""):
            missing_fields.append("Project data missing in payload")
        if not individual_id or (isinstance(individual_id, str) and individual_id.strip() == ""):
            missing_fields.append("Employee data missing in payload")

        if missing_fields:
            return None, missing_fields

        return {
            # "object_id": obj.get("id"),
            "decidalo_project_id": project_id,
            "cost_object_id": cost_object_id,
            "individual_id": individual_id,
            "search_period_start": valid_for["startDateTime"],
            "search_period_end": valid_for["endDateTime"],
            "assignment_id": resource_item.get("id"),
        }, []

    # Validate every object
    results = [validate_and_transform_object(obj, i) for i, obj in enumerate(objects)]
    transformed_data = [r[0] for r in results if r[0] is not None]
    validation_errors = [
        {"index": i, "assignment_id": objects[i].get("data", {}).get("resourceReservationItem", {}).get("id", f"object_{i}"), "missing_fields": r[1]}
        for i, r in enumerate(results) if r[1]
    ]

    # === Enhanced logging for invalid records ===
    if validation_errors:
        detailed_errors = []
        for err in validation_errors:
            obj = objects[err["index"]]
            resource_item = (obj.get("data") or {}).get("resourceReservationItem", {})
            valid_for = resource_item.get("validFor") or {}

            project_id = cost_object_id = individual_id = None
            if isinstance(resource_item.get("relatedEntity"), list):
                entities = {
                    e.get("@referredType", "").lower(): e.get("id")
                    for e in resource_item["relatedEntity"]
                    if isinstance(e, dict)
                }
                cost_object_id = entities.get("costobject")
                project_id = entities.get("project")

            if isinstance(resource_item.get("relatedParty"), list):
                individual_id = next(
                    (p.get("id")
                     for p in resource_item["relatedParty"]
                     if isinstance(p, dict)
                     and p.get("@referredType", "").lower() == "individual"),
                    None
                )

            detailed_errors.append({
                "assignment_id": err["assignment_id"],
                "decidalo_project_id": project_id,
                "cost_object_id": cost_object_id,
                "individual_id": individual_id,
                "search_period_start": valid_for.get("startDateTime"),
                "search_period_end": valid_for.get("endDateTime"),
                "status": "Exception",
                "details": err["missing_fields"]
            })

        # Log everything (return value remains unchanged)
        rail.set_result(key="log", val={
            "message": f"Missing mandatory fields in {len(detailed_errors)} event object(s)",
            "validation_errors": detailed_errors
        })

    # === Existing logic for valid records ===
    if transformed_data:
        def aggregate_records(acc, record):
            key = (record["decidalo_project_id"], record["cost_object_id"], record["individual_id"], record["assignment_id"])
            if key not in acc:
                acc[key] = record.copy()
            else:
                acc[key]["search_period_start"] = min(acc[key]["search_period_start"], record["search_period_start"])
                acc[key]["search_period_end"] = max(acc[key]["search_period_end"], record["search_period_end"])
            return acc

        return json.dumps(list(reduce(aggregate_records, transformed_data, {}).values()), indent=2)

    # No valid data
    return json.dumps([])

def get_project_data_from_response(response):
    if response and response[0].get('projectDetails'):
        pd = response[0]['projectDetails']
        return {
            "project_uri": pd.get('uri', ''),
            "project_billingtype": pd.get('billingType', {}).get('displayText', '')
        }
    return {}

def get_user_data_from_response(response):
    return [{
        'user_uri': item.get('userDetails', {}).get('uri', ''),
        'permission_set_uris': [ps.get('uri', '') for ps in item.get('permissionSets', [])]
    } for item in response] if response else []

def get_user_assignment_data(response):
    if not response:
        return {}

    # Safe access to nested properties with null checks
    project_assignment_date_range = response.get('projectAssignmentDateRange', {})
    billing_rates_allowed = response.get("billingRatesAllowedForBillingTime") or []
    default_billing_rate = response.get("defaultBillingRate")

    # Extract Project Rate URI
    project_rate_list = list(
        map(lambda x: x["billingRate"]["uri"],
            filter(
                lambda x: x.get("billingRate", {}).get("displayText") == "Project Rate",
                billing_rates_allowed if isinstance(billing_rates_allowed, list) else []
            )
        )
    )
    project_rate_uri = project_rate_list[0] if project_rate_list else []

    return {
        "assigned_start_date": project_assignment_date_range.get('startDate'),
        "assigned_end_date": project_assignment_date_range.get('endDate'),
        "billing_rate_uris": list(
            map(lambda x: x["billingRate"]["uri"],
                filter(
                    lambda x: x.get("billingRate", {}).get("displayText") != "Project Rate",
                    billing_rates_allowed if isinstance(billing_rates_allowed, list) else []
                )
            )
        ),
        "default_billing_rate_uri": (
            default_billing_rate.get("uri")
            if default_billing_rate and default_billing_rate.get("displayText") != "Project Rate"
            else None
        ),
        "project_rate_uri": project_rate_uri
    }

def extract_capacity_date_range():
    data = rail.result('get_per_day_capacity_from_api')

    ### --- For QA purpose only, to simulate data in trial instance --- ###
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("instance") == "trial":
        data = dag_run.conf.get("data")
    ### ----------------------------------------------------------------###

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return {"start_date": None, "end_date": None}
    
    if not data or not data.get("capacity"):
        return {"start_date": None, "end_date": None}

    dates = [(datetime.fromisoformat(item["applicableTimePeriod"]["validFor"]["startDateTime"].replace('Z', '+00:00')).date(),
              datetime.fromisoformat(item["applicableTimePeriod"]["validFor"]["endDateTime"].replace('Z', '+00:00')).date())
             for item in data["capacity"]]

    return {
        "start_date": min(d[0] for d in dates) if dates else None,
        "end_date": max(d[1] for d in dates) if dates else None
    }


def compare_assignment_date_range():
    data = rail.result('get_user_assigned_to_project')
    capacity_range = extract_capacity_date_range()

    assigned_start = data.get("assigned_start_date")
    assigned_end = data.get("assigned_end_date")

    data_start = datetime(assigned_start["year"], assigned_start["month"], assigned_start["day"]).date() if assigned_start else None
    data_end = datetime(assigned_end["year"], assigned_end["month"], assigned_end["day"]).date() if assigned_end else None

    start_dates = [capacity_range.get('start_date'), data_start]
    end_dates = [capacity_range.get('end_date'), data_end]

    return {
        "start_date": min(filter(None, start_dates)) if any(start_dates) else None,
        "end_date": max(filter(None, end_dates)) if any(end_dates) else None
    }


def get_assign_team_from_for_the_project_data(response):
    if not response:
        return {}
    data = response[0] if isinstance(response, list) else response
    
    specified_display_texts = {"External Contractors", "External Freelancer", "External Manual", "External services"}
    
    result = {}
    for key, items in data.items():
        if isinstance(items, list):
            if key == 'employeeTypeGroups':
                # For employee types, only keep displayText as 'name' of those NOT in specified list
                filtered_items = [
                    {"name": item.get('displayText')} 
                    for item in items 
                    if isinstance(item, dict) and item.get('displayText') not in specified_display_texts
                ]
                result[key] = filtered_items
            else:
                # For all other keys, keep original logic
                result[key] = [{"uri": item.get('uri') if isinstance(item, dict) else item} for item in items]
    
    return result

def get_capacity_items_for_trigger(dag_run):
    data = rail.result('get_per_day_capacity_from_api')

    ### --- For QA purpose only, to simulate data in trial instance --- ###
    if dag_run and dag_run.conf and dag_run.conf.get("instance") == "trial":
        data = dag_run.conf.get("data")
        if isinstance(data, dict):
            return json.dumps(data.get("capacity", []))
    ### ----------------------------------------------------------------###

    if isinstance(data, str) and data.strip().startswith('{'):
        parsed_data = json.loads(data)
        return json.dumps(parsed_data.get("capacity", []) if isinstance(parsed_data, dict) else [])
    return json.dumps([])

def do_format_logs(dag_run):
    log_artifacts = []

    child_logs = dag_run.conf.get('childlogs', [])
    other_logs = dag_run.conf.get('otherlogs', [])

    # normalize to list
    if isinstance(child_logs, list):
        log_artifacts.extend(child_logs)
    elif child_logs:
        log_artifacts.append(child_logs)

    if isinstance(other_logs, list):
        log_artifacts.extend(other_logs)
    elif other_logs:
        log_artifacts.append(other_logs)

    # flatten any nested lists safely
    flat_logs = []
    for logs in log_artifacts:
        if logs:
            flat_logs.extend(logs if isinstance(logs, list) else [logs])
    log_artifacts = flat_logs

    # load records from each artifact
    log_records = []
    for log in log_artifacts:
        records = rail.load_all_records(log)
        if records:
            log_records.extend(records)

    # format records
    final_log_records = [
        {
            **log.get('properties', {}),
            'jobid': log.get('ecid', '')
        }
        for log in log_records
    ]

    # set counts by status
    for status in ['Error', 'Success', 'Exception']:
        rail.set_result(
            key=f"{status.lower()}_record_count",
            val=sum(1 for x in final_log_records if x.get('status') == status)
        )

    return final_log_records

def chunk_child_logs(batch_size):
    """Split gathered child-log artifacts into batches of at most ``batch_size``
    to bound per-run memory, always returning at least one (possibly empty) batch.
    """
    child_logs = rail.result('gather_each_event_logs') or []
    if not isinstance(child_logs, list):
        child_logs = [child_logs]

    chunks = [child_logs[i:i + batch_size] for i in range(0, len(child_logs), batch_size)] or [[]]
    rail.set_result(key='total_parts', val=len(chunks))
    return chunks


def build_part_log_filename(log_filename, part_index, total_parts):
    """Suffix the log filename with ``_<n>`` (1-based part number) when split across
    multiple child runs; a single batch keeps the original filename unchanged.
    """
    if total_parts <= 1:
        return log_filename
    name, dot, ext = log_filename.rpartition('.')
    base = name if dot else log_filename
    suffix = f"_{part_index + 1}"
    return f"{base}{suffix}.{ext}" if dot else f"{base}{suffix}"


def check_both_tasks_failed(check_only_504=False):

    ### --- For QA purpose only, to simulate data in trial instance --- ###
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("instance") == "trial":
        return True
    ### ----------------------------------------------------------------###

    create_error = rail.result('get_create_event_data', "error")
    update_error = rail.result('get_update_event_data', "error")

    is_success = lambda error: error is None
    is_504 = lambda error: error and "504:Gateway Timeout" in error.get("exc_message", "")

    if check_only_504:
        return not(is_504(create_error)) or not(is_504(update_error))

    return (
        (is_success(create_error) and is_success(update_error)) or
        (is_success(create_error) and is_504(update_error)) or
        (is_504(create_error) and is_success(update_error)) or
        (is_504(create_error) and is_504(update_error))
    )

def map_user_permission_set(response):
    if not response:
        return []

    return list(map(lambda item: item['permissionSet']['uri'], response))


def get_updated_daterange_from_blob():

    blob_data = rail.result('get_assignment_id_data_from_blob')
    dag_run = rail.get_dag_run_conf()

    # Helper function to parse ISO datetime string to date object
    parse_iso_date = lambda date_str: datetime.fromisoformat(date_str.replace('Z', '+00:00')).date() if date_str else None

    # Extract blob dates
    blob_json = json.loads(blob_data['jsonValue']) if blob_data and blob_data.get('jsonValue') else []
    blob_item = blob_json[0] if blob_json and isinstance(blob_json, list) and blob_json else {}

    # Parse all dates in one go
    dates = {
        'blob_start': parse_iso_date(blob_item.get('assignment_search_period_start')),
        'blob_end': parse_iso_date(blob_item.get('assignment_search_period_end')),
        'conf_start': parse_iso_date(dag_run.get('search_period_start')),
        'conf_end': parse_iso_date(dag_run.get('search_period_end'))
    }

    # Filter out None values and compute min/max
    start_dates = [d for d in [dates['blob_start'], dates['conf_start']] if d]
    end_dates = [d for d in [dates['blob_end'], dates['conf_end']] if d]

    # Convert date objects back to ISO datetime strings
    min_start = min(start_dates) if start_dates else None
    max_end = max(end_dates) if end_dates else None

    return {
        "search_period_start": f"{min_start.isoformat()}T00:00:00.000Z" if min_start else None,
        "search_period_end": f"{max_end.isoformat()}T00:00:00.000Z" if max_end else None
    }

def get_all_tasks_uris_for_project(response):
    if response:
        return list(map(lambda x: x['uri'], response))
    return []

def check_allocation_details_available():
    """Validate that allocation details are available with required applicableTimePeriod data."""
    data = rail.result('get_per_day_capacity_from_api')

    # Override with trial data if applicable
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("instance") == "trial":
        data = dag_run.conf.get("data")

    # Parse JSON string if needed
    if isinstance(data, str):
        data = json.loads(data) if data else None

    # Validate: data exists, has capacity items, and all have applicableTimePeriod
    capacity_items = data.get("capacity", []) if data else []

    if not capacity_items or not all(item.get("applicableTimePeriod") for item in capacity_items):
        rail.set_result(key="log", val={
            "severity": "Exception",
            "message": "The Allocation details are not available in resource reservation"
        })
        return False

    return True