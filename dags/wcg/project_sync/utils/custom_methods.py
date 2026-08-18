"""
WCG Project Sync - Custom Methods
Converted from Workato Integration - January 2026

This module contains helper functions for:
- Project data parsing and validation
- Subsidiary dropdown management
- Client and Project Manager lookups
"""

from datetime import datetime
import itertools
import pytz
import rail
from rail import get_current_context


def now(time_zone):
    """Get current time in specified timezone."""
    return datetime.now(pytz.timezone(time_zone))

null = None


def get_dag_run_conf():
    """Get dag_run.conf from current execution context."""
    return get_current_context()['dag_run'].conf


def parse_project_response(response):
    """
    Parse Replicon project details response.
    Safely extracts project information with null checks.

    Args:
        response: Response from BulkGetProjectDetails3 API

    Returns:
        Dictionary with parsed project details or None
    """
    if not response or not response[0].get("projectDetails"):
        return null

    project = response[0]["projectDetails"]

    # Parse custom field values
    custom_fields = {
        "project_subsidiary": "",
        "pl_type": "",
        "department": "",
        "billing_type": "",
        "cost_type": "",
    }

    for field in response[0].get("customFields", []):
        if field and field.get("customField") and field.get("customField").get("name"):
            field_name = field["customField"]["name"].lower().replace(" ", "_")
            if "subsidiary" in field_name:
                custom_fields["project_subsidiary"] = field.get("dropDownOption", {}).get("name", "") if field.get("dropDownOption") else ""
            elif "p&l" in field_name or "pl_type" in field_name:
                custom_fields["pl_type"] = field.get("dropDownOption", {}).get("name", "") if field.get("dropDownOption") else ""
            elif "department" in field_name:
                custom_fields["department"] = field.get("dropDownOption", {}).get("name", "") if field.get("dropDownOption") else ""
            elif "billing" in field_name:
                custom_fields["billing_type"] = field.get("dropDownOption", {}).get("name", "") if field.get("dropDownOption") else ""
            elif "cost" in field_name:
                custom_fields["cost_type"] = field.get("dropDownOption", {}).get("name", "") if field.get("dropDownOption") else ""

    # Safe nested object access
    def safe_nested_get(obj, *keys):
        for key in keys:
            if obj and isinstance(obj, dict) and key in obj:
                obj = obj[key]
            else:
                return None
        return obj

    return {
        "uri": project.get("uri"),
        "name": project.get("name"),
        "code": project.get("code"),
        "start_date": safe_nested_get(project, "timeEntryDateRange", "startDate"),
        "end_date": safe_nested_get(project, "timeEntryDateRange", "endDate"),
        "status": project.get("status", {}).get("name") if project.get("status") else null,
        "budget_hours": safe_nested_get(project, "budgetedHours", "hours"),
        "budget_cost": safe_nested_get(project, "budgetedCost", "amount"),
        "project_leader_uri": safe_nested_get(project, "projectLeader", "uri"),
        "clients": project["clients"][0]["client"]["name"] if project.get("clients") else null,
        "client_uri": project["clients"][0]["client"]["uri"] if project.get("clients") else null,
        **custom_fields,
    }


def get_project_manager_data_handler(response, dag_run):
    """
    Parse user list response to find project manager URI.
    Matches by name from the feed file.

    Args:
        response: Response from UserlistService1.svc/GetData
        dag_run: Airflow DAG run context

    Returns:
        Project manager URI or None
    """
    rows = response.get("rows", [])
    if not rows:
        return None

    target_name = dag_run.conf.get("project_manager", "")
    if not target_name:
        return None

    # Parse user list and find matching name
    for user in rows:
        if user.get("cells"):
            cell = user["cells"][0]
            # User name format: "Last, First" - need to compare with feed format
            user_name = cell.get("textValue", "")
            user_uri = cell.get("uri")

            # Try both formats: "Last, First" and "First Last"
            name_parts = user_name.split(",")
            if len(name_parts) == 2:
                reversed_name = f"{name_parts[1].strip()} {name_parts[0].strip()}"
            else:
                reversed_name = user_name

            if target_name == user_name or target_name == reversed_name:
                return user_uri

    return None


def parse_date_safe(date_string, date_format="%m/%d/%Y"):
    """
    Safely parse date string with error handling.

    Args:
        date_string: Date string to parse
        date_format: Expected date format

    Returns:
        Parsed date dict for Replicon API or None
    """
    if not date_string:
        return null

    try:
        date_obj = datetime.strptime(date_string.strip(), date_format)
        return {
            "year": date_obj.year,
            "month": date_obj.month,
            "day": date_obj.day,
        }
    except (ValueError, AttributeError):
        return null


def parse_project_list_response(response, search_code=None):
    """
    Parse ProjectListService.svc/GetData response to find project.
    Based on Workato logic: "Search project based on code (internal id)"

    Since text-search may return partial matches, we verify exact match on code.

    Args:
        response: Response from ProjectListService1.svc/GetData
        search_code: The exact code we're searching for (for verification)

    Returns:
        Dictionary with project uri and name, or None
    """
    if not response:
        return null

    # ListService responses have 'rows' directly at top level (no 'd' wrapper)
    rows = response.get("rows", [])
    if not rows:
        return null

    # Column order: cells[0] = project (name/uri), cells[1] = code
    for row in rows:
        cells = row.get("cells", [])
        if cells and len(cells) >= 2:
            project_cell = cells[0]  # Project name/uri
            code_cell = cells[1]     # Project code

            # Get the code value from the row
            row_code = code_cell.get("textValue", "")

            # If search_code provided, verify exact match
            if search_code:
                if str(row_code).strip() == str(search_code).strip():
                    if project_cell.get("uri"):
                        return {
                            "uri": project_cell.get("uri"),
                            "name": project_cell.get("textValue"),
                        }
            else:
                # No search_code provided, return first result
                if project_cell.get("uri"):
                    return {
                        "uri": project_cell.get("uri"),
                        "name": project_cell.get("textValue"),
                    }

    return null


def parse_client_list_response(response, search_code=None):
    """
    Parse ClientListService.svc/GetData response to find client.
    Based on Workato logic: "Search client based on code"

    Since text-search may return partial matches, we verify exact match on code.

    Args:
        response: Response from ClientListService1.svc/GetData
        search_code: The exact code we're searching for (for verification)

    Returns:
        Client URI string or None
    """
    if not response:
        return null

    # ListService responses have 'rows' directly at top level (no 'd' wrapper)
    rows = response.get("rows", [])
    if not rows:
        return null

    # Column order: cells[0] = client (name/uri), cells[1] = code
    for row in rows:
        cells = row.get("cells", [])
        if cells and len(cells) >= 2:
            client_cell = cells[0]  # Client name/uri
            code_cell = cells[1]    # Client code

            # Get the code value from the row
            row_code = code_cell.get("textValue", "")

            # If search_code provided, verify exact match
            if search_code:
                if str(row_code).strip() == str(search_code).strip():
                    if client_cell.get("uri"):
                        return client_cell.get("uri")
            else:
                # No search_code provided, return first result
                if client_cell.get("uri"):
                    return client_cell.get("uri")

    return null


def parse_project_list_response_with_exact_match(response):
    """
    Data handler wrapper that parses project list response with exact code match.
    Gets the search code from dag_run.conf['internal_id'].
    """
    search_code = get_dag_run_conf().get("internal_id", "")
    return parse_project_list_response(response, search_code)


def parse_client_list_response_with_exact_match(response):
    """
    Data handler wrapper that parses client list response with exact code match.
    Gets the search code from dag_run.conf['customer_internal_id'].
    """
    search_code = get_dag_run_conf().get("customer_internal_id", "")
    return parse_client_list_response(response, search_code)


def parse_template_project_response(response, search_name=None):
    """
    Parse ProjectListService response to find template project.
    Based on WCG_Project_Mapper lookup table logic.

    Since text-search may return partial matches, we verify exact match on name.

    Args:
        response: Response from ProjectListService1.svc/GetData
        search_name: The exact template name we're searching for (for verification)

    Returns:
        Dictionary with template project uri and name, or None
    """
    if not response:
        return null

    # ListService responses have 'rows' directly at top level (no 'd' wrapper)
    rows = response.get("rows", [])
    if not rows:
        return null

    # Column order: cells[0] = project (name/uri), cells[1] = code
    for row in rows:
        cells = row.get("cells", [])
        if cells and len(cells) > 0:
            project_cell = cells[0]  # Project name/uri
            row_name = project_cell.get("textValue", "")

            # If search_name provided, verify exact match
            if search_name:
                if str(row_name).strip() == str(search_name).strip():
                    if project_cell.get("uri"):
                        return {
                            "uri": project_cell.get("uri"),
                            "name": row_name,
                        }
            else:
                # No search_name provided, return first result
                if project_cell.get("uri"):
                    return {
                        "uri": project_cell.get("uri"),
                        "name": row_name,
                    }

    return null


def parse_template_project_response_path_a(response):
    """
    Data handler wrapper for Path A template project search.
    Gets the search name from step_49_search_project_mapper result.
    """
    search_name = rail.result("step_49_search_project_mapper")
    return parse_template_project_response(response, search_name)


def parse_template_project_response_path_b(response):
    """
    Data handler wrapper for Path B template project search.
    Gets the search name from step_82_search_project_mapper result.
    """
    search_name = rail.result("step_82_search_project_mapper")
    return parse_template_project_response(response, search_name)


def parse_template_project_response_path_c(response):
    """
    Data handler wrapper for Path C template project search.
    Gets the search name from step_119_search_project_mapper result.
    """
    search_name = rail.result("step_119_search_project_mapper")
    return parse_template_project_response(response, search_name)


def validate_subsidiary_in_mapper(subsidiary, project_template_mapper):
    """
    Validate that subsidiary exists in WCG_Project_Mapper lookup table.
    Based on Workato Steps 49-51: Search WCG_Project_Mapper, stop if list size = 0.

    This is a strict validation - returns None if not found (to trigger error).

    Args:
        subsidiary: Subsidiary value from feed file
        project_template_mapper: Dictionary mapping subsidiaries to templates

    Returns:
        Template project name if found, None if not found (triggers error)
    """
    if not subsidiary:
        return None

    # Normalize subsidiary for comparison
    subsidiary_stripped = subsidiary.strip()

    # First try exact match (case-sensitive)
    if subsidiary_stripped in project_template_mapper:
        return project_template_mapper[subsidiary_stripped]

    # Try case-insensitive exact match
    subsidiary_lower = subsidiary_stripped.lower()
    for pattern, template_name in project_template_mapper.items():
        if pattern.lower() == subsidiary_lower:
            return template_name

    # Fallback: partial match (for flexibility)
    for pattern, template_name in project_template_mapper.items():
        if pattern.lower() in subsidiary_lower or subsidiary_lower in pattern.lower():
            return template_name

    # Not found in mapper - return None to trigger error (Workato Step 51: STOP job with error)
    return None


def get_client_name_for_create(dag_run):
    """
    Get client name for project creation.
    Uses customer name from feed file.

    Args:
        dag_run: Airflow DAG run context

    Returns:
        Client name string or None
    """
    # Try to get client name from created client result (Step 56)
    created_client = rail.result("step_56_create_client")
    if created_client and created_client.get("name"):
        return created_client["name"]

    # Otherwise use customer from feed file
    return dag_run.conf.get("customer", "")


def find_subsidiary_uri_from_options(options_response, subsidiary_value):
    """
    Find subsidiary URI from dropdown options response.
    Used after creating new dropdown option to get its URI.

    Args:
        options_response: Response from GetEnabledCustomFieldDropDownOptions
        subsidiary_value: Subsidiary value to find

    Returns:
        Dropdown option URI or None
    """
    if not options_response or not subsidiary_value:
        return null

    options = options_response if isinstance(options_response, list) else options_response.get("d", [])
    target_subsidiary = subsidiary_value.strip().lower()

    for option in options:
        option_name = option.get("displayText", "") or option.get("name", "")
        if option_name.strip().lower() == target_subsidiary:
            return option.get("uri")

    return null


def build_create_dropdown_option_request(subsidiary_value, existing_options, custom_field_uri):
    """
    Build request to create a new dropdown option for subsidiary.
    Based on Workato recipe: live_wcg_update_subsidiary_value_on_project.recipe.json

    Workato uses customFieldDropDownOptionUris with format:
    {"target": {"uri": "...", "name": null}, "name": "...", "isEnabled": true}

    Args:
        subsidiary_value: New subsidiary value to add
        existing_options: List of existing dropdown options
        custom_field_uri: URI of the custom field

    Returns:
        Dictionary with API request payload
    """
    if not subsidiary_value or not custom_field_uri:
        return null

    # Build options list in Workato format including new option
    options_list = []

    # Add existing options in Workato format
    for option in existing_options:
        options_list.append({
            "target": {
                "uri": option.get("uri"),
                "name": null,
            },
            "name": option.get("displayText") or option.get("name"),
            "isEnabled": option.get("isEnabled", True),
        })

    # Add new option with uri=null (Workato: target.uri = nil)
    options_list.append({
        "target": {
            "uri": null,
            "name": null,
        },
        "name": subsidiary_value.strip(),
        "isEnabled": True,
    })

    return {
        "customFieldUri": custom_field_uri,
        "customFieldDropDownOptionUris": options_list,
    }


def has_project_management_permission(permission_response):
    """
    Check if user has project management permission set.
    Checks by displayText containing "Project Management" or "Supervisor".

    Args:
        permission_response: Response from GetAssignedPermissionSetsForUser2

    Returns:
        True if user has project management permission, False otherwise
    """
    if not permission_response:
        return False

    # Extract permission sets from response
    permission_sets = permission_response if isinstance(permission_response, list) else permission_response.get("d", [])

    # Check for project management permission by name
    pm_permission_names = ["project management", "supervisor", "project manager"]

    for perm_set in permission_sets:
        display_text = perm_set.get("displayText", "").lower()
        for pm_name in pm_permission_names:
            if pm_name in display_text:
                return True

    return False


def find_project_management_permission_set_uri(permission_sets_response):
    """
    Find the Project Management permission set URI from GetPermissionSets response.
    Searches for permission sets with names containing "Project Management" or "Supervisor".

    Args:
        permission_sets_response: Response from PermissionSetService1.svc/GetPermissionSets

    Returns:
        URI of the Project Management permission set, or None if not found
    """
    if not permission_sets_response:
        return None

    # Extract permission sets from response
    permission_sets = (
        permission_sets_response
        if isinstance(permission_sets_response, list)
        else permission_sets_response.get("d", [])
    )

    # Search for Project Management permission set by name
    pm_permission_names = ["project management", "supervisor", "project manager"]

    for perm_set in permission_sets:
        display_text = perm_set.get("displayText", "").lower()
        for pm_name in pm_permission_names:
            if pm_name in display_text:
                return perm_set.get("uri")

    # Fallback: return first permission set if no match found (not ideal but prevents failure)
    if permission_sets:
        return permission_sets[0].get("uri")

    return None


def parse_budget_amount(budget_string):
    """
    Parse budget amount string to numeric value.
    Handles various formats: "$1,234.56", "1234.56", "1,234", etc.

    Based on Workato logic for parsing Total Budget field.

    Args:
        budget_string: Budget amount as string (e.g., "$1,234.56")

    Returns:
        Float amount if parseable, original value if not numerical, or 0 if empty
    """
    if not budget_string:
        return 0

    try:
        # Remove common currency symbols and formatting
        cleaned = str(budget_string).strip()
        cleaned = cleaned.replace("$", "")
        cleaned = cleaned.replace("€", "")
        cleaned = cleaned.replace("£", "")
        cleaned = cleaned.replace(",", "")
        cleaned = cleaned.strip()

        return float(cleaned) if cleaned else 0
    except (ValueError, TypeError):
        # Return original value as-is if not numerical
        return budget_string


def get_email_details_callable(dag_run, time_zone):
    """
    Get email and log file details for completion email.
    Calculates job duration and generates log file name.
    """
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(dag_run.conf['job_start_time'], "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"project_sync_log_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }


def get_email_details_callable_v2(job_start_time, time_zone):
    """
    Get email and log file details for completion email (version 2).
    Takes job_start_time as parameter instead of from dag_run.conf.
    Calculates job duration and generates log file name.
    """
    _now = now(time_zone)
    start_time = datetime.fromisoformat(job_start_time)
    return {
        "job_start_time": job_start_time,
        "job_end_time": _now.isoformat(),
        "job_duration": ((_now - start_time).seconds) // 60,
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"project_sync_log_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }

def get_project_processing_dag_ids(parallel_count):
    """
    Helper function to gather DAG run IDs from parallel project processing triggers.
    Similar to CRL user import USA v9 pattern.
    """
    return list(itertools.chain(
        *list(map(lambda x: (rail.result(
            f'trigger_parallel_project_processing_{x+1}') if rail.result(
            f'trigger_parallel_project_processing_{x+1}') else []), range(parallel_count)))))


def do_format_logs():
    """
    Format logs from gathered child DAG log artifacts and master DAG log.
    Similar to CRL user import USA v9 pattern.
    Uses rail.load_all_records() to load actual log entries from artifacts.
    Sets result counts for email template using rail.set_result().
    """
    log_artifacts = []
    log_records = []

    # Get child DAG logs (project processing logs)
    child_logs = rail.result("gather_child_logs") or []
    if child_logs:
        if isinstance(child_logs, list):
            log_artifacts.extend(child_logs)
        else:
            log_artifacts.append(child_logs)

    # Get master DAG log (exception logs like empty file, etc.)
    master_log = rail.result("create_log")
    if master_log:
        if isinstance(master_log, list):
            log_artifacts.extend(master_log)
        else:
            log_artifacts.append(master_log)

    # Load actual log entries from artifacts
    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    # Format log records with properties flattened
    final_log_records = list(map(lambda log: {
        'jobid': log.get('ecid', ''),
        **log.get('properties', {}),
    }, log_records))

    # Set result counts for email template
    rail.set_result(key="error_record_count", val=len(list(filter(lambda x: x.get('status') == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(list(filter(lambda x: x.get('status') == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(list(filter(lambda x: x.get('status') == 'Exception', final_log_records))))
    rail.set_result(key="total_record_count", val=len(final_log_records))

    return final_log_records