"""
Custom utility functions for Source Opportunities Project Sync.

All functions here are pure — explicit arguments in, a value or a raised
exception out. No ``rail.result()``/XCom lookups happen in this module;
``op_dags.py`` (and ``child_dag.py``/``main.py``) own all of that wiring and
pass already-resolved values in, the same convention already used for
``utils/request_payload.py``.
"""


def get_projects_list(response):
    """Extract the single projectDetails dict (if any) from a
    BulkGetProjectDetails2 response, as a 0-or-1-element list.

    Reused as-is from dags/deltek_internal/project_sync/utils/custom_functions.py
    — this lookup shape is not one of that integration's confirmed defects.
    """
    if not response:
        return []
    project_detail_dict = response[0].get("projectDetails")
    if not project_detail_dict:
        return []
    return [project_detail_dict]


def customFieldsToApply_for_modification_payload(list_of_dict, filter_value):
    """Find a project template's customField definition by displayText.

    Hardened vs. dags/deltek_internal/project_sync's version of this
    function: raises a named ValueError instead of silently returning None
    and crashing later on ``None.get(...)`` -> AttributeError when the
    template has no field with this displayText.
    """
    match = next(
        (item["customField"] for item in (list_of_dict or [])
         if item.get("customField", {}).get("displayText") == filter_value),
        None,
    )
    if match is None:
        raise ValueError(
            f"Template has no custom field with displayText={filter_value!r} "
            f"— cannot build customFieldsToApply for it."
        )
    return {
        "groupUri": match.get("groupUri"),
        "name": match.get("name"),
        "uri": match.get("uri"),
    }


def dropdown_uri_for_modification_payload(list_of_dict, filter_value):
    """Find a project template's dropDownOption for a customField by
    displayText.

    Hardened vs. dags/deltek_internal/project_sync's version: raises a
    named ValueError instead of silently returning None.
    """
    match = next(
        (item.get("dropDownOption") for item in (list_of_dict or [])
         if item.get("customField", {}).get("displayText") == filter_value),
        None,
    )
    if match is None:
        raise ValueError(
            f"Template has no dropDownOption for custom field "
            f"displayText={filter_value!r}."
        )
    return match


def capture_conf(**context):
    """First task in the op-DAG — captures the raw dag_run.conf.

    Needs the full Airflow context (not reachable via ``result()``, since
    nothing has been pushed to XCom yet), so it's wired directly as
    ``python_callable=custom_methods.capture_conf`` with no lambda.
    """
    dag_run = context.get("dag_run")
    return (dag_run.conf if dag_run else None) or {}


def log_failure(**context):
    """Push a structured failure record to XCom (or None if no failures).

    Always runs (trigger_rule="all_done"); the page-child gathers these
    XComs via GatherResultsFromDagRunsOperator and filters None entries.
    Needs the full Airflow context (dag_run.get_task_instances()), which
    isn't reachable via ``result()``, so it's wired directly as
    ``python_callable=custom_methods.log_failure`` with no lambda.
    """
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}

    failed_task_ids = []
    if dag_run:
        for ti in dag_run.get_task_instances():
            if str(ti.state) == "failed":
                failed_task_ids.append(ti.task_id)

    if not failed_task_ids:
        return None

    record = {
        "level":             "op",
        "opportunity_id":    conf.get("opportunityId", ""),
        "opportunity_name":  conf.get("opportunityName", ""),
        "child_dag_id":      dag.dag_id if dag else "",
        "child_run_id":      dag_run.run_id if dag_run else "",
        "page_number":       int(conf.get("pageNumber") or 0),
        "master_run_id":     conf.get("masterRunId", ""),
        "failed_task_ids":   failed_task_ids,
        "error_excerpt":     f"Failed tasks: {', '.join(failed_task_ids)}"[:500],
    }
    print(f"log_failure (op): {record}")
    return record


def validate_opportunity(opportunity, required_fields=("opportunityName", "clientName")):
    """Hard-fail (isolated to this one op-DAG run) if a required field is
    missing from the triggering opportunity.

    Pass required_fields to restrict validation to only the fields the calling
    path actually uses — e.g. the close-out path never reads clientName, so
    it passes required_fields=("opportunityName",) to avoid failing on
    legitimately-null clients for late-rejected opportunities.
    """
    opportunity = opportunity or {}
    missing = [
        field for field in required_fields
        if not opportunity.get(field)
    ]
    if missing:
        raise ValueError(
            f"Opportunity (id={opportunity.get('opportunityId')!r}) is "
            f"missing required field(s): {missing}. Cannot proceed "
            f"without them."
        )


def collect_client_uri(created_client, client_name, search_result, test=False):
    """Resolve the client uri to attach to the new project.

    ``created_client`` is the (possibly None) result of
    ``create_client_in_polaris`` — None both when that task was skipped by
    the ``client_exists`` branch and when its own XCom lookup fails, since
    the caller wraps that lookup in a try/except (a rail/XCom concern, kept
    in op_dags.py, not here).
    """
    if created_client:
        return created_client["uri"]

    search_result = search_result or {}
    rows = search_result.get("rows") or []
    if rows:
        for each_row in rows:
            cells = each_row.get("cells") or []
            if len(cells) > 1:
                client_cell = cells[1]
                if client_cell.get("uri") and client_cell.get("textValue") == client_name:
                    return client_cell["uri"]
    if test:
        return False
    raise ValueError(f"Could not resolve a client uri for clientName={client_name!r}")


def guard_template_found(template, template_name):
    """Raise a clear, named error if the project template lookup returned
    nothing, instead of crashing later on ``None.get(...)``.
    """
    template = template or []
    if not template:
        raise ValueError(
            f"Project template {template_name!r} was not found in Polaris "
            f"— cannot duplicate. Confirm the template name with the "
            f"Polaris admin."
        )
    return template


def processing_result_update_execution(opportunity):
    """Success-path result record when an existing project was updated + transitioned."""
    opportunity = opportunity or {}
    return {
        "opportunity_id":   opportunity.get("opportunityId", ""),
        "opportunity_name": opportunity.get("opportunityName", ""),
        "client_name":      opportunity.get("clientName", ""),
        "status":           "success",
        "action":           "updated_to_execution",
    }


def processing_result_create_and_update_execution(opportunity):
    """Success-path result record when no project existed yet for a Closed Won
    opportunity, so this run created one from scratch before transitioning it
    straight to Execution (skipping the Initiate stage the normal create path
    would use — the opportunity is already at 100% Closed Won by the time this
    runs).
    """
    opportunity = opportunity or {}
    return {
        "opportunity_id":   opportunity.get("opportunityId", ""),
        "opportunity_name": opportunity.get("opportunityName", ""),
        "client_name":      opportunity.get("clientName", ""),
        "status":           "success",
        "action":           "created_and_updated_to_execution",
    }


def log_not_found_in_polaris(opportunity):
    """Structured warning record when no Polaris project exists for a close-out
    opportunity. Not an error — the opportunity was likely rejected/lost before
    it ever reached Closing stage, so no project was ever created in Polaris.
    Returns a dict (does NOT raise) so the op-DAG run succeeds and surfaces the
    record in logs for visibility.
    """
    opportunity = opportunity or {}
    record = {
        "action":           "skipped_project_not_found",
        "opportunity_id":   opportunity.get("opportunityId", ""),
        "opportunity_name": opportunity.get("opportunityName", ""),
        "stage":            opportunity.get("stageName", ""),
        "probability":      opportunity.get("probability"),
    }
    print(f"log_not_found_in_polaris: {record}")
    return record


def processing_result_close_out(opportunity):
    """Success-path result record for a close-out op-DAG run."""
    opportunity = opportunity or {}
    return {
        "opportunity_id":   opportunity.get("opportunityId", ""),
        "opportunity_name": opportunity.get("opportunityName", ""),
        "stage":            opportunity.get("stageName", ""),
        "status":           "success",
        "action":           "closed",
    }


def processing_result(opportunity, existing_projects):
    """Build the success-path result record for this op-DAG run."""
    opportunity = opportunity or {}
    already_existed = len(existing_projects or []) > 0
    return {
        "opportunity_id":   opportunity.get("opportunityId", ""),
        "opportunity_name": opportunity.get("opportunityName", ""),
        "client_name":      opportunity.get("clientName", ""),
        "status":           "success",
        "action":           "skipped_already_exists" if already_existed else "created",
    }
