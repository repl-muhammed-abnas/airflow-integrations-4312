"""
Request payload builders for the project-creation op-DAG
(source_opportunities_project_sync -> Polaris).

Unlike dags/deltek_internal/project_sync's version of this module, these
builders take explicit arguments instead of reaching into ``rail.result(...)``
themselves — op_dags.py's lambdas own the XCom wiring, so this module stays a
plain function of its inputs and reads directly off OpportunityItem's real
camelCase keys (opportunityName, clientName, engagementContractType, ...).
"""
import json
import uuid
import logging
from datetime import date

from dateutil.parser import parse as date_parser

from resource_planner.source_opportunities_project_sync import config

null = None


def parse_opportunity_date_to_replicon_format(date_string):
    """Parse an ISO date string (e.g. "2025-12-09") to Replicon's
    {"year", "month", "day"} shape, or None if missing/unparseable."""
    if not date_string:
        return None
    try:
        parsed_date = date_parser(date_string)
        return {"year": parsed_date.year, "month": parsed_date.month, "day": parsed_date.day}
    except Exception as e:
        logging.warning(f"Failed to parse date '{date_string}': {e}")
        return None


def resolve_project_start_date(opportunity):
    """Replicon {"year", "month", "day"} for the new project's start date —
    the opportunity's startDate if present/parseable, else today (the run
    date), matching this repo's existing date.today() fallback convention
    (task_resource_allocation_export_webhooks/utils.py)."""
    start_date = parse_opportunity_date_to_replicon_format(opportunity.get("startDate"))
    if start_date:
        return start_date
    today = date.today()
    return {"year": today.year, "month": today.month, "day": today.day}


def resolve_project_template_name(opportunity):
    """Look up opportunity['engagementContractType'] in
    config.ENGAGEMENT_CONTRACT_TYPE_TEMPLATE_ATTR_MAP via .get(), not a bare
    subscript — engagementContractType is Optional[str] on the real
    OpportunityItem model, so a bare dict[key] would raise an unhelpful
    KeyError on the realistic None case instead of a clear, actionable error.
    """
    contract_type = opportunity.get("engagementContractType")
    attr_name = config.ENGAGEMENT_CONTRACT_TYPE_TEMPLATE_ATTR_MAP.get(contract_type)
    if not attr_name:
        raise ValueError(
            f"Opportunity {opportunity.get('opportunityName')!r} "
            f"(id={opportunity.get('opportunityId')!r}) has "
            f"engagementContractType={contract_type!r}, which is not mapped in "
            f"ENGAGEMENT_CONTRACT_TYPE_TEMPLATE_ATTR_MAP "
            f"(known keys: {list(config.ENGAGEMENT_CONTRACT_TYPE_TEMPLATE_ATTR_MAP)}). "
            f"Add a mapping in config.py or fix the source data."
        )
    template_name = getattr(config, attr_name, None)
    if not template_name:
        raise ValueError(
            f"config.{attr_name} is not set — cannot resolve a project "
            f"template for engagementContractType={contract_type!r}."
        )
    return template_name


def build_search_client_payload(client_name):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:client-list-column:name",
            "urn:replicon:client-list-column:client",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:name",
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": client_name,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null,
                },
                "filterDefinitionUri": null,
            },
            "value": null,
            "filterDefinitionUri": null,
        },
    }


def build_create_client_payload(client_name):
    return {
        "target": null,
        "modifications": {
            "nameToApply": {"value": client_name},
            "codeToApply": null,
            "descriptionToApply": null,
            "statusToApply": True,
            "clientContactToApply": null,
            "clientAddressToApply": null,
            "billingAddressToApply": null,
            "billingRatesToApply": null,
            "clientManagerToApply": null,
            "clientSharingToApply": null,
            "expenseCodesToApply": null,
            "customFieldsToApply": [],
            "taxProfileToApply": null,
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4()),
    }


def build_search_existing_project_payload(opportunity_name):
    return {
        "projects": [
            {"uri": null, "name": opportunity_name, "code": null, "parameterCorrelationId": null}
        ]
    }


def build_get_project_template_payload(template_name):
    return {
        "projects": [
            {"uri": null, "name": template_name, "code": null, "parameterCorrelationId": null}
        ]
    }


def build_create_duplicate_project_payload(opportunity, project_template):
    """CreateProjectCopyBatch2 payload.

    Fixes dags/deltek_internal/project_sync's confirmed defect: sourceProject.uri
    is taken from the resolved template lookup (get_project_template's result),
    not a hardcoded literal URN that ignores the lookup entirely.
    """
    template = (project_template or [{}])[0]
    source_uri = template.get("uri")
    if not source_uri:
        raise ValueError(
            f"Project template lookup returned no uri — cannot duplicate. "
            f"Template response: {template}"
        )

    return {
        "copyParameter": {
            "sourceProject": {
                "uri": source_uri,
                "name": null,
                "code": null,
                "parameterCorrelationId": null,
            },
            "destinationProjectInfo": {
                "name": opportunity.get("opportunityName"),
                "code": opportunity.get("opportunityNumber"),
                "dateRange": {
                    "startDate": resolve_project_start_date(opportunity),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null,
                },
                "statusLabel": null,
                "clients": [],
                "program": null,
                "portfolio": null,
                "keyValues": [],
            },
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:copy-from-project",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:copy-from-project",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:copy-date",
            "rateTableEntryCopyOptionUri": "urn:replicon:rate-table-entry-copy-option:copy-from-project",
            "billingContractCopyOptionUri": "urn:replicon:billing-contract-copy-option:copy",
            "projectDependentTimeEntryObjectExtensionFieldCopyOptionUri": "urn:replicon:project-dependent-time-entry-object-extension-field-copy-option:copy",
            "shiftDatesByProjectStartDateOffset": "false",
            "taskResourceEstimatesCopyOptionUri": "urn:replicon:task-resource-estimate-copy-option:copy-estimates-with-resource=selection",
        },
    }


def build_processing_batch_in_background_payload(response):
    return {"batchUri": response}


def build_modify_project_payload(opportunity):
    """CreateProjectOrApplyModifications payload for a project targeted by name.

    Used by both the op-create path (modifying the newly-duplicated project
    whose name was just set via destinationProjectInfo.name) and the
    op-update-execution path (modifying an already-existing project whose name
    matches the opportunity). Targets by name — the same lookup-by-name
    convention used elsewhere in this repo.

    customFieldsToApply is intentionally empty: the target templates'
    real custom-field schema inside Polaris is still unconfirmed. Populate this
    using custom_methods.customFieldsToApply_for_modification_payload /
    dropdown_uri_for_modification_payload once the Polaris admin confirms the
    templates' actual custom fields.
    """
    start_date = parse_opportunity_date_to_replicon_format(opportunity.get("startDate"))
    services_revenue = opportunity.get("servicesRevenue")

    return {
        "target": {
            "uri": null,
            "name": opportunity.get("opportunityName"),
            "code": null,
            "parameterCorrelationId": null,
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": null,
            "descriptionToApply": null,
            "percentCompletedToApply": null,
            "startDateToApply": {"value": start_date} if start_date else null,
            "endDateToApply": null,
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": null,
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": null,
            "isProjectLeaderApprovalRequired": null,
            "costTypeToApply": null,
            "isTimeEntryAllowed": null,
            "expenseCodesToApply": null,
            "estimatedHoursToApply": null,
            "budgetedHoursToApply": null,
            "estimatedCostToApply": null,
            "budgetedCostToApply": null,
            "expenseBudgetedCostToApply": null,
            "totalEstimatedContractValueToApply": {
                "value": {
                    "amount": str(services_revenue),
                    "currency": {"uri": null, "name": null, "symbol": "USD$"},
                }
            } if services_revenue is not None else null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": null,
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [],
            "resourceAssignmentModifications": null,
            "resourceProjectAssignmentModifications": null,
            "billingContractModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [],
            "portfolioToApply": null,
            "locationToApply": null,
            "divisionToApply": null,
            "serviceCenterToApply": null,
            "costCenterToApply": null,
            "departmentGroupToApply": null,
            "employeeTypeGroupToApply": null,
        },
        "projectModificationOptionUri": config.project_modification_save_uri,
        "unitOfWorkId": str(uuid.uuid4()),
    }


def build_update_client_payload(project_uri, client_uri):
    return {
        "projectUri": project_uri,
        "clients": [
            {
                "client": {
                    "uri": client_uri,
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null,
                },
                "costAllocationPercentage": "100",
            }
        ],
    }


_WORKFLOW_STATE_MUTATION = (
    'mutation PutProjectWorkflowState('
    '$projectId: String!, '
    '$projectWorkflowStateId: ProjectWorkflowStage!) {\n'
    '  putProjectWorkflowState: putProjectWorkflowState3(\n'
    '    projectId: $projectId\n'
    '    projectWorkflowStateId: $projectWorkflowStateId\n'
    '  ) {\n'
    '    id\n'
    '    uri\n'
    '    displayText\n'
    '    __typename\n'
    '  }\n'
    '}\n'
)


def build_workflow_state_mutation_payload(project_uri, state_id):
    """GraphQL payload for putProjectWorkflowState3.

    state_id: one of config.POLARIS_*_STATE_ID ("INITIATE", "EXECUTION", "CLOSEOUT")
    Returns a json.dumps()-serialised list — the RAIL wire format for Polaris GraphQL.
    """
    return json.dumps([{
        'operationName': 'PutProjectWorkflowState',
        'variables': {
            'projectId': project_uri,
            'projectWorkflowStateId': state_id,
        },
        'query': _WORKFLOW_STATE_MUTATION,
    }])
