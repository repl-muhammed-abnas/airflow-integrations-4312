def search_project_by_name(project_name):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code",
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:project-list-filter:name",
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {"value": {"text": project_name}},
        },
    }


def assign_member(project_uri, resource_uri):
    return {
        "projectUri": project_uri,
        "resourceUri": resource_uri,
        "projectTeamMemberAssignmentOptionUri":
            "urn:replicon:project-team-member-assignment-option:assign",
    }


def unassign_member(project_uri, resource_uri):
    return {
        "projectUri": project_uri,
        "resourceUri": resource_uri,
        "projectTeamMemberAssignmentOptionUri":
            "urn:replicon:project-team-member-assignment-option:force-unassign",
    }


def get_children_tasks(project_uri):
    return {"parentUri": project_uri}


def put_task_assignments(project_uri, resource_uri, task_uris):
    return {
        "projectUri": project_uri,
        "resourceUri": resource_uri,
        "taskUris": task_uris,
    }


def update_default_billing_rate(client_uri, billing_rate_uri):
    return {
        "clientUri": client_uri,
        "billingRateUri": billing_rate_uri,
        "isAllowedByDefaultOnNewProjects": "true",
    }


def put_member_billing_rates(project_uri, resource_uri, billing_rate_uri):
    return {
        "projectUri": project_uri,
        "resourceUri": resource_uri,
        "billingRateUris": [billing_rate_uri],
    }
