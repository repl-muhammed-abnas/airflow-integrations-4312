import rail

null = None

def get_project_details_payload(dag_run):
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf["item"]["projectname"],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }

def get_update_billing_rate_payload(dag_run):
    return {
        "projectUri": rail.result('get_project_details')[0]["projectDetails"]["uri"],
        "billingRateUri": dag_run.conf["billing_rate_uri"],
        "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
    }
