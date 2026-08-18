import rail
null = None


def get_custom_fields_prevailing_wage(wage_uri, wage_rate):
    return {
        "customField": {
            "uri": wage_uri,
            "name": null,
            "groupUri": null
        },
        "text": null,
        "date": null,
        "dropDownOption": {
            "uri": null,
            "name": null
        },
        "number": wage_rate.strip()
    }


def get_prevailing_wage_task_request(dag_run):
    return {
        "project": {
            "uri": dag_run.conf["projecturi"],
            "name": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": dag_run.conf["taskname"],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": dag_run.conf["taskname"],
            "code": dag_run.conf["taskcode"],
            "description": null,
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "1",
            "estimatedHours": null,
            "isClosed": "0",
            "customFieldValues": [
                get_custom_fields_prevailing_wage(
                    dag_run.conf["Prevailing wages RT uri"], dag_run.conf["rate1"]),
                get_custom_fields_prevailing_wage(
                    dag_run.conf["Prevailing wages OT uri"], dag_run.conf["rate2"]),
                get_custom_fields_prevailing_wage(
                    dag_run.conf["Prevailing wages DT uri"], dag_run.conf["rate3"])
            ],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": [
                {
                    "uri": null,
                    "resourcePlaceholderParameterCorrelationId": null,
                    "user": null,
                    "department": {
                        "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null
                }
            ]
        }
    }


def get_basic_task_request(dag_run):
    return {
        "project": {
            "uri": dag_run.conf["projecturi"],
            "name": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": dag_run.conf["taskname"],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": dag_run.conf["taskname"],
            "code": dag_run.conf["taskcode"],
            "description": null,
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "1",
            "estimatedHours": null,
            "isClosed": "0",
            "customFieldValues": [],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": [
                {
                    "uri": null,
                    "resourcePlaceholderParameterCorrelationId": null,
                    "user": null,
                    "department": {
                        "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null
                }
            ]
        }
    }
