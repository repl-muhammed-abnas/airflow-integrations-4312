import rail
null = None

def get_add_project_task_payload(dag_run):
    return {
        "project": {
          "uri": dag_run.conf["parenturi"],
          "name": null,
          "code": null,
          "parameterCorrelationId": null
        },
        "task": {
          "target": {
            "uri": null,
            "name": dag_run.conf["taskname"],
            "parent": null,
            "parameterCorrelationId": null
          },
          "name":dag_run.conf["taskname"],
          "code": dag_run.conf["taskcode"],
          "description": dag_run.conf["taskdescription"],
          "percentCompleted": 0,
          "isTimeEntryAllowed": "true",
          "isClosed": "false",
          "estimatedCost": {
            "amount": 0,
            "currency": {
                "uri": null,
                "name": null,
                "symbol": "€"
            }
          },
          "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
        }
    }

def get_bulk_resource_assignments_payload():
    return {
      "taskUri": rail.result("create_project_task")["uri"] if rail.result("create_project_task")["uri"] else null,
      "resourceUris": [ data["resource"]["uri"] for data in rail.result("get_all_project_team_members") if data["resource"]["uri"] ],
      "isAssigned": "true"
    }
