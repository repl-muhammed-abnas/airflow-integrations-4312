import rail
import uuid

def get_user_report_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_timesheet_report_details")["uri"],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_timesheeturi(dag_run):
    return {
        "timesheet": {
            "uri": dag_run.conf['timesheeturi']
        }
    }

def get_force_approve_payload(dag_run):
    return {
        "timesheetUri": dag_run.conf['timesheeturi'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Integration"
    }