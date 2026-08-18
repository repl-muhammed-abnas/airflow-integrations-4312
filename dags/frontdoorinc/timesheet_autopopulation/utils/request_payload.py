import uuid
import rail
null = None

def get_report_generate_batch_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues": "",
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_repopulate_timesheet_payload(dag_run):
    return {
        "timesheet": {
            "uri": dag_run.conf["timesheet_uri"],
            "user": null,
            "date": null
        },
        "script": {
            "uri": dag_run.conf["script_uri"],
            "slug": null,
            "name": null
        },
        "overwrite": "true",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_enqueue_recalculate_script_payload(dag_run):
    return {
        "timesheet": {
            "uri": dag_run.conf["timesheet_uri"],
            "user": null,
            "date": null
        }
    }
