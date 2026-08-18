import uuid
import rail




def get_payload_all_project_task_report_generation():
    return {
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_all_project_task_report_details')['uri'],
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

def get_payload_create_task_hierarchy(dag_run):
    taskHierarchy = [ {
            "target": {
                "uri": task['taskuri']
            },
            "taskModificationToApply": {
                "isClosed": task['taskstatus'].lower(),
                "isTimeEntryAllowed": "true"
            }
        } for task in dag_run.conf['task']]
    return {
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "taskHierarchy": taskHierarchy,
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4()) + dag_run.conf['projectname']
}
