import rail
import uuid


null = None 

def get_update_user_payload(dag_run):
    return {
        "target": {
            "uri": rail.result('get_user_details')[0]["userDetails"]["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "scheduleTypeSchedule": [
            {
                "dateRange": {
                "startDate": rail.parse_date(dag_run.conf["valid_from"], "%d.%m.%Y"),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
                },
                "item": {
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                "officeSchedule": {
                    "officeScheduleUri": dag_run.conf["schedule_uri"],
                    "name": null
                }
                }
            }
            ],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }