import rail
import uuid


null = None 

def get_update_user_payload(dag_run):
    modifications = {
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
    }
    
    if dag_run.conf.get("holiday_calendar_uri") and dag_run.conf.get("holiday_calendar_effective_date"):
        modifications["holidayCalendarSchedule"] = [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["holiday_calendar_effective_date"], "%d.%m.%Y"),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf["holiday_calendar_uri"],
                    "name": null
                }
            }
        ]

    return {
        "target": {
            "uri": rail.result('get_user_details')[0]["userDetails"]["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": modifications,
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }