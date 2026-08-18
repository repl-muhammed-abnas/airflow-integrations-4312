from uuid import uuid4
import rail
null = None

DATE_FORMAT="%d/%m/%Y"

def get_put_and_submit_timeoff(dag_run):
    start_date = str(dag_run.conf["start_date"]).strip()
    end_date = str(dag_run.conf["end_date"]).strip()
    time_off_start_end = {}
    time_of_day = {}
    if dag_run.conf["start_hours"].strip():
        time_of_day = {
                "hour": int(dag_run.conf["start_hours"].split(":")[0]),
                "minute": int(dag_run.conf["start_hours"].split(":")[1]),
                "second": 0
            }
    if dag_run.conf["duration_type"] == "D" and float(dag_run.conf["duration"]) == 0.5:
        time_off_start_end = {
            "timeOffStart": {
                "date": rail.parse_date(start_date, "%d/%m/%Y"),
                "relativeDuration": "urn:replicon:time-off-relative-duration:half-day",
                "timeOfDay": time_of_day if time_of_day else null
            },
            "timeOffEnd": {
                "date": rail.parse_date(end_date, "%d/%m/%Y"),
                "relativeDuration": "urn:replicon:time-off-relative-duration:full-day"
            }
        }
    elif (dag_run.conf["duration_type"] == "D" and float(dag_run.conf["duration"]) > 0.5)\
        or (dag_run.conf["duration_type"] == "H" and start_date != end_date):
        time_off_start_end = {
            "timeOffStart": {
                "date": rail.parse_date(start_date, "%d/%m/%Y"),
                "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                "timeOfDay": time_of_day if time_of_day else null
            },
            "timeOffEnd": {
                "date": rail.parse_date(end_date, "%d/%m/%Y"),
                "relativeDuration": "urn:replicon:time-off-relative-duration:full-day"
            }
        }
    elif dag_run.conf["duration_type"] == "H":
        hours = 0
        hour_and_minute = float(dag_run.conf["hours"]) if dag_run.conf["hours"] else float(dag_run.conf["duration"])
        hours = int(hour_and_minute)
        minutes = float(hour_and_minute) - int(hours)
        if minutes > 0:
            if minutes < 1.0:
                minutes *= 60
                minutes = int(minutes)

        time_off_start_end = {
            "timeOffStart": {
                "date": rail.parse_date(start_date, "%d/%m/%Y"),
                "specificDuration": {
                    "hours": int(hours),
                    "minutes": int(minutes),
                    "seconds": 0
                },
                "timeOfDay": time_of_day if time_of_day else null
            },
            "timeOffEnd": {
                "date": rail.parse_date(end_date, "%d/%m/%Y"),
                "relativeDuration": "urn:replicon:time-off-relative-duration:full-day"
            }
        }
    return {
        "timeOff": {
            "owner": {
                "uri": dag_run.conf["useruri"]
            },
            "timeOffType": {"uri": dag_run.conf["time_off_type_uri"]},
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": time_off_start_end,
            "userExplicitEntries": [],
            "comments": "",
            "objectExtensionFieldValues": [
                {
                    "definition": {
                    "uri": dag_run.conf["people_soft_unique_id_uri"],
                    "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf["peoplesoft_unique_id"],
                    "fileValue": null,
                    "jsonValue": null
                }
            ]
        },
        "unitOfWorkId": str(uuid4()),
        "comments": ""
    }


def get_time_off_details_for_uniqueid(dag_run):
    unique_id = dag_run.conf["people_soft_unique_id_uri"].split(":")[-1]
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon:time-off-list-column:time-off-type",
            "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:"+ str(unique_id),
            "urn:replicon:time-off-list-column:start-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:"+ str(unique_id)
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": {
                "text": dag_run.conf["peoplesoft_unique_id"],
            },
            "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }
