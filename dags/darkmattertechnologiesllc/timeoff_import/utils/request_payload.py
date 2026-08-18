from datetime import datetime
import uuid
import os
import rail


null = None

def get_date_in_json(date_string):
    date_obj = datetime.strptime(date_string, "%m/%d/%Y")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }


def get_timeoff_details_payload():
    return {
        "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
    }


def get_conf(item):
    return {
        **item,
        'booking_id_oef_value': rail.result('get_booking_id_oef_value')['booking_id_oef_value'],
        'timeoff_type_uri': rail.find_first_by_attr_and_get_attr(
            rail.result('get_timeoff_details'),
            'displayText', item['time_off_type'], 'uri'),
        'filename': (rail.result('new_file_sensor')).split('/')[-1],
        'create_log': rail.result('create_log')
    }

def get_bulk_users_payload(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf["employee_id"],
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": null
    }


def get_time_off_details_on_booking_id(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" + dag_run.conf['booking_id_oef_value'],
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:end-date",
                "urn:replicon:time-off-list-column:total-duration",
                "urn:replicon:time-off-list-column:approval-status"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:"+dag_run.conf['booking_id_oef_value']
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": dag_run.conf["unique_id"],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_put_and_submit_timeoff_payload(dag_run, add_or_update):
    if add_or_update == 'add':
        booked_duration = dag_run.conf['total_units']
    else:
        booked_duration = rail.result('get_total_hours')
    booked_duration = str(round(float(booked_duration), 2))
    hour, minute = str(booked_duration).split(".")
    minute = minute + "0" if len(minute) == 1 else minute
    start_date = dag_run.conf["time_off_date"]
    relative_duration = null
    start_specific_duration = {
        "hours": hour,
        "minutes": str(int((int(minute) * 60)/100)),
        "seconds": "0",
        "milliseconds": "0",
        "microseconds": "0"
    }
    timeoff_end = {
        "date": get_date_in_json(start_date),
        "timeOfDay": null,
        "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
        "specificDuration": null
    }

    return {
        "timeOff": {
            "target": null if add_or_update == 'add' else {"uri":dag_run.conf['timeoff_uri']},
            "owner": {
                "uri": dag_run.conf["user_uri"],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf["timeoff_type_uri"],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_date_in_json(start_date),
                    "timeOfDay": null,
                    "relativeDuration": relative_duration,
                    "specificDuration": start_specific_duration
                },
                "timeOffEnd": timeoff_end
            },
            "userExplicitEntries": [],
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                    "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['booking_id_oef_value'],
                    "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf["unique_id"],
                    "fileValue": null,
                    "jsonValue": null
                }
            ] if dag_run.conf["booking_id_oef_value"] else [],
        },
        "comments": "Submitted by Replicon Admin",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_approve_booking_payload(task_name):
    return {
        "timeOffUri": rail.result(task_name)["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Replicon Admin"
    }


def do_has_file_content():
    with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
        return os.path.getsize(artifact.local_filename) > 0


def get_booking_id_oef_value_payload():
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:object-extension-tag-definition-list-column:name",
            "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
        ],
        "sort": [],
        "filterExpression": null
    }
