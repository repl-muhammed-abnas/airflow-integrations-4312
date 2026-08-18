from datetime import datetime
import uuid
import os
import rail


null = None
DATE_FORMAT = "%Y-%m-%d"

def get_timeoff_details_payload():
    return {
        "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
    }


def get_conf(timeoff_mapper_names, item):
    return {
        **item,
        'booking_id_oef_value': rail.result('get_booking_id_oef_value')['booking_id_oef_value'],
        'timeoff_type_uri': rail.find_first_by_attr_and_get_attr(
            rail.result('get_timeoff_details'),
            'displayText', timeoff_mapper_names.get(item['type']['name'],""), 'uri'),
        'timeoff_name': timeoff_mapper_names.get(item['type']['name'], item['type']['name'])
    }

def get_bulk_users_payload(dag_run):
    employee_info = rail.result("get_employee_number")
    return {
        "users": [
            {
                "employeeId": employee_info['employeeNumber'],
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
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
                    "text": dag_run.conf["id"],
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

def get_user_explicit_entries(dates):
    response = []
    for date, hour in dates.items():
        try:
            hour = int(hour)
        except:
            hour = round(float(hour), 2)
        entry = {
            "date": rail.parse_date(date, DATE_FORMAT),
            "relativeDurationUri": null,
            "specificDuration": hour,
            "timeStarted": null,
            "timeEnded": null
        }
        response.append(entry)
    return response


def get_put_and_submit_timeoff_payload(dag_run, add_or_update):
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
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-explicit-user-entries",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": rail.parse_date(dag_run.conf["start"], DATE_FORMAT)
                },
                "timeOffEnd": {
                    "date": rail.parse_date(dag_run.conf["end"], DATE_FORMAT)
                }
            },
            "userExplicitEntries": get_user_explicit_entries(dag_run.conf["dates"]),
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                    "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['booking_id_oef_value'],
                    "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf["id"],
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
