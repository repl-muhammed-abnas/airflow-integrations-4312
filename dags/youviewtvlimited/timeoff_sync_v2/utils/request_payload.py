from datetime import datetime
import uuid
import rail

null = None

def get_date_in_json(date_string):
    date_obj = datetime.strptime(date_string, "%Y-%m-%d")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

def get_bulk_users_payload(dag_run):
    return {
        "users": [
            {
                "employeeId": null,
                "loginName": dag_run.conf["booking_data"]["employeeEmail"],
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_book_timeoff_conf(timeoff_types_mapper, item):
    replicon_timeoff_type_name = timeoff_types_mapper.get(
        item["policyTypeDisplayName"].lower())
    return {
        "booking_data": item,
        "replicon_timeoff_type_name": replicon_timeoff_type_name,
        "get_absense_time_off_type": rail.result('get_absense_time_off_type').get(
            replicon_timeoff_type_name),
        "log_artifact": rail.result("create_log"),
        "booking_id_oef_value": rail.result('get_booking_id_oef_value')['booking_id_oef_value']
    }

def get_timeoff_end_details(dag_run):
    if dag_run.conf["booking_data"]["endPortion"] != "all_day":
        relative_duration = "urn:replicon:time-off-relative-duration:half-day"
    else:
        relative_duration = "urn:replicon:time-off-relative-duration:full-day"
    return {
        "date": get_date_in_json(dag_run.conf["booking_data"]["endDate"]),
        "timeOfDay": null,
        "relativeDuration": relative_duration,
        "specificDuration": null
    }

def get_put_and_submit_timeoff_payload(dag_run):
    start_date = dag_run.conf["booking_data"]["startDate"]
    if dag_run.conf["booking_data"]["startPortion"] != "all_day":
        relative_duration = "urn:replicon:time-off-relative-duration:half-day"
    else:
        relative_duration = "urn:replicon:time-off-relative-duration:full-day"
    start_specific_duration = null
    timeoff_end = get_timeoff_end_details(dag_run)

    return {
        "timeOff": {
            "target": null,
            "owner": {
                "uri": rail.result("get_user_info")["userDetails"]["uri"],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf["get_absense_time_off_type"],
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
            "comments": "",
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                    "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['booking_id_oef_value'],
                    "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf["booking_data"]['requestId'],
                    "fileValue": null,
                    "jsonValue": null
                }
            ],
        },
        "comments": "Submitted by Replicon Admin",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_approve_holiday_booking_payload():
    return {
        "timeOffUri": rail.result("put_and_submit_timeoff_booking_for_user")["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Replicon Admin"
    }

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

def get_time_off_details_on_booking_id(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" + dag_run.conf['booking_id_oef_value'],
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:end-date"
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
                    "text": dag_run.conf["booking_data"]['requestId'],
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
