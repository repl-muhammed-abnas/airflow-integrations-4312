"""
Request payload builders for T-Systems ICT India Time Off Import
"""

import uuid
from datetime import datetime
import rail
import logging

null = None
DATE_FORMAT = "%d.%m.%Y"
TIME_FORMAT = "%H:%M"

MANDATORY_FIELDS = {
        "employee_id":"CID",
        "transaction_id":"Transaction ID",
        "booking_start_date":"Start Date",
        "booking_end_date":"End Date",
        "time_off_type":"Time Off Type",
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")

    return rail.smartjoin_by_delim(missing_fields, ";")

def get_user_on_empid_payload(dag_run):
    return{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:employee-id",
                "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
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
                    "text": dag_run.conf['employee_id'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_time_off_details_on_transaction_id(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" + dag_run.conf['hidden_oef_value'],
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:end-date",
                "urn:replicon:time-off-list-column:start-day-start-time",
                "urn:replicon:time-off-list-column:end-day-end-time",
                "urn:replicon:time-off-list-column:total-effective-hours"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:"+dag_run.conf['hidden_oef_value']
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
                    "text": dag_run.conf['transaction_id'],
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

def validate_is_update_required(dag_run):
    data = rail.result('get_time_off_details_on_transaction_id')[0]
    timeofftype_uri_from_file = dag_run.conf['timeoff_type_detail'][0]['uri']
    if timeofftype_uri_from_file == data['timeoff_type_uri']:
        if dag_run.conf['booking_start_date'] == data['timeoff_start_date']:
            if dag_run.conf['booking_end_date'] == data['timeoff_end_date']:
                if (dag_run.conf['booking_start_date'] == dag_run.conf['booking_end_date']) and \
                    dag_run.conf['booking_start_time'] and dag_run.conf['duration_hours']:
                    if dag_run.conf['booking_start_time']!= data['timeoff_start_time'] or float(dag_run.conf['duration_hours']) != float(data['hours']):
                        return True
                return False
    return True

def get_replicon_date(date_str):
    if not date_str:
        return None
    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }

def get_replicon_time(time_str):
    if not time_str:
        return None
    time =datetime.strptime(time_str,TIME_FORMAT)
    return {
        "hour": time.hour,
        "minute": time.minute
    }

def is_partial_day(dag_run):
    return (
        dag_run.conf['booking_start_date'] == dag_run.conf['booking_end_date']
        and bool(dag_run.conf.get('booking_start_time'))
        and bool(dag_run.conf.get('duration_hours'))
    )

def get_relative_duration(dag_run):
    if is_partial_day(dag_run):
        return "urn:replicon:time-off-relative-duration:partial-day"
    return "urn:replicon:time-off-relative-duration:full-day"

def get_specific_duaration(dag_run):
    if is_partial_day(dag_run):
        duration_in_seconds = float(dag_run.conf['duration_hours']) * 60.0 * 60.0
        return {
          "hours": 0,
          "minutes": 0,
          "seconds": int(duration_in_seconds),
          "milliseconds": 0,
          "microseconds": 0
        }
    return null

def get_put_timeoff_entry_payload(status, dag_run):
    return {
        "target": {
            "uri": rail.result('get_time_off_details_on_transaction_id')[0]['timeoff_uri'] if status == 'reopen' else null

        },
        "modifications": {
            "owner": {
                "value": {
                    "uri": rail.result('get_user_on_empid')[0]['uri'],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                }
            },
            "timeOffType": {
                "value": {
                    "uri": dag_run.conf['timeoff_type_detail'][0]['uri'],
                    "name": null
                }
            },
            "comments": null,
            "entryConfigurationMethodUri": {
                "value": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule"
            },
            "multiDayUsingStartEndDate": {
                "value": {
                    "timeOffStart": {
                        "date": get_replicon_date(dag_run.conf['booking_start_date']),
                        "timeOfDay": get_replicon_time(dag_run.conf['booking_start_time']) if is_partial_day(dag_run) else null,
                        "relativeDuration": get_relative_duration(dag_run) if not is_partial_day(dag_run) else null,
                        "specificDuration": get_specific_duaration(dag_run)
                    },
                    "timeOffEnd": {
                        "date": get_replicon_date(dag_run.conf['booking_end_date']),
                        "timeOfDay": null,
                        "relativeDuration": get_relative_duration(dag_run) if not is_partial_day(dag_run) else null,
                        "specificDuration": null
                    }
                }
            },
            "userExplicitEntries": [],
            "extensionFields":  [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                    {
                        "definition": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['hidden_oef_value'],
                        "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": dag_run.conf['transaction_id'],
                        "fileValue": null,
                        "jsonValue": null
                    }
                    ]
                }
                ]if status == 'new' else [],
            "customFields": []
        },
        "comments": "Modified by Timeoff Import Integration" if status == 'reopen' else "Added by TimeOff Import Integration",
        "unitOfWorkId": str(uuid.uuid4())
    }

