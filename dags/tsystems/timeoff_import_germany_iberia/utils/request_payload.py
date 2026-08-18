"""
Request payload generators for T-Systems Germany/Iberia Time Off Import
"""

import rail
from datetime import datetime

null = None

DATE_FORMAT = "%d.%m.%Y"
TIME_FORMAT = "%H:%M"

def get_mandatory_fields_exception_message(item):
    """
    Generate exception message for missing mandatory fields
    
    Args:
        item: Time off record
        
    Returns:
        str: Exception message
    """
    missing_fields = []
    
    if not item.get('employee_id'):
        missing_fields.append('CID')
    if not item.get('transaction_id'):
        missing_fields.append('Transaction ID')
    if not item.get('booking_start_date'):
        missing_fields.append('Start Date')
    if not item.get('booking_end_date'):
        missing_fields.append('End Date')
    if not item.get('time_off_type'):
        missing_fields.append('Time Off Type')
    
    return f"Mandatory fields missing: {', '.join(missing_fields)}"

def get_user_on_empid_payload(dag_run):
    """
    Get payload for fetching user by employee ID
    
    Args:
        dag_run: Airflow DAG run context
        
    Returns:
        dict: API payload
    """
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
    """
    Get payload for fetching time off by transaction ID
    
    Args:
        dag_run: Airflow DAG run context
        
    Returns:
        dict: API payload
    """
    
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

def get_relative_duration(dag_run):
    if dag_run.conf['booking_start_time'] and dag_run.conf['duration_hours']:
        return "urn:replicon:time-off-relative-duration:partial-day"
    return "urn:replicon:time-off-relative-duration:full-day"

def get_specific_duaration(dag_run):
    if dag_run.conf['booking_start_time'] and dag_run.conf['duration_hours']:
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
        "timeOff": {
            "target": {
                "uri": rail.result('get_time_off_details_on_transaction_id')[0]['timeoff_uri'] if status == 'reopen' else rail.result('create_time_off_draft')
            },
            "owner": {
                "uri": rail.result('get_user_on_empid')[0]['uri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_type_detail'][0]['uri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['booking_start_date']),
                    "timeOfDay": get_replicon_time(dag_run.conf['booking_start_time']) if dag_run.conf['booking_start_time'] else null,
                    "relativeDuration": get_relative_duration(dag_run) if not dag_run.conf['booking_start_time'] else null,
                    "specificDuration": get_specific_duaration(dag_run)
                },
                "timeOffEnd": {
                    "date": get_replicon_date(dag_run.conf['booking_end_date']),
                    "timeOfDay": null,
                    "relativeDuration": get_relative_duration(dag_run) if not dag_run.conf['booking_start_time'] else null,
                    "specificDuration": null
                }
            },
            "userExplicitEntries": [],
            "comments": "Modified by Timeoff Import India Integration" if status == 'reopen' else "Added by TimeOff Import India Integration",
            "customFieldValues": []
        }
    }

def get_put_timeoff_transaction_id_oef_value_payload(dag_run):
    return {
        "timeOffUri": rail.result('publish_time_off_draft')['uri'],
        "extensionFieldValues": [
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
