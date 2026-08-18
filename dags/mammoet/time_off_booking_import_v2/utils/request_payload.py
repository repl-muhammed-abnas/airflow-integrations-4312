from datetime import datetime, timedelta
from functools import lru_cache
from uuid import uuid4
import rail

null = None

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = "%H:%M:%S"

def get_replicon_date(date_str):
    if not date_str:
        return None
    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


def get_hidden_oef_value_payload():
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

def get_child_conf(item, dag_run, instance):
    def get_date(date):
        if not date:
            return None
        year = date['year']
        month = date['month']
        day = date['day']
        return str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2)

    @lru_cache(maxsize=32)
    def get_timeoff_details():
        return rail.load_all_records(dag_run.conf['timeoff_details'])

    def get_end_date(formatted_start_date, formatted_end_date):
        start_date =  datetime.strptime(formatted_start_date, DATE_FORMAT)
        end_date = datetime.strptime(formatted_end_date, DATE_FORMAT)
        if end_date ==  datetime.strptime("9999-12-31",DATE_FORMAT) :
            return datetime.strftime(start_date + timedelta(days=365), DATE_FORMAT)
        return formatted_end_date

    return {
        'employee_id': item['employee_id'],
        'sf_booking_id': item['sf_booking_id'],
        'start_date': item['start_date'],
        'end_date': get_end_date(item['start_date'], item['end_date']),
        'start_time': null if item['end_date'] == "9999-12-31" and item['days'] == "0" else
            (item['start_time']).split('.')[0] if item['start_time'] and int(float(item['days'])) < 1 else null,
        'end_time': null if item['end_date'] == "9999-12-31" and item['days'] == "0" else
            (item['end_time']).split('.')[0] if item['end_time'] and int(float(item['days'])) < 1 else null,
        'no_of_days': item['days'],
        'time_off_booking_status': item['time_off_booking_status'],
        'time_off_type_description': item['time_off_type_description'],
        'timeoff_uri': rail.find_first_by_attr_and_get_attr(get_timeoff_details(), 'description', item['time_off_type_description'], 'uri'),
        'user_uri': rail.result('get_user_on_empid')[0]['uri'],
        'hidden_oef_value': dag_run.conf['hidden_oef_value'],
        'available_timeoff_uris': rail.result('get_all_assigned_time_off_type_for_user'),
        'user_start_date': get_date(rail.result('get_user_info')[0]['start_date']),
        'user_end_date': get_date(rail.result('get_user_info')[0]['end_date']),
        'employee_log': rail.result('create_employee_log'),
        "actual_end_date_payload": item['end_date'],
        "duration": item['duration']
    }

def validate_dates(dag_run, config):
    if int(float(dag_run.conf['no_of_days']))< 1 and \
        not (dag_run.conf['actual_end_date_payload'] == "9999-12-31" and dag_run.conf['no_of_days'] == "0"):
        if not dag_run.conf['start_time'] or not dag_run.conf['end_time']:
            return False
    if int(float(dag_run.conf['no_of_days']))< 1 and dag_run.conf['start_time'] and dag_run.conf['start_time']:
        if not dag_run.conf['duration']:
            return False
    if dag_run.conf['user_start_date']:
        format_userstartdate = datetime.strptime(
            dag_run.conf['user_start_date'], DATE_FORMAT)
        format_timeoffstartdate = datetime.strptime(
            dag_run.conf['start_date'], DATE_FORMAT)
        # v1.9 (RIT-20246): allow time-off start date == user hire date for trial/uat only
        allow_same_day_start = config.allow_same_day_timeoff_start
        start_date_ok = (format_userstartdate <= format_timeoffstartdate) if allow_same_day_start \
            else (format_userstartdate < format_timeoffstartdate)
        if dag_run.conf['user_end_date']:
            format_userenddate = datetime.strptime(
                dag_run.conf['user_end_date'], DATE_FORMAT)
            format_timeoffenddate = datetime.strptime(
                dag_run.conf['end_date'], DATE_FORMAT)
            return start_date_ok and (format_userenddate >= format_timeoffenddate)
        return start_date_ok
    return False

def get_time_off_details_on_sf_booking_id(dag_run):
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
                "urn:replicon:time-off-list-column:end-day-end-time"
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
                    "text": dag_run.conf['sf_booking_id'],
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
    data = rail.result('get_time_off_details_on_sf_booking_id')[0]
    timeofftype_uri_from_file = dag_run.conf['timeoff_uri']
    if timeofftype_uri_from_file == data['timeoff_type_uri']:
        if dag_run.conf['start_date'] == data['timeoff_start_date']:
            if dag_run.conf['end_date'] == data['timeoff_end_date']:
                if dag_run.conf['start_time'] == data['timeoff_start_time']:
                    if dag_run.conf['end_time'] == data['timeoff_end_time']:
                        return False
    return True

def get_submit_time_off_entry_payload():
    return {
        "timeOffUri": rail.result('get_time_off_details_on_sf_booking_id')[0]['timeoff_uri']
            if rail.result('get_time_off_details_on_sf_booking_id') else rail.result('create_and_publish_timeoff')['uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Force Approved By TimeOff Import Integration"
    }

MANDATORY_FIELDS = {
        "sf_booking_id":"external_code",
        "start_date": "start_date",
        "end_date": "end_date",
        "time_off_type_description": "time_type_external_code",
        "employee_id": "user_id",
        "time_off_booking_status": "approval_status",
        "no_of_days":"days"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def get_repicon_time(time_str):
    if not time_str:
        return None
    time =datetime.strptime(time_str,TIME_FORMAT)
    return {
        "hour": time.hour,
        "minute": time.minute,
        "second": time.second
    }

def get_relative_duration(dag_run):
    if dag_run.conf['start_time'] and dag_run.conf['end_time']:
        return "urn:replicon:time-off-relative-duration:partial-day"
    return "urn:replicon:time-off-relative-duration:full-day"

def get_specific_duaration(dag_run):
    if dag_run.conf['start_time'] and dag_run.conf['end_time']:
        duration_in_seconds = float(dag_run.conf['duration']) * 60.0 * 60.0
        return {
          "hours": 0,
          "minutes": 0,
          "seconds": int(duration_in_seconds),
          "milliseconds": 0,
          "microseconds": 0
        }
    return null

def get_create_and_publish_timeoff_payload(dag_run):
    return {
        "timeOff": {
            "target": null,
            "owner": {
                "uri": dag_run.conf['user_uri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_uri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['start_date']),
                    "timeOfDay": get_repicon_time(dag_run.conf['start_time']) if dag_run.conf['start_time'] else null,
                    "relativeDuration": get_relative_duration(dag_run) if not dag_run.conf['start_time'] and not dag_run.conf['end_time'] else null,
                    "specificDuration": get_specific_duaration(dag_run)
                },
                "timeOffEnd": {
                    "date": get_replicon_date(dag_run.conf['end_date']),
                    "timeOfDay": null,
                    "relativeDuration": get_relative_duration(dag_run),
                    "specificDuration": null
                }
            },
            "userExplicitEntries": [],
            "comments": null,
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                    "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['hidden_oef_value'],
                    "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf['sf_booking_id'],
                    "fileValue": null,
                    "jsonValue": null
                }
            ]
        },
        "comments": "Added by TimeOff Import Integration",
        "unitOfWorkId": str(uuid4())
    }

def get_reopen_and_put_timeoff_payload(dag_run):
    return {
        "timeOff": {
            "target": {
            "uri": rail.result('get_time_off_details_on_sf_booking_id')[0]['timeoff_uri']
            },
            "owner": {
                "uri": dag_run.conf['user_uri'],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_uri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['start_date']),
                    "timeOfDay": get_repicon_time(dag_run.conf['start_time']) if dag_run.conf['start_time'] else null,
                    "relativeDuration": get_relative_duration(dag_run) if not dag_run.conf['start_time'] and not dag_run.conf['end_time'] else null,
                    "specificDuration": get_specific_duaration(dag_run)
                },
                "timeOffEnd": {
                    "date": get_replicon_date(dag_run.conf['end_date']),
                    "timeOfDay": null,
                    "relativeDuration": get_relative_duration(dag_run),
                    "specificDuration": null
                }
            },
            "userExplicitEntries": [],
            "comments": null,
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                    "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['hidden_oef_value'],
                    "name": null
                    },
                    "tag": null,
                    "numericValue": null,
                    "textValue": dag_run.conf['sf_booking_id'],
                    "fileValue": null,
                    "jsonValue": null
                }
            ]
        },
        "comments": "Modified by Timeoff Import Integration",
        "unitOfWorkId": str(uuid4())
    }

def get_invalid_datetime_exception(dag_run):
    if int(float(dag_run.conf['no_of_days']))< 1 and dag_run.conf['start_time'] and dag_run.conf['start_time']:
        if not dag_run.conf['duration']:
            return "Number of hours field not present for partial day booking"
    if int(float(dag_run.conf['no_of_days']))< 1:
        if not dag_run.conf['start_time'] or not dag_run.conf['end_time']:
            return "StartTime/EndTime not present for Partial day booking"
    return 'Invalid Timeoff Startdate/Enddate'
