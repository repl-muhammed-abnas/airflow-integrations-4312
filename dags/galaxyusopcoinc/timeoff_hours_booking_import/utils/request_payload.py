from datetime import datetime
import uuid
from uuid import uuid4
import os
from functools import lru_cache
import rail
from galaxyusopcoinc.timeoff_hours_booking_import.utils import custom_methods


null = None

DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = "%H:%M:%S"


def get_date_in_json(date_string):
    date_obj = datetime.strptime(date_string, "%m/%d/%Y")
    return {
        "year": date_obj.year,
        "month": date_obj.month,
        "day": date_obj.day
    }

@lru_cache(maxsize=32)
def get_timeoff_type_details(dag_run):
    return rail.load_all_records(dag_run.conf['timeoff_type_details'])

def get_date(date):
    if not date:
        return None
    year = date['year']
    month = date['month']
    day = date['day']
    return str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2)

def get_child_conf(item, dag_run):
    return {
        **item,
        'timeoff_uri': rail.find_first_by_attr_and_get_attr(get_timeoff_type_details(dag_run), 'description', item['plan_ref_id'], 'uri'),
        'user_uri': rail.result('get_user_details')['uri'],
        'booking_id_oef_value': dag_run.conf['booking_id_oef_value'],
        'available_timeoff_uris': rail.result('get_all_assigned_time_off_type_for_user'),
        'user_start_date': get_date(rail.result('get_user_details')['start_date']),
        'user_end_date': get_date(rail.result('get_user_details')['end_date']),
        'log': dag_run.conf['log']
    }


def get_timeoff_add_payload(dag_run):
    dag_run.conf['available_timeoff_uris'].append(dag_run.conf['timeoff_uri'])
    return {
        "userUri": dag_run.conf['user_uri'],
        "timeOffTypeUris": dag_run.conf['available_timeoff_uris']
    }


def get_approve_holiday_booking_payload():
    return {
        "timeOffUri": rail.result("put_and_submit_timeoff_booking_for_user_add")["uri"],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Replicon Admin"
    }


def get_time_off_details_on_booking_id(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
            dag_run.conf['booking_id_oef_value'],
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


def get_time_off_booking_details(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
            dag_run.conf['booking_id_oef_value'],
                "urn:replicon:time-off-list-column:total-effective-hours"
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
                    "text": dag_run.conf['booking_id'],
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


def get_all_timesheet_for_user():
    get_required_details = rail.load_all_records(
        rail.result('get_min_max_dates_from_query'))[0]
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet-status",
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-period",
            "urn:replicon:timesheet-list-column:timesheet-owner"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": rail.parse_date(get_required_details['start_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                            "endDate": rail.parse_date(get_required_details['end_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": rail.result("get_user_details")['uri'],
                    }
                }
            }
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


def get_reopen_and_put_timeoff_payload(dag_run):
    hours = float(dag_run.conf['hours']) + \
        float(rail.result("get_time_off_booking_details")['timeoff_hours'])
    return {
        "timeOff": {
            "target": {
                "uri": rail.result('get_time_off_booking_details')['timeoff_uri']
            },
            "owner": {
                "uri": dag_run.conf['user_uri']
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_uri']
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['timeoff_date']),
                    "specificDuration": {
                        "hours": 0,
                        "minutes": 0,
                        "seconds": int(float(hours) * 3600),
                        "milliseconds": 0,
                        "microseconds": 0
                    }
                }
            },
            "objectExtensionFieldValues": [
                {
                    "definition": {
                        "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['booking_id_oef_value']
                    },
                    "textValue": dag_run.conf['booking_id']
                }
            ]
        },
        "comments": 'Time Entry Submitted by Integration',
        "unitOfWorkId": str(uuid4())
    }


def get_create_and_publish_timeoff_payload(dag_run):
    return {
        "timeOff": {
            "target": null,
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
                    "date": get_replicon_date(dag_run.conf['timeoff_date']),
                    "timeOfDay": null,
                    "relativeDuration": null,
                    "specificDuration": {
                        "hours": 0,
                        "minutes": 0,
                        "seconds": int(float(dag_run.conf['hours']) * 3600),
                        "milliseconds": 0,
                        "microseconds": 0
                    }
                },
                "timeOffEnd": null
            },
            "userExplicitEntries": [],
            "comments": null,
            "customFieldValues": [],
            "objectExtensionFieldValues": [
                {
                    "definition": {
                        "uri":  "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['booking_id_oef_value'],
                    },
                    "tag": null,
                    "numericValue": null,
                    "fileValue": null,
                    "jsonValue": null,
                    "textValue": dag_run.conf['booking_id']
                }
            ]
        },
        "comments": 'Time Entry Submitted by Integration',
        "unitOfWorkId": str(uuid4())
    }


def get_submit_time_off_entry_payload():
    return {
        "timeOffUri": rail.result('reopen_and_put_timeoff_update')['timeoff_uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Approved by Integration"
    }
