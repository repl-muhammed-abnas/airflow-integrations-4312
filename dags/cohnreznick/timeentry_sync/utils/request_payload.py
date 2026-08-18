from datetime import datetime
from uuid import uuid4
import rail
from cohnreznick.timeentry_sync.utils import custom_methods

null = None


def get_all_timesheet_for_user():
    get_required_details = rail.result('get_required_details')
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
                            "startDate": rail.parse_date(get_required_details['min_entry_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                            "endDate": rail.parse_date(get_required_details['max_entry_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT)
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
                    "uri": get_required_details['user_uri']
                    }
                }
            }
        }
    }


def get_timeentry_id_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group",
            "urn:replicon:time-entry-revision-group-list-column:entry-date",
            "urn:replicon:time-entry-revision-group-list-column:hours",
            "urn:replicon:time-entry-revision-group-list-column:project",
            "urn:replicon:time-entry-revision-group-list-column:task",
            dag_run.conf['timeentryid_column_uri'],
            "urn:replicon:time-entry-revision-group-list-column:comments",
            "urn:replicon:time-entry-revision-group-list-column:approval-status"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": dag_run.conf['timeentryid_filter_definition_uri']
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "number": dag_run.conf['entry_id'],
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_epoch_time():
    return str(round((datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)).total_seconds()))


def get_metadata_payload(key_uri: str, value_uri=null, text_value=null, bool_value=null):
    if key_uri.endswith('billing-rate'):
        value_uri = value_uri['uri']
    return {
        "keyUri": key_uri,
        "value": {
            "bool": bool_value,
            "uri": value_uri,
            "text": text_value
        }
    }


def get_custom_metadata(data):
    meta_data_key_uri = "urn:replicon:time-entry-metadata-key:"
    meta_data_list = []

    if data['billing_rate']:
        meta_data_list.append(get_metadata_payload(
            key_uri=f"{meta_data_key_uri}billing-rate",
            value_uri=data['billing_rate']
        ))

    meta_data_list.append(get_metadata_payload(
        key_uri=f"{meta_data_key_uri}is-billable",
        bool_value=data['is_billable']
    ))

    meta_data_list.append(get_metadata_payload(
            key_uri=f"{meta_data_key_uri}task",
            value_uri=data['task_lvl_1_uri']
        ))

    if data['comments']:
        meta_data_list.append(get_metadata_payload(
            key_uri=f"{meta_data_key_uri}comments",
            text_value=data['comments']
        ))

    meta_data_list.append({
        "keyUri": "urn:replicon:widget-ui-metadata-key:initial-row-number",
        "value": {
            "number": get_epoch_time(),
        }
    }
    )
    return list(filter(None, meta_data_list))


def get_put_entry_payload(dag_run, caller):
    return {
        "timeEntry": {
            "target": {
                "uri": null,
                "parameterCorrelationId": null
            },
            "user": {
                "uri": rail.result("search_users")[0]['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "entryDate": {
                "year": rail.result(f'build_time_transaction_details_{caller}').get('entrydate_year'),
                "month": rail.result(f'build_time_transaction_details_{caller}').get('entrydate_month'),
                "day": rail.result(f'build_time_transaction_details_{caller}').get('entrydate_day')
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": 0,
                    "minutes": 0,
                    "seconds": dag_run.conf['hoursquantityinseconds'],
                    "milliseconds": 0,
                    "microseconds": 0
                },
                "timePair": null
            },
            "customMetadata": get_custom_metadata(caller),
            "extensionFieldValues": rail.result(f'build_oef_values_list_{caller}')
        },
        "unitOfWorkId": str(uuid4())
    }


def get_timeentry_uri():
    return {
        "uri": rail.result("search_time_entry_by_id")[0]['timeentryrevisiongroup'],
    } if bool(rail.result("search_time_entry_by_id")) else null


def get_seconds_from_hours(hours):
    return int(float(hours) * 3600) if hours != '' else 0


def get_interval(hours,seconds=0):
    return {
        "hours": 0,
        "minutes": 0,
        "seconds": get_seconds_from_hours(hours)+seconds,
        "milliseconds": 0,
        "microseconds": 0
    }


def add_entry_id(entry_id_udf_uri, entry_id: int):
    return[
        {
            "definition": {
                "uri": entry_id_udf_uri,
                "name": null
            },
            "numericValue": entry_id,
        }
    ]


def put_time_entry_revision_payload(dag_run):
    return {
        "timeEntryRevisionGroup": {
            "target": get_timeentry_uri(),
            "user": {
                "uri": dag_run.conf['user_uri']
            },
            "entryDate": rail.parse_date(dag_run.conf['entry_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": get_interval(dag_run.conf['actual_hours']),
                "timePair": null
            },
            "customMetadata": get_custom_metadata(dag_run.conf),
            "extensionFieldValues": add_entry_id(dag_run.conf['entryid_oef_uri'], dag_run.conf['entry_id'])
        },
        "unitOfWorkId": str(uuid4())
    }

def update_time_entry_revision_payload(dag_run):
    return {
        "timeEntryRevisionGroup": {
            "target": get_timeentry_uri(),
            "user": {
                "uri": dag_run.conf['user_uri']
            },
            "entryDate": rail.parse_date(dag_run.conf['entry_date'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": get_interval(float(dag_run.conf['actual_hours'])),
                "timePair": null
            },
            "customMetadata": get_custom_metadata(dag_run.conf),
            "extensionFieldValues": add_entry_id(dag_run.conf['entryid_oef_uri'], dag_run.conf['entry_id'])
        },
        "unitOfWorkId": str(uuid4())
    }
