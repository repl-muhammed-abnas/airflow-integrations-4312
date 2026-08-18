from datetime import datetime
from uuid import uuid4
from dateutil.parser import parse as dp
import rail
from mammoet.time_data_import_v2.utils import custom_methods

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
                    "uri": get_required_details['user_uri'],
            }
        }
    }
    }
}

def get_timeentry_id_payload(dag_run,entry_id):
    if entry_id == 'sap_id':
        column_uri = dag_run.conf['sapid_column_uri']
        filter_uri = dag_run.conf['sapid_filter_definition_uri']
        time_entryid = dag_run.conf['ref_counter_id'] if dag_run.conf['ref_counter_id'] else dag_run.conf['counter']
    else:
        column_uri = dag_run.conf['repliconid_column_uri']
        filter_uri = dag_run.conf['repliconid_filter_definition_uri']
        time_entryid = dag_run.conf['extdocumentno']
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group",
            "urn:replicon:time-entry-revision-group-list-column:entry-date",
            "urn:replicon:time-entry-revision-group-list-column:hours",
            "urn:replicon:time-entry-revision-group-list-column:project",
            "urn:replicon:time-entry-revision-group-list-column:task",
            column_uri,
            "urn:replicon:time-entry-revision-group-list-column:comments",
            "urn:replicon:time-entry-revision-group-list-column:approval-status"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "filterDefinitionUri": filter_uri
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                "text": time_entryid
                }
            }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:date-range"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf['workdate'],custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                    "endDate": rail.parse_date(dag_run.conf['workdate'],custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
                }
                }
            }
            }
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

    if data['abs_att_type'] == 'BRK':
        meta_data_list.append(get_metadata_payload(
                key_uri=f"{meta_data_key_uri}break-type",
                text_value='Break',
                value_uri= data['activity_uri']
            )) if data['abs_att_type'] else None

    else:
        meta_data_list.append(get_metadata_payload(
            key_uri=f"{meta_data_key_uri}is-billable",
            bool_value=True
        ))

        meta_data_list.append(get_metadata_payload(
                key_uri=f"{meta_data_key_uri}activity",
                value_uri=data['activity_uri']
            )) if data['activity_uri'] else None

        meta_data_list.append(get_metadata_payload(
                key_uri=f"{meta_data_key_uri}task",
                value_uri=data['task_to_use_uri']
            )) if data['task_to_use_uri'] else None

        meta_data_list.append(get_metadata_payload(
                key_uri=f"{meta_data_key_uri}project",
                value_uri=data['project_uri']
            )) if not data['task_to_use_uri'] else None

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
    } if rail.result("search_time_entry_by_id") else {
        "uri": rail.result("search_time_entry_by_counter_id")[0]['timeentryrevisiongroup'],
    } if rail.result("search_time_entry_by_counter_id") else {
        "uri": rail.result("add_time_entry")['uri'],
    } if rail.result("add_time_entry") else None


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


def add_entry_id_oefs(conf,action):
    oef_list = []
    attendence_type_details= []

    if 'Netherlands' in conf['user_location']:
        attendence_type_details.append(['attendence_type_nl_oef_uri','attendence_type_nl_dropdown_value'])

    if 'Belgium' in conf['user_location']:
        attendence_type_details.append(['attendence_type_be_oef_uri','attendence_type_be_dropdown_value'])

    oef_list.append({
            "definition": {
                "uri": conf['sap_id_oef_uri']
            },
            "textValue": conf['counter']
        }) if conf['counter'] else None

    oef_list.append({
            "definition": {
                "uri": conf['replicon_id_oef_uri']
            },
            "textValue":conf['extdocumentno'] if action == "oef_update" else rail.result('get_time_entry_details')
        }) if conf['extdocumentno'] or action else None

    oef_list.append({
            "definition": {
                "uri": conf[attendence_type_details[0][0]]
            },
            "tag": {
                "uri": conf[attendence_type_details[0][1]]
            }
        }) if conf['abs_att_type'] in ['MFS','BRK'] and attendence_type_details and action != "oef_update" else oef_list.append({
            "definition": {
                "uri": conf[attendence_type_details[0][0]]
            },
            "tag": {
                "uri": rail.result("get_time_entry_details_for_update")[0]
            }
        }) if rail.result("get_time_entry_details_for_update") else None

    oef_list.append({
            "definition": {
                "uri": conf['time_entry_type_oef_uri']
            },
            "textValue": conf['abs_att_type']
        }) if conf['abs_att_type'] and conf['abs_att_type'] not in ['BRK'] else None

    oef_list.append({
            "definition": {
                "uri": conf['account_indicator_oef_uri']
            },
            "textValue": conf['account_indicator']
        })

    return oef_list

def get_time_payload(time, action = False):
    return {
        "hour": 23,
        "minute": 59,
        "second": 59
    } if time == '00:00:00' and action == 'out' else {
        "hour": dp(time).hour,
        "minute": dp(time).minute,
        "second": dp(time).second
    }


def put_time_entry_revision_payload(dag_run,action = False):
    return {
        "timeEntryRevisionGroup": {
            "target": get_timeentry_uri(),
            "user": {
                "uri": dag_run.conf['user_uri']
            },
            "entryDate": rail.parse_date(dag_run.conf['workdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "timePair": {
                    "startTime": get_time_payload(dag_run.conf['starttime']),
                    "endTime": get_time_payload(dag_run.conf['endtime'],"out"),
                }
            },
            "customMetadata": get_custom_metadata(dag_run.conf),
            "extensionFieldValues": add_entry_id_oefs(dag_run.conf,action)
        },
        "unitOfWorkId": str(uuid4())
    }

def update_time_entry_revision_payload(dag_run,action = False):
    return {
        "timeEntryRevisionGroup": {
            "target": get_timeentry_uri(),
            "user": {
                "uri": dag_run.conf['user_uri']
            },
            "entryDate": rail.parse_date(dag_run.conf['workdate'], custom_methods.FEED_ENTRYDATE_DATE_FORMAT),
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "timePair": {
                    "startTime": get_time_payload(dag_run.conf['starttime']),
                    "endTime": get_time_payload(dag_run.conf['endtime'],"out"),
                }
            },
            "customMetadata": get_custom_metadata(dag_run.conf),
            "extensionFieldValues": add_entry_id_oefs(dag_run.conf,action)
        },
        "unitOfWorkId": str(uuid4())
    }
