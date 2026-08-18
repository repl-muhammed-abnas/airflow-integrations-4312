import uuid
import rail
from pwcglobal.absense_data_pre_population_v2.utils import custom_method

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_work_type_oef_uri(worktype_mapper):
    worktypeoefuri_list = []
    for item in worktype_mapper:
        worktypeoefuri_list.append(rail.result('get_all_object_extension_field_bindings').get(item['taskname']))

    return '|'.join(worktypeoefuri_list)

def get_hours_in_seconds(hours):
    return int(float(hours) * 3600) if hours != '' else 0


def get_process_prepopulation_data(item, worktype_mapper):
    return {
        'TransactionDate': item['TransactionDate'],
        'TimeEntryID': item['TimeEntryID'],
        'InternalWorkRelationship': {
            'InternalPerson': {
                'PartyId': item['InternalWorkRelationship']['InternalPerson']['PartyId']
            },
            'PwCLegalEntity': {
                'PartyId': item['InternalWorkRelationship']['PwCLegalEntity']['PartyId']
            },
            'PartyAlternateIdentifier': [
                {
                    'AlternateIdentifierType': item['InternalWorkRelationship']['PartyAlternateIdentifier'][0]['AlternateIdentifierType'],
                    'AlternateIdentifierValue': item['InternalWorkRelationship']['PartyAlternateIdentifier'][0]['AlternateIdentifierValue']
                },
                {
                    'AlternateIdentifierType': item['InternalWorkRelationship']['PartyAlternateIdentifier'][1]['AlternateIdentifierType'],
                    'AlternateIdentifierValue': item['InternalWorkRelationship']['PartyAlternateIdentifier'][1]['AlternateIdentifierValue']
                }
            ]
        },
        'ChargeCode': {
            'ChargeCode': item['ChargeCode']['ChargeCode'],
            'WorkItem': {
                'WorkItemType': item['ChargeCode']['WorkItem']['WorkItemType']
            }
        },
        'HoursQuantity': item['HoursQuantity'],
        'Comments': item['Comments'],
        'WorkType': item['WorkType'],
        'WorkLocation': item['WorkLocation'],
        'reporturi': null,
        'userfilteruri': null,
        'entrydatefilteruri': null,
        'wdidoefuri': rail.result('get_all_object_extension_field_bindings').get('wdid'),
        'worklocationoefuri': rail.result('get_all_object_extension_field_bindings').get('worklocation'),
        'worktypeoefuri': get_work_type_oef_uri(worktype_mapper),
        'wdid_oedfilteruri': null,
        'timeentryiduri': rail.result('get_all_object_extension_field_bindings').get('timeentryid'),
        'userloginname': rail.find_first_by_attr_and_get_attr(
            item['InternalWorkRelationship']['PartyAlternateIdentifier'], 'AlternateIdentifierType', 'PwC GUID', 'AlternateIdentifierValue'),
        'hoursquantityinseconds': get_hours_in_seconds(item['HoursQuantity']),
        'timeentryid_column_uri': rail.result('get_time_entry_all_columns'),
        'timeentryid_filter_definition_uri': rail.result('get_time_entry_all_filter_definitions'),
    }


def get_search_user_param(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:department-group",
            "urn:replicon:user-list-column:location",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:email-address",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:supervisor",
            "urn:replicon:user-list-column:employee-type",
            "urn:replicon:user-list-column:timesheet-period-type",
            "urn:replicon:user-list-column:start-date",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:hourly-cost",
            "urn:replicon:user-list-column:user-specific-billing-rate",
            "urn:replicon:user-list-column:timesheet-approval-path",
            "urn:replicon:user-list-column:time-off-approval-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                    "text": dag_run.conf['userloginname'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_timesheet_info_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:timesheet-list-column:approval-status",
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-status-2"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
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
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": {
                            "startDate": custom_method.get_replicon_date(dag_run.conf['TransactionDate']),
                            "endDate": custom_method.get_replicon_date(dag_run.conf['TransactionDate']),
                            "relativeDateRangeUri": null,
                            "relativeDateRangeAsOfDate": null
                        },
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null
                    },
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": rail.result("search_users")[0]['useruri'],
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
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
            },
            "value": null,
            "filterDefinitionUri": null
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
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": dag_run.conf['timeentryid_filter_definition_uri']
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
                    "text": dag_run.conf['TimeEntryID'],
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


def get_oef_tags_payload(dag_run):
    return {
        "page": "1",
        "pageSize": "10000",
        "objectExtensionTagDefinitionUri": dag_run.conf['worklocationoefuri'],
        "textSearch": {
                "queryText": dag_run.conf['WorkLocation'],
                "searchInDisplayText": "1",
                "searchInName": "1"
        }
    }


def get_custom_metadata(caller):
    meta_data_list = []
    if rail.result('update_task_project_metadata'):
        meta_data_list.append(rail.result('update_task_project_metadata'))
    if rail.result(f'build_oef_for_comments_{caller}'):
        meta_data_list.append(rail.result(f'build_oef_for_comments_{caller}'))

    meta_data_list.append({
        "keyUri": "urn:replicon:widget-ui-metadata-key:initial-row-number",
        "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "number": custom_method.get_epoch_time(),
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null,
            "collection": []
        }
    }
    )
    return meta_data_list


def get_update_entry_payload(dag_run, caller):
    return {
        "timeEntryRevisionGroup": {
            "target": {
                "uri": rail.result("search_time_entry_by_id")[0]['timeentryrevisiongroup'],
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
        "unitOfWorkId": str(uuid.uuid4())
    }


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
        "unitOfWorkId": str(uuid.uuid4())
    }
