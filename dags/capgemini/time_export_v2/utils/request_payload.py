import pendulum
from airflow.exceptions import AirflowFailException
import rail

null = None

def create_time_export_payload():
    start_date = pendulum.parse(rail.result("logging_details")["export_start_date"])
    end_date = pendulum.parse(rail.result("logging_details")["export_end_date"])
    return {
        'columnUris': [],
        'filterExpression': {
            'leftExpression': {
                'leftExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:time-data-export-filter:entry-date-range'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:in',
                    'rightExpression': {
                        'value': {
                            'uris': [],
                            'dateRange': {
                                'startDate': {
                                    'year': start_date.year,
                                    'month': start_date.month,
                                    'day': start_date.day
                                },
                                'endDate': {
                                    'year': end_date.year,
                                    'month': end_date.month,
                                    'day': end_date.day
                                },
                                'relativeDateRangeUri': null,
                                'relativeDateRangeAsOfDate': null
                            }
                        }
                    }
                },
                'operatorUri': 'urn:replicon:filter-operator:and',
                'rightExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:time-data-export-filter:time-data-export-status'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:in',
                    'rightExpression': {
                        'value': {
                            'uris': [
                                'urn:replicon:time-data-item-time-data-export-status:none'
                            ]
                        }
                    }
                }
            },
            'operatorUri': 'urn:replicon:filter-operator:and',
            'rightExpression': {
                'leftExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:time-data-export-filter:timesheet-only-approval-status'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:in',
                    'rightExpression': {
                        'value': {
                            'uris': [
                                'urn:replicon:approval-status:approved'
                            ]
                        }
                    }
                },
                'operatorUri': 'urn:replicon:filter-operator:and',
                'rightExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:time-data-export-filter:location'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:in',
                    'rightExpression': {
                        'value': {
                            'uris': rail.result("get_allowed_location_uris")
                        }
                    }
                }
            }
        }
    }

def create_export_status_complete_batch_payload(export_uri):
    return {
        "target": {
            "uri": rail.result(export_uri),
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }

def create_export_status_batch_payload(status):
    return {
        "target": {
            "uri": rail.result("get_export_uri_failed"),
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:draft" if status == "draft"
            else ("urn:replicon:time-data-export-status:cancelled" if status == "cancel" else null)
    }

def get_create_download_batch(export_uri):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [rail.result(export_uri)],
                },
            },
        },
        "fileFormatScriptUri": rail.result("get_time_download_script")
    }

def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']

def get_allowed_location_uris_payload(export_locations):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:location-list-column:location"
            ],
        "filterExpression": {
        "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:location-list-filter:text"
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
            "text": export_locations,
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
        },
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
            ]
        }
