import calendar
import pendulum
from dateutil.relativedelta import relativedelta
import rail

null = None


def _get_filter_expression(now):
    """Build the 4 nested AND filter expression for timeoff export.

    Filters:
    1. entry-date-range: 1st of (current_month - 2) to last day of (current_month + 2)
    2. time-data-export-status: none (only unexported)
    3. time-entry-type: time-off
    4. time-entry-approval-status: approved
    """
    start = now - relativedelta(months=2)
    end = now + relativedelta(months=2)
    last_day = calendar.monthrange(end.year, end.month)[1]

    return {
        "leftExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {
                                "year": start.year,
                                "month": start.month,
                                "day": 1
                            },
                            "endDate": {
                                "year": end.year,
                                "month": end.month,
                                "day": last_day
                            }
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            "urn:replicon:time-data-item-time-data-export-status:none"
                        ]
                    }
                }
            }
        },
        "operatorUri": "urn:replicon:filter-operator:and",
        "rightExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            "urn:replicon:time-entry-type:time-off"
                        ]
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            "urn:replicon:approval-status:approved"
                        ]
                    }
                }
            }
        }
    }


def get_row_counts_batch_payload():
    now = pendulum.now('UTC')
    return {
        "filterExpressions": [
            _get_filter_expression(now)
        ],
        "columnUris": [
            "urn:replicon:time-data-export-column:user",
            "urn:replicon:time-data-export-column:entry-date"
        ]
    }


def get_row_counts_results_payload():
    return {
        "timeDataItemRowCountsBatchUri": rail.result('create_row_counts_batch')
    }


def get_create_export_payload():
    now = pendulum.now('UTC')
    return {
        "columnUris": [],
        "filterExpression": _get_filter_expression(now)
    }


def get_export_batch_results_payload():
    return {
        "timeDataExportBatchUri": rail.result('create_export_batch')
    }


def get_update_export_name_payload():
    timestamp = pendulum.now('UTC').format('YYYY-MM-DDTHH:mm:ss')
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        },
        "name": f"RP_timeoff_export_{timestamp}"
    }


def get_mark_as_completed_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }


def get_download_batch_payload(file_format_uri):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [rail.result('get_export_batch_results')['timeDataExportUri']],
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
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "fileFormatScriptUri": file_format_uri()
    }


def get_download_url_payload():
    return {
        "timeDataDownloadBatchUri": rail.result('create_download_batch')
    }


def get_timeoff_types_payload():
    return {
        "page": 1,
        "pagesize": 10000,
        "columnUris": [
            "urn:replicon:time-off-type-list-column:name"
        ],
        "sort": [],
        "filterExpression": null
    }


def create_export_status_batch_payload(status):
    status_map = {
        "draft": "urn:replicon:time-data-export-status:draft",
        "cancel": "urn:replicon:time-data-export-status:cancelled",
        "complete": "urn:replicon:time-data-export-status:complete"
    }
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        },
        "statusUri": status_map.get(status)
    }
