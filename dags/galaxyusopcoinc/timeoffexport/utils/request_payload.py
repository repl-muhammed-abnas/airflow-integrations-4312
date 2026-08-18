from datetime import datetime
import functools
import rail
import pendulum
from dateutil.relativedelta import relativedelta

null = None


def get_export_name():
    return 'INT014_Timeoff_Booking_' + (datetime.now()).strftime("%Y%m%d%H%M")


def log_export_name():
    return get_export_name()


def get_timeoffdata_row_counts_batch_payload(time_zone):
    effective_date = pendulum.now(time_zone) - relativedelta(months=2)
    return {
        "filterExpressions": [
            {
                "leftExpression": {
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
                                            "year": effective_date.year,
                                            "month": effective_date.month,
                                            "day": 1
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
        ],
        "columnUris": [
            "urn:replicon:time-data-export-column:user",
            "urn:replicon:time-data-export-column:entry-date"
        ]
    }


def get_timeoffdata_row_counts_results_payload():
    return {
        "timeDataItemRowCountsBatchUri": rail.result('create_timeoffdata_row_counts_batch')
    }


def get_create_export_payload(time_zone):

    effective_date = pendulum.now(time_zone) - relativedelta(months=2)
    return {
        "columnUris": [],
        "filterExpression": {
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
                                    "year": effective_date.year,
                                    "month": effective_date.month,
                                    "day": 1
                                },
                                "endDate": null,
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null
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
    }


def get_export_batch_results_payload():
    return {
        "timeDataExportBatchUri": rail.result('create_export')
    }


def get_update_export_name_payload(export_file_name):
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        },
        "name": export_file_name()
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

@functools.lru_cache(maxsize=32)
def get_timeoff_units():
    return rail.result('get_timeoff_units')

@functools.lru_cache(maxsize=128)
def get_timeoffdate(_date_str):
    return (datetime.strptime(_date_str, '%Y/%m/%d')).strftime('%Y-%m-%d')

def translate_row(items):
    return {
        'employeeid': items['employeeid'] if items['employeeid'] else '',
        'timeoffentryid': items['timeoffentryid'] if items['timeoffentryid'] else '',
        'timeoffdate': get_timeoffdate(items['timeoffdate']) if items['timeoffdate'] else '',
        'timeoffamount': items['timeoffamount'] if items['timeoffamount'] else '',
        'timeoffdescription': items['timeoffdescription'] if items['timeoffdescription'] else '',
        'starttime': items['starttime'],
        'endtime': items['endtime'],
        'timeofftype': items['timeofftype'] if items['timeofftype'] else '',
        'positionid': items['positionid'] if items['positionid'] else '',
        'comments': "Time Off Booking",
        'units': 'Hours' if rail.find_first_by_attr_and_get_attr(get_timeoff_units(),
            'description', items['timeoffdescription'], 'measurementUnitUri').split(':')[-1] == 'hours'
        else 'Workdays'
    }.values()


def get_rename_export_payload(export_file_name):
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        },
        "name": export_file_name + '_Nodata'
    }


def get_timeoff_units_payload():
    return {
        "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
    }


def get_cancel_timeoff_export_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        }
    }


def get_revert_draft_payload():
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        }
    }

def create_export_status_batch_payload(status):
    return {
        "target": {
            "uri": rail.result('get_export_batch_results')['timeDataExportUri'],
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:draft" if status == "draft"
            else ("urn:replicon:time-data-export-status:cancelled" if status == "cancel" else null)
    }
