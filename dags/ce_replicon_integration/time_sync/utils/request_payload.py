from datetime import date, datetime, timedelta
import rail

null = None

def get_create_payrun_download_batch_payload():
    payrunuri = rail.result('get_payrun_batch_result')['payRunUri']
    columnUris = rail.result('get_payrun_columns')
    fileFormatScriptUri = rail.result('get_payroll_download_script')
    return {
        "columnUris": columnUris,
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [payrunuri]
                }
            }
        },
        "fileFormatScriptUri": fileFormatScriptUri
    }

def get_create_payrun_batch_payload(dag_run):
    startDate = date.today() - timedelta(days=90)
    endDate = date.today() + timedelta(days=30)
    columnUris = rail.result("get_payrun_columns")#dag_run.conf["columnUris"]

    return {
    	"columnUris": columnUris,
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:pay-run-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uri": "urn:replicon:payable-time-pay-run-status:none"
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:pay-run-filter:payable-time-approval-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uri": "urn:replicon:payable-time-approval-status:approved"
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:pay-run-filter:entry-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": rail.get_replicon_date(startDate),
                            "endDate": rail.get_replicon_date(endDate)
                        }
                    }
                }
            }
        }
    }

def get_payrun_uri_payload():
    return {
        "target": {
            "uri": rail.result('get_payrun_batch_result')['payRunUri']
        }
    }

def get_create_time_export_download_batch_payload():
    timeDataExportUri = rail.result('get_time_export_batch_result')['timeDataExportUri']
    columnUris = rail.result('get_time_data_columns')
    fileFormatScriptUri = rail.result('get_time_export_download_script')
    return {
        "columnUris": columnUris,
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [timeDataExportUri]
                }
            }
        },
        "fileFormatScriptUri": fileFormatScriptUri
    }


def get_create_time_export_batch_payload_request(dag_run, config):
    """Build export request with column URIs and filters."""
    columnUris = rail.result("get_time_data_columns")['column_uris']
    return {
        "columnUris": columnUris,
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
                                "startDate": rail.get_replicon_date(datetime.now() - timedelta(days=config.lookback_days)),
                                "endDate": rail.get_replicon_date(datetime.now() + timedelta(days=config.lookahead_days))
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
                                "urn:replicon:time-data-item-time-data-export-status:" + config.export_filter_export_status,
                            ]
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            "urn:replicon:approval-status:" + config.export_filter_timesheet_status
                        ]
                    }
                }
            }
        }
    }