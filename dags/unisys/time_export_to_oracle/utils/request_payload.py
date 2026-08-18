from airflow.exceptions import AirflowFailException
import rail

null = None

def create_time_export_payload():
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
                        "startDate": rail.result("logging_details")["export_start_date_json"],
                        "endDate": rail.result("logging_details")["export_end_date_json"],
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
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                "value": {
                    "uris": [
                    "urn:replicon:approval-status:approved"
                    ]
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
                    "urn:replicon:time-entry-type:worked-time",
                    "urn:replicon:time-entry-type:time-off"
                    ]
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
        "statusUri": f"urn:replicon:time-data-export-status:{status}"
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