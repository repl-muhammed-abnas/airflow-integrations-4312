from airflow.exceptions import AirflowFailException
import rail

null = None


def create_time_export_payload():
    """
    Generate payload for creating time data export batch.
    
    Returns:
        dict: Payload for CreateTimeDataExportBatch API
    """
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
                                'startDate': rail.result("logging_details")["export_start_date_json"],
                                'endDate': rail.result("logging_details")["export_end_date_json"],
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
                    'filterDefinitionUri': 'urn:replicon:time-data-export-filter:approval-status'
                },
                'operatorUri': 'urn:replicon:filter-operator:in',
                'rightExpression': {
                    'value': {
                        'uris': [
                            'urn:replicon:approval-status:approved'
                        ]
                    }
                }
            }
        }
    }


def create_export_status_complete_batch_payload(export_uri):
    """
    Generate payload for marking export status as complete.
    
    Args:
        export_uri: URI of the export to update
        
    Returns:
        dict: Payload for CreateTimeDataExportStatusBatch API
    """
    return {
        "target": {
            "uri": rail.result(export_uri),
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }


def create_export_status_batch_payload(status):
    """
    Generate payload for updating export status.
    
    Args:
        status: Status to set (e.g., "draft", "cancelled")
        
    Returns:
        dict: Payload for CreateTimeDataExportStatusBatch API
    """
    return {
        "target": {
            "uri": rail.result("get_export_uri_failed"),
            "name": null
        },
        "statusUri": f"urn:replicon:time-data-export-status:{status}"
    }


def get_create_download_batch(export_uri):
    """
    Generate payload for creating download batch for time export.
    
    Args:
        export_uri: URI of the export to download
        
    Returns:
        dict: Payload for CreateTimeDataDownloadBatch API
    """
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
    """
    Extract export URI from response, raising exception on error.
    
    Args:
        response: Response from GetCreateTimeDataExportBatchResults API
        
    Returns:
        str: Export URI
        
    Raises:
        AirflowFailException: If response contains error
    """
    if response.get('error'):
        raise AirflowFailException(response)
    return response['timeDataExportUri']