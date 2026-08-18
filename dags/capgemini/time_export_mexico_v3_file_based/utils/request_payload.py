from airflow.exceptions import AirflowFailException

null = None

def get_create_download_batch(dag_run):
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
                    "uris": [dag_run.conf["time_export_uri"]],
                },
            },
        },
        "fileFormatScriptUri": dag_run.conf["file_format_uri"]
    }

def retrieve_export_uri(response):
    if response['error']:
        raise AirflowFailException(response)
    return response['timeDataExportUri']
