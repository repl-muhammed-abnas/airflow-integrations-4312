import json


def time_data_download_parameters(file_format_script_uri, dag_run):
    return json.dumps({
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [dag_run.conf['time_export_uri']],
                },
            },
        },
        "fileFormatScriptUri": file_format_script_uri
    })
