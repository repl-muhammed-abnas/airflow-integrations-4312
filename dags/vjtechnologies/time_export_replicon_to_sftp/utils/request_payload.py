def create_timedata_download_batch_payload(dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                    "uris": [
                        "urn:replicon:time-entry-type:worked-time"
                    ]
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
            }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:division"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                "uris": [
                    dag_run.conf['companycode_uri']
                ]
                }
            }
            }
        },
        "fileFormatScriptUri": dag_run.conf['file_format_uri']
    }

def create_timedata_export_batch_payload(dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                    "uris": [
                        "urn:replicon:time-entry-type:worked-time"
                    ]
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
            }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:division"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                "uris": [
                    dag_run.conf['companycode_uri']
                ]
                }
            }
            }
        }
    }
