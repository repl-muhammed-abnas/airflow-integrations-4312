"""
Test script for python_callable functions
"""

null = None


def create_download_batch_payload(dag_run):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [
                            "urn:replicon:approval-status:approved"
                        ],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "urn:replicon:time-data-item-time-data-export-status:none",
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
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [
                                    "urn:replicon:time-entry-type:time-off",
                                    "urn:replicon:time-entry-type:worked-time",
                                    "urn:replicon:time-entry-type:allocation-time"
                                ],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
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
                                    "startDate": dag_run.conf['startDate'],
                                    "endDate": dag_run.conf['endDate'],
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
                                "dateTimeUtc": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "fileFormatScriptUri": dag_run.conf['fileFormatScriptUri']
    }


def get_create_time_data_export_batch_payload(dag_run):
    return {
        "columnUris": [
            "urn:replicon:time-data-export-column:user-department-code",
            "urn:replicon:time-data-export-column:employee-id",
            dag_run.conf['beneficiarycode_columnuri'],
            dag_run.conf['protocolcode_columnuri'],
            dag_run.conf['compassrootcode_columnuri'],
            "urn:replicon:time-data-export-totals-column:hours",
            "urn:replicon:time-data-export-column:entry-date",
            dag_run.conf['compass_dropdown'],
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [
                            "urn:replicon:approval-status:approved"
                        ],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "urn:replicon:time-data-item-time-data-export-status:none",
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
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [
                                    "urn:replicon:time-entry-type:time-off",
                                    "urn:replicon:time-entry-type:worked-time",
                                    "urn:replicon:time-entry-type:allocation-time"
                                ],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
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
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
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
                                    "startDate": dag_run.conf['startDate'],
                                    "endDate": dag_run.conf['endDate'],
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
                                "dateTimeUtc": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }
