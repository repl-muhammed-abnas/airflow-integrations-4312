import uuid
import rail

null = None


def get_users_by_id_payload(dag_run):
    return {
        "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
        "sort": [],
        "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                            "text": dag_run.conf['userid'],
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
                }
    }


def get_all_tasks_enabled_payload(dag_run):
    return {
        "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:task-list-column:task",
                    "urn:replicon:task-list-column:code",
                    "urn:replicon:task-list-column:enabled"
                ],
        "sort": [],
        "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:task-list-filter:text"
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
                            "text": dag_run.conf['taskcode'],
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
                }
    }


def get_putnewtimeentries_40_payload(dag_run):
    return {
        "timeEntry": {
            "target": {
                "uri": null,
                "parameterCorrelationId": "replicon"+str(uuid.uuid4())
            },
            "user": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "entryDate": {
                "year": dag_run.conf['entrydateyear'],
                "month": dag_run.conf['entrydatemonth'],
                "day": dag_run.conf['entrydateday']
            },
            "timeAllocationTypeUris":  [
                "urn:replicon:time-allocation-type:project",
                "urn:replicon:time-allocation-type:attendance"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": rail.result('log_minutesworkedtouse_9'),
                    "milliseconds": "0",
                    "microseconds": "0"
                },
                "timePair": null
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": dag_run.conf['taskuri'],
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:widget-ui-metadata-key:row-number",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": rail.result('log_rownumbertopass_5'),
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ],
            "extensionFieldValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_putnewtimeentries_36_payload(dag_run):
    return {
        "timeEntry": {
            "target": {
                "uri": null,
                "parameterCorrelationId": str(uuid.uuid4())
            },
            "user": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "entryDate": {
                "year": dag_run.conf['entrydateyear'],
                "month": dag_run.conf['entrydatemonth'],
                "day": dag_run.conf['entrydateday']
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:project",
                "urn:replicon:time-allocation-type:attendance"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": rail.result('log_minutesworkedtouse_9'),
                    "milliseconds": "0",
                    "microseconds": "0"
                },
                "timePair": null
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result('log_taskuri'),
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:widget-ui-metadata-key:row-number",
                    "value": {
                        "uri": null,
                        "slug": null,
                        "bool": null,
                        "date": null,
                        "number": rail.result('log_rownumbertopass_5'),
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "collection": []
                    }
                }
            ],
            "extensionFieldValues": []
        },
        "unitOfWorkId": str(uuid.uuid4())
    }
