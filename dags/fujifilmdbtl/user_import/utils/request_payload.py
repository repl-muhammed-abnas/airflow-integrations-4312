from uuid import uuid4
import rail

null = None


def get_search_user_payload_for_supervisor(text_search_term):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:login-name",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:email-address",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": text_search_term
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_data_for_department_based_on_name_and_code(text_search_term_1, text_search_term_2):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
                "urn:replicon:department-list-column:department",
                "urn:replicon:department-list-column:code",
            "urn:replicon:department-list-column:enabled",
            "urn:replicon:department-list-column:parent-department"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:department-list-filter:enabled"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": "true",
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
                        "filterDefinitionUri": "urn:replicon:department-list-filter:text"
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
                            "text": text_search_term_1,
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
                        "filterDefinitionUri": "urn:replicon:department-list-filter:text"
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
                            "text": text_search_term_2,
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
                "value": null,
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_input_payload_for_user_pay_rate_modification(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['useruri']
        },
        "template": null,
        "modifications": {
            "payRateSchedule": [
                {
                    "dateRange": {
                        "startDate": {
                            "year": rail.result('date_split_todaysdate_40')['year'],
                            "month": rail.result('date_split_todaysdate_40')['month'],
                            "day": rail.result('date_split_todaysdate_40')['day']
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "hourlyRate": {
                            "amount": float(dag_run.conf['hourlyratejobdata']) if dag_run.conf['assignedshift'] == "1" else float(dag_run.conf['hourlyrate2']),
                            "currency": {
                                "uri": rail.result('_adhoc_http_action_148')['uri'],
                                "name": null,
                                "symbol": null
                            }
                        }
                    }
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4()),
    }
