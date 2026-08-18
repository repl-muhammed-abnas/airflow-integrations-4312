from datetime import datetime
import rail

null = None
DATE_FORMAT = "%Y-%m-%d"

def get_timeoff_booking_details_payload(dag_run):
    return {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:time-off-list-column:time-off",
                    "urn:replicon:time-off-list-column:approval-status",
                    "urn:replicon:time-off-list-column:total-duration",
                    "urn:replicon:time-off-list-column:time-off-type"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": rail.result('get_user_uri'),
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
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
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
                            "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
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
                                    "startDate": rail.get_replicon_date(datetime.strptime(dag_run.conf['timeoffdate'], DATE_FORMAT)),
                                    "endDate": rail.get_replicon_date(datetime.strptime(dag_run.conf['timeoffdate'], DATE_FORMAT)),
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }

def put_time_off2_partialbooking_payload(dag_run):
    return {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_new_time_off_draft')
                    },
                    "owner": {
                        "uri": rail.result('get_user_uri'),
                        "loginName": None,
                        "parameterCorrelationId": None
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": None
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": rail.get_replicon_date(datetime.strptime(dag_run.conf['timeoffdate'], DATE_FORMAT)),
                            "timeOfDay": None,
                            "relativeDuration": None,
                            "specificDuration":{
                                "hours": str(int(float(dag_run.conf['amount']) * 8)) if dag_run.conf['unit'] == 'Days' else str(dag_run.conf['amount']),
                                "minutes": "0",
                                "seconds": "0",
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": None
                    },
                    "userExplicitEntries": [],
                    "comments": "Added by Replicon Integration",
                    "customFieldValues": []
                }
            }

def put_time_off2_update_payload(dag_run):
    return  {
                "timeOff": {
                    "target": {
                        "uri": rail.result('create_edit_time_off_draft')
                    },
                    "owner": {
                        "uri": rail.result("get_user_uri"),
                        "loginName": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": rail.get_replicon_date(datetime.strptime(dag_run.conf['timeoffdate'], DATE_FORMAT)),
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": "0",
                                "minutes": "0",
                                "seconds": rail.result('log_getthedifferencehoursin_seconds')
                                                if float(dag_run.conf['amount']) < 0 else rail.result('log_add_hours_to_existing_in_seconds'),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": null
                    },
                    "userExplicitEntries": [],
                    "comments": "Updated by Replicon Integration",
                    "customFieldValues": []
                }
            }
