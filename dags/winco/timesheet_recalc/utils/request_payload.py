from datetime import datetime
import rail
null = None


def get_process_time_data_records_conf(item):
    return {
        **{k: v if v is not None else '' for k, v in item.items()}
    }


def get_timesheet_data(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['entrystartdate'], '%m-%d-%Y')
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:timesheet-list-column:timesheet",
            "urn:replicon:timesheet-list-column:timesheet-period",
            "urn:replicon:timesheet-list-column:timesheet-owner",
            "urn:replicon:timesheet-list-column:timesheet-status",
            "urn:replicon:timesheet-list-column:regular-time-duration",
            "urn:replicon:timesheet-list-column:overtime-duration",
            "urn:replicon:timesheet-list-column:time-off-duration",
            "urn:replicon:timesheet-list-column:total-payable-duration",
            "urn:replicon:timesheet-list-column:due-date",
            "urn:replicon:timesheet-list-column:timesheet-script-calculation-status"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:timesheet-list-filter:is-in-data-access-level"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": "urn:replicon:timesheet-data-access-level:payroll-data-access-scope"
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:timesheet-list-filter:timesheet-period-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "dateRange": {
                                "startDate": {
                                    "year": effective_date.year,
                                    "month": effective_date.month,
                                    "day": effective_date.day
                                },
                                "endDate": {
                                    "year": effective_date.year,
                                    "month": effective_date.month,
                                    "day": effective_date.day
                                }
                            }
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:timesheet-list-filter:department-of-timesheet-owner"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": rail.result('get_department_details')[0]['uri']
                        }
                    }
                }
            }
        }
    }


def get_timesheets_payload(dag_run):
    return {
        "timesheets": list(map(lambda item: item['timesheeturi'], dag_run.conf['timesheetdetails']))
    }


def get_report_data():
    return {
        "reportUri": rail.result('get_report_details')['uri']
    }


def get_users__data():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:department",
            "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "operatorUri": "urn:replicon:filter-operator:equal",
                "filterDefinitionUri": "urn:replicon:user-list-filter:department"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": rail.result('get_department_details')[0]['uri'],
                }
            }
        }
    }


def get_report_filter(dag_run):
    datefilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_report_deatails2')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri')
    userfilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_report_deatails2')['filterConfiguration']['enabledFilters'], 'displayText', "UserFilter", 'uri')
    paycodefilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_report_deatails2')['filterConfiguration']['enabledFilters'], 'displayText', "PayCodeFilter", 'uri')
    date_filter = [{
        "reportFilterUri": datefilter,
        "value": null
    },
        {
        "reportFilterUri": datefilter,
        "value": dag_run.conf['entrystartdate']
    },
        {
        "reportFilterUri": datefilter,
        "value": dag_run.conf['entryenddate']
    }]
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_report_details")["uri"],
                "filterValues":
                    date_filter + create_user_reportfilter(
                        userfilter) + create_paycode_reportfilter(paycodefilter),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def create_user_reportfilter(userfilter):
    return list(map(lambda data: {
        "reportFilterUri": userfilter,
        "value": data['useruri'].split(':')[4].strip()
    }, rail.result('get_users_details')))


def create_paycode_reportfilter(paycodefilter):
    return list(map(lambda data: {
        "reportFilterUri": paycodefilter,
        "value": data['uri'].split(':')[4].strip()
    }, rail.result('get_all_paycode_details')))
