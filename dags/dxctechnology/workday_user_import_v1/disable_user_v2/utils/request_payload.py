from json import loads
from datetime import datetime
from rail import result
from dateutil.parser import parse as date_parser

REPORT_DATE_FORMAT = "%d %B %Y"

def disable_users_report_generation_params():
    return {
        "reportParameters": [
            {
                "reportUri": result("get_report_details")['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def convert_date_str_to_json_date(date_str, date_format=None):
    _date = date_parser(date_str)
    if date_format:
        _date = datetime.strptime(date_str, date_format)
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }


def get_process_disable_user_conf(item):
    return {
        "user_name": item["user_name"],
        "end_date": item["user_end_date"],
        "user_uri": item["user_uri"],
        "login_name": item["login_name"],
        "country": item["current_location"].split(
            "/")[0].strip() if item["current_location"] else "No Location",
        "company_code_type": item["current_company_code"].split(
            "/")[0].strip() if item["current_company_code"] else "No Company Code",
        "user_end_date_json": convert_date_str_to_json_date(item["user_end_date"], REPORT_DATE_FORMAT),
        "starting_balance_set_to_uri": result("get_starting_balance_script"),
        "prevent_balance_overdraw_uri": result("get_prevent_balance_overdraw_script"),
        "disable_required": "Yes",
        "employee_type": item["employee_type"],
        "employee_type_check": item["employee_type"] not in ["SOW Contractor", "Contractor", "Agency Contractor"],
        'user_start_date': item['user_start_date'],
        'user_start_date_json': convert_date_str_to_json_date(item['user_start_date'], REPORT_DATE_FORMAT),
        'fte_pct': item['fte_pct']
    }


def get_trigger_id(config, item):
    return f"{config.disable_user_process_each_user_dag_id}_batch_{str(int(item['record_id'])%config.process_disable_user_dag_count)}"


def get_user_timeoff_balance_summary_payload(dag_run):
    return {
        "account": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
        },
        "asOfDate": dag_run.conf['user_end_date_json']
    }


def get_update_policy_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
        },
        "policySetScheduleEntries": loads(result("format_timeoff_polices_to_assign"))
    }

def get_time_entry_details_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group",
            "urn:replicon:time-entry-revision-group-list-column:timesheet",
            "urn:replicon:time-entry-revision-group-list-column:entry-date",
            "urn:replicon:time-entry-revision-group-list-column:time-entry-status"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {
                                "year": result('calculate_deletion_date')['year'],
                                "month": result('calculate_deletion_date')['month'],
                                "day": result('calculate_deletion_date')['day']
                            }
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-entry-revision-group-list-filter:user"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['user_uri']
                    }
                }
            }
        }
    }

def get_timeoff_details_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:time-off-list-column:time-off",
            "urn:replicon:time-off-list-column:start-date",
            "urn:replicon:time-off-list-column:end-date",
            "urn:replicon:time-off-list-column:time-off-type"

        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-date-range"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {
                                "year": result('calculate_deletion_date')['year'],
                                "month": result('calculate_deletion_date')['month'],
                                "day": result('calculate_deletion_date')['day']
                            }
                        }
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-off-list-filter:time-off-owner"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['user_uri']
                    }
                }
            }
        }
    }

def get_time_punch_details_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:time-punch-list-column:time-punch"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-punch-list-filter:user"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": dag_run.conf['user_uri']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-punch-list-filter:time-punch-date-time"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "dateRange": {
                            "startDate": {
                                "year": result('calculate_deletion_date')['year'],
                                "month": result('calculate_deletion_date')['month'],
                                "day": result('calculate_deletion_date')['day']
                            }
                        }
                    }
                }
            }
        }
    }
