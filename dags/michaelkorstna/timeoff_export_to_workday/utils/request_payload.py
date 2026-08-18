import json
from datetime import datetime
from airflow.models import Variable
import rail

null = None


# Factory functions for Workday endpoint/data callables
def create_get_workday_post_timeoff_endpoint(config):
    """Factory function that returns a callable for getting Workday POST timeoff endpoint"""
    def get_workday_post_timeoff_endpoint(dag_run):
        if config.instance == 'trial':
            return ''
        employee_id = dag_run.conf['employeeid']
        return f'/ccx/api/absenceManagement/v1/{config.workday_tenant}/workers/{employee_id}/requestTimeOff'
    return get_workday_post_timeoff_endpoint


def create_get_workday_report_endpoint(config):
    """Factory function that returns a callable for getting Workday report endpoint"""
    def get_workday_report_endpoint():
        if config.instance == 'trial':
            return ''
        return f'/ccx/service/customreport2/{config.workday_tenant}/{config.workday_report_template_name}/{config.workday_report_name}?Organizations%21WID={config.workday_org_wid}&Include_Managers=1&End_Date={rail.result("format_date_for_report")}&Start_Date={rail.result("format_date_for_report")}&Include_Subordinate_Organizations=1&format=json'
    return get_workday_report_endpoint


def create_get_workday_report_entries(config):
    """Factory function that returns a callable for getting Workday report entries"""
    def get_workday_report_entries():
        if config.skip_workday_report_query:
            mock_data = Variable.get(config.workday_report_mock_var_name, default_var=[], deserialize_json=True)
            return mock_data.get('Report_Entry', []) if mock_data else []
        return rail.result('query_workday_report').get('Report_Entry', [])
    return get_workday_report_entries


def create_time_data_export_batch_payload_new():
    """Build payload for CreateTimeDataExportBatch for new records"""
    start_date = rail.result("logging_details")['export_start_date_json']
    end_date = rail.result("logging_details")['export_end_date_json']
    return {
        "columnUris": [
            "urn:replicon:time-data-export-column:employee-id",
            "urn:replicon:time-data-export-column:user-login-name",
            "urn:replicon:time-data-export-column:time-off-id",
            "urn:replicon:time-data-export-column:time-off-code-name",
            "urn:replicon:time-data-export-totals-column:hours",
            "urn:replicon:time-data-export-column:current-time-entry-approval-status",
            "urn:replicon:time-data-export-column:entry-date",
            "urn:replicon:time-data-export-column:time-off-code-description"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            "urn:replicon:approval-status:waiting",
                            "urn:replicon:approval-status:approved"
                        ]
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": ["urn:replicon:time-data-item-time-data-export-status:none"]
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": ["urn:replicon:time-entry-type:time-off"]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [],
                                "dateRange": {
                                    "startDate": {
                                        "year": start_date["year"],
                                        "month": start_date["month"],
                                        "day": start_date["day"]
                                    },
                                    "endDate": {
                                        "year": end_date["year"],
                                        "month": end_date["month"],
                                        "day": end_date["day"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }


def create_time_data_export_batch_payload_delta():
    """Build payload for CreateTimeDataExportBatch for delta records"""
    end_date = rail.result("logging_details")['export_end_date_json']
    return {
        "columnUris": [
            "urn:replicon:time-data-export-column:employee-id",
            "urn:replicon:time-data-export-column:user-login-name",
            "urn:replicon:time-data-export-column:time-off-id",
            "urn:replicon:time-data-export-column:time-off-code-name",
            "urn:replicon:time-data-export-totals-column:hours",
            "urn:replicon:time-data-export-column:current-time-entry-approval-status",
            "urn:replicon:time-data-export-column:entry-date",
            "urn:replicon:time-data-export-column:time-off-code-description"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": ["urn:replicon:approval-status:approved"]
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": ["urn:replicon:time-data-item-time-data-export-status:none"]
                        }
                    }
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": ["urn:replicon:time-entry-type:time-off"]
                            }
                        }
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "value": {
                                "uris": [],
                                "dateRange": {
                                    "startDate": None,
                                    "endDate": {
                                        "year": end_date["year"],
                                        "month": end_date["month"],
                                        "day": end_date["day"]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }


def create_time_data_download_batch_payload(dag_run, export_uri_task_id):
    """Build payload for CreateTimeDataDownloadBatch"""
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
                    "uris": [rail.result(export_uri_task_id)]
                }
            }
        },
        "fileFormatScriptUri": dag_run.conf['file_format_script_uri']
    }


def get_timeoff_details_payload(dag_run):
    """Build payload for GetTimeOffDetails2 API"""
    return {
        "timeOffUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:time-off:{dag_run.conf['timeoffbookingid']}"
    }


def update_time_data_export_name_payload(export_uri_task_id, name):
    """Build payload for UpdateTimeDataExportName API"""
    return {
        "target": {
            "uri": rail.result(export_uri_task_id)
        },
        "name": name
    }


def create_export_status_complete_batch_payload(export_uri_task_id):
    return {
        "target": {
            "uri": rail.result(export_uri_task_id),
            "name": null
        },
        "statusUri": "urn:replicon:time-data-export-status:complete"
    }


def create_export_status_batch_payload(status):
    """Build payload for CreateTimeDataExportStatusBatch API with specified status"""
    return {
        "target": {
            "uri": rail.result('get_export_uri_failed'),
            "name": null
        },
        "statusUri": f"urn:replicon:time-data-export-status:{status}"
    }


def build_enter_time_off_payload(dag_run, include_comment=False):
    """Build JSON payload for Workday REST API requestTimeOff endpoint"""
    time_off_entry_id = rail.result('create_unique_id')
    # Convert date from MM/DD/YYYY to YYYY-MM-DD
    entry_date_obj = datetime.strptime(dag_run.conf['entrydate'], "%m/%d/%Y")
    entry_date = entry_date_obj.strftime("%Y-%m-%d")
    hours = float(dag_run.conf['hours'])
    time_off_code = dag_run.conf['timeofftypedescription']

    day_entry = {
        "date": entry_date,
        "dailyQuantity": hours,
        "timeOffType": {
            "id": time_off_code
        },
        "id": time_off_entry_id
    }

    # Get comment if needed (from extract_reason task for UK Other Paid Leave)
    if include_comment:
        try:
            comment_value = rail.result('extract_reason')
            if comment_value:
                day_entry["comment"] = comment_value
        except (KeyError, TypeError):
            # Comment not available, continue without it
            pass

    return json.dumps({"days": [day_entry]})


def build_correct_time_off_payload(dag_run):
    """Build JSON payload for Workday REST API correctTimeOff endpoint

    Note: In Workday REST API, corrections are done by setting dailyQuantity
    to the new value (or 0 to cancel). This uses the same requestTimeOff endpoint
    but with the existing time off entry reference.
    """
    time_off_entry_id = rail.result('create_unique_id')
    # Convert date from MM/DD/YYYY to YYYY-MM-DD
    entry_date_obj = datetime.strptime(dag_run.conf['entrydate'], "%m/%d/%Y")
    entry_date = entry_date_obj.strftime("%Y-%m-%d")
    hours = float(dag_run.conf['hours'])
    time_off_code = dag_run.conf['timeofftypedescription']

    day_entry = {
        "date": entry_date,
        "dailyQuantity": hours,
        "timeOffType": {
            "id": time_off_code
        },
        "id": time_off_entry_id
    }

    return json.dumps({"days": [day_entry]})