from datetime import date
import uuid
from rail.lib.ecid import get_dagrun_ecid
import rail
from four_liberty.task_import.utils import custom_methods

null = None


def get_project_details_payload():
    return {
        "projects": [
            {
                "uri": null,
                "name": rail.result('project_file_name'),
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_task_details_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:task-list-column:task",
            "urn:replicon:task-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:task-list-filter:project"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": rail.result('bulk_get_project_details3')['uri'],
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
        }
    }


def get_duplicatetasklist():
    rows = custom_methods.get_data_from_document(rail.result('query_list_59'))

    return list(map(lambda row: {
        "taskname": row['taskname'],
        "taskuri": row['taskuri'],
        "isenabled": row['isenabled']
    }, rows)) if rail.result('query_list_59') else []


def get_task_create_conf(item):
    return {
        "Budgetcodename": item['Budget_Code_Name'].strip() if item['Budget_Code_Name'] else null,
        "Budgetcode": item['Budget_Code'].strip() if item['Budget_Code'] else null,
        "Substation_WorkOrderName": item['Substation___Work_Order_Name'].strip() if item['Substation___Work_Order_Name'] else null,
        "WorkOrder": item['Work_Order'].strip() if item['Work_Order'] else null,
        "InternalOrder": item['Internal_Order'].strip() if item['Internal_Order'] else null,
        "FERCCode": item['FERC_Code'].strip() if item['FERC_Code'] else null,
        "SystemStatus": item['System_Status'].strip() if item['System_Status'] else null,
        "WorkOrderStatus": item['Work_Order_Status'].strip() if item['Work_Order_Status'] else null,
        "projecturi": rail.result('bulk_get_project_details3')['uri'],
        "FERCCode_UDFuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'FERC Code', 'uri'),
        "SystemStatus_UDFuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'System Status', 'uri'),
        "WorkOrderStatus_UDFuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Work Order Status', 'uri'),
        "projectname": rail.result('project_file_name'),
        "TaskName": item['Task_Name'].strip() if item['Task_Name'] else null,
        "Opendate": item['Open_Date'].strip() if item['Open_Date'] else null,
        "TECOdate": item['TECO_Date'].strip() if item['TECO_Date'] else null,
        "closedate": item['Close_Date'].strip() if item['Close_Date'] else null,
        "budgetCodeNameUdf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Budget Code Name', 'uri'),
        "budgetCodeUdf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Budget Code', 'uri'),
        "Substation_workOrderNameUdf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Work Order Name', 'uri'),
        "WorkOrderUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Work Order', 'uri'),
        "InternalOrderUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Internal Order', 'uri'),
        "OpenDateUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Open Date', 'uri'),
        "CloseDateUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Close Date', 'uri'),
        "TecoDateUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'TECO Date', 'uri'),
        "Taskuri": rail.find_first_by_attr_and_get_attr(rail.result('get_taskdetails'), 'taskname', item['Task_Name'].strip(), 'taskuri'),
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
    }


def get_task_update_conf(item):
    return {
        "Budgetcodename": item['Budget_Code_Name'].strip() if item['Budget_Code_Name'] else null,
        "Budgetcode": item['Budget_Code'].strip() if item['Budget_Code'] else null,
        "Substation_WorkOrderName": item['Substation___Work_Order_Name'].strip() if item['Substation___Work_Order_Name'] else null,
        "WorkOrder": item['Work_Order'].strip() if item['Work_Order'] else null,
        "InternalOrder": item['Internal_Order'].strip() if item['Internal_Order'] else null,
        "FERCCode": item['FERC_Code'].strip() if item['FERC_Code'] else null,
        "SystemStatus": item['System_Status'].strip() if item['System_Status'] else null,
        "WorkOrderStatus": item['Work_Order_Status'].strip() if item['Work_Order_Status'] else null,
        "projecturi": rail.result('bulk_get_project_details3')['uri'],
        "FERCCode_UDFuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'FERC Code', 'uri'),
        "SystemStatus_UDFuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'System Status', 'uri'),
        "WorkOrderStatus_UDFuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Work Order Status', 'uri'),
        "projectname": rail.result('project_file_name'),
        "TaskName": item['Task_Name'].strip() if item['Task_Name'] else null,
        "Opendate": item['Open_Date'].strip() if item['Open_Date'] else null,
        "TECOdate": item['TECO_Date'].strip() if item['TECO_Date'] else null,
        "closedate": item['Close_Date'].strip() if item['Close_Date'] else null,
        "budgetCodeNameUdf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Budget Code Name', 'uri'),
        "budgetCodeUdf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Budget Code', 'uri'),
        "Substation_workOrderNameUdf_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Work Order Name', 'uri'),
        "WorkOrderUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Work Order', 'uri'),
        "InternalOrderUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Internal Order', 'uri'),
        "OpenDateUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Open Date', 'uri'),
        "CloseDateUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Close Date', 'uri'),
        "TecoDateUdfUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'TECO Date', 'uri'),
        "Taskuri": rail.find_first_by_attr_and_get_attr(rail.result('get_taskdetails'), 'taskname', item['Task_Name'].strip(), 'taskuri'),
        "parentjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
    }


def get_task_update_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['Taskuri']
        },
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "modifications": {
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['budgetCodeNameUdf_uri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": dag_run.conf['Budgetcodename'] if dag_run.conf['Budgetcodename'] else null
                },
                {
                    "customField": {
                        "uri": dag_run.conf['SystemStatus_UDFuri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": dag_run.conf['SystemStatus'] if dag_run.conf['SystemStatus'] else null
                },
                {
                    "customField": {
                        "uri": dag_run.conf['OpenDateUdfUri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": dag_run.conf['Opendate'] if dag_run.conf['Opendate'] else null
                },
                {
                    "customField": {
                        "uri": dag_run.conf['CloseDateUdfUri'],
                        "name": null,
                        "groupUri": null
                    },
                    "text": dag_run.conf['closedate'] if dag_run.conf['closedate'] else null
                },
                {
                    "customField": {
                        "uri": dag_run.conf['TecoDateUdfUri'],
                        "groupUri": null,
                        "name": null
                    },
                    "text": dag_run.conf['TECOdate'] if dag_run.conf['TECOdate'] else null
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4()),
    }


def get_adhoc_http_action_10_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['Taskuri']
        },
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "modifications": {
            "isTimeEntryAllowed": "true",
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['WorkOrderStatus_UDFuri']
                    },
                    "dropDownOption": {
                        "name": dag_run.conf['WorkOrderStatus'].upper()
                    }
                }
            ],
            "timeEntryEndDateToApply": {
                "date": {
                    "year": int(date.today().strftime("%Y")),
                    "month": int(date.today().strftime("%m")),
                    "day": int(date.today().strftime("%d"))
                }
            }
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_adhoc_http_action_12_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['Taskuri']
        },
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "modifications": {
            "isTimeEntryAllowed": "true",
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['WorkOrderStatus_UDFuri']
                    },
                    "dropDownOption": {
                        "name": dag_run.conf['WorkOrderStatus'].upper()
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_adhoc_http_action_14_payload(dag_run):
    return {
        "target": {
            "uri": dag_run.conf['Taskuri']
        },
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "modifications": {
            "isTimeEntryAllowed": "true",
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['WorkOrderStatus_UDFuri']
                    },
                    "dropDownOption": {
                        "name": dag_run.conf['WorkOrderStatus'].upper()
                    }
                }
            ],
            "timeEntryEndDateToApply": {
                "date": null
            }
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_adhoc_http_action_17_payload(dag_run):
    return {
        "modifications": {
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['budgetCodeNameUdf_uri']
                    },
                    "text": dag_run.conf['Budgetcodename'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['budgetCodeUdf_uri']
                    },
                    "text": dag_run.conf['Budgetcode'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['Substation_workOrderNameUdf_uri']
                    },
                    "text": dag_run.conf['Substation_WorkOrderName'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['WorkOrderUdfUri']
                    },
                    "text": dag_run.conf['WorkOrder'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['InternalOrderUdfUri']
                    },
                    "text": dag_run.conf['InternalOrder'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['FERCCode_UDFuri']
                    },
                    "text": dag_run.conf['FERCCode'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['SystemStatus_UDFuri']
                    },
                    "text": dag_run.conf['SystemStatus'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['WorkOrderStatus_UDFuri']
                    },
                    "text": null,
                    "dropDownOption": {
                        "name": dag_run.conf['WorkOrderStatus'].upper()
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['OpenDateUdfUri'] if dag_run.conf['OpenDateUdfUri'] else null
                    },
                    "text": dag_run.conf['Opendate'],
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['CloseDateUdfUri']
                    },
                    "text": dag_run.conf['closedate'] if dag_run.conf['closedate'] else null,
                    "dropDownOption": {
                        "name": null
                    }
                },
                {
                    "customField": {
                        "uri": dag_run.conf['TecoDateUdfUri']
                    },
                    "text": dag_run.conf['TECOdate'] if dag_run.conf['TECOdate'] else null,
                    "dropDownOption": {
                        "name": null
                    }
                }
            ],
            "name": dag_run.conf['TaskName'],
            "isClosed": "false",
            "timeAndExpenseEntryTypeToApply": {
                        "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "isTimeEntryAllowed": "true"
        },
        "target": null,
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_adhoc_http_action_19_payload(dag_run):
    return {
        "target": {
            "uri": rail.result("_adhoc_http_action_17")['uri']
        },
        "project": {
            "uri": dag_run.conf['projecturi']
        },
        "modifications": {
            "isTimeEntryAllowed": "true",
            "customFieldsToApply": [
                {
                    "customField": {
                        "uri": dag_run.conf['WorkOrderStatus_UDFuri']
                    },
                    "dropDownOption": {
                        "name": dag_run.conf['WorkOrderStatus'].upper()
                    }
                }
            ],
            "timeEntryEndDateToApply": {
                "date": {
                    "year": int(date.today().strftime("%Y")),
                    "month": int(date.today().strftime("%m")),
                    "day": int(date.today().strftime("%d"))
                }
            }
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_adhoc_http_action_22_payload(dag_run,item):
    return {
        "taskHierarchy": [{
            "target": {
                "uri": item['taskuri']
            },
            "taskModificationToApply": {
                "timeEntryEndDateToApply": {
                    "date": {
                        "year": int(date.today().strftime("%Y")),
                        "month": int(date.today().strftime("%m")),
                        "day": int(date.today().strftime("%d"))
                    }
                },
                "isClosed": "true"
            }
        }],
        "unitOfWorkId": str(uuid.uuid4()),
        "project": {
            "uri": dag_run.conf['projecturi']
        }
    }


def bulk_update_task_team_members_data():
    return {
        "taskUri": rail.result('_adhoc_http_action_17')['uri'],
        "resourceUris": rail.result("get_all_project_team_assignment") if rail.result("get_all_project_team_assignment") else [],
        "isAssigned": "true"
    }
