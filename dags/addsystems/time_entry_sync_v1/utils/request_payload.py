from datetime import datetime
import uuid
import rail
from rail import get_current_context
from rail.lib.ecid import get_dagrun_ecid
null = None


def get_dag_run_conf():
    return get_current_context()['dag_run'].conf


def mandatory_fields_check(dag_run):
    return (dag_run.conf['item']['UserInitials'] and dag_run.conf['item']['EventAddDate'] and dag_run.conf['item']['CustomerCode']
            and dag_run.conf['item']['EventSummary'] and dag_run.conf['item']['ClienteleCallNum']
            and dag_run.conf['item']['ProjName'])


def get_task_state(task_id):
    return get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_search_user_payload(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf['item']['UserInitials']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def get_task_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:task-list-column:task",
                "urn:replicon:task-list-column:enabled",
                "urn:replicon:task-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:task-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['item']['ProjName'],
                    },
                },
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "uri": rail.result('get_project_details')['uri'],
                    },
                },
            },
        }
    }


def get_client_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['item']['CustomerCode']
                }
            }
        }
    }


def get_time_entry_payload_nonbillable(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['EventAddDate'].split('T')[0].strip(), '%Y-%m-%d').date()

    return {
        "timeEntryRevisionGroup": {
            "user": {
                "uri": rail.result('search_user')[0]['userDetails']['uri']
            },
            "entryDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": int(float(dag_run.conf['item']['NonBillableTime']) * 3600),
                    "milliseconds": "0",
                    "microseconds": "0"
                }
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:is-billable",
                    "value": {
                        "bool": 'false'
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:client",
                    "value": {
                        "uri": rail.result('get_client_data')[0]['clienturi']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result('get_task_data')[0]['Taskuri']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:comments",
                    "value": {
                        "text": dag_run.conf['item']['EventSummary']
                    }
                }
            ],
            "extensionFieldValues": [
                {
                    "textValue": dag_run.conf['item']['RecordHash'],
                    "definition": {
                        "uri": rail.result('get_timeentry_oef')[0]['uri'],
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_time_entry_payload_billable(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['EventAddDate'].split('T')[0].strip(), '%Y-%m-%d').date()

    return {
        "timeEntryRevisionGroup": {
            "user": {
                "uri": rail.result('search_user')[0]['userDetails']['uri']
            },
            "entryDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": int(float(dag_run.conf['item']['BillableTime']) * 3600),
                    "milliseconds": "0",
                    "microseconds": "0"
                }
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:is-billable",
                    "value": {
                        "bool": 'true'
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:client",
                    "value": {
                        "uri": rail.result('get_client_data')[0]['clienturi']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result('get_task_data')[0]['Taskuri']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:comments",
                    "value": {
                        "text": dag_run.conf['item']['EventSummary']
                    }
                }
            ],
            "extensionFieldValues": [
                {
                    "textValue": dag_run.conf['item']['RecordHash'],
                    "definition": {
                        "uri": rail.result('get_timeentry_oef')[0]['uri'],
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_submit_timesheet_payload(dag_run):
    return {
        "timesheetUris": list(map(lambda item: item['timesheeturi'], dag_run.conf['timesheetdetails'])),
        "comments": "Submitted by integration",
        "submitOptions": []
    }


def get_timesheet_for_date(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['EventAddDate'].split('T')[0].strip(), '%Y-%m-%d').date()

    return {
        "userUri": rail.result('search_user')[0]['userDetails']['uri'],
        "date": {
            "year": effective_date.year,
            "month": effective_date.month,
            "day": effective_date.day
        },
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }


def get_timeentry_details(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group",
            "urn:replicon:time-entry-revision-group-list-column:entry-date",
            "urn:replicon:time-entry-revision-group-list-column:hours",
            "urn:replicon:time-entry-revision-group-list-column:project",
            "urn:replicon:time-entry-revision-group-list-column:task",
            "urn:replicon:time-entry-revision-group-list-column:comments",
            "urn:replicon:time-entry-revision-group-list-column:approval-status"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": rail.result('get_timeentry_revision_filters')[0]['uri']
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['item']['RecordHash']
                }
            }
        }
    }


def get_update_time_entry_payload_billable(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['EventAddDate'].split('T')[0].strip(), '%Y-%m-%d').date()

    total_duration = rail.find_first_by_attr_and_get_attr(
        rail.result('get_time_entry_details'), 'billable', True)
    return {
        "timeEntryRevisionGroup": {
            "target": {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'), 'billable', True, 'uri')
            },
            "user": {
                "uri": rail.result('search_user')[0]['userDetails']['uri']
            },
            "entryDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": int(((total_duration['hours']*3600) if (total_duration) else 0) + ((total_duration['minutes']*60) if (total_duration)
                                   else 0)) + int(float(dag_run.conf['item']['BillableTime']) * 3600),
                    "milliseconds": "0",
                    "microseconds": "0"
                }
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:is-billable",
                    "value": {
                        "bool": 'true'
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:client",
                    "value": {
                        "uri": rail.result('get_client_data')[0]['clienturi']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result('get_task_data')[0]['Taskuri']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:comments",
                    "value": {
                        "text": dag_run.conf['item']['EventSummary']
                    }
                }
            ],
            "extensionFieldValues": [
                {
                    "textValue": dag_run.conf['item']['RecordHash'],
                    "definition": {
                        "uri": rail.result('get_timeentry_oef')[0]['uri'],
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_time_entry_payload_nonbillable(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['EventAddDate'].split('T')[0].strip(), '%Y-%m-%d').date()

    total_duration = rail.find_first_by_attr_and_get_attr(
        rail.result('get_time_entry_details'), 'billable', False)

    return {
        "timeEntryRevisionGroup": {
            "target": {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_time_entry_details'), 'billable', False, 'uri')
            },
            "user": {
                "uri": rail.result('search_user')[0]['userDetails']['uri']
            },
            "entryDate": {
                "year": effective_date.year,
                "month": effective_date.month,
                "day": effective_date.day
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "interval": {
                "hours": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": int(((total_duration['hours']*3600) if (total_duration) else 0) + ((total_duration['minutes']*60) if (total_duration)
                                   else 0)) + int(float(dag_run.conf['item']['NonBillableTime']) * 3600),
                    "milliseconds": "0",
                    "microseconds": "0"
                }
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:is-billable",
                    "value": {
                        "bool": 'false'
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:client",
                    "value": {
                        "uri": rail.result('get_client_data')[0]['clienturi']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result('get_task_data')[0]['Taskuri']
                    }
                },
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:comments",
                    "value": {
                        "text": dag_run.conf['item']['EventSummary']
                    }
                }
            ],
            "extensionFieldValues": [
                {
                    "textValue": dag_run.conf['item']['RecordHash'],
                    "definition": {
                        "uri": rail.result('get_timeentry_oef')[0]['uri'],
                    }
                }
            ]
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_time_entry_details():
    return {
        "timeEntryRevisionGroups": rail.result('search_timeentry')
    }


def get_log_data(dag_run):
    return {
        "TransmissionId": dag_run.conf['webhook']['data']['TransmissionId'],
        "EntryDetails": get_entry_data(),
        "JOB_ID": get_dagrun_ecid(dag_run)
    }


def get_entry_data():
    entry_details = rail.load_all_records(rail.result("render_logs_csv"))
    return list(map(lambda details: {
        "InternalId": int(details["InternalId"]),
        "Status": details["Status"],
        "Description": details["Description"],
        "Entrydate": details["Entrydate"]
    }, entry_details))


def get_time_entries_for_user_and_user(dag_run):
    effective_date = datetime.strptime(
        dag_run.conf['item']['EventAddDate'].split('T')[0].strip(), '%Y-%m-%d').date()

    return {
        "user": {
            "uri": rail.result('search_user')[0]['userDetails']['uri']
        },
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

# pylint: disable=too-many-return-statements


def get_message(dag_run, billable_status, non_billable_status, updated_manually_billable, updated_manually_non_billable):

    if (rail.get_current_context()['dag_run'].get_task_instance(
            'is_manually_updated').current_state() == 'success'):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status+" Beacuse (Billable or Non-billable) time entry is manually updated"
    if (((billable_status == 'success') and (updated_manually_billable == 'success'))
        and ((non_billable_status ==
              'success') and (updated_manually_non_billable == 'success'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status
    if (((billable_status == 'skipped') and (updated_manually_billable == 'skipped'))
        and ((non_billable_status ==
              'success') and (updated_manually_non_billable == 'success'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status
    if (((billable_status == 'success') and (updated_manually_billable == 'success'))
        and ((non_billable_status ==
              'skipped') and (updated_manually_non_billable == 'skipped'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status
    if (((billable_status == 'skipped') and (updated_manually_billable == 'skipped'))
        and ((non_billable_status ==
              'skipped') and (updated_manually_non_billable == 'skipped'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" and Non Billable=" + \
            str(dag_run.conf['item']['NonBillableTime'])+"  is "+rail.get_current_context()['dag_run'].get_task_instance(
            'update_time_entry_nonbillable').current_state()
    if (((billable_status == 'skipped') and (updated_manually_billable == 'success'))
        and ((non_billable_status ==
              'success') and (updated_manually_non_billable == 'success'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" Since it is manually updated and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status
    if (((billable_status == 'success') and (updated_manually_billable == 'success'))
        and ((non_billable_status ==
              'skipped') and (updated_manually_non_billable == 'success'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status+" Since it is manually updated"
    if (((billable_status == 'skipped') and (updated_manually_billable == 'success'))
        and ((non_billable_status ==
              'skipped') and (updated_manually_non_billable == 'success'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" Since it is manually updated and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status+" Since it is manually updated"
    if (((billable_status == 'skipped') and (updated_manually_billable == 'success'))
        and ((non_billable_status ==
              'skipped') and (updated_manually_non_billable == 'skipped'))):
        # pylint: disable=line-too-long
        return "Billable="+str(dag_run.conf['item']['BillableTime'])+" is " + billable_status+" Since it is manually updated and Non Billable="+str(dag_run.conf['item']['NonBillableTime'])+"  is "+non_billable_status
    return None


def get_severity(billable_status, non_billable_status):

    return "Success" if ((billable_status == 'success') and (non_billable_status == 'success')) else 'Skipped'
