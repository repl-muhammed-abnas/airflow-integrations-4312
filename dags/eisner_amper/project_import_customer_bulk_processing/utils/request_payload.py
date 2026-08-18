from datetime import datetime
import uuid
import rail


null = None

def get_all_mandatory_check_clients(dag_run):
    return dag_run.conf['item']['ClientCode'] and dag_run.conf['item']['ClientName'] and dag_run.conf['item']['EAClientCode'] \
    and dag_run.conf['item']['EAClientName']

def get_all_mandatory_check_projects(dag_run):
    return dag_run.conf['item']['ProjectStatus'] and dag_run.conf['item']['ProjectCode'] and dag_run.conf['item']['ProjectName'] and \
    dag_run.conf['item']['ProjectStartDate'] and dag_run.conf['item']['ProjectEndDate'] and dag_run.conf['item']['TimeAndExpenseEntryType']

def get_all_mandatory_check_tasks(dag_run):
    return dag_run.conf['task_name'] and dag_run.conf['task_code']

MANDATORY_FIELDS = {
    "client_fields": {
        "ClientCode":"Client Code",
        "ClientName": "Client Name",
        "EAClientName": "EA Client Name",
        "EAClientCode": "EA Client Code"
    },
    "project_fields": {
        "ProjectStatus":"Project Status",
        "ProjectCode": "Project Code",
        "ProjectName": "Project Name",
        "ProjectStartDate": "Project StartDate",
        "ProjectEndDate": "Project EndDate",
        "TimeAndExpenseEntryType": "Time And Expense EntryType"
    }
    }

def get_exception_message(dag_run, mandatory_fields):
    missing_fields = []
    for payload_key, log_value in mandatory_fields.items():
        if not dag_run.conf['item'][payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    return rail.smartjoin_by_delim(missing_fields, ";")


def get_exception_message_tasks(dag_run):
    message=[]
    if not dag_run.conf["task_name"]:
        message.append('TaskName is not present in payload')
    if not dag_run.conf["task_code"]:
        message.append('TaskCode is not present in payload')
    return rail.smartjoin_by_delim(message, ";")

def get_client_payload(dag_run):
    return {
        "page": 1,
        "pagesize": 100000,
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:name",
            "urn:replicon:client-list-column:code"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
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
                    "text": dag_run.conf['item']['ClientCode'],
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

def get_client_udfs(dag_run):
    udfs=[]

    def add_udf(textvalue, uri):
        udfs.append(
            {
                "customField": {
                "uri": uri,
                "name": null,
                "groupUri": null
                },
                "text": textvalue,
                "date": null,
                "dropDownOption": null,
                "number": null
            }
        )
    add_udf(dag_run.conf['item']['EAClientCode'], dag_run.conf['eaclientcodeudfuri'])
    add_udf(dag_run.conf['item']['EAClientName'], dag_run.conf['eaclientnameudfuri'])

    return udfs

def does_client_exist():
    if rail.result('search_client_in_replicon') !=[]:
        return True
    return False

def apply_client_modifications_payload(dag_run):
    return {
        "target": null if not does_client_exist() else {
                "uri": rail.result('search_client_in_replicon')[0]['clienturi'],
                "name": null,
                "code": null,
                "parameterCorrelationId": null
            },
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['item']["ClientName"]
            },
            "codeToApply": {
                "value": dag_run.conf['item']["ClientCode"]
            } if not does_client_exist() else null,
            "descriptionToApply": null,
            "statusToApply": "true",
            "clientContactToApply": null,
            "clientAddressToApply": null,
            "billingAddressToApply": null,
            "billingRatesToApply": null,
            "clientManagerToApply": null,
            "clientSharingToApply": null,
            "expenseCodesToApply": null,
            "customFieldsToApply": get_client_udfs(dag_run),
            "taxProfileToApply": null
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_user_info_on_empid(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:division"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                    "text": dag_run.conf['item']['ProjectManager'],
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

def get_replicon_date(date_str, date_format='%Y-%m-%dT%H:%M:%S.%f'):
    if not date_str:
        return None

    try:
        _date = datetime.strptime(date_str, date_format)
        return {
            'year': _date.year,
            'month': _date.month,
            'day': _date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_wbs_date_range(dag_run):
    return {
        'startDate': get_replicon_date(dag_run.conf['item']['ProjectStartDate']),
        'endDate': get_replicon_date(dag_run.conf['item']['ProjectEndDate'])
    }

def assign_permission_set(dag_run):
    return {
        "userUri": rail.result('get_user_info_on_empid')[0]['uri'],
        "permissionSetUri": dag_run.conf['projectmanagerpermissionuri']
    }

def project_leader_to_apply(dag_run):
    if dag_run.conf['item']['ProjectManager'] and rail.result('get_user_info_on_empid'):
        return {
            "user": {
                "uri": rail.result('get_user_info_on_empid')[0]['uri'],
                "loginName": null,
                "parameterCorrelationId": null
            }
        }
    return null

def get_project_oefs(dag_run):
    oefs = []

    def add_oef(definitionuri, taguri):
        oefs.append(
            {
                "definition": {
                    "uri": definitionuri,
                    "name": null
                },
                "tag": {
                    "uri": taguri,
                    "slug": null,
                    "tagName": null
                }if taguri else null,
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        )
    add_oef(dag_run.conf['projectprofiledefinitionuri'], dag_run.conf['projectprofiletaguri'])
    add_oef(dag_run.conf['projecttypedefinitionuri'], dag_run.conf['projecttypetaguri'])

    return oefs


def create_projectorapply_modifications(dag_run):

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["item"]["ProjectName"]
        },
        "codeToApply":  {
            "value": dag_run.conf["item"]["ProjectCode"]
        },
        "descriptionToApply": null,
        "percentCompletedToApply": null,
        "startDateToApply": {"date": get_wbs_date_range(dag_run)['startDate']},
        "endDateToApply": {"date": get_wbs_date_range(dag_run)['endDate']},
        "billingTypeToApply":{
            "value": "urn:replicon:billing-type:time-and-material"
            },
        "clientBillingAllocationMethodToApply": null,
        "clientAssignmentsSchedulesToApply":{
            "clients": [
                {
                "client": {
                    "uri": dag_run.conf['clienturi'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": null
            },
        "statusToApply": {
            "uri": null,
            "name": 'In Progress' if dag_run.conf["item"]["ProjectStatus"] == "P003" else 'Completed'
        },
        "projectWorkflowStateToApply": null,
        "clientRepresentativeToApply": null,
        "programToApply": null,
        "projectLeaderToApply": project_leader_to_apply(dag_run),
        "isProjectLeaderApprovalRequired": True,
        "costTypeToApply": null,
        "isTimeEntryAllowed": "false",
        "estimatedHoursToApply": null,
        "estimatedCostToApply": null,
        "defaultBillingCurrencyToApply": null,
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": ("urn:replicon:time-and-expense-entry-type:billable"
            if dag_run.conf['item']['TimeAndExpenseEntryType'] == 'No' else "urn:replicon:time-and-expense-entry-type:non-billable"),
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": [
                {
                "billingRate": {
                    "name": "Work Package Rate"
                }
                }
            ] if dag_run.conf['item']['TimeAndExpenseEntryType'] == 'No' else [],
            },
        "billingContractToApply": null,
        "fixedBid": null,
        "customFieldsToApply": [],
        "resourceAssignmentModifications": null,
        "resourceProjectAssignmentModifications": {
            "resourcesToAdd": [
                {
                    "resource": {
                        "department": {
                            "uri": rail.result('get_all_user_uri')
                        }
                    },
                    "billingRates": [
                        {
                            "name": "Work Package Rate"
                        }
                    ] if dag_run.conf['item']['TimeAndExpenseEntryType'] == 'No' else []
                }
            ]
        },
        "keyValuesToApply": [],
        "objectExtensionFieldsToApply": get_project_oefs(dag_run)
    }

    return {
        "target": None,
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_cost_center_code_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:code",
            "urn:replicon:cost-center-list-column:full-path",
            "urn:replicon:cost-center-list-column:effectively-enabled"
        ],
        "sort": [],
        "filterExpression": null
    }

def update_project_cost_center_payload():
    return {
        "projectUri": rail.result("create_projectorapply_modifications")["uri"],
        "costCenter": {"uri": rail.result("search_cost_center_code")[0]['costcenteruri']}
    }

def get_task_payload(data):
    return list(map(lambda task: {
        "target": None,
        "taskModificationToApply": {
                "name": task["TaskCode"] + " " + task["TaskName"],
                "codeToApply": {
                    "value": task['TaskCode']
                },
                "isClosed": 'false',
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "percentCompleted": "0",
                "isTimeEntryAllowed": "true",
                "resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "department": {
                            "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                        }
                    }
                ]
                },
            }
    }, data))

def get_data_to_process(dag_run):
    data = [dag_run.conf['item']['WorkItemSet']['WorkItem']] if isinstance(
       dag_run.conf['item']['WorkItemSet']['WorkItem'], (dict)) else dag_run.conf['item']['WorkItemSet']['WorkItem']
    for index, item in enumerate(data.copy()):
        item['index'] = index + 1
    return data

def get_batch_put_task_payload(dag_run):
    return {
        "project": {
            "uri": rail.result("create_projectorapply_modifications")["uri"],
        },
        "taskHierarchy": get_task_payload(get_data_to_process(dag_run)),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
