from datetime import datetime
import uuid
import rail
from eisner_amper.project_import_internal_bulk_processing.mapper.time_entry_codes_mapper import time_entry_code_mapper


null = None

def get_all_mandatory_check_clients(dag_run):
    return dag_run.conf['item']['ClientCode'] and dag_run.conf['item']['ClientName']

def get_all_mandatory_check_projects(dag_run):
    return dag_run.conf['item']['ProjectStatus'] and dag_run.conf['item']['ProjectCode'] and dag_run.conf['item']['ProjectName'] and \
    dag_run.conf['item']['ProjectStartDate'] and dag_run.conf['item']['ProjectEndDate'] and dag_run.conf['item']['ProjectProfile'] and \
        dag_run.conf['item']['ProjectType']

def get_all_mandatory_check_tasks(dag_run):
    return dag_run.conf['task_name'] and dag_run.conf['task_code']

MANDATORY_FIELDS = {
    "client_fields": {
        "ClientCode":"Client Code",
        "ClientName": "Client Name"
    },
    "project_fields": {
        "ProjectStatus":"Project Status",
        "ProjectCode": "Project Code",
        "ProjectName": "Project Name",
        "ProjectStartDate": "Project StartDate",
        "ProjectEndDate": "Project EndDate",
        "ProjectProfile": "Project Profile",
        "ProjectType": "Project Type"
    },
    "task_fields": {
        "TaskCode":"Task Code",
        "TaskName": "Task Name"
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
                "value": 'EisnerAmper '+ dag_run.conf['item']["ClientName"]
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
            "customFieldsToApply": null,
            "taxProfileToApply": null
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
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

def does_wbs_exist():
    return bool(rail.result('get_project_details_based_on_wbs'))

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details_based_on_wbs')['uri']
        }
    return null


def get_project_oefs(dag_run):
    oefs = []

    def add_oef(definitionuri, taguri, textvalue = null):
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
                "textValue": textvalue if textvalue else null,
                "fileValue": null,
                "jsonValue": null
            }
        )
    profile_uri = dag_run.conf['projectprofiletaguri'][dag_run.conf["item"]["ProjectProfile"]]
    type_uri = dag_run.conf['projecttypetaguri'][dag_run.conf["item"]["ProjectType"]]

    add_oef(dag_run.conf['projectprofiledefinitionuri'], profile_uri)
    add_oef(dag_run.conf['projecttypedefinitionuri'], type_uri)

    if dag_run.conf["item"]["ProjectCode"]:
        code = dag_run.conf["item"]["ProjectCode"][len(dag_run.conf["item"]["ProjectCode"])-4:]
        check_code_in_mapper = rail.find_first_by_attr_and_get_attr(time_entry_code_mapper, 'project_code', code, 'time_entry_code')
        if check_code_in_mapper:
            add_oef(dag_run.conf['timeentrycodedefinitionuri'], null,check_code_in_mapper)

    return oefs


def create_projectorapply_modifications(dag_run):

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["item"]["ProjectName"]
        },
        "codeToApply":  {
            "value": dag_run.conf["item"]["ProjectCode"]
        } if not does_wbs_exist() else null,
        "descriptionToApply": null,
        "percentCompletedToApply": null,
        "startDateToApply": {"date": get_wbs_date_range(dag_run)['startDate']},
        "endDateToApply": {"date": get_wbs_date_range(dag_run)['endDate']},
        "billingTypeToApply":{
            "value": "urn:replicon:billing-type:time-and-material"
            }  if not does_wbs_exist() else null,
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
            "name": 'In Progress' if dag_run.conf["item"]["ProjectStatus"] in ['10','12'] else 'Completed' if dag_run.conf[
                "item"]["ProjectStatus"] in ['40','42'] else None        },
        "projectWorkflowStateToApply": null,
        "clientRepresentativeToApply": null,
        "programToApply": null,
        "projectLeaderToApply": null,
        "isProjectLeaderApprovalRequired": True,
        "costTypeToApply": null,
        "isTimeEntryAllowed": "false",
        "estimatedHoursToApply": null,
        "estimatedCostToApply": null,
        "defaultBillingCurrencyToApply": null,
        "timeAndMaterials": null,
        "billingContractToApply": null,
        "fixedBid": null,
        "customFieldsToApply": [],
        "resourceAssignmentModifications":  {
            "resourcesToAdd": [
                {
                    "user": null,
                    "department": {
                        "uri": rail.result('get_all_user_uri'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "employeeType": null,
                    "departmentGroup": null
                }
            ],
            "resourcesToRemove": []
        },
        "keyValuesToApply": [],
        "objectExtensionFieldsToApply": get_project_oefs(dag_run)
    }

    return {
        "target": get_create_project_target_param(),
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

def get_default_task_data(config):
    return {
        "project": {
            "uri": rail.result("create_projectorapply_modifications")["uri"],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": config.default_task_name,
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": config.default_task_name,
            "code": config.default_task_code,
            "description": null,
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": null,
            "isClosed": "false",
            "customFieldValues": [],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": [
                {
                    "uri": rail.result("get_all_user_uri"),
                    "resourcePlaceholderParameterCorrelationId": null,
                    "user": null,
                    "department": null,
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "departmentGroup": null,
                    "employeeTypeGroup": null
                }
            ],
            "keyValues": [],
            "historicalKeyValues": [],
            "extensionFieldValues": []
        }
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
    data = [dag_run.conf['item']['YY1_ActivityTypeSet']['YY1_ActivityType']] if isinstance(
        dag_run.conf['item']['YY1_ActivityTypeSet']['YY1_ActivityType'], (dict)) else dag_run.conf['item']['YY1_ActivityTypeSet']['YY1_ActivityType']
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

