import uuid
import datetime
import rail


def get_query_data():
    return rail.load_all_records(rail.result('get_all_jira_for_specified_project'))


def search_client_data(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['customer'],
                },
                "filterDefinitionUri": None
            },
        }
    }


def create_client_payload(dag_run):
    return {
        "client": {
            "target": {
                "name": dag_run.conf['customer'],
            },
            "name": dag_run.conf['customer'],
            "isActive": "0",
        }
    }


def billing_rate_update(data):
    return {
        "clientUri": f"{data}",
        "billingRateUri": "urn:replicon:user-specific-billing-rate",
        "isAllowedByDefaultOnNewProjects": "true"
    }


def get_all_project_data():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
                "urn:replicon:project-list-column:status",
            "urn:replicon:project-list-column:start-date",
            "urn:replicon:project-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": None
    }


def create_project_payload(dag_run):
    billing_type = dag_run.conf['Billingtype']
    end_date = datetime.datetime.strptime(
        dag_run.conf['end_date'], "%m/%d/%Y")
    x = datetime.datetime.now()
    if billing_type == "Time and Material":
        return {
            "target": None,
            "modifications": {
                "nameToApply": {
                    "value": dag_run.conf['Projectname']
                },
                "startDateToApply": {
                    "date": {
                        "year": x.strftime("%Y"),
                        "month": x.strftime("%m"),
                        "day": x.strftime("%d")
                    }
                },
                "endDateToApply": {
                    "date": get_dates(end_date)
                },
                "statusToApply": {
                    "name": "In Progress"
                },
                "costTypeToApply": {
                    "uri": "urn:replicon:cost-type:operational"
                },
                "isTimeEntryAllowed": "0",
                "timeAndMaterials": {
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                },
                "customFieldsToApply": [
                    {
                        "customField": {
                            "uri": dag_run.conf['createdby_project'],
                        },
                        "text": "jiraintegration",
                    }
                ],
            },
            "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }

    if billing_type == "Fixed Bid":
        return {
            "modifications": {
                "nameToApply": {
                    "value": dag_run.conf['Projectname']
                },
                "startDateToApply": {
                    "date": {
                        "year": x.strftime("%Y"),
                        "month": x.strftime("%m"),
                        "day": x.strftime("%d")
                    }
                },
                "endDateToApply": {
                    "date": get_dates(end_date)
                },
                "billingTypeToApply": {
                    "value": "urn:replicon:billing-type:fixed-bid"
                },
                "statusToApply": {
                    "name": "In Progress"
                },
                "costTypeToApply": {
                    "uri": "urn:replicon:cost-type:operational"
                },
                "isTimeEntryAllowed": "false",
                "fixedBid": {
                    "rate": {
                        "amount": "0",
                        "currency": {
                            "name": "US Dollar",
                        }
                    },
                    "fixedBidBillingFrequencyUri": "urn:replicon:fixed-bid-frequency:monthly"
                },
                "customFieldsToApply": [
                    {
                        "customField": {
                            "uri": dag_run.conf['createdby_project'],
                        },
                        "text": "jiraintegration",
                    }
                ],
                "resourceAssignmentModifications": {
                    "resourcesToAdd": [
                        {
                            "department": {
                                "name": "Odessa Technologies Inc",
                            },
                        }
                    ],
                    "resourcesToRemove": []
                },
                "keyValuesToApply": [],
                "objectExtensionFieldsToApply": []
            },
            "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }

    return None

def add_custom_field_data_to_list(dag_run, task_type=None):
    date= datetime.datetime.now()
    udf_data= []

    def add_usf_dates(uri):
        udf_data.append({
                "customField": {
                    "uri": uri,
                },
                "date": {
                    "year": date.strftime("%Y"),
                    "month": date.strftime("%m"),
                    "day": date.strftime("%d")
                }
            }
        )

    def get_udfs(uri,text):
        udf_data.append({
            "customField": {
                "uri": uri,
            },
            "text": text if text != 'None' else None,
        }
    )

    if task_type:
        get_udfs(dag_run.conf['createdby_task_URI'],'jiraintegration')

    epic_summary = rail.result("get_parent_issue_data")['summary'] if dag_run.conf['Epicid'] != 'None'  else None

    add_usf_dates(dag_run.conf['Lasttimeentrydate_URI']),
    get_udfs(dag_run.conf['issuetype_uri'],dag_run.conf['Issuetype']),
    get_udfs(dag_run.conf['parentid_uri'],dag_run.conf['Parentjira']),
    get_udfs(dag_run.conf['epiclink_uri'],epic_summary),
    get_udfs(dag_run.conf['epicid_uri'],dag_run.conf['Epicid']),
    get_udfs(dag_run.conf['epicsummary_uri'],epic_summary)

    return udf_data

def get_update_task_payload(dag_run, item):
    return {
        "project": {
            "uri": dag_run.conf['Repliconprojecturi'],
        },
        "taskHierarchy": [
            {
                "target": {
                    "uri": item['Taskuri'],
                },
                "taskModificationToApply": {
                    "isClosed": "false",
                    "customFieldsToApply": add_custom_field_data_to_list(dag_run),
                }
            }
        ],
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def create_task_payload(dag_run):
    billing_type = dag_run.conf['Billingtype']
    end_date = datetime.datetime.strptime(
        dag_run.conf['end_date'], "%m/%d/%Y")
    if billing_type == "Time and Material":
        x = datetime.datetime.now()
        return {
            "taskHierarchy": [
                {
                    "taskModificationToApply": {
                        "name": dag_run.conf['Key'],
                        "codeToApply": {
                            "value": dag_run.conf['Summary'][slice(0, 49)]
                        },
                        "isClosed": "false",
                        "timeEntryStartDateToApply": {
                            "date": {
                                    "year": x.strftime("%Y"),
                                    "month": x.strftime("%m"),
                                    "day": x.strftime("%d")
                            }
                        },
                        "timeEntryEndDateToApply": {
                            "date": get_dates(end_date)
                        },
                        "timeAndExpenseEntryTypeToApply": {
                            "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                        },
                        "isTimeEntryAllowed": "true",
                        "costTypeToApply": {
                            "value": "urn:replicon:cost-type:operational"
                        },
                        "customFieldsToApply": add_custom_field_data_to_list(dag_run),
                    }
                }
            ],
            "project": {
                "uri": rail.result("create_project")['uri'],
            },
            "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }

    if billing_type == "Fixed Bid":
        x = datetime.datetime.now()
        return {
            "taskHierarchy": [
                {
                    "target": None,
                    "taskModificationToApply": {
                        "name": dag_run.conf['Key'],
                        "codeToApply": {
                            "value": dag_run.conf['Summary'][slice(0, 49)]
                        },
                        "isClosed": "false",
                        "timeEntryStartDateToApply": {
                            "date": {
                                "year": x.strftime("%Y"),
                                "month": x.strftime("%m"),
                                "day": x.strftime("%d")
                            }
                        },
                        "timeEntryEndDateToApply": {
                            "date": get_dates(end_date)
                        },
                        "timeAndExpenseEntryTypeToApply": {
                            "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                        },
                        "isTimeEntryAllowed": "true",
                        "costTypeToApply": {
                            "value": "urn:replicon:cost-type:operational"
                        },
                        "customFieldsToApply": add_custom_field_data_to_list(dag_run),
                        "resourceAssignmentModifications": {
                            "resourcesToAdd": [
                                {
                                    "department": {
                                        "name": "Odessa Technologies Inc"
                                    }
                                }
                            ]
                        },
                    }
                }
            ],
            "project": {
                "uri": rail.result("create_project")['uri'],
            },
            "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }

    return None


def apply_client(dag_run):
    return {
        "projectUri": rail.result("create_project")['uri'],
        "clientUri": dag_run.conf['Clienturi'] if dag_run.conf['Clienturi'] else None,
        "optionUri": "urn:replicon:project-apply-new-client-option:update-billing-rates-and-expense-codes"
    }


def get_task_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:task-list-column:task",
                "urn:replicon:task-list-column:enabled"
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
                        "text": dag_run.conf['Key'],
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
                        "uri": dag_run.conf['Repliconprojecturi'],
                    },
                },
            },
        }
    }


def get_dates(start_date):
    return {
            "year": start_date.strftime("%Y"),
            "month": start_date.strftime("%m"),
            "day": start_date.strftime("%d")
        }


def update_time_and_material_project_task_data(dag_run, end_date):
    start_date = datetime.datetime.strptime(
        dag_run.conf['Repliconprojectstartdate'], "%m/%d/%Y") if dag_run.conf['Repliconprojectstartdate'] else None
    end_date = datetime.datetime.strptime(
        dag_run.conf['end_date'], "%m/%d/%Y")
    return {
        "project": {
            "uri": dag_run.conf['Repliconprojecturi']
        },
        "task": {
            "target": {
                "name": dag_run.conf['Key'],
            },
            "name": dag_run.conf['Key'],
            "code": dag_run.conf['Summary'][slice(0, 49)],
            "timeEntryDateRange": {
                "startDate": get_dates(start_date),
                "endDate": get_dates(end_date),
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "isClosed": "false",
            "customFieldValues": add_custom_field_data_to_list(dag_run, 'update'),
            "costTypeUri": "urn:replicon:cost-type:operational",
            "assignedResources": []
        }
    }


def bulk_update_task_team_members_data():
    return {
        "taskUri": rail.result("update_time_and_material_project_task")['uri'],
        "resourceUris": rail.result("get_all_team_members") if rail.result("get_all_team_members") else [],
        "isAssigned": "true"
    }


def update_fixed_bid_project_task_data(dag_run, end_date):
    x = datetime.datetime.now()
    end_date = datetime.datetime.strptime(
        dag_run.conf['end_date'], "%m/%d/%Y")
    return {
        "project": {
            "uri": dag_run.conf['Repliconprojecturi'],
        },
        "task": {
            "target": {
                "name": dag_run.conf['Key'],
            },
            "name": dag_run.conf['Key'],
            "code": dag_run.conf['Summary'],
            "timeEntryDateRange": {
                "startDate": {
                    "date": {
                        "year": x.strftime("%Y"),
                        "month": x.strftime("%m"),
                        "day": x.strftime("%d")
                    }
                },
                "endDate": {
                    "date": get_dates(end_date)
                },
            },
            'costTypeUri': 'urn:replicon:cost-type:operational',
            'timeAndExpenseEntryTypeUri': 'urn:replicon:time-and-expense-entry-type:billable',
            "assignedResources": [
                {
                    "department": {
                        "name": "Odessa Technologies Inc",
                    },
                }
            ],
            "customFieldValues": add_custom_field_data_to_list(dag_run, 'update'),
        }
    }

def get_data(config):
    return {
            "fields": {
                "customfield_10070" if (config.company_key).lower() == 'odessasandbox' else "customfield_10130": {
                    "value": "Yes"
                }
            }
    }
