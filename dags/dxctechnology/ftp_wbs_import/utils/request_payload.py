import uuid
import re
import rail
from dxctechnology.ftp_wbs_import.utils import python_callable_method

null = None


def get_division_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
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
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_client_list_search_param(client_name):
    return {
        "page": 1,
        "pagesize": 100000000,
        "columnUris": [
            "urn:replicon:client-list-column:name",
            "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:name"
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
                    "text": client_name,
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


def get_put_client_param(client_name):
    return {
        "client": {
            "target": {
                "uri": null,
                "name": client_name,
                "code": null,
                "parameterCorrelationId": null
            },
            "name": client_name,
            "code": null,
            "comment": null,
            "clientManager": null,
            "billingContact": null,
            "clientAddress": null,
            "billingAddress": null,
            "isActive": True,
            "customFieldValues": [],
            "billingRates": [],
            "expenseCodesAllowedByDefaultOnNewProjects": [],
            "defaultBillingCurrency": null
        }
    }


def search_programs(program_name):
    return {
        "page": 1,
        "pagesize": 100000000,
        "columnUris": [
            "urn:replicon:program-list-column:program"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:program-list-filter:text"
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
                    "text": program_name,
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


def get_put_program(program_name_updated):
    return {
        "program": {
            "target": {
                "uri": null,
                "name": program_name_updated
            },
            "name": program_name_updated,
            "dateRange": null,
            "programManager": {
                "uri": null,
                "loginName": null,
                "parameterCorrelationId": null
            },
            "budget": null,
            "isActive": True
        }
    }


def get_wbs_conf(item):
    return{
        'WBS': item['ProjectName'] if item['ProjectName'] else null,
        'Projectgroup': item['ProjectGroup'] if item['ProjectGroup'] else null,
        'Projectcode': item['ProjectCode'],
        'Status': 'In Progress' if item['Status'] == 'Active' else 'Completed' if item['Status'] == 'Inactive' else null,
        'Companycodename': item['ProjectGroup'],
        'Companycodelog': rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", item['ProjectGroup'], "uri")
        if rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", item['ProjectGroup'], "parent")
        else null,
        'Companycode': rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", item['ProjectGroup'], "uri")
        if rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", item['ProjectGroup'], "parent") == 'FTP'
        else null,
        'Projecttype': rail.find_first_by_attr_and_get_attr(rail.result("get_oef_dropdown_values_project_type")['tags'], "name",
                                                            item['Type'], "uri") if item['Type'] else null,
        'Projecttypeuri': rail.result("get_project_oefs")['projecttype'],
        'projectstart': (item['Project_StartDate'][2:4] + '/' + item['Project_StartDate'][0:2] + '/' + item['Project_StartDate'][4:8])
        if item['Project_StartDate'] else null,
        'projectend': (item['Project_EndDate'][2:4] + '/' + item['Project_EndDate'][0:2] + '/' + item['Project_EndDate'][4:8])
        if item['Project_EndDate'] else null,
        'Projectmanager': item['ProjectManager_EmpID'],
        'Coprojectmanager': item['ProjectCoManager_EmpID'],
        'Billingindicator':  rail.find_first_by_attr_and_get_attr(rail.result("get_oef_dropdown_ftp_billing_indicator")['tags'],
                                                                  "name", item['Billing_Indicator'], "uri") if item['Billing_Indicator'] else null,
        'Billingindicatoruri': rail.result("get_project_oefs")['billingelement'],
        'Businessarea': item['ProjectUDF'] if item['ProjectUDF'] else null,
        'Parentwbs': item['Project'] if item['Project'] else null,
        'Projectmanagerpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"), "name", 'Limited WBS Manager', "uri"),
        'Projectstartday': item['Project_StartDate'][2:4] if item['Project_StartDate'] else null,
        'Projectstartmonth': item['Project_StartDate'][0:2] if item['Project_StartDate'] else null,
        'Projectstartyear': item['Project_StartDate'][4:8] if item['Project_StartDate'] else null,
        'Projectendday': item['Project_EndDate'][2:4] if item['Project_EndDate'] else null,
        'Projectendmonth': item['Project_EndDate'][0:2] if item['Project_EndDate'] else null,
        'Projectendyear': item['Project_EndDate'][4:8] if item['Project_EndDate'] else null,
        'Businessareauri': rail.result("get_project_oefs")['businessarea'],
        'Parentwbsuri': rail.result("get_project_oefs")['parentwbs'],
        'Wbstypeuri': rail.result("get_project_oefs")['wbstype'],
        'Masterwbsuri': rail.result("get_project_oefs")['masterwbs'],
        'Enduserpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"), "name", 'Manager', "uri"),
        'Ftpuri': rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", 'FTP', "uri"),
        'Adminpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"), "name", 'Project Team Assignment Data Import', "uri"),
        'Organizationunituri': rail.find_first_by_attr_and_get_attr(rail.result("get_department_groups"), "displayText", 'DXC', "uri"),
        'Profitcenteruri': rail.result("get_project_oefs")['profitcenter'],
        'Stageuri': rail.result("get_project_oefs")['stage'],
        'Objectclassuri': rail.result("get_project_oefs")['objectclass'],
        'Customeruri': rail.result("get_project_oefs")['customer'],
        'Functionalareauri': rail.result("get_project_oefs")['functionalarea'],
        'Producturi': rail.result("get_project_oefs")['product'],
        'Salesforceopportunityiduri': rail.result("get_project_oefs")['salesforceopportunityid'],
        'Profitcenter': item['PRCTR'] if item['PRCTR'] else null,
        'Objectclass': item['SCOPE'] if item['SCOPE'] else null,
        'Customer': item['CUST_USR00'] if item['CUST_USR00'] else null,
        'Functionalarea': item['FunctionalArea'] if item['FunctionalArea'] else null,
        'Product': item['Product'] if item['Product'] else null,
        'Stage': item['Stage'] if item['Stage'] else null,
        'Salesforceoppourtunityid': item['SalesForceID'] if item['SalesForceID'] else null,
        'Salesforceoppourtunityiduri': rail.result("get_project_oefs")['salesforceopportunityid'],
        'Iwono': item['IWONo'] if item['IWONo'] else null,
        'Clientname': item['Client'] if item['Client'] else null,
        'Iwonouri': rail.result("get_project_oefs")['iwono']
    }


def get_all_mandatory_check():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return bool(dag_run_conf['Projectgroup'] and dag_run_conf['WBS'] and dag_run_conf['Projectcode'] and dag_run_conf['projectstart'] and
                dag_run_conf['projectend'] and dag_run_conf['Status'] and dag_run_conf['Parentwbs'] and dag_run_conf['Projectmanager']
                and dag_run_conf['Projecttype'] and dag_run_conf['Companycode'] and dag_run_conf['Companycodename'])


def get_properties_exception():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        'projectname': dag_run_conf['WBS'] if dag_run_conf['WBS'] else null,
        'projectcode': dag_run_conf['Projectcode'] if dag_run_conf['Projectcode'] else null,
        'status': 'Exception'
    }


def get_properties_error():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        'projectname': dag_run_conf['WBS'] if dag_run_conf['WBS'] else null,
        'projectcode': dag_run_conf['Projectcode'] if dag_run_conf['Projectcode'] else null,
        'status': 'Error'
    }


def get_user_on_empid_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
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
                        "text": dag_run_conf['Projectmanager'],
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
            "operatorUri": "urn:replicon:filter-operator:or",
            "rightExpression": {
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
                        "text": dag_run_conf['Coprojectmanager'],
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
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_user_on_empid_payload_2():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:end-date"
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
                    "text": dag_run_conf['Projectmanager'],
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


def get_project_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run_conf['Projectcode'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_create_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "project": {
            "target": {"name": dag_run_conf['Projectcode']},
            "projectInfo": {
                "name": dag_run_conf['Projectcode'],
                "projectStatusLabel": {"name": dag_run_conf['Status']},
                "percentCompleted": 0,
                "isTimeEntryAllowed": True,
                "isProjectLeaderApprovalRequired": True,
                "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                "timeAndMaterials": {
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                },
            }
        }
    }

# pylint: disable=too-many-branches


def get_project_modifications():
    context = rail.get_current_context()
    conf = context['dag_run'].conf
    pl_updates = rail.result('determine_necessary_projectmanager_updates')

    project_leader = {
        "user": {
            "uri": pl_updates['user_uri'], "loginName": null, "parameterCorrelationId": null}} \
            if pl_updates['should_apply'] and pl_updates['user_uri'] else null

    def pass_timeandmaterials_payload():
        return {
            "timeAndExpenseEntryTypeUri": null,
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": []
        }
    timeandmaterials = null if rail.result(
        'load_project') else pass_timeandmaterials_payload()

    def render_date(name):
        parsed = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', conf[name])
        return{
            "date": {
                "year": int(parsed[3]),
                "month": int(parsed[2]),
                "day": int(parsed[1]),
            }
        } if parsed else null
    oefs = []

    def add_oef(name):
        oefs.append({
            "definition": {
                    "uri": conf[f'{name}uri'],
                    "name": null
                    },
            "tag": {
                "uri": null,
                "slug": null,
                "tagName": {
                    "name": null,
                    "tagDefinitionUri": null
                }
            },
            "textValue": conf[f'{name}'],
        })

    if conf['Businessarea']:
        add_oef('Businessarea')

    if conf['Parentwbs']:
        add_oef('Parentwbs')

    if conf['Profitcenter']:
        add_oef('Profitcenter')

    if conf['Objectclass']:
        add_oef('Objectclass')

    if conf['Customer']:
        add_oef('Customer')

    if conf['Functionalarea']:
        add_oef('Functionalarea')

    if conf['Product']:
        add_oef('Product')

    if conf['Stage']:
        add_oef('Stage')

    if conf['Salesforceoppourtunityid']:
        add_oef('Salesforceoppourtunityid')

    if conf['Iwono']:
        add_oef('Iwono')

    oefs.append({
                "definition": {
                    "uri": conf['Wbstypeuri'],
                    "name": null
                },
                "tag": {
                    "uri": null,
                    "slug": null,
                    "tagName": {
                        "name": 'Xchanging WBS',
                        "tagDefinitionUri": null
                    }
                }
                }
                )
    if conf['Billingindicator']:
        oefs.append({
                    "definition": {
                        "uri": conf['Billingindicatoruri'],
                        "name": null
                    },
                    "tag": {
                        "uri": conf['Billingindicator'],
                        "slug": null,
                        "tagName": {
                            "name": null,
                            "tagDefinitionUri": null
                        }
                    }
                    }
                    )

    if conf['Projecttype']:
        oefs.append({
                    "definition": {
                        "uri": conf['Projecttypeuri'],
                        "name": null
                    },
                    "tag": {
                        "uri": conf['Projecttype'],
                        "slug": null,
                        "tagName": {
                            "name": null,
                            "tagDefinitionUri": null
                        }
                    }
                    }
                    )

    if conf['Masterwbsuri']:
        oefs.append({
            "definition": {
                    "uri": conf['Masterwbsuri'],
                    "name": null
                    },
            "tag": {
                "uri": null,
                "slug": null,
                "tagName": {
                    "name": null,
                    "tagDefinitionUri": null
                }
            },
            "textValue": 'WBS',
        }
        )

    return {
        "target": {
            "uri": rail.result('get_project_uri'),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": {
                "value": conf['WBS']
            },
            "descriptionToApply": {
                "value": conf['WBS']
            },
            "percentCompletedToApply": 0,
            "startDateToApply": render_date('projectstart'),
            "endDateToApply": render_date('projectend'),
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": {
                "uri": null,
                "name": conf['Status']
            },
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply":  {
                "program": {
                    "uri": null,
                    "name": conf['Parentwbs']
                }
            },
            "projectLeaderToApply": project_leader,
            "isProjectLeaderApprovalRequired": True,
            "costTypeToApply": null,
            "isTimeEntryAllowed": True,
            "estimatedHoursToApply": null,
            "estimatedCostToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": timeandmaterials,
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [],
            "resourceAssignmentModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": oefs
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_oef_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "objectUri": rail.result('get_project_uri'),
        "value": {
            "definition": {
                "uri": dag_run_conf['Parentwbsuri'],
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": null,
            "fileValue": null
        }
    }


def get_update_billingindicator_oef_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "objectUri": rail.result('get_project_uri'),
        "value": {
            "definition": {
                "uri": dag_run_conf['Billingindicatoruri'],
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": null,
            "fileValue": null
        }
    }


def get_update_businessarea_oef_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "objectUri": rail.result('get_project_uri'),
        "value": {
            "definition": {
                "uri": dag_run_conf['Businessareauri'],
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": null,
            "fileValue": null
        }
    }


def get_update_masterwbsuri_oef_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "objectUri": rail.result('get_project_uri'),
        "value": {
            "definition": {
                "uri": dag_run_conf['Masterwbsuri'],
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": null,
            "fileValue": null
        }
    }


def get_update_division_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "projectUri": rail.result('get_project_uri'),
        "division": {
            "uri": dag_run_conf['Companycode'],
            "parentUri": null,
            "name": null
        }
    }


def get_access_scopes_payload():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return {
        "projectUri": rail.result('get_project_uri'),
        "teamMemberDataAccessScopes": [
            {
                "locations": [],
                "divisions": [
                    {
                        "uri": dag_run_conf['Ftpuri'],
                        "parentUri": null,
                        "name": null
                    }
                ],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [
                    {
                        "uri": dag_run_conf['Organizationunituri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                ],
                "employeeTypeGroups": []
            }
        ]
    }


def get_put_keyvalue_payload():
    return {
        "projectUri": rail.result('get_project_uri'),
        "keyValue": {
            "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
            "value": {
                "uri": "urn:replicon:project-team-member-assignment-type:automatically-assign-task",
                "slug": null,
                "bool": null,
                "date": null,
                "number": null,
                "text": null,
                "time": null,
                "calendarDayDurationValue": null,
                "workdayDurationValue": null,
                "dateRange": null,
                "collection": []
            }
        }
    }


def get_unavailable_meassage():
    dag_run_conf = python_callable_method.get_dag_run_conf()
    return (',').join(list(filter(null, [
        "Project Manager " +
        dag_run_conf['Projectmanager'] + " is not available in Replicon"
        if rail.result("user_details")['useruri'] is null else null,
        "Project CoManager " +
        (dag_run_conf['Coprojectmanager'] if dag_run_conf['Coprojectmanager']
         else '') + " is not available in Replicon"
        if rail.result("user_details")['comanageruri'] is null else null,
    ])))


def get_client_uri_payload():
    client_name = python_callable_method.get_dag_run_conf()['Clientname']
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:client-list-column:name",
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:name"
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
                    "text": client_name,
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


def get_update_client_payload():
    return {
        "projectUri": rail.result('get_project_uri'),
        "clientUri": rail.result('get_client_uri')[0]['cells'][2]['uri'],
        "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
    }


def get_remove_client_payload():
    return {
        "projectUri": rail.result('get_project_uri'),
        "clientUri": null,
        "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
    }
