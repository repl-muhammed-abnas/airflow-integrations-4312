import uuid
import re
import rail

null = None

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

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

def get_cost_center_uri(cost_centers,mapper):
    jsonValue=mapper
    data=rail.result(cost_centers)
    result = list(
        map(lambda x: {
            'name':x['Cost centre names'],
            'uri': rail.find_first_by_attr_and_get_attr(data,'displayText',x['Cost centre names'],'uri'),
            'check': 'Yes' if rail.find_first_by_attr_and_get_attr(data,'displayText',x['Cost centre names']) else 'No'
        },jsonValue)
    )
    return [i for i in result if i['check'] == 'Yes']

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

def get_remedy_conf(item):
    get_cost_senter_uris = list(set(list(map(lambda dict: dict['uri'] ,rail.result('get_cost_center_uri')))))
    return{
        'WBS': item['Project_Name'] if item['Project_Name'] else null,
        'Projectcode': item['Project_Code'],
        'Status': 'In Progress' if item['Project_Status'] == 'ACTIVE' else 'Completed' if item['Project_Status'] == 'INACTIVE' else null,
        'Companycodename': item['CompanyCode'],
        'Companycode': rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", item['CompanyCode'], "uri")
                        if rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", item['CompanyCode'], "parent") == 'FTP'
                        else null,
        'projectstart': (item['Start_date'][2:4] + '/' + item['Start_date'][0:2]  + '/'+ item['Start_date'][4:8] ) if item['Start_date'] else null,
        'projectend': (item['End_date'][2:4]  + '/' + item['End_date'][0:2] + '/'+ item['End_date'][4:8]) if item['End_date'] else null,
        'Projectmanager': item['ProjectManager_EmpID'],
        'Coprojectmanager': item['ProjectCoManager_EmpID'],
        'Parentwbs': item['WBS_Element'] if item['WBS_Element'] else null,
        'Projectmanagerpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"), "name",'Limited WBS Manager', "uri"),
        'Parentwbsuri': rail.result("get_project_oefs")['parentwbs'],
        'Wbstypeuri': rail.result("get_project_oefs")['wbstype'],
        'Enduserpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"), "name",'Manager', "uri"),
        'Masterwbs': rail.result("get_project_oefs")['masterwbs'],
        'Ftpuri': rail.find_first_by_attr_and_get_attr(rail.result("get_company_codes"), "name", 'FTP', "uri"),
        'Adminpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_set"), "name",'Project Team Assignment Data Import', "uri"),
        'Costcenteruris': get_cost_senter_uris,
        'Organizationunituri': rail.find_first_by_attr_and_get_attr(rail.result("get_department_groups"), "displayText",'DXC', "uri"),
        'Programname': item['PROJ_CODE'] if item['PROJ_CODE'] else null

        }

def get_all_mandatory_check():
    dag_run_conf = get_dag_run_conf()
    return bool(dag_run_conf['WBS'] and dag_run_conf['Projectcode'] and dag_run_conf['projectstart'] and \
        dag_run_conf['projectend'] and dag_run_conf['Status'] and dag_run_conf['Projectmanager'] \
        and dag_run_conf['Companycode'] and dag_run_conf['Companycodename'])

def get_properties_exception():
    dag_run_conf = get_dag_run_conf()
    return {
        'projectname': dag_run_conf['WBS'] if dag_run_conf['WBS'] else null,
        'projectcode': dag_run_conf['Projectcode'] if dag_run_conf['Projectcode'] else null,
        'status': 'Exception'
    }

def get_properties_error():
    dag_run_conf = get_dag_run_conf()
    return {
        'projectname': dag_run_conf['WBS'] if dag_run_conf['WBS'] else null,
        'projectcode': dag_run_conf['Projectcode'] if dag_run_conf['Projectcode'] else null,
        'status': 'Error'
    }

def get_user_on_empid_payload():
    dag_run_conf = get_dag_run_conf()
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
    dag_run_conf = get_dag_run_conf()
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
    dag_run_conf = get_dag_run_conf()
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
    dag_run_conf = get_dag_run_conf()
    return {
                "project": {
                    "target": {"name": dag_run_conf['Projectcode']},
                    "projectInfo": {
                        "name":dag_run_conf['Projectcode'],
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

def get_project_modifications():
    context = rail.get_current_context()
    conf = context['dag_run'].conf
    pl_updates = rail.result('determine_necessary_projectmanager_updates')

    project_leader = {
        "user": {
            "uri": pl_updates['user_uri'],"loginName": null,"parameterCorrelationId": null }} if pl_updates['should_apply'] and pl_updates['user_uri'] else null

    def pass_program_payload():
        return {
            "program":{
                "uri": rail.result('search_program')[0]['uri'],
                "name": null,
            }
        }

    program_required = null if rail.result('search_program')==[] or conf['Programname'] == null else pass_program_payload()

    def pass_timeandmaterials_payload():
        return {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": []
        }
    timeandmaterials = null if rail.result('load_project') else pass_timeandmaterials_payload()

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
    if conf['Parentwbs']:
        add_oef('Parentwbs')
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
    oefs.append({
                "definition": {
                    "uri": conf['Masterwbs'],
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
                    "numericValue": null,
                    "textValue": 'RO',
                    "fileValue": null,
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
                    "programToApply": program_required,
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
    dag_run_conf = get_dag_run_conf()
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

def get_project_teammember_payload():
    dag_run_conf = get_dag_run_conf()
    return {
            "projectUri": rail.result('get_project_uri'),
            "resourceUri": dag_run_conf['Costcenteruris'],
            "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
        }

def get_update_division_payload():
    dag_run_conf = get_dag_run_conf()
    return {
            "projectUri": rail.result('get_project_uri'),
            "division": {
                "uri": dag_run_conf['Companycode'],
                "parentUri": null,
                "name": null
            }
        }

def get_access_scopes_payload():
    dag_run_conf = get_dag_run_conf()
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
    dag_run_conf = get_dag_run_conf()
    return (',').join(list(filter(null,[
                "Project Manager " + dag_run_conf['Projectmanager'] + " is not available in Replicon"
                if rail.result("user_details")['useruri'] is null else null,\
                "Project CoManager " + (dag_run_conf['Coprojectmanager'] if dag_run_conf['Coprojectmanager'] else '') + " is not available in Replicon"
                if rail.result("user_details")['comanageruri'] is null else null,\
           ])))
