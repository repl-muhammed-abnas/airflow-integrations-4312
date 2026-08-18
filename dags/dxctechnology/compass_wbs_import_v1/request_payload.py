import uuid
import re
import rail

null = None

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_client_list_search_param(dag_run):
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
                    "text": dag_run.conf['client'],
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


def get_put_client_param(dag_run):
    return {
        "client": {
            "target": {
                "uri": null,
                "name": dag_run.conf['client'],
                "code": null,
                "parameterCorrelationId": null
            },
            "name": dag_run.conf['client'],
            "code": dag_run.conf['client_code'],
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


def get_program_list_search_param(program_name):
    return {
        "page": 1,
        "pagesize": 100000000,
        "columnUris": [
            "urn:replicon:program-list-column:name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:program-list-filter:name"
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


def get_put_program_param(program_name):
    return {
        "program": {
            "target": {
                "uri": null,
                "name": program_name
            },
            "name": program_name,
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


def get_update_client_param():
    conf = get_dag_run_conf()
    client_data = rail.result('get_client_info')
    if conf['client'] and len(client_data) > 0:
        return {
            "projectUri": rail.result('get_project_uri'),
            "clientUri": rail.result('get_client_info'),
            "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
        }
    return null

def get_project_update_modifications():
    context = rail.get_current_context()
    conf = context['dag_run'].conf
    project_leader_updates = rail.result('determine_necessary_projectleader_updates')
    project_leader = {
        "user": {
            "uri": project_leader_updates['user_uri']}} if project_leader_updates['should_apply'] and project_leader_updates['user_uri'] else None

    def render_date(name):
        parsed = re.match(r'^(\d{4})(\d{2})(\d{2})$', conf[name])
        return {
            "date": {
                "year": int(parsed[1]),
                "month": int(parsed[2]),
                "day": int(parsed[3]),
            }
        } if parsed else None

    oefs = []
    def add_oef(name):
        oefs.append({
            'definition': {'uri': conf[f'{name}uri']},
            'tag': {'uri': conf[name]} if conf[name] else None,
        })
    add_oef('timetrackingattribute')
    add_oef('compassprojecttype')
    add_oef('globalwbsindicator')

    if is_child_wbs() and is_gsap_company_code():
        oefs.append({
            'definition': {'uri': conf['iwowbsindicatoruri']},
            'tag': None,
        })
    else:
        add_oef('iwowbsindicator')

    oefs.append({
        'definition': {'uri': conf['wbstypeuri']},
        'tag': {'tagName': {
            'name': 'COMPASS WBS',
            'tagDefinitionUri': conf['wbstypeuri']
        }},
    })
    # pylint: disable=line-too-long
    if conf['salesforceopportunityid'] :
        oefs.append({'definition': {'uri': conf['salesforceopportunityiduri']}, 'textValue': conf['salesforceopportunityid'] })
    if conf['salesforceopportunityname'] :
        oefs.append({'definition': {'uri': conf['salesforceopportunitynameuri']}, 'textValue': conf['salesforceopportunityname']})

    return {
        "target": {
            "uri": rail.result('get_project_uri'),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": {"value": conf['description'] if conf['description']  else None},
            "descriptionToApply": {"value": conf['description'] if conf['description']  else None},
            "percentCompletedToApply": 0,
            "startDateToApply": render_date('startdate'),
            "endDateToApply": render_date('enddate'),
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null,

            "statusToApply": { "uri": null,"name": conf['status']},
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply":  {
            "program": { "uri": null,"name": conf['program']} if conf['programid']  else None},
            "projectLeaderToApply": project_leader,
            "isProjectLeaderApprovalRequired": True,
            "costTypeToApply": null,
            "estimatedHoursToApply": null,
            "estimatedCostToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": null,
                "billingRateFrequency": null,
                "billingRateFrequencyDuration": null,
                "billingRates": []
            } if rail.result('create_project') else None,
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": [],
            "resourceAssignmentModifications": null,
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": oefs,
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_child_projects(dag_run):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            dag_run.conf["parentwbscolumnuri"]
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": dag_run.conf['parentwbsfilteruri']
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
                    "text": dag_run.conf['wbs'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def get_update_time_tracking_attribute(dag_run):
    return {
            "objectUri": dag_run.conf['childwbsuri'],
            "value": {
                "definition": {
                    "uri": dag_run.conf['timetrackingattributedefinitionuri']
                },
                "tag": {
                    "uri": dag_run.conf['timetrackingattributetaguri']
                } if dag_run.conf['timetrackingattributetaguri'] else null
            }
        }

def is_child_wbs():
    if rail.result('load_project'):
        current_wbs_oef_values = rail.result('load_project')['extensionFieldValues']
        if current_wbs_oef_values:
            return bool(rail.find_first_by_attr_and_get_attr(current_wbs_oef_values, 'definition.displayText', 'Parent WBS', 'textValue'))
    return False

def get_parent_wbs_info():
    current_wbs_oef_values = rail.result('load_project')['extensionFieldValues']
    parent_wbs_oef_value = rail.find_first_by_attr_and_get_attr(current_wbs_oef_values, 'definition.displayText', 'Parent WBS', 'textValue')
    return {
        "projects": [
            {
                "name": parent_wbs_oef_value
            }
            ]
        }

def is_gsap_company_code():
    conf = get_dag_run_conf()
    parent_project_info =  rail.result('get_parent_wbs_info')
    if parent_project_info:
        assigned_division_uri = rail.result('get_parent_wbs_info')['division']['uri']
        gsap_divisions = rail.load_all_records(conf['gsapcompanycodes'])
        return bool(rail.find_first_by_attr_and_get_attr(gsap_divisions, 'uri', assigned_division_uri, 'uri'))
    return False
