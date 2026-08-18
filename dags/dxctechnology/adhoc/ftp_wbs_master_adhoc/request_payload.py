import uuid
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

def get_child_conf(item):
    return{
        'wbs': item['Project_Name'] if item['Project_Name'] else null,
        'projectcode': item['Project_Code'],
        'status': 'In Progress' if item['Status'] == 'Active' else 'Completed' if item['Status'] == 'Inactive' else null,
        'profitcenteruri': rail.result("get_project_oefs")['PRCTR'],
        'objectclassuri':rail.result("get_project_oefs")['SCOPE'],
        'customeruri':rail.result("get_project_oefs")['CUST_USR00'],
        'functionalareauri':rail.result("get_project_oefs")['FunctionalArea'],
        'producturi':rail.result("get_project_oefs")['Product'],
        'stageuri':rail.result("get_project_oefs")['Stage'],
        'salesforceoppurtunityiduri':rail.result("get_project_oefs")['SalesForceID'],
        'iwonouri':rail.result("get_project_oefs")['IWONo'],
        'profitcenter':item['PRCTR'],
        'objectclass':item['SCOPE'],
        'customer':item['CUST_USR00'],
        'functionalarea':item['FunctionalArea'],
        'product':item['Product'],
        'stage':item['Stage'],
        'salesforceoppurtunityid':item['SalesForceID'],
        'iwono':item['IWONo'],

    }

def get_all_mandatory_check():
    dag_run_conf = get_dag_run_conf()
    return bool(dag_run_conf['wbs'] )

def get_properties_exception():
    dag_run_conf = get_dag_run_conf()
    return {
        'projectname': dag_run_conf['wbs'] if dag_run_conf['wbs'] else null,
        'projectcode': dag_run_conf['projectcode'] if dag_run_conf['projectcode'] else null,
        'status': 'Exception'
    }

def get_properties_success():
    dag_run_conf = get_dag_run_conf()
    return {
        'projectname': dag_run_conf['wbs'] if dag_run_conf['wbs'] else null,
        'projectcode': dag_run_conf['projectcode'] if dag_run_conf['projectcode'] else null,
        'status': 'Success'
    }

def get_project_payload():
    dag_run_conf = get_dag_run_conf()
    return {
        "projects": [
            {
            "uri": null,
            "name": dag_run_conf['projectcode'],
            "code": null,
            "parameterCorrelationId": null
            }
        ]
    }

def get_project_modifications():
    context = rail.get_current_context()
    conf = context['dag_run'].conf

    oefs = []

    def add_oef_type(name):
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
                    "numericValue": null,
                    "textValue": conf[f'{name}'],
                    "fileValue": null,
                }

        )

    if conf['profitcenter']:
        add_oef_type('profitcenter')

    if conf['objectclass']:
        add_oef_type('objectclass')

    if conf['customer']:
        add_oef_type('customer')

    if conf['functionalarea']:
        add_oef_type('functionalarea')

    if conf['product']:
        add_oef_type('product')

    if conf['stage']:
        add_oef_type('stage')

    if conf['salesforceoppurtunityid']:
        add_oef_type('salesforceoppurtunityid')

    if conf['iwono']:
        add_oef_type('iwono')

    return {
                "target": {
                    "uri": rail.result('load_project')['uri'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "nameToApply": null,
                    "codeToApply": null,
                    "descriptionToApply":  null,
                    "percentCompletedToApply": 0,
                    "startDateToApply": null,
                    "endDateToApply": null,
                    "billingTypeToApply": null,
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": null,
                    "statusToApply":  null,
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": null,
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": True,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": True,
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
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
