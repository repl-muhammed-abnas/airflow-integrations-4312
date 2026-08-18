from datetime import datetime
import uuid
import rail


null = None


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_client_list_search_param(client_id):
    return {
        "page": 1,
        "pagesize": 100000000,
        "columnUris": [
            "urn:replicon:client-list-column:code",
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
                    "text": client_id,
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


def get_all_mandatory_fields_check():
    dag_run_conf = get_dag_run_conf()
    return (dag_run_conf['clientid'] and dag_run_conf['clientname'])


def get_properties_client_exception():
    dag_run_conf = get_dag_run_conf()
    return {
        'assignmentid': '',
        'assignmenttitle': '',
        'clientid': dag_run_conf['clientid'] if dag_run_conf['clientid'] else null,
        'clientname': dag_run_conf['clientname'] if dag_run_conf['clientname'] else null,
        'projectid': '',
        'projectname': '',
        'status': 'Exception'
    }


def get_client_mofifications_payload():
    dag_run_conf = get_dag_run_conf()
    custom_fields = []

    def add_custom_fields(name):
        custom_fields.append({
            "customField": {
                "uri": dag_run_conf[f'{name}uri'],
                "name": null,
                "groupUri": null
            },
            "text": dag_run_conf[f'{name}'],
            "date": null,
            "dropDownOption": null,
            "number": null
        }
        )
    if dag_run_conf['billingclient']:
        add_custom_fields('billingclient')

    return {
        "target": null if rail.result('search_client_in_replicon') == [] else {
            "uri": null,
            "name": null,
            "code": dag_run_conf['clientid'],
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null if rail.result('search_client_in_replicon') != [] else {
                "value": dag_run_conf['clientname']
            },
            "codeToApply": null if rail.result('search_client_in_replicon') != [] else {
                "value": dag_run_conf['clientid']
            },
            "descriptionToApply": null if not dag_run_conf['description'] else {
                "value": dag_run_conf['description']
            },
            "statusToApply": null,
            "clientContactToApply": null,
            "clientAddressToApply": {
                "address":  null if not dag_run_conf['clientstreet'] else {
                    "value": dag_run_conf['clientstreet']
                },
                "city":  null if not dag_run_conf['clientcity'] else {
                    "value": dag_run_conf['clientcity']
                },
                "stateProvince": null if not dag_run_conf['clientstate'] else {
                    "value": dag_run_conf['clientstate']
                },
                "country": {
                    "value": {
                        "uri": "urn:replicon:country:united-states",
                        "name": null,
                    }
                },
                "zipPostalCode": null if not dag_run_conf['clientzip'] else {
                    "value": dag_run_conf['clientzip']
                },
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
            },
            "billingAddressToApply": {
                "address": null,
                "city": null,
                "stateProvince": null,
                "country": {
                    "value": {
                        "uri": "urn:replicon:country:united-states",
                        "name": null
                    }
                },
                "zipPostalCode": null,
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
            },
            "billingRatesToApply": null,
            "clientManagerToApply": null,
            "clientSharingToApply": null,
            "expenseCodesToApply": null,
            "customFieldsToApply": custom_fields,
            "taxProfileToApply":  null if not dag_run_conf['tax'] else {
                "taxProfile": {
                    "uri": null,
                    "name": dag_run_conf['tax'],
                    "parameterCorrelationId": null
                }
            }
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_currency_payload():
    dag_run_conf = get_dag_run_conf()
    return{
        "clientUri": rail.result('apply_client_modifications')['uri'],
        "currency": {
            "uri": null,
            "name": null,
            "symbol": dag_run_conf['currency']
        }
    }


def get_datetime_object(date_str):
    if not date_str:
        return None
    try:
        datetime.strptime(date_str, '%m-%d-%Y')
        return date_str
    except:  # pylint: disable=bare-except
        return 'Invalid'


def get_all_mandatory_fields_check_projects():
    dag_run_conf = get_dag_run_conf()
    return (dag_run_conf['projectid'] and dag_run_conf['projectname'] and dag_run_conf['projectstatus']
            and (dag_run_conf['projectstartdate'] != 'Invalid' and dag_run_conf['projectstartdate']) and (dag_run_conf['projectenddate'] != 'Invalid')
            and dag_run_conf['projecttype'] and dag_run_conf['clientid']
            and dag_run_conf['clientname'] and (not dag_run_conf['client_error_log']))


def get_project_payload():
    dag_run_conf = get_dag_run_conf()
    return {
        "projects": [
            {
                "uri": null,
                "name": null,
                "code": dag_run_conf['projectid'],
                "parameterCorrelationId": null
            }
        ]
    }


def get_create_payload():
    dag_run_conf = get_dag_run_conf()

    def get_billing_type_uri(billingtype):
        billing_type = ''
        if billingtype == 'Time and Material':
            billing_type = "urn:replicon:billing-type:time-and-material"
        elif billingtype == 'Non-Billable':
            billing_type = "urn:replicon:billing-type:non-billable"
        elif billingtype == 'Fixed Price':
            billing_type = "urn:replicon:billing-type:fixed-bid"
        return billing_type

    billing_type_uri = get_billing_type_uri(dag_run_conf['projecttype'])

    def get_time_and_material():
        return {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": []
        }
    time_and_materials = null if dag_run_conf['projecttype'] != 'Time and Material' else get_time_and_material(
    )

    def get_replicon_date(date_str):
        if not date_str:
            return None
        try:
            date = datetime.strptime(date_str, '%m-%d-%Y')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }
        except:  # pylint: disable=bare-except
            return None

    return {
        "project": {
            "target": {"code": dag_run_conf['projectid']},
            "projectInfo": {
                "name": dag_run_conf['projectname'],
                "code": dag_run_conf['projectid'],
                "timeEntryDateRange": null if dag_run_conf['projecttype'] != 'Fixed Price' else{
                    "startDate": get_replicon_date(dag_run_conf['projectstartdate']),
                    "endDate": get_replicon_date(dag_run_conf['projectenddate']),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "projectStatusLabel": {"name": dag_run_conf['projectstatus']},
                "percentCompleted": 0,
                "isTimeEntryAllowed": False,
                "isProjectLeaderApprovalRequired": True,
                "billingTypeUri": billing_type_uri,
                "timeAndMaterials": time_and_materials,
            }
        }
    }


def get_project_modifications():
    context = rail.get_current_context()
    conf = context['dag_run'].conf

    def get_replicon_date(date_str):
        if not date_str:
            return None
        try:
            date = datetime.strptime(date_str, '%m-%d-%Y')
            return {
                "date": {
                    "year": date.year,
                    "month": date.month,
                    "day": date.day
                }
            }
        except:  # pylint: disable=bare-except
            return None

    oef = []

    def add_dropdown_oef(name):
        oef.append({
            "definition": {
                "uri": conf[f'{name}uri'],
                "name": null
            },
            "tag": {
                "uri": conf['tag_project_status_uri'],
                "slug": null,
                "tagName": {
                    "name": null,
                    "tagDefinitionUri": null
                }
            },
            "numericValue": null,
            "textValue": null,
            "fileValue": null,
        }
        )

    if conf['projectstatus']:
        add_dropdown_oef('projectstatus')

    return {
        "target": {
            "uri": rail.result('get_project_uri'),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "nameToApply": null,
            "codeToApply": null,
            "descriptionToApply": null,
            "percentCompletedToApply": 0,
            "startDateToApply": get_replicon_date(conf['projectstartdate']),
            "endDateToApply": get_replicon_date(conf['projectenddate']),
            "billingTypeToApply": null,
            "clientBillingAllocationMethodToApply": null,
            "clientAssignmentsSchedulesToApply": null if not conf['clientid'] else {
                "clients": [
                    {
                        "client": {
                            "uri": null,
                            "name": null,
                            "code": conf['clientid'],
                            "parameterCorrelationId": null
                        },
                        "costAllocationPercentage": "100"
                    }
                ],
                "effectiveDate": null
            },
            "statusToApply": {
                "uri": null,
                "name": conf['projectstatus']
            },
            "projectWorkflowStateToApply": null,
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": null,
            "isProjectLeaderApprovalRequired": True,
            "costTypeToApply": null,
            "isTimeEntryAllowed": False,
            "estimatedHoursToApply": null,
            "estimatedCostToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": null,
            "billingContractToApply": null,
            "fixedBid": null,
            "customFieldsToApply": null,
            "resourceAssignmentModifications": null,
            "keyValuesToApply": null,
            "objectExtensionFieldsToApply": oef
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_project_manager_payload():
    dag_run_conf = get_dag_run_conf()
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
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
                    "text": dag_run_conf['projectclientcontact'],
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


def get_project_team_member_payload():
    return {
        "projectUri": rail.result('get_project_uri'),
        "asOfDate": null
    }


def get_project_tasks_payload():
    return {
        "parentUri": rail.result('get_project_uri')
    }


def get_all_mandatory_fields_check_tasks():
    dag_run_conf = get_dag_run_conf()
    return (dag_run_conf['assignmentid'] and dag_run_conf['assignmenttitle'] and (dag_run_conf['assignmentstartdate']
            and dag_run_conf['assignmentstartdate'] != 'Invalid') and dag_run_conf['assignmentenddate'] != 'Invalid'
            and (dag_run_conf['assignmentstatus'] == 'Active' or dag_run_conf['assignmentstatus'] == 'Closed')
            and dag_run_conf['personid'] and dag_run_conf['solomonid']
            and dag_run_conf['clientcontactassignmentlevel'])


def get_create_task_payload():
    dag_run_conf = get_dag_run_conf()

    def get_replicon_date(date_str):
        if not date_str:
            return None
        try:
            date = datetime.strptime(date_str, '%m-%d-%Y')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }
        except:  # pylint: disable=bare-except
            return None

    return {
        "project": {
            "uri": dag_run_conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "task": {
            "target": {
                "uri": null,
                "name": dag_run_conf['assignmenttitle'],
                "parent": null,
                "parameterCorrelationId": null
            },
            "name": dag_run_conf['assignmenttitle'],
            "code": dag_run_conf['assignmentid'],
            "description": null,
            "timeEntryDateRange": {
                "startDate": get_replicon_date(dag_run_conf['assignmentstartdate']),
                "endDate": get_replicon_date(dag_run_conf['assignmentenddate']),
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": null,
            "isClosed": "false",
            "customFieldValues": null,
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": null,
            "keyValues": null,
            "historicalKeyValues": null,
            "extensionFieldValues": null
        }
    }


def get_resource_payload():
    dag_run_conf = get_dag_run_conf()
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled"
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
                    "text": dag_run_conf['personid'],
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


def get_add_resource_payload():
    dag_run_conf = get_dag_run_conf()
    return {
        "projectUri": dag_run_conf['projecturi'],
        "resourceUri": rail.result('get_resource_details')[0]['uri'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def get_task_modifications_payload():
    dag_run_conf = get_dag_run_conf()

    def get_replicon_date(date_str):
        if not date_str:
            return None
        try:
            date = datetime.strptime(date_str, '%m-%d-%Y')
            return {
                "date": {
                    "year": date.year,
                    "month": date.month,
                    "day": date.day
                }
            }
        except:  # pylint: disable=bare-except
            return None

    oefs = []

    def add_oef_type(name):
        oefs.append({
            "definition": {
                    "uri": dag_run_conf[f'{name}uri'],
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
            "textValue": dag_run_conf[f'{name}'],
            "fileValue": null,
        }

        )

    if dag_run_conf['personid'] and \
            (not rail.find_first_by_attr_and_get_attr(rail.result('get_oef_values'), 'displayText', 'Employee ID')):
        add_oef_type('personid')
    if dag_run_conf['solomonid'] and \
            (not rail.find_first_by_attr_and_get_attr(rail.result('get_oef_values'), 'displayText', 'Solomon ID')):
        add_oef_type('solomonid')
    if dag_run_conf['clientcontactassignmentlevel']:
        add_oef_type('clientcontactassignmentlevel')
    if dag_run_conf['assignmentbillingclient']:
        add_oef_type('assignmentbillingclient')
    if dag_run_conf['billingclientstreet']:
        add_oef_type('billingclientstreet')
    if dag_run_conf['billingclientcity']:
        add_oef_type('billingclientcity')
    if dag_run_conf['billingclientstate']:
        add_oef_type('billingclientstate')
    if dag_run_conf['billingclientzip']:
        add_oef_type('billingclientzip')
    if dag_run_conf['assignmentcontact']:
        add_oef_type('assignmentcontact')
    if dag_run_conf['assignmentcontactemail']:
        add_oef_type('assignmentcontactemail')
    if dag_run_conf['projectclientcontact']:
        add_oef_type('projectclientcontact')
    if dag_run_conf['clientmanager']:
        add_oef_type('clientmanager')

    return {
        "target": {
            "uri": rail.result('get_task_uri'),
            "name": null,
            "parent": null,
            "parameterCorrelationId": null
        },
        "project": {
            "uri": dag_run_conf['projecturi'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": null,
            "codeToApply": null,
            "descriptionToApply": null,
            "isClosed": "true" if dag_run_conf['assignmentstatus'] == 'Closed' else 'false',
            "timeEntryStartDateToApply": get_replicon_date(dag_run_conf['assignmentstartdate']),
            "timeEntryEndDateToApply": get_replicon_date(dag_run_conf['assignmentenddate']),
            "timeAndExpenseEntryTypeToApply": null,
            "isTimeEntryAllowed": "true",
            "costTypeToApply": null,
            "estimatedHoursToApply": null,
            "estimatedCostToApply": null,
            "resourceAssignmentModifications": null,
            "customFieldsToApply": null,
            "keyValuesToApply": null,
            "objectExtensionFieldsToApply": oefs
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_properties_task_exception():
    dag_run_conf = get_dag_run_conf()
    return {
        'assignmentid': dag_run_conf['assignmentid'] if dag_run_conf['assignmentid'] else null,
        'assignmenttitle': dag_run_conf['assignmenttitle'] if dag_run_conf['assignmenttitle'] else null,
        'clientid': dag_run_conf['clientid'] if dag_run_conf['clientid'] else null,
        'clientname': dag_run_conf['clientname'] if dag_run_conf['clientname'] else null,
        'projectid': dag_run_conf['projectid'] if dag_run_conf['projectid'] else null,
        'projectname': dag_run_conf['projectname'] if dag_run_conf['projectname'] else null,
        'status': 'Exception'
    }


def get_properties_task_success():
    dag_run_conf = get_dag_run_conf()
    return {
        'assignmentid': dag_run_conf['assignmentid'] if dag_run_conf['assignmentid'] else null,
        'assignmenttitle': dag_run_conf['assignmenttitle'] if dag_run_conf['assignmenttitle'] else null,
        'clientid': dag_run_conf['clientid'] if dag_run_conf['clientid'] else null,
        'clientname': dag_run_conf['clientname'] if dag_run_conf['clientname'] else null,
        'projectid': dag_run_conf['projectid'] if dag_run_conf['projectid'] else null,
        'projectname': dag_run_conf['projectname'] if dag_run_conf['projectname'] else null,
        'status': 'Success'
    }


def get_properties_client_success():
    dag_run_conf = get_dag_run_conf()
    return {
        'assignmentid': '',
        'assignmenttitle': '',
        'clientid': dag_run_conf['clientid'] if dag_run_conf['clientid'] else null,
        'clientname': dag_run_conf['clientname'] if dag_run_conf['clientname'] else null,
        'projectid': '',
        'projectname': '',
        'status': 'Success',
    }


def get_update_project_fixed_bid_rate_payload():
    return {
        "projectUri": rail.result('create_project')['uri'],
        "rate": {
            "amount": "0",
            "currencyUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1"
        },
        "projectFixedBidBillingFrequencyUri": "urn:replicon:fixed-bid-frequency:end-of-project"
    }


def get_oef_values_payload():
    return {
        "objectUri": rail.result('get_task_uri'),
        "bindingContextUri": "urn:replicon:object-type:task"
    }


def get_update_oef_payload():
    dag_run_conf = get_dag_run_conf()
    return {
        "objectUri": rail.result('get_task_uri'),
        "value": {
            "definition": {
                "uri": dag_run_conf['personiduri'],
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": null,
            "fileValue": null
        }
    }
