from datetime import datetime
import hashlib
import uuid
import rail

null = None


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def is_wbs_project():
    conf = get_dag_run_conf()
    return conf['type'] == 'WBS'


def is_icwbs_project_exist():
    return bool(rail.result('get_project_info_based_on_icwbsnumber')) and bool(rail.result(
        'get_project_info_based_on_icwbsnumber')[0]['projectDetails'])


def is_wbs_project_exist():
    return bool(rail.result('get_project_info_based_on_wbs_element')) and bool(rail.result(
        'get_project_info_based_on_wbs_element')[0]['projectDetails'])


def get_enabled_divisions_company_codes_payload():
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
                    "bool": True,
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


def get_combined_tags_param(existing_tags, new_tag_names):
    tag_list = list(map(lambda x: {
        "target": {
            "uri": x['uri']
        },
        "name": x["name"],
        "code": x["code"],
        "description": x["description"],
        "isEnabled": x["isEnabled"]
    }, existing_tags['tags']))

    tag_list = tag_list + list(map(lambda x: {
        "target": {
            "name": x
        },
        "name": x,
        "isEnabled": True
    }, new_tag_names))

    return tag_list


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


def get_put_client_param(client_name, client_code):
    return {
        "client": {
            "target": {
                "uri": null,
                "name": client_name,
                "code": null,
                "parameterCorrelationId": null
            },
            "name": client_name,
            "code": client_code,
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


def get_cost_center_create_param(cost_center):
    return {
        "costCenter": {
            "name": null,
            "uri": null,
            "parent": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": cost_center,
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": True
        },
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_iwo_wbs_oef_update_param():
    dag_run_conf = get_dag_run_conf()
    return {
        "objectUri": rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'],
        "value": {
            "definition": {
                "uri": dag_run_conf['icwbselementuri']
            },
            "textValue": dag_run_conf['ServiceOrderNumberActivityOperation'] if dag_run_conf['type'] == "SO" else dag_run_conf['WBSElement'],
        }
    }


def get_icwbs_iwo_oef_update_param():
    dag_run_conf = get_dag_run_conf()
    project_custom_fields = rail.result("get_project_info_based_on_icwbsnumber")[
        0]['projectDetails']['extensionFieldValues']
    project_iwo_custom_fields = list(filter(lambda x: x['name'] == 'IWO WBS Element', list(map(lambda item: {
        "name": item['definition']['displayText'],
        "textvalue": item['textValue']
    }, project_custom_fields))))

    new_iwowbsnumber = ''
    iwowbsnumber_list = project_iwo_custom_fields[0]['textvalue'] if project_iwo_custom_fields else None
    if iwowbsnumber_list:
        for i in dag_run_conf['WBSElement']:
            if i not in iwowbsnumber_list.split("|"):
                iwowbsnumber_list = iwowbsnumber_list + "|" + i
                new_iwowbsnumber = iwowbsnumber_list
            else:
                new_iwowbsnumber = iwowbsnumber_list
    else:
        new_iwowbsnumber = '|'.join(dag_run_conf['WBSElement'])

    return {
        "objectUri": rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'],
        "value": {
            "definition": {
                "uri": dag_run_conf['icwbselementuri']
            },
            "textValue": new_iwowbsnumber,
        }
    }


def get_user_query_filter_expr():
    dag_run_conf = get_dag_run_conf()

    responsible_person_field = 'PersonResponsibleNumber' if is_wbs_project(
    ) else 'SOPersonResponsible'
    applicant_field = 'WBSOwner2Number' if is_wbs_project() else 'SOPartnerWBSOwner2'

    if dag_run_conf[responsible_person_field] and dag_run_conf[applicant_field]:
        return {
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
                        "text": dag_run_conf[responsible_person_field],
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
                        "text": dag_run_conf[applicant_field],
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
    return {
        "leftExpression": {
            "leftExpression": null,
            "operatorUri": null,
            "rightExpression": null,
            "value": null,
            "filterDefinitionUri": "urn:replicon:user-list-filter:text"},
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
                "text": dag_run_conf[responsible_person_field] if dag_run_conf[responsible_person_field] else dag_run_conf[applicant_field],
                "time": null,
                "calendarDayDurationValue": null,
                "workdayDurationValue": null,
                "dateRange": null,
                "dateTimeUtc": null,
                "dateTimeUtcRange": null},
            "filterDefinitionUri": null},
        "value": null,
        "filterDefinitionUri": null
    }


def get_user_based_on_empid_param():
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:end-date",
            "urn:replicon:user-list-column:location"
        ],
        "sort": [],
        "filterExpression": get_user_query_filter_expr()
    }

def get_parent_division(parent):
    if not parent:
        return "NA"
    if parent['parent']:
        return get_parent_division(parent['parent'])
    return parent['division']['displayText']

def get_assign_permissionset_to_user_param():
    project_leaders  = get_projectleader_uris()
    project_leaders_current_effective_groups = rail.result('get_manager_co_manager_effective_groups')
    project_management_policy_uri = "urn:replicon:policy:project-management"
    permssions_to_add = []
    # policy_data_access_scope = None
    dag_run_conf = get_dag_run_conf()
    permissionSets = []
    policydataaccessscopes = []
    # pylint: disable=cell-var-from-loop
    for user_uri in project_leaders:
        ignore_project_management_permission = False
        current_user_permissions = list(filter(
            lambda x: x['user']['uri'] == user_uri, rail.result('get_permission_sets_for_user')))

        if rail.find_first_by_attr_and_get_attr(
            current_user_permissions,
            "policyUri",
            project_management_policy_uri):
            # as per the CR if there is any project-management permission assigned to the manager or co-manager
            # will not be over-written by the default `Limited WBS` permission
            ignore_project_management_permission = True

        if not ignore_project_management_permission:
            if rail.find_first_by_attr_and_get_attr(current_user_permissions,"policyUri",
                        project_management_policy_uri, 'permissionSet.uri') != dag_run_conf['projectmanagerpermissionuri']:
                permssions_to_add.append(
                    dag_run_conf['projectmanagerpermissionuri'])

        if rail.find_first_by_attr_and_get_attr(current_user_permissions,"policyUri",
                'urn:replicon:policy:user', 'permissionSet.uri') != dag_run_conf['enduserpermissionuri']:
            permssions_to_add.append(
                dag_run_conf['enduserpermissionuri'])

            policy_data_access_scope = {
                "userUri": user_uri,
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:user",
                        "employeeTypeGroups": list(
                            map(
                                lambda x: {
                                    'employeeTypeGroup': {'uri': x['uri']}
                                },
                                dag_run_conf['employeetyperestrictiongroup']))
                    }
                ]
            }

            policydataaccessscopes.append(policy_data_access_scope)

        permissionSets = permissionSets + list(map(lambda x: {
            "userUri": user_uri,
            "permissionSetUri": x}, permssions_to_add))
    return {'policydataaccessscopes': policydataaccessscopes,
            'permissionSets': permissionSets}


def get_projectleadertoapply_param():
    user_info = rail.result('get_user_based_on_empid')
    if rail.result('validate_user_based_on_empid', 'can_assign_manager'):
        return {
            "user": {
                "uri": user_info['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
            }
        }
    return null


def get_comangger_apply_param():
    return rail.result('get_user_based_on_empid')['comanageruri']


def get_replicon_date(date_str):
    if not date_str:
        return None
    # date format in 20060401
    date = datetime.strptime(date_str, '%Y%m%d')
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }


def get_wbs_custom_field_param():
    conf = get_dag_run_conf()
    udf_param_list = []

    if conf['Changedon']:
        udf_param_list.append(
            {'customField': {'uri': conf['changedonuri']}, 'date': get_replicon_date(conf['Changedon'])})

    if not is_wbs_project():
        if conf['CreatedOnDate']:
            udf_param_list.append(
                {'customField': {'uri': conf['wocreateddateuri']}, 'date': get_replicon_date(conf['CreatedOnDate'])})

        if conf['ChangedOnDate']:
            udf_param_list.append(
                {'customField': {'uri': conf['wochangeddateuri']}, 'date': get_replicon_date(conf['ChangedOnDate'])})

    return udf_param_list


# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
def get_wbs_oef_list_param(is_client_dag_triggered):
    conf = get_dag_run_conf()
    oef_param_list = []

    oef_param_list.append(
        {'definition': {'uri': conf['wbstypeudfuri']}, "tag": {
            "uri": null,
            "slug": null,
            "tagName": {
                "name": "C1 WBS",
                "tagDefinitionUri": null
            }
        }, })

    oef_param_list.append(
        {'definition': {'uri': conf['wbssouri']}, 'textValue': 'WBS' if is_wbs_project() else 'SO'})

    if not is_wbs_project():
        oef_param_list.append(
            {
                'definition': {
                    'uri': conf['parentwbsuri']},
                'textValue': conf['WBSElement']
            }
        )

    if conf['InternalSAPObjectNumber']:
        oef_param_list.append(
            {
                'definition': {
                    'uri': conf['internalsapobjectnumberudfuri']},
                'textValue': conf['InternalSAPObjectNumber']})

    if conf['ProjectType']:
        oef_param_list.append({'definition': {'uri': conf['projecttypeuri']}, 'tag': {
            'uri': conf['projecttypevalueuri']}})

    if conf['AccountAssignmentIndicator']:
        oef_param_list.append(
            {
                'definition': {
                    'uri': conf['accountassignmentindicatoruri']},
                'textValue': conf['AccountAssignmentIndicator']})

    if conf['itemcategoryvalueuri']:
        oef_param_list.append({'definition': {'uri': conf['itemcategoryuri']}, 'tag': {
            'uri': conf['itemcategoryvalueuri']}})

    if conf['applicantnameuri'] and (conf['WBSOwner2Name'] if is_wbs_project() else conf['SOPartnerWBSOwner2Name']):
        oef_param_list.append(
            {
                'definition': {
                    'uri': conf['applicantnameuri']
                },
                'textValue': conf['WBSOwner2Name'] if is_wbs_project() else conf['SOPartnerWBSOwner2Name']
            }
        )

    if conf['Changedby']:
        oef_param_list.append(
            {'definition': {'uri': conf['changedbyudfuri']}, 'textValue': conf['Changedby']})

    if conf['IWO']:
        oef_param_list.append(
            {'definition': {'uri': conf['iwonumberuri']}, 'textValue': conf['IWO']})

    if conf['ICWBSNumber']:
        oef_param_list.append(
            {
                'definition':
                {
                    'uri': conf['icwbselementuri']
                },
                    'textValue': null
            })
        oef_param_list.append(
            {
                'definition':
                {
                    'uri': conf['parentwbsuri']
                },
                    'textValue': conf['ICWBSNumber']
            })

    if is_parent_gsap():
        oef_param_list.append(
            {
                'definition': {
                    'uri': conf['parentprojectdefuri']},
                'numericValue': conf['ICWBSNumber']
            }
        )
        if not can_copy_project_from_icwbs():
            current_parent_oef_values = rail.result('get_project_info_based_on_icwbsnumber')[
                0]['projectDetails']['extensionFieldValues']

            reference_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Reference Mandatory', 'tag.uri')
            if reference_mandatory_parent_tag_uri:
                oef_param_list.append(
                    {
                        'definition': {
                            'uri': conf['referencemandatoryuri']
                        },
                        'tag': {
                            'uri': reference_mandatory_parent_tag_uri
                        }
                    }
                )
            else:
                oef_param_list.append(
                    {
                        'definition': {
                            'uri': conf['referencemandatoryuri']
                        },
                        'tag': null
                    }
                )

            comments_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Comments Mandatory', 'tag.uri')
            if comments_mandatory_parent_tag_uri:
                oef_param_list.append(
                    {
                        'definition': {
                            'uri': conf['commentsmandatoryuri']
                        },
                        'tag': {
                            'uri': comments_mandatory_parent_tag_uri
                        }
                    }
                )
            else:
                oef_param_list.append(
                    {
                        'definition': {
                            'uri': conf['commentsmandatoryuri']
                        },
                        'tag': null
                    }
                )
            gsaptaskrequired_tag_uri = rail.find_first_by_attr_and_get_attr(current_parent_oef_values,'definition.displayText','GSAP Task Required','tag.uri')
            if gsaptaskrequired_tag_uri:
                oef_param_list.append(
                    {
                        'definition': {
                            'uri': conf['gsaptaskrequireduri']
                        },
                        'tag': {
                            'uri': gsaptaskrequired_tag_uri
                        }
                    }
                )
            else:
                oef_param_list.append(
                    {
                        'definition': {
                            'uri': conf['gsaptaskrequireduri']
                        },
                        'tag': null
                    }
                )

    if conf['ServiceOrderType']:
        oef_param_list.append({'definition': {'uri': conf['serviceordertypeuri']}, 'tag': {
            'uri': conf['serviceordertypevalueuri']}})

    if conf['Plant']:
        oef_param_list.append(
            {'definition': {'uri': conf['planturi']}, 'textValue': conf['Plant']})

    if conf['InternalServiceOrderobjectnumber']:
        oef_param_list.append(
            {
                'definition': {
                    'uri': conf['internalserviceorderobjectnumberuri']},
                'textValue': conf['InternalServiceOrderobjectnumber']})

    if conf['serviceofferingvalue']:
        oef_param_list.append(
            {'definition': {'uri': conf['serviceofferinguri']}, 'textValue': conf['serviceofferingvalue']})

    if conf['salesforceoppidvalue']:
        oef_param_list.append(
            {'definition': {'uri': conf['salesforceoppiduri']}, 'textValue': conf['salesforceoppidvalue']})

    if conf['salesforceoppnamevalue']:
        oef_param_list.append(
            {'definition': {'uri': conf['salesforceoppnameuri']}, 'textValue': conf['salesforceoppnamevalue']})

    if not is_client_dag_triggered:
        if conf['C1HighLevelCustomerName']:
            oef_param_list.append(
                {'definition': {'uri': conf['C1HighLevelCustomerNameuri']}, 'textValue': conf['C1HighLevelCustomerName']})

        if conf['C1HighLevelCustomerId']:
            oef_param_list.append(
                {'definition': {'uri': conf['C1HighLevelCustomerIduri']}, 'textValue': conf['C1HighLevelCustomerId']})
    return oef_param_list


def get_wbs_date_range():
    conf = get_dag_run_conf()
    start_date_field = 'WBSStartDate' if is_wbs_project() else 'BasicStartDate'
    status_field = 'wbsrepliconstatus' if is_wbs_project() else 'sorepliconstatus'
    current_date_str = datetime.now().strftime('%Y%m%d')
    end_date_to_apply = None
    if not can_copy_project_from_icwbs():
        end_date_field = 'WBSFinishDate' if is_wbs_project() else 'BasicFinishDate'
        end_date_to_apply = get_replicon_date(conf[end_date_field]) if conf[end_date_field] else get_replicon_date(
            current_date_str) if (conf[status_field] == "Completed") else None
    return {
        'startDate': get_replicon_date(conf[start_date_field]) if get_replicon_date(conf[start_date_field]) else None,
        'endDate': end_date_to_apply
    }


def get_wbs_defaultBillingCurrencyToApply():
    conf = get_dag_run_conf()
    if conf['currencyuri']:
        return {
            "currency": {
                "uri": conf['currencyuri'],
                "name": null,
                "symbol": null
            }
        }
    return null


def get_wbs_codeToApply():
    conf = get_dag_run_conf()
    code_field = 'Description' if is_wbs_project() else 'ServiceOrderText'
    if conf[code_field]:
        return {
            "value": conf[code_field]
        }
    return null


def get_project_copy_batch_param():
    return {
        "copyParameter": {
            "sourceProject": {
                "uri": rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri']},
            "destinationProjectInfo": {
                "name": get_dag_run_conf()["WBSElement"] if is_wbs_project() else get_dag_run_conf()["ServiceOrderNumberActivityOperation"],
                "code": null,
                "dateRange": get_wbs_date_range(),
                "statusLabel": null,
                "clients": []},
            "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
            "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:copy-date",
            "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:do-not-copy",
            "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:do-not-copy",
            "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:do-not-copy",
            "projectDependentTimeEntryObjectExtensionFieldCopyOptionUri": "urn:replicon:project-dependent-time-entry-object-extension-field-copy-option:copy"}}


def get_name_to_apply(conf):
    if is_wbs_project_exist():
        return null
    if can_copy_project_from_icwbs():
        return null
    return {"value": conf['WBSElement' if is_wbs_project(
    ) else 'ServiceOrderNumberActivityOperation']}


def can_copy_project_from_icwbs():
    return bool(rail.result('get_project_info_based_on_wbs_element') and not rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails'] and
                get_dag_run_conf()['ICWBSNumber'] and rail.result('get_project_info_based_on_icwbsnumber') and
                rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails'] and
                ( len(rail.result('get_all_project_task')) > 0 or
                  len(rail.result('get_all_attribute_1_project_dependant_fields')) > 0 or
                  len(rail.result('get_all_attribute_2_project_dependant_fields')) > 0 or
                  (is_parent_gsap() and len(rail.result('get_all_gsap_tasks_project_dependant_fields')) > 0)
                )
            )


def get_create_project_target_param():
    if is_wbs_project_exist():
        return {"uri": rail.result('get_project_info_based_on_wbs_element')[
            0]['projectDetails']['uri']}
    if can_copy_project_from_icwbs():
        return {
            "uri": rail.result('get_projectcopy_batch_results')['project']['uri']
        }
    return null


def get_billing_rates():
    return [
        {
            "billingRate": {
                "uri": null,
                "name": "LEGACY BILLING|Billable"
            },
            "rateSchedule": null
        },
        {
            "billingRate": {
                "uri": null,
                "name": "LEGACY BILLING|Non-Billable"
            },
            "rateSchedule": null
        }
    ]


def create_projectorapply_modification_param(is_client_dag_triggered):
    conf = get_dag_run_conf()

    modifications = {
        "nameToApply": get_name_to_apply(conf),
        "codeToApply": get_wbs_codeToApply(),
        "descriptionToApply": get_wbs_codeToApply() if not is_wbs_project() or is_wbs_project_exist() else null,
        "percentCompletedToApply": 0,
        "startDateToApply": {"date": get_wbs_date_range()['startDate']},
        "endDateToApply": {"date": get_wbs_date_range()['endDate']},
        "billingTypeToApply": null,
        "clientBillingAllocationMethodToApply": null,
        "clientAssignmentsSchedulesToApply": null,
        "statusToApply": {
            "uri": null,
            "name": conf['wbsrepliconstatus'] if is_wbs_project() else conf['sorepliconstatus']
        },
        "projectWorkflowStateToApply": null,
        "clientRepresentativeToApply": null,
        "programToApply": {
            "program": {
                "uri": null,
                "name": conf['program']
            }
        },
        "projectLeaderToApply": get_projectleadertoapply_param(),
        "isProjectLeaderApprovalRequired": True,
        "costTypeToApply": null,
        "estimatedHoursToApply": null,
        "estimatedCostToApply": null,
        "defaultBillingCurrencyToApply": get_wbs_defaultBillingCurrencyToApply(),
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": [] if is_wbs_project() else get_billing_rates()
        } if (not is_wbs_project_exist() or can_copy_project_from_icwbs()) else null,
        "billingContractToApply": null,
        "fixedBid": null,
        "customFieldsToApply": get_wbs_custom_field_param(),
        "resourceAssignmentModifications": null,
        "keyValuesToApply": [
            {
                "keyUri": "urn:replicon:project-key-value-key:source-input-reference-id",
                "value": {
                    "text": conf['Wbsmd5']
                }
            }
        ] if conf.get('Wbsmd5') else [],
        "objectExtensionFieldsToApply": get_wbs_oef_list_param(is_client_dag_triggered)
    }

    # For isTimeEntryAllowed below values to be set as per condtins
    # 1. WBS not present and can_copy_project_from_icwbs is True -> null
    # 2. if WBS present -> null
    # 3. if WBS not present and can_copy_project_from_icwbs is False -> True
    if (not is_wbs_project_exist() and not can_copy_project_from_icwbs()):
        modifications['isTimeEntryAllowed'] = True

    if is_icwbs_project_exist():
        modifications['isTimeEntryAllowed'] = rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['isTimeEntryAllowed']

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def update_enddate_payload():
    conf = get_dag_run_conf()
    end_date_field = 'WBSFinishDate' if is_wbs_project() else 'BasicFinishDate'
    status_field = 'wbsrepliconstatus' if is_wbs_project() else 'sorepliconstatus'
    current_date_str = datetime.now().strftime('%Y%m%d')

    # Calculate the end date to apply
    end_date_to_apply = get_replicon_date(conf[end_date_field]) if conf[end_date_field] else get_replicon_date(
        current_date_str) if (conf[status_field] == "Completed") else None

    modifications = {
        "endDateToApply": {"date": end_date_to_apply}
    }

    return {
        "target": {"uri": rail.result('create_projectorapply_modifications')['uri']},
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_eligibleprojectteammember_dataaccessscopes():
    return {
        "projectUri": rail.result('create_projectorapply_modifications')['uri'],
        "teamMemberDataAccessScopes": [
            {
                "locations": [],
                "divisions": [
                    {
                        "uri": get_dag_run_conf()['teamassignmentvisibilityuri'],
                        "parentUri": null,
                        "name": null}],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [
                    {
                        "uri": get_dag_run_conf()['organizationunituri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null}],
                "employeeTypeGroups": []}]}


def get_keyvalue_for_project():
    return {
        "projectUri": rail.result('create_projectorapply_modifications')['uri'],
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
                "collection": []}}}


def get_team_assignment_visibility(item):
    return 'IWO' if item['ProjectType'] == "ES" and item['DXCProjectID'] and item['DXCProjectID'].startswith('E') else \
           'IWO' if item['ProjectType'] == "IC" and item['DXCProjectID'] and item['DXCProjectID'].startswith('X') else \
           'IWO' if item['ProjectType'] == "FT" else \
           item['ServiceOrderCompanyCode'] if item['ServiceOrderNumber'] else \
           item['CompanyCode']

# pylint: disable=line-too-long


def get_icwbsnumber_dag_confg(item):
    return {
        'ICWBSNumber': item['parent'],
        'WBSElement': item['child'],
        'icwbselementuri': rail.find_first_by_attr_and_get_attr(rail.result(
            "get_all_object_extension_field"), "name", "IWO WBS Element", "uri"),
    }


def get_project_dag_confg(item):
    item_val_md5 = hashlib.md5(
        (','.join(val if val else '' for val in item.values())).encode('utf-8'))
    wbs_md5 = item_val_md5.hexdigest()
    return {
        'ProjectDefinition': item['ProjectDefinition'],
        'ProjectDescription': item['ProjectDescription'],
        'ProjectIdentifier': item['ProjectIdentifier'],
        'WBSElement': item['DXCProjectID'],
        'Description': item['WBSElementName'],
        'InternalSAPObjectNumber': item['InternalSAPObjectNumber'],
        'PersonResponsibleNumber': item['PrimaryWBSOwner1'],
        'PersonResponsibleName': item['PrimaryWBSOwnerName'],
        'CompanyCode': item['CompanyCode'],
        'ProjectType': item['ProjectType'],
        'AccountAssignmentIndicator': item['AccountAssignmentIndicator'],
        'WBSElementCurrency': item['Currency'],
        'WBSStartDate': item['ContractLineStartDate'],
        'WBSFinishDate': item['ContractLineEndDate'],
        'WBSElementSystemStatus': item['WBSSTATUS'],
        'ItemCategory': item['ContractType'],
        'WBSOwner2Number': item['WBSOwner2'],
        'WBSOwner2Name': item['WBSOwner2Name'],
        'Changedby': item['Changedby'],
        'Changedon': item['Changedon'],
        'IWO': item['IWO'],
        'ICWBSNumber': item['ICWBSNumber'],
        'ServiceOrderNumberActivityOperation': item['ServiceOrderNumber'],
        'ServiceOrderType': item['ServiceOrderType'],
        'ServiceOrderText': item['ServiceOrderText'],
        'CreatedOnDate': item['CreatedOnDate'],
        'ChangedOnDate': item['ChangedOnDate'],
        'ServiceOrderCompanyCode': item['ServiceOrderCompanyCode'],
        'Plant': item['Plant'],
        'ServiceOrderSystemStatus': item['ServiceOrderSystemStatus'],
        'BasicStartDate': item['BasicStartDate'],
        'BasicFinishDate': item['BasicFinishDate'],
        'InternalServiceOrderobjectnumber': item['ServiceOrderInternalSAPobjectnumber'],
        'SOPersonResponsible': item['SOPersonResponsible'],
        'SOPersonResponsibleName': item['SOPersonResponsibleName'],
        'SOPartnerWBSOwner2': item['SOPartnerWBSOwner2'],
        'SOPartnerWBSOwner2Name': item['SOPartnerWBSOwner2Name'],

        'wbstypeudfuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "WBS Type", "uri"),
        'internalsapobjectnumberudfuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "WBS internal object number", "uri"),
        'companycodeuri': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_divisions_company_codes"), "name", item['CompanyCode'], "uri"),
        'serviceordercompanycodeuri': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_divisions_company_codes"), "name", item['ServiceOrderCompanyCode'], "uri"),
        'projecttypeuri': rail.result("get_all_object_extension_field_projects")['projecttype'],
        'projecttypevalueuri': rail.find_first_by_attr_and_get_attr(rail.result("init_updated_project_type_oef_values")['tags'], "name", item['ProjectType'], "uri"),
        'accountassignmentindicatoruri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Account Assignment Indicator", "uri"),
        'currencyuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_currencies"), "displayText", item['Currency'], "uri"),
        'itemcategoryuri': rail.result("get_all_object_extension_field_projects")['itemcategory'],
        'itemcategoryvalueuri': rail.find_first_by_attr_and_get_attr(rail.result("get_oef_drop_down_values_item_category")['tags'], "name", item['ContractType'], "uri"),
        'applicantnameuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Applicant Name", "uri"),
        'changedonuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), "displayText", "WBS/WO/SO Changed On", "uri"),
        'changedbyudfuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "WBS/WO/SO Changed By", "uri"),
        'iwonumberuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "IWO Number", "uri"),
        'icwbselementuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "IWO WBS Element", "uri"),
        'serviceordertypeuri': rail.result("get_all_object_extension_field_projects")['serviceordertype'],
        'serviceordertypevalueuri': rail.find_first_by_attr_and_get_attr(rail.result("get_oef_drop_down_values_service_order_type")['tags'], "name", item['ServiceOrderType'], "uri"),
        'wocreateddateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), "displayText", "Service Order created date", "uri"),
        'wochangeddateuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), "displayText", "Service Order Changed date", "uri"),
        'planturi': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Plant", "uri"),
        'internalserviceorderobjectnumberuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Internal SAP Object ID", "uri"),
        'type': "SO" if item['ServiceOrderNumber'] else "WBS",
        'wbsrepliconstatus': "Completed" if item['WBSSTATUS'] and 'CLSD' in item['WBSSTATUS'] else 'In Progress',
        'sorepliconstatus': "Completed" if item['ServiceOrderSystemStatus'] and 'CLSD' in item['ServiceOrderSystemStatus'] else 'In Progress',
        'wbssouri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Master WBS (SO, WO)", "uri"),
        'parentwbsuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Parent WBS", "uri"),
        'program': f"{item['ProjectDefinition']}-{item['ProjectDescription']}" if item['ProjectDescription'] else f"{item['ProjectDefinition']}-",
        'client': item['HigherLevelCustomerID'],
        'costcenter': item['ResponsibleCostCenter'],
        'serviceofferinguri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "DXC Service Offering ID", "uri"),
        'serviceofferingvalue': item['ServiceOffering'],
        'salesforceoppiduri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Salesforce Opportunity ID", "uri"),
        'salesforceoppidvalue': item['SalesforceOpportunityID'],
        'salesforceoppnameuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Salesforce Opportunity Name", "uri"),
        'salesforceoppnamevalue': item['SalesforceOpportunityName'],
        'teamassignmentvisibilityuri': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_divisions_company_codes"), "name", get_team_assignment_visibility(item), 'parenturi'),
        'teamassignmentvisibility': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_divisions_company_codes"), "name", get_team_assignment_visibility(item), 'parent'),
        'employeetyperestrictiongroup': rail.result('get_all_employeetype_groups'),
        'enduserpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", "Manager", "uri"),
        'adminpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", "Project Team Assignment Data Import", "uri"),
        'projectmanagerpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", "Limited WBS Manager", "uri"),
        'organizationunituri': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_department_groups"), "displayText", "DXC", "uri"),
        'timetrackingattributeuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Time Tracking Required Attribute", "uri"),
        'russiaiwowbsuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_custom_fields"), "displayText", "Russia IWO WBS", "uri"),
        'russiaiwowbsyesoption': rail.find_first_by_attr_and_get_attr(rail.result("get_rusia_custom_field_dropdown_options"), "displayText", "Yes", "uri"),
        'russiaiwowbsnoption': rail.find_first_by_attr_and_get_attr(rail.result("get_rusia_custom_field_dropdown_options"), "displayText", "No", "uri"),
        'Wbsmd5': wbs_md5,
        'C1HighLevelCustomerName': item['HigherLevelCustomerName'],
        'C1HighLevelCustomerId': item['HigherLevelCustomerID'],
        'C1HighLevelCustomerNameuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "C1 High Level Customer Name", "uri"),
        'C1HighLevelCustomerIduri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "C1 High Level Customer ID", "uri"),
        'projectidentifieruri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "WBS Offering Group", "uri"),
        'projectidentifierdropdownuri': rail.find_first_by_attr_and_get_attr(rail.result("get_oef_drop_down_values_project_identifier")['tags'], "name", item['ProjectIdentifier'], "uri"),
        'psaflaguri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "PSA Flag", "uri"),
        'psaflagoptionuri': rail.find_first_by_attr_and_get_attr(rail.result("get_oef_drop_down_values_psaflag")['tags'], "name", 'X', "uri"),
        'psacompanycodeuri': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_divisions_company_codes"), "name", 'PSA', "uri"),
        'c1companycodeuri': rail.find_first_by_attr_and_get_attr(rail.result("get_enabled_divisions_company_codes"), "name", 'C1', "uri"),
        'gsapcompanycodes': rail.result('get_gsap_company_codes'),
        'parentprojectdefuri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "Parent Project", "uri"),
        'compasscompanycodes': rail.result('get_compass_company_codes'),
        'gsaptaskrequireduri': rail.result("get_all_object_extension_field_projects")['gsaptaskrequired'],
        'referencemandatoryuri': rail.result("get_all_object_extension_field_projects")['referencemandatory'],
        'commentsmandatoryuri': rail.result("get_all_object_extension_field_projects")['commentsmandatory'],
        'tnmindicatoruri': rail.find_first_by_attr_and_get_attr(rail.result("get_all_object_extension_field"), "name", "COMPASS T&M Indicator", "uri"),
    }


def get_icwbs_result(conf):
    icwbs_result = rail.result('get_project_info_based_on_icwbsnumber')
    if conf['ICWBSNumber'] and icwbs_result and icwbs_result[0]['projectDetails']:
        return icwbs_result[0]['projectDetails']
    return null

def get_project_identifier_oef_param(dag_run):
    return {
            "objectUri": rail.result('create_projectorapply_modifications')['uri'],
            "value": {
                "definition": {
                    "uri": dag_run.conf['projectidentifieruri'],
                    "name": null
                },
                "tag": {
                    "uri": dag_run.conf['projectidentifierdropdownuri'],
                },
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        }

def get_psa_flag_oef_param(dag_run):
    return {
            "objectUri": rail.result('create_projectorapply_modifications')['uri'],
            "value": {
                "definition": {
                    "uri": dag_run.conf['psaflaguri'],
                    "name": null
                },
                "tag": {
                    "uri": dag_run.conf['psaflagoptionuri'],
                } if dag_run.conf['ProjectType'] == 'RP' else null,
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        }

def get_assign_team_psa_param(dag_run):
    return {
        "projectUri": rail.result('create_projectorapply_modifications')['uri'],
        "teamMemberDataAccessScopes": [
            {
                "divisions": [
                    {
                        "uri": dag_run.conf['psacompanycodeuri'] if dag_run.conf['ProjectType'] == 'RP' else dag_run.conf['c1companycodeuri'],
                    }
                ],
                "departmentGroups": [
                    {
                        "uri": dag_run.conf['organizationunituri'],
                    }
                ]
            }
        ]
    }

def get_time_tracking_oef_param():
    conf = get_dag_run_conf()
    icwbs_result = get_icwbs_result(conf)
    if icwbs_result:
        time_tracking_oef = rail.find_first_by_attr_and_get_attr(
            icwbs_result['extensionFieldValues'],
            'tag.definition.displayText',
            "Time Tracking Required Attribute")
        if time_tracking_oef and time_tracking_oef['tag']['displayText']:
            return {
                "objectUri": rail.result('create_projectorapply_modifications')['uri'],
                "value": {
                    "definition": {
                        "uri": conf['timetrackingattributeuri'],
                        "name": null
                    },
                    "tag": {
                        "uri": time_tracking_oef['tag']['uri'],
                    },
                    "numericValue": null,
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                }
            }
        return null
    return null

def get_tnm_indicator_oef_param():
    conf = get_dag_run_conf()
    icwbs_result = get_icwbs_result(conf)
    if icwbs_result:
        if rail.result('create_projectorapply_modifications'):
            object_uri = rail.result('create_projectorapply_modifications')['uri']
        else:
            object_uri = rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails']['uri']

        tnm_indiactor_oef = rail.find_first_by_attr_and_get_attr(
            icwbs_result['extensionFieldValues'],
            'tag.definition.displayText',
            "COMPASS T&M Indicator")
        if tnm_indiactor_oef and tnm_indiactor_oef['tag']['displayText']:
            return {
                "objectUri": object_uri,
                "value": {
                    "definition": {
                        "uri": conf['tnmindicatoruri'],
                        "name": null
                    },
                    "tag": {
                        "uri": tnm_indiactor_oef['tag']['uri'],
                    },
                    "numericValue": null,
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                }
            }
        return {
            "objectUri": object_uri,
            "value": {
                "definition": {
                "uri": conf['tnmindicatoruri']
                }
            }
        }
    return null

def get_update_russia_udf_param():
    conf = get_dag_run_conf()
    icwbs_result = get_icwbs_result(conf)
    drop_down_uri = conf['russiaiwowbsyesoption'] if icwbs_result and \
        icwbs_result['division']['displayText'] == 'RUES' else conf['russiaiwowbsnoption']
    return {
        "objectUri": rail.result('create_projectorapply_modifications')['uri'],
        "customFieldUri": conf['russiaiwowbsuri'],
        "customFieldDropDownOptionUri": drop_down_uri
    }


def can_update_client():
    conf = get_dag_run_conf()
    return bool(conf['client'])


def get_update_client_param():
    conf = get_dag_run_conf()
    if conf['client']:
        client_data = rail.result('get_client_info')['rows']
        row_index = rail.find_index_by_attr(
            list(map(lambda x: x['cells'][0], client_data)), 'textValue', conf['client'])
        return {
            "projectUri": rail.result('create_projectorapply_modifications')['uri'],
            "clientUri": client_data[row_index]['cells'][1]['uri'],
            "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
        }
    return null


def get_permission_sets_for_user_param():
    return {
        "userUris": get_projectleader_uris()
    }


def get_projectleader_uris():
    userUris = []
    if rail.result('validate_user_based_on_empid', 'can_assign_manager'):
        userUris.append(rail.result('get_user_based_on_empid')['useruri'])
    if rail.result('validate_user_based_on_empid', 'can_assign_co_manager'):
        userUris.append(rail.result('get_user_based_on_empid')['comanageruri'])
    return userUris


def get_process_attribute_records(attr_selection):
    return {
        "page": 1,
        "pageSize": 10000,
        "textSearch": null,
        "project": {
            "uri": rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldDefinition": {
            "uri": null,
            "name": attr_selection
        }
    }


def get_apply_attribute_project(task_selection):
    return {
        "project": {
            "uri": null,
            "name": get_dag_run_conf()["WBSElement"] if is_wbs_project() else get_dag_run_conf()["ServiceOrderNumberActivityOperation"],
            "code": null,
            "parameterCorrelationId": null
        },
        "objectExtensionFieldTags": {
            "tagsToAdd": rail.result(task_selection),
            "tagsToRemove": []
        }
    }

def get_inherit_psa_flag_payload(dag_run):
    def get_parent_psa_flag_tag_uri():
        current_parent_oef_values = rail.result('get_project_info_based_on_icwbsnumber')[
                0]['projectDetails']['extensionFieldValues']
        if current_parent_oef_values:
            psa_flag_parent_tag_uri_parent = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.uri')
            return psa_flag_parent_tag_uri_parent
        return null

    return {
            "objectUri": rail.result('create_projectorapply_modifications')['uri'],
            "value": {
                "definition": {
                    "uri": dag_run.conf['psaflaguri'],
                    "name": null
                },
                "tag": {
                    "uri": get_parent_psa_flag_tag_uri(),
                } if get_parent_psa_flag_tag_uri() else null,
                "numericValue": null,
                "textValue": null,
                "fileValue": null,
                "jsonValue": null
            }
        }

def is_parent_gsap():
    conf = get_dag_run_conf()
    if is_icwbs_project_exist():
        division = rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['division']
        if division:
            assigned_division_uri = rail.result('get_project_info_based_on_icwbsnumber')[0]['projectDetails']['division']['uri']
            gsap_divisions=rail.load_all_records(conf['gsapcompanycodes'])
            if rail.find_first_by_attr_and_get_attr(gsap_divisions, 'uri', assigned_division_uri, 'uri'):
                return True
    return False

def is_companycode_compass(assigned_division_uri, dag_run):
    compass_divisions=rail.load_all_records(dag_run.conf['compasscompanycodes'])

    if rail.find_first_by_attr_and_get_attr(compass_divisions, 'uri', assigned_division_uri, 'uri'):
        return 'COMPASS'
    return None

def check_wbsofferinggrp_psaflag(dag_run):

    if is_icwbs_project_exist():
        assigned_division_uri = rail.result('get_project_info_based_on_icwbsnumber')[
            0]['projectDetails']['division']['uri']

        if is_companycode_compass(assigned_division_uri, dag_run) and is_wbs_project_exist():

            current_wbs_oef_values = rail.result('get_project_info_based_on_wbs_element')[
                0]['projectDetails']['extensionFieldValues']

            if current_wbs_oef_values:
                wbs_offering_grp_tag_value =  rail.find_first_by_attr_and_get_attr(
                    current_wbs_oef_values, 'definition.displayText', 'WBS Offering Group', 'tag.displayText')
                psa_flag_tag_value= rail.find_first_by_attr_and_get_attr(
                    current_wbs_oef_values, 'definition.displayText', 'PSA Flag', 'tag.displayText')
                return (wbs_offering_grp_tag_value == 'Velocity Only' and psa_flag_tag_value == 'X')

    return False
