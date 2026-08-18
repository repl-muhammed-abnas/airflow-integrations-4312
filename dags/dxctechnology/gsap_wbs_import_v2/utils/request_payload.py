from datetime import date, datetime
import json
import uuid
import dateutil.parser
import rail

null = None


def get_replicon_date(date_str, date_format='%d.%m.%Y'):
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


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()


def get_all_enabled_company_codes():
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


def get_put_client_param(clientname):
    return {
        "client": {
            "target": {
                "uri": null,
                "name": clientname,
                "code": null,
                "parameterCorrelationId": null
            },
            "name": clientname,
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


def get_project_conf(item):
    gsap_company_codes = list(filter(
        lambda x: x['parent'] == 'GSAP', rail.result('get_all_enabled_company_codes')))
    return{
        'wbsname': item['WBS_Name'],
        'wbscode': item['WBS_Code'],
        'companycode': item['Company_Code'],
        'projecttype': item['Project_Type'],
        'profitcenter': item['Profit_Centre'],
        'taskindicator': item['Task_Indicator'],
        'startdate': item['Project_Start'],
        'enddate': item['Project_End'],
        'projectmanagerid': item['Primary_Project_Manager_ID'],
        'projectmanagername': item['Primary_Project_Manager_Name'],
        'wbscurrency': item['WBS_Currency'],
        'c1compassparentwbs': item['Parent_Project'],
        'gsapparentwbs': item['WBS_Parent_Project'],
        'salesforceoppurtunityid': item['Salesforce_Opportunity_ID'],
        'soldtoparty': item['Sold_to_Party'],
        'clientname': item['Customer_Name'],
        'controllingarea': item['Controlling_Area'],
        'psaflag': item['PSA_Flag'],
        'referencemandatory': item['Reference_Mandatory'],
        'commentsmandatory': item['Comments_Mandatory'],
        "perner_user": rail.find_first_by_attr_and_get_attr(rail.load_all_records(rail.result("get_required_users_details")),
                                                            'perner', item['Primary_Project_Manager_ID']),
        'companycodeuri': rail.find_first_by_attr_and_get_attr(gsap_company_codes, 'name', item['Company_Code'], 'uri'),
        'limitedwbsmanageruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'displayText', 'Limited WBS Manager', 'uri'),
        'manageruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'displayText', 'Manager', 'uri'),
        'managerconnecturi': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'displayText', 'Manager - Connect', 'uri'),
        'employeetyperestrictiongroup': rail.result('get_all_employeetype_groups'),
        'enableddivisions': rail.result('get_all_enabled_company_codes'),
        'gsapprojecttypeuri': rail.result('get_all_object_extension_fields')['gsapprojecttypeuri'],
        'gsapprojecttypetaguris': rail.result('get_updated_oef_drop_down_values_gsap_project_type')['tags'],
        'projecttypeuri': rail.result('get_all_object_extension_fields')['projecttypeuri'],
        'projecttypetaguris': rail.result('get_oef_drop_down_values_project_type')['tags'],
        'profitcenteruri': rail.result('get_all_object_extension_fields')['profitcenteruri'],
        'wbscurrencyuri': rail.result('get_all_object_extension_fields')['wbscurrencyuri'],
        'parentwbsuri': rail.result('get_all_object_extension_fields')['parentwbsuri'],
        'parentwbsfilteruri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_filter_definitions'), 'name', 'Parent WBS', 'uri'),
        'parentwbscolumnuri': rail.result('get_all_columns'),
        'salesforceoppurtunityiduri': rail.result('get_all_object_extension_fields')['salesforceoppurtunityuri'],
        'soldtopartyuri': rail.result('get_all_object_extension_fields')['soldtopartyuri'],
        'controllingareauri': rail.result('get_all_object_extension_fields')['controllingareauri'],
        'psaflaguri': rail.result('get_all_object_extension_fields')['psaflaguri'],
        'psaflagtaguri': rail.result('get_oef_drop_down_values_psa_flag')['tags'],
        'referencemandatoryuri': rail.result('get_all_object_extension_fields')['referencemandatoryuri'],
        'referencemandatorytaguris': rail.result('get_oef_drop_down_values_reference_mandatory')['tags'],
        'commentsmandatoryuri': rail.result('get_all_object_extension_fields')['commentsmandatoryuri'],
        'commentsmandatorytaguris': rail.result('get_oef_drop_down_values_comments_mandatory')['tags'],
        'itemcategoryuri': rail.result('get_all_object_extension_fields')['itemcategoryuri'],
        'itemcategoryuris': rail.result('get_oef_drop_down_values_item_category')['tags'],
        'taskindicatoruri': rail.result('get_all_object_extension_fields')['taskindicatoruri'],
        'taskindicatortaguris': rail.result('get_oef_drop_down_values_task_indicator')['tags'],
        'wbstypeuri': rail.result('get_all_object_extension_fields')['wbstypeuri'],
        'wbstypetaguris': rail.result('get_oef_drop_down_values_wbs_type')['tags'],
        'iwoindicatoruri': rail.result('get_all_object_extension_fields')['iwoindicatoruri'],
        'iwoindicatortaguris': rail.result('get_oef_drop_down_values_iwo_indicator')['tags'],
        'timetrackingattributeuri': rail.result('get_all_object_extension_fields')['timetrackingattributeuri'],
        'costcenteruricollection': rail.result('cost_center_collection'),
        'gsapdivisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_company_codes'), 'name', 'GSAP', 'uri'),
        'iwodivisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_company_codes'), 'name', 'IWO', 'uri'),
        'organizationuri': rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_department_groups'), 'displayText', 'DXC', 'uri'),
        'tasktypeuri': rail.result('get_task_type_udf'),
        'australialocations': list(map(lambda item: {'uri': item['uri']}, rail.result('get_all_locations'))),
        'iwowbselementuri': rail.result('get_all_object_extension_fields')['iwowbselementuri'],
        'psadivisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_company_codes'), 'name', 'PSA', 'uri'),
        'parentserviceorderuri': rail.result('get_all_object_extension_fields')['parentserviceorderuri'],
        'tnmindicatoruri': rail.result('get_all_object_extension_fields')['tnmindicatoruri']
    }


def mandatory_fields_check(dag_run):
    return (dag_run.conf['wbsname'] and dag_run.conf['companycode'] and dag_run.conf['startdate']
            and dag_run.conf['enddate'] and (get_replicon_date(dag_run.conf['startdate'])) and (get_replicon_date(dag_run.conf['enddate'])))


def get_project_info_on_parentwbs(dag_run):
    return{
        "projects": [
            {
                "name": dag_run.conf['c1compassparentwbs'] if dag_run.conf['c1compassparentwbs'] else dag_run.conf['gsapparentwbs']
            }
        ]
    }


def does_parent_project_exist():
    return bool(rail.result('get_project_info_on_parentwbs') and rail.result(
        'get_project_info_on_parentwbs')[0]['projectDetails'])


def does_wbs_exist():
    return bool(rail.result('get_project_info_based_on_wbs_element')) and bool(rail.result(
        'get_project_info_based_on_wbs_element')[0]['projectDetails'])


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
                    "text": dag_run.conf['projectmanagerid'],
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


def test_enddate():
    end_date = (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['enddate']

    if not end_date:
        return False
    end_date_format = str(dateutil.parser.parse(end_date)).split(' ', maxsplit=1)[0]
    return ((((datetime.strptime(end_date_format, '%Y-%m-%d')).date()).isoformat() <= date.today().isoformat()))


def test_contractor():
    current_employee_grp_full_path = (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[
        0]['employeegrpfullpath']
    if not current_employee_grp_full_path:
        return False
    current_employee_grp = current_employee_grp_full_path.split('|')[0]
    if current_employee_grp == 'Contractor':
        return True
    return False


def get_permission_sets_for_project_manager():
    return {
        "userUris": [(rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['uri']]
    }


def assign_policyDataAccessScopes_to_projectmanager():
    data = rail.result('check_for_required_permissions')[
        'policydataaccessscopes']
    return {
        "userUri": (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['uri'],
        "policyDataAccessScopes": data
    }


def can_copy_project_from_parent(dag_run):
    return bool(rail.result('get_project_info_based_on_wbs_element') and not rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails'] and
                (dag_run.conf['c1compassparentwbs'] or dag_run.conf['gsapparentwbs']) and rail.result('get_project_info_on_parentwbs') and
                rail.result('get_project_info_on_parentwbs')[0]['projectDetails'])


def get_wbs_date_range(dag_run):
    return{
        'startDate': get_replicon_date(dag_run.conf['startdate']),
        'endDate': get_replicon_date(dag_run.conf['enddate'])
    }


def check_parent_division(dag_run):
    if dag_run.conf['gsapparentwbs']:
        return 'GSAP'
    if dag_run.conf['c1compassparentwbs']:
        c1_divisions = list(
            filter(lambda x: x['parent'] == 'C1', dag_run.conf['enableddivisions']))
        compass_divisions = list(
            filter(lambda x: x['parent'] == 'COMPASS', dag_run.conf['enableddivisions']))
        assigned_division_uri = rail.result('get_project_info_on_parentwbs')[
            0]['projectDetails']['division']['uri']
        if rail.find_first_by_attr_and_get_attr(c1_divisions, 'uri', assigned_division_uri, 'uri'):
            return 'C1'
        if rail.find_first_by_attr_and_get_attr(compass_divisions, 'uri', assigned_division_uri, 'uri'):
            return 'COMPASS'
    return None


def get_project_copy_batch_param(dag_run):
    return {
    "copyParameter": {
        "sourceProject": {
            "uri": rail.result('get_project_info_on_parentwbs')[0]['projectDetails']['uri']},
        "destinationProjectInfo": {
            "name": dag_run.conf["wbsname"],
            "code": null,
            "dateRange": get_wbs_date_range(dag_run),
            "statusLabel": null,
            "clients": []
        },
        "taskCopyOptionUri": "urn:replicon:project-copy-task-copy-option:copy",
        "taskDateCopyOptionUri": "urn:replicon:task-date-copy-option:copy-date",
        "teamCopyOptionUri": "urn:replicon:project-copy-team-copy-option:do-not-copy",
        "billingRateCopyOptionUri": "urn:replicon:project-copy-billing-rate-copy-option:do-not-copy",
        "expenseCodeCopyOptionUri": "urn:replicon:project-copy-expense-code-copy-option:do-not-copy",
        "projectDependentTimeEntryObjectExtensionFieldCopyOptionUri": "urn:replicon:project-dependent-time-entry-object-extension-field-copy-option:copy",
        }
    }


def get_create_project_target_param(dag_run):
    if can_copy_project_from_parent(dag_run):
        return {
            "uri": rail.result('get_projectcopy_batch_results')['project']['uri']
        }
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_info_based_on_wbs_element')[0]['projectDetails']['uri']
        }
    return null


def is_diwo_wbs(dag_run):
    if does_parent_project_exist() and dag_run.conf['gsapparentwbs']:
        current_parent_oef_values = rail.result('get_project_info_on_parentwbs')[
            0]['projectDetails']['extensionFieldValues']
        if current_parent_oef_values:
            current_parent_controllingarea_value = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Controlling Area', 'textValue')
            if current_parent_controllingarea_value:
                if dag_run.conf['projecttype'] == '19' and\
                        dag_run.conf['controllingarea'] == '1004' and current_parent_controllingarea_value == '1004' and\
                        dag_run.conf['soldtoparty'].startswith("I"):
                    return True
            return False
        return False
    return False


def get_eligibleprojectteammember_dataaccessscopes(dag_run):
    diwo_division_uri =  [{'uri': dag_run.conf['companycodeuri']}]

    def is_parent_psa_flag_exist():
        if does_parent_project_exist():
            current_parent_oef_values = rail.result('get_project_info_on_parentwbs')[0]['projectDetails']['extensionFieldValues']
            if current_parent_oef_values:
                psa_flag_parent = rail.find_first_by_attr_and_get_attr(current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.displayText')
                if psa_flag_parent == 'X':
                    return True
        return False

    def get_division():
        # pylint: disable=too-many-return-statements
        if (not dag_run.conf['gsapparentwbs'] and not dag_run.conf['c1compassparentwbs']):
            if dag_run.conf['psaflag'] =='X' or dag_run.conf['psaflag'] =='x':
                return dag_run.conf['psadivisionuri']
            return dag_run.conf['companycodeuri']

        if is_diwo_wbs(dag_run) and is_parent_psa_flag_exist():
            return  dag_run.conf['psadivisionuri']

        if not does_parent_project_exist():
            if dag_run.conf['gsapparentwbs'] or dag_run.conf['c1compassparentwbs']:
                if dag_run.conf['psaflag'] =='X' or dag_run.conf['psaflag'] =='x':
                    return dag_run.conf['psadivisionuri']
                return dag_run.conf['iwodivisionuri']

        if does_parent_project_exist():
            if is_parent_psa_flag_exist():
                return dag_run.conf['psadivisionuri']
            return dag_run.conf['iwodivisionuri']

        return dag_run.conf['companycodeuri']

    return {
        "projectUri": rail.result('create_projectorapply_modifications')['uri'],
        "teamMemberDataAccessScopes": [
        {
            "locations": [],
            "divisions": diwo_division_uri if is_diwo_wbs(dag_run) and not is_parent_psa_flag_exist() else [
                {
                    "uri": get_division(),
                    "parentUri": null,
                    "name": null
                }
            ],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [
                {
                    "uri": dag_run.conf['organizationuri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                }
            ],
            "employeeTypeGroups": []
        }
        ]
    }


def get_cost_centers():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:cost-center-list-column:effectively-enabled"
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
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_oefs(dag_run):
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-nested-blocks
    oefs = []

    def add_text_oef(textvalue, uri):
        oefs.append(
            {
                "definition": {
                    "uri": uri,
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": textvalue,
                "fileValue": null,
                "jsonValue": null
            }
        )

    if dag_run.conf['profitcenter']:
        add_text_oef(dag_run.conf['profitcenter'], dag_run.conf['profitcenteruri'])
    else:
        add_text_oef(null, dag_run.conf['profitcenteruri'])
    if dag_run.conf['wbscurrency']:
        add_text_oef(dag_run.conf['wbscurrency'], dag_run.conf['wbscurrencyuri'])
    else:
        add_text_oef(null, dag_run.conf['wbscurrencyuri'])
    if dag_run.conf['salesforceoppurtunityid']:
        add_text_oef(dag_run.conf['salesforceoppurtunityid'], dag_run.conf['salesforceoppurtunityiduri'])
    else:
        add_text_oef(null, dag_run.conf['salesforceoppurtunityiduri'])
    if dag_run.conf['soldtoparty']:
        add_text_oef(dag_run.conf['soldtoparty'], dag_run.conf['soldtopartyuri'])
    else:
        add_text_oef(null, dag_run.conf['soldtopartyuri'])
    if dag_run.conf['controllingarea']:
        add_text_oef(dag_run.conf['controllingarea'], dag_run.conf['controllingareauri'])
    else:
        add_text_oef(null, dag_run.conf['controllingareauri'])
    if dag_run.conf['c1compassparentwbs']:
        add_text_oef(dag_run.conf['c1compassparentwbs'], dag_run.conf['parentwbsuri'])
    if dag_run.conf['gsapparentwbs']:
        add_text_oef(dag_run.conf['gsapparentwbs'], dag_run.conf['parentwbsuri'])


    def add_dropdown_oef(definitionuri, taguri):
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

    wbstypetaguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['wbstypetaguris'], 'name', 'GSAP', 'uri')
    add_dropdown_oef(dag_run.conf['wbstypeuri'], wbstypetaguri)

    if dag_run.conf['projecttype']:
        gsapprojecttypeuri = rail.find_first_by_attr_and_get_attr(dag_run.conf['gsapprojecttypetaguris'], 'name', dag_run.conf['projecttype'], 'uri')
        add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], gsapprojecttypeuri)
    else:
        add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)
    if dag_run.conf['psaflag']:
        psaflagtaguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['psaflagtaguri'], 'name', (dag_run.conf['psaflag']).upper(), 'uri')
        add_dropdown_oef(dag_run.conf['psaflaguri'], psaflagtaguri)
    else:
        add_dropdown_oef(dag_run.conf['psaflaguri'], null)
    if dag_run.conf['referencemandatory']:
        referencetaguri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['referencemandatorytaguris'], 'name', (dag_run.conf['referencemandatory']).upper(), 'uri')
        add_dropdown_oef(dag_run.conf['referencemandatoryuri'], referencetaguri)
    else:
        add_dropdown_oef(dag_run.conf['referencemandatoryuri'], null)
    if dag_run.conf['commentsmandatory']:
        commentstaguri = rail.find_first_by_attr_and_get_attr(
            dag_run.conf['commentsmandatorytaguris'], 'name', (dag_run.conf['commentsmandatory']).upper(), 'uri')
        add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], commentstaguri)
    else:
        add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], null)
    if dag_run.conf['taskindicator']:
        taskindicatortaguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['taskindicatortaguris'], 'name', (dag_run.conf['taskindicator']).upper(), 'uri')
        add_dropdown_oef(dag_run.conf['taskindicatoruri'], taskindicatortaguri)
    else:
        add_dropdown_oef(dag_run.conf['taskindicatoruri'], null)

    return oefs


def get_iwo_oefs(dag_run):
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    current_parent_oef_values = rail.result('get_project_info_on_parentwbs')[
            0]['projectDetails']['extensionFieldValues']
    if does_wbs_exist():
        current_wbs_oef_values = rail.result('get_project_info_based_on_wbs_element')[
            0]['projectDetails']['extensionFieldValues']

    oefs = []

    def add_text_oef(textvalue, uri):
        oefs.append(
            {
                "definition": {
                    "uri": uri,
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": textvalue,
                "fileValue": null,
                "jsonValue": null
            }
        )

    add_text_oef(null,dag_run.conf['iwowbselementuri'])

    if dag_run.conf['c1compassparentwbs']:
        if check_parent_division(dag_run) == 'C1':
            if current_parent_oef_values:
                current_masterwbs_oef_value = rail.find_first_by_attr_and_get_attr(
                    current_parent_oef_values, 'definition.displayText', 'Master WBS (SO, WO)', 'textValue')
                if current_masterwbs_oef_value == "SO":
                    add_text_oef(dag_run.conf['c1compassparentwbs'], dag_run.conf['parentserviceorderuri'])

        if not does_wbs_exist():
            add_text_oef(dag_run.conf['c1compassparentwbs'], dag_run.conf['parentwbsuri'])
        else:
            if current_wbs_oef_values:
                current_parentwbs_oef_value = rail.find_first_by_attr_and_get_attr(current_wbs_oef_values, 'definition.displayText', 'Parent WBS', 'textValue')
                if current_parentwbs_oef_value != dag_run.conf['c1compassparentwbs']:
                    add_text_oef(dag_run.conf['c1compassparentwbs'], dag_run.conf['parentwbsuri'])
            else:
                add_text_oef(dag_run.conf['c1compassparentwbs'], dag_run.conf['parentwbsuri'])

    if dag_run.conf['gsapparentwbs']:
        if not does_wbs_exist():
            add_text_oef(dag_run.conf['gsapparentwbs'], dag_run.conf['parentwbsuri'])
        else:
            if current_wbs_oef_values:
                current_parentwbs_oef_value = rail.find_first_by_attr_and_get_attr(current_wbs_oef_values, 'definition.displayText', 'Parent WBS', 'textValue')
                if current_parentwbs_oef_value != dag_run.conf['gsapparentwbs']:
                    add_text_oef(dag_run.conf['gsapparentwbs'], dag_run.conf['parentwbsuri'])
            else:
                add_text_oef(dag_run.conf['gsapparentwbs'], dag_run.conf['parentwbsuri'])

    if is_diwo_wbs(dag_run):
        if dag_run.conf['profitcenter']:
            add_text_oef(dag_run.conf['profitcenter'], dag_run.conf['profitcenteruri'])
        else:
            add_text_oef(null, dag_run.conf['profitcenteruri'])
        if dag_run.conf['wbscurrency']:
            add_text_oef(dag_run.conf['wbscurrency'], dag_run.conf['wbscurrencyuri'])
        else:
            add_text_oef(null, dag_run.conf['wbscurrencyuri'])
        if dag_run.conf['salesforceoppurtunityid']:
            add_text_oef(dag_run.conf['salesforceoppurtunityid'], dag_run.conf['salesforceoppurtunityiduri'])
        else:
            add_text_oef(null, dag_run.conf['salesforceoppurtunityiduri'])
        if dag_run.conf['soldtoparty']:
            add_text_oef(dag_run.conf['soldtoparty'], dag_run.conf['soldtopartyuri'])
        else:
            add_text_oef(null, dag_run.conf['soldtopartyuri'])
        if dag_run.conf['controllingarea']:
            add_text_oef(dag_run.conf['controllingarea'], dag_run.conf['controllingareauri'])
        else:
            add_text_oef(null, dag_run.conf['controllingareauri'])

    if not is_diwo_wbs(dag_run):
        if current_parent_oef_values:
            profit_center_oef_value = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Profit Center', 'textValue')
            add_text_oef(profit_center_oef_value, dag_run.conf['profitcenteruri'])
            currency_oef_value = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'WBS Currency', 'textValue')
            add_text_oef(currency_oef_value, dag_run.conf['wbscurrencyuri'])
            salesforceid_oef_value = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Salesforce Opportunity ID', 'textValue')
            add_text_oef(salesforceid_oef_value,dag_run.conf['salesforceoppurtunityiduri'])
            soldtoparty_oef_value = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Sold to Party', 'textValue')
            add_text_oef(soldtoparty_oef_value,dag_run.conf['soldtopartyuri'])
            controllingarea_oef_value = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'Controlling Area', 'textValue')
            add_text_oef(controllingarea_oef_value,dag_run.conf['controllingareauri'])
        else:
            add_text_oef(null, dag_run.conf['profitcenteruri'])
            add_text_oef(null, dag_run.conf['wbscurrencyuri'])
            add_text_oef(null, dag_run.conf['salesforceoppurtunityiduri'])
            add_text_oef(null, dag_run.conf['soldtopartyuri'])
            add_text_oef(null, dag_run.conf['controllingareauri'])


    def add_dropdown_oef(definitionuri, taguri):
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

    if current_parent_oef_values:
        psa_flag_parent_tag_uri = rail.find_first_by_attr_and_get_attr(current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.uri')
        if psa_flag_parent_tag_uri:
            add_dropdown_oef(dag_run.conf['psaflaguri'], psa_flag_parent_tag_uri)
        else:
            add_dropdown_oef(dag_run.conf['psaflaguri'], null)
        reference_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Reference Mandatory', 'tag.uri')
        if reference_mandatory_parent_tag_uri:
            add_dropdown_oef(dag_run.conf['referencemandatoryuri'], reference_mandatory_parent_tag_uri)
        else:
            add_dropdown_oef(dag_run.conf['referencemandatoryuri'], null)
        comments_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
            current_parent_oef_values, 'definition.displayText', 'Comments Mandatory', 'tag.uri')
        if comments_mandatory_parent_tag_uri:
            add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], comments_mandatory_parent_tag_uri)
        else:
            add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], null)
        taskindicator_tag_uri = rail.find_first_by_attr_and_get_attr(current_parent_oef_values, 'definition.displayText', 'GSAP Task Required', 'tag.uri')
        if taskindicator_tag_uri:
            add_dropdown_oef(dag_run.conf['taskindicatoruri'], taskindicator_tag_uri)
        else:
            add_dropdown_oef(dag_run.conf['taskindicatoruri'], null)

    if not current_parent_oef_values:
        if not is_diwo_wbs(dag_run):
            add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)
        add_dropdown_oef(dag_run.conf['psaflaguri'], null)
        add_dropdown_oef(dag_run.conf['referencemandatoryuri'], null)
        add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], null)
        add_dropdown_oef(dag_run.conf['taskindicatoruri'], null)

    if is_diwo_wbs(dag_run):
        if dag_run.conf['projecttype']:
            gsapprojecttypeuri = rail.find_first_by_attr_and_get_attr(dag_run.conf['gsapprojecttypetaguris'], 'name', dag_run.conf['projecttype'], 'uri')
            add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], gsapprojecttypeuri)
        else:
            add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)
        taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['wbstypetaguris'], 'name', 'DIWO', 'uri')
        add_dropdown_oef(dag_run.conf['wbstypeuri'], taguri)
    else:
        if dag_run.conf['c1compassparentwbs']:
            taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['wbstypetaguris'], 'name', 'IWO', 'uri')
            add_dropdown_oef(dag_run.conf['wbstypeuri'], taguri)
        else:
            taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['wbstypetaguris'], 'name', 'GSAP', 'uri')
            add_dropdown_oef(dag_run.conf['wbstypeuri'], taguri)

    if not is_diwo_wbs(dag_run):
        if current_parent_oef_values:
            if check_parent_division(dag_run) == 'C1':
                project_type_parent_tag_name = rail.find_first_by_attr_and_get_attr(current_parent_oef_values,
                 'definition.displayText', 'Project Type', 'tag.displayText')
                if project_type_parent_tag_name:
                    gsap_project_type_taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['gsapprojecttypetaguris'],
                     'name', str(project_type_parent_tag_name), 'uri')
                    if gsap_project_type_taguri:
                        add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], gsap_project_type_taguri)
                else:
                    add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)

                taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['iwoindicatortaguris'], 'name', 'C1', 'uri')
                add_dropdown_oef(dag_run.conf['iwoindicatoruri'], taguri)
                item_category_tag_uri = rail.find_first_by_attr_and_get_attr(current_parent_oef_values,
                 'definition.displayText', 'Item Category', 'tag.uri')
                if item_category_tag_uri:
                    add_dropdown_oef(dag_run.conf['itemcategoryuri'], item_category_tag_uri)
                else:
                    add_dropdown_oef(dag_run.conf['itemcategoryuri'], null)

            if check_parent_division(dag_run) == 'COMPASS':
                project_type_parent_tag_name = rail.find_first_by_attr_and_get_attr(current_parent_oef_values,
                 'definition.displayText', 'Project Type', 'tag.displayText')
                if project_type_parent_tag_name:
                    gsap_project_type_taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['gsapprojecttypetaguris'],
                     'name', str(project_type_parent_tag_name), 'uri')
                    if gsap_project_type_taguri:
                        add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], gsap_project_type_taguri)
                else:
                    add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)

                taguri = rail.find_first_by_attr_and_get_attr(dag_run.conf['projecttypetaguris'], 'name', 'CP', 'uri')
                add_dropdown_oef(dag_run.conf['projecttypeuri'], taguri)
                timetracking_attribute_tag_uri = rail.find_first_by_attr_and_get_attr(
                    current_parent_oef_values, 'definition.displayText', 'Time Tracking Required Attribute', 'tag.uri')
                if timetracking_attribute_tag_uri:
                    add_dropdown_oef(dag_run.conf['timetrackingattributeuri'], timetracking_attribute_tag_uri)
                else:
                    add_dropdown_oef(dag_run.conf['timetrackingattributeuri'], null)

            if check_parent_division(dag_run) == 'GSAP':
                gsap_project_type_parent_tag_uri = rail.find_first_by_attr_and_get_attr(current_parent_oef_values,
                 'definition.displayText', 'GSAP Project Type', 'tag.uri')
                if gsap_project_type_parent_tag_uri:
                    add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], gsap_project_type_parent_tag_uri)
                else:
                    add_dropdown_oef(dag_run.conf['gsapprojecttypeuri'], null)


    return oefs

def get_parent_psaflag():
    if does_parent_project_exist():
        current_parent_oef_values = rail.result('get_project_info_on_parentwbs')[
            0]['projectDetails']['extensionFieldValues']
        if current_parent_oef_values:
            psa_flag_parent_tag_uri_parent = rail.find_first_by_attr_and_get_attr(
                current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.displayText')
            if psa_flag_parent_tag_uri_parent != 'X':
                return True
        return False
    return False

def create_projectorapply_modifications(dag_run):
    def project_leader_to_apply():
        if not rail.result('log_no_projectmanger_in_feed_file') and\
                not rail.result('log_no_projectmanger_not_available') and\
                not rail.result('log_end_date_in_past') and \
                not rail.result('log_project_manager_outside_australia'):
            return {
                "user": {
                    "uri": (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                }
            }
        return null

    keyvalues = []
    if (not is_diwo_wbs(dag_run) and dag_run.conf['gsapparentwbs']) and get_parent_psaflag():
        keyvalues.append({
            "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
            "value": {
                "uri": "urn:replicon:project-team-member-assignment-type:manually-assign-task",
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
        })
    else:
        keyvalues.append({
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
        })

    def get_billingrates():
        if does_parent_project_exist():
            if dag_run.conf['c1compassparentwbs'] and check_parent_division(dag_run) == 'C1' :
                return rail.result('get_all_labour_types')
        return []

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["wbsname"]
        },
        "codeToApply":  {
            "value": dag_run.conf["wbscode"]
        } if dag_run.conf['wbscode'] else null,
        "descriptionToApply": null,
        "percentCompletedToApply": null,
        "startDateToApply": {"date": get_wbs_date_range(dag_run)['startDate']},
        "endDateToApply": {"date": get_wbs_date_range(dag_run)['endDate']},
        "billingTypeToApply": null,
        "clientBillingAllocationMethodToApply": null,
        "clientAssignmentsSchedulesToApply": null,
        "statusToApply": {
            "uri": null,
            "name": 'In Progress',
        },
        "projectWorkflowStateToApply": null,
        "clientRepresentativeToApply": null,
        "programToApply": null,
        "projectLeaderToApply": project_leader_to_apply(),
        "isProjectLeaderApprovalRequired": True,
        "costTypeToApply": null,
        "estimatedHoursToApply": null,
        "estimatedCostToApply": null,
        "defaultBillingCurrencyToApply": null,
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "billingRateFrequency": null,
            "billingRateFrequencyDuration": null,
            "billingRates": get_billingrates()
        } if can_copy_project_from_parent(dag_run) or not does_wbs_exist() else null,
        "billingContractToApply": null,
        "fixedBid": null,
        "customFieldsToApply": [],
        "resourceAssignmentModifications": null,
        "keyValuesToApply": keyvalues,
        "objectExtensionFieldsToApply": get_iwo_oefs(dag_run) if can_copy_project_from_parent(dag_run)
         else (get_iwo_oefs(dag_run) if does_parent_project_exist() else get_oefs(dag_run))
    }

    if (not dag_run.conf['c1compassparentwbs'] and not dag_run.conf['gsapparentwbs']) or not does_parent_project_exist():
        modifications['isTimeEntryAllowed'] = False

    if can_copy_project_from_parent(dag_run) or does_parent_project_exist():
        if check_parent_division(dag_run) == 'COMPASS':
            modifications['isTimeEntryAllowed'] = True
        if check_parent_division(dag_run) == 'C1' or check_parent_division(dag_run) == 'GSAP':
            assigned_parent_timeentrycheck = rail.result('get_project_info_on_parentwbs')[
                0]['projectDetails']['isTimeEntryAllowed']
            modifications['isTimeEntryAllowed'] = assigned_parent_timeentrycheck

    return {
        "target": get_create_project_target_param(dag_run),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def is_project_type_available(dag_run):
    if rail.find_first_by_attr_and_get_attr(dag_run.conf['gsapprojecttypetaguris'], 'name', dag_run.conf['projecttype'], 'uri'):
        return True
    return False


def update_client(status):
    return {
        "projectUri": rail.result('create_projectorapply_modifications')['uri'],
        "clientUri": rail.result('get_client_info')[0]['clienturi'] if status == 'update' else null,
        "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
    }


def can_be_potential_parent(dag_run):
    if not dag_run.conf['c1compassparentwbs'] and not dag_run.conf['gsapparentwbs']:
        return True
    return False


def get_project_list_payload(dag_run):
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
                    "text": dag_run.conf['wbsname'],
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


def get_process_child_wbs_conf(item, dag_run):
    return{
        'wbsname': item['textValue'],
        'parentwbsname': dag_run.conf['wbsname'],
        'profitcenteruri': dag_run.conf['profitcenteruri'],
        'wbscurrencyuri': dag_run.conf['wbscurrencyuri'],
        'salesforceoppurtunityiduri': dag_run.conf['salesforceoppurtunityiduri'],
        'soldtopartyuri': dag_run.conf['soldtopartyuri'],
        'controllingareauri': dag_run.conf['controllingareauri'],
        'gsapprojecttypeuri': dag_run.conf['gsapprojecttypeuri'],
        'psaflaguri': dag_run.conf['psaflaguri'],
        'referencemandatoryuri': dag_run.conf['referencemandatoryuri'],
        'commentsmandatoryuri': dag_run.conf['commentsmandatoryuri'],
        'taskindicatoruri': dag_run.conf['taskindicatoruri'],
        'projecttype': dag_run.conf['projecttype'],
        'controllingarea': dag_run.conf['controllingarea'],
        'soldtoparty': dag_run.conf['soldtoparty'],
        'wbstypetaguris': dag_run.conf['wbstypetaguris'],
        'wbstypeuri': dag_run.conf['wbstypeuri'],
        'tasktypeuri': dag_run.conf['tasktypeuri'],
        'gsap_divisions': list(filter(lambda x: x['parent'] == 'GSAP', dag_run.conf['enableddivisions']))
    }


def get_update_oef_payload():
    return {
        "target": {
            "uri": rail.result('get_child_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "objectExtensionFieldsToApply": rail.result('get_all_parent_oefs')
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_all_project_tasks_payload(URI):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:task-list-column:task",
            "urn:replicon:task-list-column:full-path",
            "urn:replicon:task-list-column:parent",
            "urn:replicon:task-list-column:enabled",
            "urn:replicon:task-list-column:code",
            "urn:replicon:task-list-column:start-date",
            "urn:replicon:task-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:task-list-filter:project"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "uri": URI
                }
            }
        }
    }

def get_put_task_payload(dag_run):
    master_task = rail.result('get_parent_wbs_task_details')[0]

    def get_custom_fields():

        if not master_task['customFields']:
            return []

        dropdown_value = rail.find_first_by_attr_and_get_attr(
            master_task['customFields'], "customField.displayText", 'Task Type', 'text')
        if not dropdown_value:
            return []

        return [
            {
                "customField": {
                    "uri": dag_run.conf['task_type'],
                },
                "dropDownOption": {
                    "name": dropdown_value,
                }
            }
        ]

    def timeEntryDateRange():
        if not dag_run.conf['start_date'] and not dag_run.conf['end_date']:
            return null

        return {
            "startDate": get_replicon_date(dag_run.conf['start_date'], "%d %B %Y"),
            "endDate": get_replicon_date(dag_run.conf['end_date'], "%d %B %Y"),
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }

    return {
        "project": {
            "uri": dag_run.conf['processing_wbs_uri'],
        },
        "task": {
            "target": {
                "name": dag_run.conf['taskname'],
                "parent": {
                    "uri": rail.result('get_parent_task_details')[0]['uri'],
                } if dag_run.conf['level'] != '1' else null,
            },
            "name": dag_run.conf['taskname'],
            "code": null if dag_run.conf['code'] in ['None', None, ''] else dag_run.conf['code'],
            "description": null,
            "percentCompleted": "0",
            "timeEntryDateRange": timeEntryDateRange(),
            "isTimeEntryAllowed": True,
            "isClosed": False,
            "customFieldValues": get_custom_fields(),
            "timeAndExpenseEntryTypeUri": master_task['timeAndExpenseEntryType']['uri']
            if master_task['timeAndExpenseEntryType'] else null,
            "assignedResources": []
        }
    }


def get_iwoelement_dag_confg(item):
    return{
        'parentwbs': item['Parent_Project'] if item['Parent_Project'] else item['WBS_Parent_Project'],
        'iwowbselementuri': rail.result('get_all_object_extension_fields')['iwowbselementuri']
    }


def update_iwo_wbs_oef(dag_run):
    project_oef_values = rail.result("get_project_info_based_on_parent")[
        0]['projectDetails']['extensionFieldValues']
    parent_iwo_element_field = list(filter(lambda x: x['name'] == 'IWO WBS Element', list(map(lambda item: {
        "name": item['definition']['displayText'],
        "textvalue": item['textValue']
    }, project_oef_values))))

    child_data = rail.load_all_records(rail.result('query_all_childs'))
    filter_childdata = list(map(lambda item: item['WBS_Name'], child_data))

    new_iwowbsnumber = ''
    iwowbsnumber_list = parent_iwo_element_field[0]['textvalue'] if parent_iwo_element_field else None
    if iwowbsnumber_list:
        for i in filter_childdata:
            if i not in iwowbsnumber_list.split("|"):
                iwowbsnumber_list = iwowbsnumber_list + "|" + i
                new_iwowbsnumber = iwowbsnumber_list
            else:
                new_iwowbsnumber = iwowbsnumber_list
    else:
        new_iwowbsnumber = '|'.join(filter_childdata)

    return {
        "objectUri": rail.result('get_project_info_based_on_parent')[0]['projectDetails']['uri'],
        "value": {
            "definition": {
                "uri": dag_run.conf['iwowbselementuri']
            },
            "textValue": new_iwowbsnumber,
        }
    }


def is_iwo_project_exist():
    return (rail.result('get_project_info_based_on_parent') and rail.result(
        'get_project_info_based_on_parent')[0]['projectDetails'])


def get_all_locations():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
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
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_completion_message():
    project_status = ''
    if not rail.result('get_all_exception_logs'):
        project_status = 'Project Updated Successfully' if does_wbs_exist(
        ) else 'Project Added Successfully'
    else:
        project_status = 'Project Updated Partially, ' + \
            str(rail.result('get_all_exception_logs'))[
                1:-1] if does_wbs_exist() else 'Project Added Partially, '+ \
        str(rail.result('get_all_exception_logs'))[1:-1]

    return project_status


def get_severity():
    if rail.result('get_all_exception_logs'):
        return 'Exception'
    return 'Success'

def get_blob_rows(item):
    return [item['wbsUri'], item['wbsName'], item['labourType'], item['labourTypeUri'], item['startDate'], item['endDate']]

def get_json_value_payload(dag_run):
    data = rail.load_all_records(
        rail.result('write_existing_blob_records'))
    return json.dumps(list(map(lambda item: {
        'wbsUri': dag_run.conf['wbsuri'],
        'wbsName': dag_run.conf['wbs'],
        'labourType': item['labourtype'],
        'labourTypeUri': item['labourtypeuri'],
        'startDate': item['startdate'],
        'endDate': item['enddate']
    }, data)))

def get_all_labour_types():
    return{
        "projects": [
            {
            "uri": rail.result('get_project_info_on_parentwbs')[0]['projectDetails']['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
            }
        ]
}

def get_blob_update(dag_run):
    return{
        'wbs': dag_run.conf['wbsname'],
        'parentwbs': dag_run.conf['c1compassparentwbs'],
        'wbsuri': rail.result('create_projectorapply_modifications')['uri']
    }

def update_iwo_wbs_element(dag_run):
    if does_wbs_exist():
        project_oef_values = rail.result("get_project_info_based_on_wbs_element")[
            0]['projectDetails']['extensionFieldValues']
        parent_iwo_element_field = list(filter(lambda x: x['name'] == 'IWO WBS Element', list(map(lambda item: {
            "name": item['definition']['displayText'],
            "textvalue": item['textValue']
        }, project_oef_values))))

    child_data = rail.result('get_data_of_child_wbs')
    filter_childdata = list(map(lambda item: item['textValue'], child_data))

    new_iwowbsnumber = ''
    if does_wbs_exist():
        iwowbsnumber_list = parent_iwo_element_field[0]['textvalue'] if parent_iwo_element_field else None
    else:
        iwowbsnumber_list = None

    if iwowbsnumber_list:
        for i in filter_childdata:
            if i not in iwowbsnumber_list.split("|"):
                iwowbsnumber_list = iwowbsnumber_list + "|" + i
                new_iwowbsnumber = iwowbsnumber_list
            else:
                new_iwowbsnumber = iwowbsnumber_list
    else:
        new_iwowbsnumber = '|'.join(filter_childdata)

    return {
        "objectUri": rail.result('create_projectorapply_modifications')['uri'],
        "value": {
            "definition": {
                "uri": dag_run.conf['iwowbselementuri']
            },
            "textValue": new_iwowbsnumber,
        }
    }

def test_division(dag_run):
    if not rail.result('get_child_project_details')['division']:
        return False
    gsap_division = list(map(lambda item: item['name'],dag_run.conf['gsap_divisions']))
    if rail.result('get_child_project_details')['division']['displayText'] in gsap_division:
        return True
    return False

def can_enable_project_manager():
    user_status = (rail.result('get_user_info_on_empid') or rail.result('get_user_info'))[0]['status']
    return not bool(user_status)

def update_oef_fields_c1_compass(dag_run):
    oefs = []
    current_parent_oef_values = rail.result('get_parent_project_details')['extensionFieldValues']
    def add_dropdown_oef(definitionuri, taguri):
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

    psa_flag_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
        current_parent_oef_values, 'definition.displayText', 'PSA Flag', 'tag.uri')
    add_dropdown_oef(dag_run.conf['psaflaguri'], psa_flag_parent_tag_uri)

    reference_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
        current_parent_oef_values, 'definition.displayText', 'Reference Mandatory', 'tag.uri')
    add_dropdown_oef(dag_run.conf['referencemandatoryuri'], reference_mandatory_parent_tag_uri)

    comments_mandatory_parent_tag_uri = rail.find_first_by_attr_and_get_attr(
        current_parent_oef_values, 'definition.displayText', 'Comments Mandatory', 'tag.uri')
    add_dropdown_oef(dag_run.conf['commentsmandatoryuri'], comments_mandatory_parent_tag_uri)

    taskindicator_tag_uri = rail.find_first_by_attr_and_get_attr(
        current_parent_oef_values, 'definition.displayText', 'GSAP Task Required', 'tag.uri')
    add_dropdown_oef(dag_run.conf['taskindicatoruri'], taskindicator_tag_uri)

    return {
        "target": {
            "uri": rail.result('get_child_project_details')['uri'],
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "objectExtensionFieldsToApply": oefs
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_tnm_indicator_oef_param(dag_run):
    parent_project_details = rail.result('get_project_info_on_parentwbs')[0]['projectDetails']
    tnm_indiactor_tag_uri = rail.find_first_by_attr_and_get_attr(
            parent_project_details['extensionFieldValues'],'definition.displayText',"COMPASS T&M Indicator", 'tag.uri')

    return {
        "objectUri": rail.result('create_projectorapply_modifications')['uri'],
        "value": {
            "definition": {
                "uri": dag_run.conf['tnmindicatoruri'],
                "name": null
            },
            "tag": {
                "uri": tnm_indiactor_tag_uri,
            } if tnm_indiactor_tag_uri else null,
            "numericValue": null,
            "textValue": null,
            "fileValue": null,
            "jsonValue": null
        }
    }
