import json
import rail
null = None


def get_create_billable_non_billable_billing_rates_param(name, extension):
    name_extension = name + "|Billable" if extension.casefold() == "billable" else name + \
        "|Non-Billable"
    return {
        "billingRate": {
            "target": {
                "uri": null,
                "name": name_extension
            },
            "name": name_extension,
            "description": null,
            "isEnabled": "true",
            "rateSchedule": null
        }
    }


def get_project_dag_confg(item):
    return {
        'personnelnumber': item['PersonnelNumber'],
        'objectid': item['ObjectID'],
        'wbselement': item['WBSElement_SO'],
        'wbseelementdescription': item['WBSElement_SO_Description'],
        'items': item['Items'],
        'billingrate': rail.result('get_all_billing_rates_1'),
        'name': get_name_uri_employeeid_status(item, "username"),
        'useruri': get_name_uri_employeeid_status(item, "useruri"),
        'employeeid': get_name_uri_employeeid_status(item, "employeeid"),
        'status': get_name_uri_employeeid_status(item, "userstatus"),
        'companycode': get_name_uri_employeeid_status(item, "companycodefullpath")
    }


def get_name_uri_employeeid_status(item, selection):
    employee_id = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "employeeid", item["PersonnelNumber"], selection)
    if bool(employee_id):
        return employee_id
    ia_perner_id = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "iapernerid", item['PersonnelNumber'], selection)
    if bool(ia_perner_id):
        return ia_perner_id
    cwf_c1_alternate_id = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_active_user"), "cwfalternateid", item["PersonnelNumber"], selection)
    if bool(cwf_c1_alternate_id):
        return cwf_c1_alternate_id
    return null


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_project_dag_c1_confg():
    return {
        'personnelnumber': get_dag_run_conf()['personnelnumber'],
        'objectid': get_dag_run_conf()['objectid'],
        'wbselement': get_dag_run_conf()['wbselement'],
        'wbseelementdescription': get_dag_run_conf()['wbseelementdescription'],
        'items': get_dag_run_conf()['items'],
        'billingrate': get_dag_run_conf()['billingrate'],
        'name': get_dag_run_conf()['name'],
        'useruri': get_dag_run_conf()['useruri'],
        'employeeid': get_dag_run_conf()['employeeid'],
        'status': get_dag_run_conf()['status'],
        'companycode': "C1",
        'childwbs': ""
    }


def get_project_dag_compass_confg(item):
    return {
        'personnelnumber': get_dag_run_conf()['personnelnumber'],
        'objectid': get_dag_run_conf()['objectid'],
        'wbselement': get_dag_run_conf()['wbselement'],
        'wbseelementdescription': get_dag_run_conf()['wbseelementdescription'],
        'items': get_dag_run_conf()['items'],
        'billingrate': get_dag_run_conf()['billingrate'],
        'name': get_dag_run_conf()['name'],
        'useruri': get_dag_run_conf()['useruri'],
        'employeeid': get_dag_run_conf()['employeeid'],
        'status': get_dag_run_conf()['status'],
        'companycode': "COMPASS",
        'childwbs': item.split(" - ")[0].strip()
    }


def get_assignmentdaterange_payload():
    assignment = rail.result('assignment_details')
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "resourceUri": get_dag_run_conf()['useruri'],
        "dateRange": {
            "startDate": assignment['startdate'] if assignment['startdate'] else null,
            "endDate": assignment['enddate'] if assignment['enddate'] else null,
            "relativeDateRange": null,
            "relativeDateRangeAsOfDate": null}}


def get_assign_user_payload():
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "resourceUri": get_dag_run_conf()['useruri'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def new_uri_assign():
    assigned_billing_rate = rail.result('assignedBillingRates')
    data_item = get_dag_run_conf()['items']
    labour_type_payload = list(map(lambda x: x['LaborType'], data_item))
    uri_billable_name = [labour_type +
                         "|Billable" for labour_type in labour_type_payload]
    uri_nonbillable_name = [
        labour_type + "|Non-Billable" for labour_type in labour_type_payload]
    uri_billable = [rail.find_first_by_attr_and_get_attr(get_dag_run_conf()['billingrate'], "displayText", billable_name, "uri", False)
                    for billable_name in uri_billable_name]
    uri_nonbillable = [rail.find_first_by_attr_and_get_attr(get_dag_run_conf()['billingrate'], "displayText", nonbillable_name, "uri", False)
                       for nonbillable_name in uri_nonbillable_name]
    if not assigned_billing_rate:
        return [*uri_billable, *uri_nonbillable]
    uri_billable_assigned = list(
        map(lambda x: x['billingRate']['uri'], assigned_billing_rate))
    uri_nonbillable_assigned = list(
        map(lambda x: x['billingRate']['uri'], assigned_billing_rate))
    uri_billable_to_be_assigned = [
        uri_bill for uri_bill in uri_billable if uri_bill not in uri_billable_assigned]
    uri_nonbillable_to_be_assigned = [
        uri_nonbill for uri_nonbill in uri_nonbillable if uri_nonbill not in uri_nonbillable_assigned]
    return [*uri_billable_to_be_assigned, *uri_nonbillable_to_be_assigned]


def assigned_payload():
    uri = rail.result('get_project_info_based_on_wbs_element')[
        0]['projectDetails']['uri']
    return json.dumps({
        "projectUri": uri,
        "resourceUri": get_dag_run_conf()['useruri'],
        "billingRateUris": new_uri_assign(),
        "assigned": "true"
    })


def get_child_wbs_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            rail.result('get_all_columns')[0]['uri']
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": rail.result('get_all_filter_defination')[0]['uri']
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
                    "text": get_dag_run_conf()['wbselement'],
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


def get_project_details_payload(dag_run):
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf['childwbs'] if dag_run.conf['companycode'] == "COMPASS" else dag_run.conf['wbselement'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }
