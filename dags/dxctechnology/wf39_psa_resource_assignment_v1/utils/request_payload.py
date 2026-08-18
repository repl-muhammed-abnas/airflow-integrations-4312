import json
from datetime import datetime
import rail
null = None


def get_create_billing_rates_param(
        dag_run, item):
    return {
        "billingRate": {
            "target": {
                "name": dag_run.conf['name'] + item
            },
            "name": dag_run.conf['name'] + item,
            "description": null,
            "isEnabled": True
        }
    }


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%m/%d/%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_process_billing_rate_wbs_conf(item):
    return {
        'wbs': item['wbs'],
        'billing_rates_from_replicon': rail.result("get_all_billing_rates"),
        'input_combined_data': rail.result("input_combined_data_collection"),
        'active_user': rail.result("load_report_data")
    }


def get_project_payload(dag_run):
    return {"projects": [{"uri": null, "name": dag_run.conf['wbs'],
                          "code": null, "parameterCorrelationId": null}]}


def get_division_detail():
    data = rail.result("get_project_info_from_project_service")[
        'division']['uri']
    return {
        "divisionUri": data
    }


def get_user_division_detail():
    data = rail.result("get_user_info")[
        'divisionSchedule'][-1]['division']['uri']
    return {
        "divisionUri": data
    }


def get_project_dag_confg(dag_run, item):
    all_data = rail.result("get_assignable_billing_rates")
    wbs_emp_data = list(filter(lambda x: item['wbs'] == x['wbs']
                        and item['employeeid'] == x['employeeid'], all_data))

    return {
        'user': item['employeeid'],
        'wbselement': item['wbs'],
        'labourtype': item['role'],
        'startdate': wbs_emp_data[0]['startdate'],
        'enddate': wbs_emp_data[0]['enddate'],
        'billingrate': dag_run.conf['billing_rates_from_replicon'],
        'name': get_name_uri_employeeid_status(item, "username"),
        'useruri': get_name_uri_employeeid_status(item, "useruri"),
        'status': get_name_uri_employeeid_status(item, "userstatus"),
    }


def get_name_uri_employeeid_status(item, selection):
    employee_id = rail.find_first_by_attr_and_get_attr(
        rail.result("get_active_user"), "employeeid", item["employeeid"], selection)
    if bool(employee_id):
        return employee_id
    ia_perner_id = rail.find_first_by_attr_and_get_attr(
        rail.result("get_active_user"), "iapernerid", item['employeeid'], selection)
    if bool(ia_perner_id):
        return ia_perner_id
    cwf_c1_alternate_id = rail.find_first_by_attr_and_get_attr(
        rail.result("get_active_user"), "cwfalternateid", item["employeeid"], selection)
    if bool(cwf_c1_alternate_id):
        return cwf_c1_alternate_id
    return null


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_project_details_payload(dag_run):
    return {
        "projects": [
            {
                "uri": null,
                "name": dag_run.conf['wbselement'],
                "code": null,
                "parameterCorrelationId": null
            }
        ]
    }


def get_assign_user_payload():
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "resourceUri": get_dag_run_conf()['useruri'],
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }


def get_assignmentdaterange_payload():
    return {
        "projectUri": rail.result("get_project_info_based_on_wbs_element")[0]["projectDetails"]["uri"],
        "resourceUri": get_dag_run_conf()['useruri'],
        "dateRange": {
            "startDate": get_replicon_date(get_dag_run_conf()['startdate']) if get_dag_run_conf()['startdate'] else null,
            "endDate": get_replicon_date(get_dag_run_conf()['enddate']) if get_dag_run_conf()['enddate'] else null,
            "relativeDateRange": null,
            "relativeDateRangeAsOfDate": null}}


def new_uri_assign():
    assigned_billing_rate = rail.result('assignedBillingRates')
    data_item = get_dag_run_conf()['labourtype']
    uri_billable_name = [data_item + "|Billable"]
    uri_nonbillable_name = [data_item + "|Non-Billable"]
    uri_billable = [rail.find_first_by_attr_and_get_attr(get_dag_run_conf()['billingrate'], "displayText", billable_name, "uri", False)
                    for billable_name in uri_billable_name]
    uri_nonbillable = [rail.find_first_by_attr_and_get_attr(get_dag_run_conf()['billingrate'], "displayText", nonbillable_name, "uri", False)
                       for nonbillable_name in uri_nonbillable_name]
    if not data_item:
        return []
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
