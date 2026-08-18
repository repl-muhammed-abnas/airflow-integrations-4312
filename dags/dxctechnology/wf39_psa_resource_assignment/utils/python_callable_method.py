from datetime import datetime
import rail
from dxctechnology.wf39_psa_resource_assignment.utils import request_payload


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_input_combined_list(billing_rates_wbs_task_id):
    all_billing_rates_wbs = get_data_from_document(
        rail.result(billing_rates_wbs_task_id))
    return list(
        map(lambda x: {
            'wbs': x['wbs'],
            'role': x['role'],
            'startdate': datetime.strptime(x['startdate'], '%Y-%m-%d').strftime("%m/%d/%Y") if x['startdate'] else "",
            'enddate': datetime.strptime(x['enddate'], '%Y-%m-%d').strftime("%m/%d/%Y") if x['enddate'] else "",
            'employeeid': x['employeeid']
        }, all_billing_rates_wbs)
    )


def active_user(dag_run):
    jsonValue = rail.load_all_records(dag_run.conf['active_user'])
    return list(
        map(lambda x: {
            'username': x['User Name'],
            'loginname': x['Login Name'],
            'employeeid': x['Employeeid'],
            'iapernerid': x['IA Perner ID'],
            'cwfalternateid': x['CWF C1 alternate ID'],
            'useruri': x['UserUri'],
            'userstatus': x['User Status'],
            'companycodefullpath': x['Company Code (Current) (Full Path)']
        }, jsonValue))


def get_assignable_billing_rates(dag_run):
    billing_rates_queried = get_data_from_document(
        rail.result('query_billing_rates_for_wbs'))
    input_combined_list = get_data_from_document(
        dag_run.conf['input_combined_data'])
    return list(
        map(lambda x: {
            'wbs': x['wbs'],
            'employeeid': x['employeeid'],
            'startdate': min(list(map(lambda item: datetime.strptime(
                item['startdate'], '%m/%d/%Y'), list(filter(
                    lambda item: item['wbs'] == x['wbs'] and item['employeeid'] == x['employeeid'] and item['startdate'],
                    input_combined_list))))).strftime('%m/%d/%Y'),
            'enddate': max(list(map(lambda item: datetime.strptime(
                item['enddate'], '%m/%d/%Y'), list(filter(
                    lambda item: item['wbs'] == x['wbs'] and item['employeeid'] == x['employeeid'] and item['enddate'],
                    input_combined_list))))).strftime('%m/%d/%Y')
        }, billing_rates_queried)
    )


def logMessage():
    conf = request_payload.get_dag_run_conf()
    personelNumber = conf['user']
    wbsElementSO = conf['wbselement']
    message = []
    if not bool(personelNumber):
        message.append("Personnel number not present for the record")
    if not bool(wbsElementSO):
        message.append("WBS element not present for the record")
    return ', '.join(message)


def project_status(get_project_info):
    value = rail.result(get_project_info)[
        0]['projectDetails']['status']['name']
    if value not in ['Completed', 'Archived', 'Cancelled']:
        return True
    return False


def is_LT_assigned():
    data_item = request_payload.get_dag_run_conf()['labourtype']
    if not bool(rail.result('get_all_project_team_assignment')):
        return True
    data = rail.result('assignedBillingRates')
    uri_billable_name = [data_item + "|Billable"]
    uri_nonbillable_name = [data_item + "|Non-Billable"]
    uri_billable = [rail.find_first_by_attr_and_get_attr(request_payload.get_dag_run_conf()['billingrate'], "displayText", billable_name, "uri", False)
                    for billable_name in uri_billable_name]
    uri_nonbillable = [rail.find_first_by_attr_and_get_attr(request_payload.get_dag_run_conf()['billingrate'], "displayText", nonbillable_name, "uri", False)
                       for nonbillable_name in uri_nonbillable_name]
    uri_billable_assigned = len(list(filter(
        lambda x: x['billingRate']['uri'] in uri_billable, data))) == len(uri_billable_name)
    uri_nonbillable_assigned = len(list(filter(
        lambda x: x['billingRate']['uri'] in uri_nonbillable, data))) == len(uri_nonbillable_name)
    if uri_billable_assigned or uri_nonbillable_assigned:
        return False
    return True


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

# pylint: disable=too-many-return-statements


def is_assignment_out_of_range(dag_run):
    start_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        dag_run.conf['startdate'])) if dag_run.conf['startdate'] else ""
    end_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        dag_run.conf['enddate'])) if dag_run.conf['enddate'] else ""
    wbs_start_date = get_date_from_replicon_date(rail.result("get_project_info_based_on_wbs_element")[
        0]["projectDetails"]["timeEntryDateRange"]["startDate"]) if rail.result(
        "get_project_info_based_on_wbs_element")[0]["projectDetails"]["timeEntryDateRange"]["startDate"] else ""
    wbs_end_date = get_date_from_replicon_date(rail.result("get_project_info_based_on_wbs_element")[
        0]["projectDetails"]["timeEntryDateRange"]["endDate"]) if rail.result(
        "get_project_info_based_on_wbs_element")[0]["projectDetails"]["timeEntryDateRange"]["endDate"] else ""

    if start_date and wbs_start_date:
        if start_date < wbs_start_date:
            return True
    if end_date and wbs_end_date:
        if end_date > wbs_end_date:
            return True
    return False


def assignedBillingRates(get_all_project_team_assignment):
    return rail.result(get_all_project_team_assignment)[0]['billingRatesAllowedForBillingTime']


def is_user_out_of_range(dag_run):
    start_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        dag_run.conf['startdate'])) if dag_run.conf['startdate'] else ""
    end_date = get_date_from_replicon_date(request_payload.get_replicon_date(
        dag_run.conf['enddate'])) if dag_run.conf['enddate'] else ""
    user_start_date = get_date_from_replicon_date(rail.result("get_user_info")["userDetails"]["employmentDateRange"]["startDate"]) if rail.result(
        "get_user_info")["userDetails"]["employmentDateRange"]["startDate"] else ""
    user_end_date = get_date_from_replicon_date(rail.result("get_user_info")["userDetails"]["employmentDateRange"]["endDate"]) if rail.result(
        "get_user_info")["userDetails"]["employmentDateRange"]["endDate"] else ""

    if start_date and user_start_date:
        if start_date < user_start_date:
            return True
    if end_date and user_end_date:
        if end_date > user_end_date:
            return True
    return False


def get_out_of_range_message(dag_run):
    log = []
    if is_assignment_out_of_range(dag_run):
        log.append(
            'Assignment start/end date outside the WBS start/end date')
    if is_user_out_of_range(dag_run):
        log.append(
            'Assignment start/end date outside the users start/end date')
    return ';'.join(log)


def is_assignemnt_date_out_of_range(dag_run):
    if is_assignment_out_of_range(dag_run):
        return True
    if is_user_out_of_range(dag_run):
        return True
    return False


def is_project_c1():
    data = rail.result("get_division_detail")
    if data['code'] == "C1":
        return True
    return False


def is_user_c1():
    data = rail.result("get_user_division_detail")
    if data['code'] == "C1":
        return True
    return False
