from datetime import datetime
import rail
from dxctechnology.c1_iwo_leanstaffing import request_payload
null = None


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%Y%m%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def project_status(get_project_info):
    value = rail.result(get_project_info)[
        0]['projectDetails']['status']['name']
    if value not in ['Completed', 'Archived', 'Cancelled']:
        return True
    return False


def assigment_json_details():
    conf = request_payload.get_dag_run_conf()
    data = conf['items']
    startDate = data[0]['AssignmentStart'] if bool(data) else ""
    endDate = data[0]['AssignmentEnd'] if bool(data) else ""
    return {
        "startdate": get_replicon_date(startDate),
        "enddate": get_replicon_date(endDate)
    }


def logMessage():
    conf = request_payload.get_dag_run_conf()
    personelNumber = conf['personnelnumber']
    wbsElementSO = conf['wbselement']
    message = []
    if not bool(personelNumber):
        message.append("Personnel number not present for the record")
    if not bool(wbsElementSO):
        message.append("WBS element not present for the record")
    return ', '.join(message)


def assignedBillingRates(get_all_project_team_assignment):
    return rail.result(get_all_project_team_assignment)[0]['billingRatesAllowedForBillingTime']


def active_user(load_report_data):
    jsonValue = rail.load_all_records(rail.result(load_report_data))
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


def is_LT_assigned():
    data_item = request_payload.get_dag_run_conf()['items']
    labour_types = [i for i in list(
        map(lambda x: x['LaborType'], data_item)) if bool(i)]
    if not bool(labour_types):
        return False
    if not bool(rail.result('get_all_project_team_assignment')):
        return True
    data = rail.result('assignedBillingRates')
    labour_type_payload = list(set(labour_types))
    uri_billable_name = [labour_type +
                         "|Billable" for labour_type in labour_type_payload]
    uri_nonbillable_name = [
        labour_type + "|Non-Billable" for labour_type in labour_type_payload]
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
