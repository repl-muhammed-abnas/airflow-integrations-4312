from datetime import datetime
import rail
from dxctechnology.psa_resource_assignment_v1.utils import custom_methods

null = None


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


def project_division(task_detail):
    data = rail.result(task_detail)['division']['displayText']
    user_div = custom_methods.get_conf()['companycode'].split("/")[-1].strip()
    return user_div == data


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def assigment_json_details(dag_run):
    return {
        "startDate": get_replicon_date(dag_run.conf['assignmentStartDate']),
        "endDate": get_replicon_date(dag_run.conf['assignmentEndDate'])
    }


def project_wbs_type():
    data = rail.result('get_child_project_details')[
        'extensionFieldValues']
    filter_wbs = list(
        filter(lambda x: x['definition']['displayText'] == 'WBS Type', data))
    return filter_wbs[0]['tag']['displayText'] if filter_wbs else ""
