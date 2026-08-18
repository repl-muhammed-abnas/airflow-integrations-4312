from datetime import datetime
import rail
from dxctechnology.gsap_billing_key_master.utils import custom_methods

null = None


def get_valid_wbs_records():
    records = rail.result('get_wbs_records_from_xml')
    return [record for record in records if record['wbs'] and record['empid'] and record['tasklevel1'] and
            record['assignmentStartDate'] and record['assignmentEndDate']]


def get_blank_wbs_records():
    records = rail.result('get_wbs_records_from_xml')
    return [record for record in records if not record['wbs'] or not record['empid'] or not record['tasklevel1'] or
            not record['assignmentStartDate'] or not record['assignmentEndDate']]


def parent_project_name(task_detail):
    data = rail.result(task_detail)
    parent_wbs = list(filter(
        lambda x: x['definition']['displayText'] == "Parent WBS", data['extensionFieldValues']))
    return parent_wbs[0]['textValue']


def parent_project_division(task_detail):
    data = rail.result(task_detail)['division']
    replicon_division = custom_methods.get_conf()['divisions']
    parent_division= None
    for div in replicon_division:
        if data['displayText'] in replicon_division[div]:
            parent_division = div
            break
    return parent_division


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%d.%m.%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def assigment_json_details(dag_run):
    startDate = dag_run.conf['assignmentStartDate']
    endDate = dag_run.conf['assignmentEndDate']
    return {
        "startDate": get_replicon_date(startDate),
        "endDate": get_replicon_date(endDate)
    }


def project_date_range(task_detail):
    timedaterange = rail.result(task_detail)[
        "timeEntryDateRange"]
    return {
        'startdate': (str(timedaterange['startDate']['day']) + '/' + str(timedaterange['startDate']['month'])
                      + '/' + str(timedaterange['startDate']['year'])) if bool(timedaterange['startDate']) else null,
        'enddate': (str(timedaterange['endDate']['day']) + '/' + str(timedaterange['endDate']['month'])
                    + '/' + str(timedaterange['endDate']['year'])) if bool(timedaterange['endDate']) else null
    }


def retrieve_task_list(task_details):
    tasks = rail.result(task_details)
    tasks_list = []

    for task in tasks:
        name = task['name']
        code = task['code'] if task['code'] else ''
        tasks_list.append({'name': name,
                           'code': code,
                           'enddate': (str(task['timeEntryDateRange']['endDate']['day']) + '/' + str(task['timeEntryDateRange']['endDate']['month'])
                                       + '/' + str(task['timeEntryDateRange']['endDate']['year'])) if bool(task['timeEntryDateRange']['endDate']) else null,
                           'oef': rail.find_first_by_attr_and_get_attr(task['customFields'], "customField.displayText", "Task Type", "text"),
                           'uri': task['uri']
                           })
    return tasks_list


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
            'companycodefullpath': x['Company Code (Current) (Full Path)'],
            'perner': x['PERNER']
        }, jsonValue))


def set_dag_run_conf_ancestry(ancestry, context):
    context['dag_run'].conf['_ancestry'] = ancestry

def get_process_unique_wbs_conf_reprocess(item, context):
    set_dag_run_conf_ancestry(item['properties']['_ancestry'], context)
    item['properties']['reprocess_count'] = int(item['properties'].get('reprocess_count', 0)) + 1
    return {**item['properties']}

def do_format_logs():
    log_records = []

    logs = [rail.result("create_log")] + (rail.result("gather_process_each_wbs_attribute_logs")
        if rail.result("gather_process_each_wbs_attribute_logs") else [])

    for log in logs:
        each_log_records = rail.load_all_records(log)
        if each_log_records:
            log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        'ecid': log['ecid']
        }, log_records))

    rail.set_result(key="get_success_logs", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_errored_logs", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_exception_logs", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_skipped_logs", val=len(list(filter(lambda item: item['status']=="skipped", final_log_records))))

    return final_log_records
