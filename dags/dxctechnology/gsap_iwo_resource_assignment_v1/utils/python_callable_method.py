from datetime import datetime
import rail
from dxctechnology.gsap_iwo_resource_assignment_v1.utils import custom_methods

null = None


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
    users = []
    by_employeeid = {}
    by_iapernerid = {}
    by_perner = {}

    for x in jsonValue:
        user = {
            'username': x['User Name'],
            'loginname': x['Login Name'],
            'employeeid': x['Employeeid'],
            'iapernerid': x['IA Perner ID'],
            'cwfalternateid': x['CWF C1 alternate ID'],
            'useruri': x['UserUri'],
            'userstatus': x['User Status'],
            'companycodefullpath': x['Company Code (Current) (Full Path)'],
            'perner': x['PERNER']
        }
        users.append(user)

        # Build indexes for O(1) lookup
        if x['Employeeid']:
            by_employeeid[x['Employeeid']] = user
        if x['IA Perner ID']:
            by_iapernerid[x['IA Perner ID']] = user
        if x['PERNER']:
            by_perner[x['PERNER']] = user

    rail.set_result(key='by_employeeid', val=by_employeeid)
    rail.set_result(key='by_iapernerid', val=by_iapernerid)
    rail.set_result(key='by_perner', val=by_perner)

    return users


def find_user_by_empid(empid):
    """Find user by employee ID with O(1) lookup using indexed dictionaries"""
    by_employeeid = rail.result("get_active_user", key="by_employeeid")
    if empid in by_employeeid:
        return by_employeeid[empid]

    by_iapernerid = rail.result("get_active_user", key="by_iapernerid")
    if empid in by_iapernerid:
        return by_iapernerid[empid]

    by_perner = rail.result("get_active_user", key="by_perner")
    if empid in by_perner:
        return by_perner[empid]

    return None


def group_records_by_wbs():
    """
    Group valid records by WBS to avoid redundant API calls for same project.
    Each WBS will have a list of employees to process.
    """
    records = rail.result('get_wbs_records_from_xml')

    wbs_groups = {}
    blank_records = []

    for record in records:
        # Validate required fields
        if not (record['wbs'] and record['empid'] and record['tasklevel1']
                and record['assignmentStartDate'] and record['assignmentEndDate']):
            blank_records.append(record)
            continue

        # Find user URI using indexed lookup
        user = find_user_by_empid(record['empid'])

        wbs_name = record['wbs']
        if wbs_name not in wbs_groups:
            wbs_groups[wbs_name] = {
                'wbs': wbs_name,
                'tasklevel1': record['tasklevel1'],
                'employees': []
            }

        wbs_groups[wbs_name]['employees'].append({
            'empid': record['empid'],
            'useruri': user['useruri'] if user else None,
            'assignmentStartDate': record['assignmentStartDate'],
            'assignmentEndDate': record['assignmentEndDate'],
        })

    rail.set_result(key='blank_records', val=blank_records)
    rail.set_result(key='total_employee_records', val=len(records) - len(blank_records))

    return list(wbs_groups.values())


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
    success_count = 0
    error_count = 0
    exception_count = 0
    skipped_count = 0

    for log in log_records:
        record = {
            **log['properties'],
            'ecid': log['ecid']
        }
        final_log_records.append(record)

        status = record.get('status')
        if status == "Success":
            success_count += 1
        elif status == "Error":
            error_count += 1
        elif status == "Exception":
            exception_count += 1
        elif status == "skipped":
            skipped_count += 1

    rail.set_result(key="get_success_logs", val=success_count)
    rail.set_result(key="get_errored_logs", val=error_count)
    rail.set_result(key="get_exception_logs", val=exception_count)
    rail.set_result(key="get_skipped_logs", val=skipped_count)

    return final_log_records
