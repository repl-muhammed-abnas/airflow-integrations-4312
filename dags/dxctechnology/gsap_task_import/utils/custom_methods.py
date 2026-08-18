from datetime import datetime, date
import rail

INPUT_DATE_FORMAT = '%d.%m.%Y'


def get_log_missing_required_fields_msg(item):
    msg = []
    msg.append(
        "Start date is not available" if not item['task_start_date'] else None)
    msg.append(
        "End date is not available" if not item['task_end_date'] else None)
    return ", ".join([m for m in msg if m is not None])


def get_log_invalid_task_records_for_project(item):
    return {
        'wbs': item['wbs'],
        'task': item['task_name'],
        'status': 'Exception',
        'details': get_log_missing_required_fields_msg(item)
    }


def get_trigger_conf(item, dag_run, is_child=False):
    return{
        "file_name": dag_run.conf['file_name'],
        "is_child": is_child,
        "parent_wbs": dag_run.conf['wbs'] if is_child else "",
        "parent_wbs_uri": dag_run.conf['parent_wbs_uri'] if is_child else "",
        "project_name": dag_run.conf['wbs'] if not is_child else dag_run.conf['child_wbs'],
        "project_uri": rail.result('get_project_details')['uri'],
        "project_startdate": rail.result("get_project_details")['timeEntryDateRange']['startDate'],
        "project_enddate": rail.result("get_project_details")['timeEntryDateRange']['endDate'],
        "billingkey_task_name": item['task_name'],
        "billingkey_task_uri": item['task_uri'],
        "user_list": rail.result("get_project_team_member_details"),
        "task_type_oef_uri": dag_run.conf['task_type_oef_uri'],
        "gsap_task_option_uri": dag_run.conf['gsap_task_option_uri']
    }


def get_log_out_of_range(dag_run, item):
    # pylint: disable=consider-using-f-string
    return {
        'wbs': item['project_name'],
        'task': item['task_name'],
        'status': 'Skipped',
        # pylint= disable: line-too-long
        'details': "Task %s is %s than project %s" % (("start date", "less than", "start date")
                                                      if (datetime.strptime(dag_run.conf['task_start_date'], INPUT_DATE_FORMAT).date() <
                                                          convert_to_date(dag_run.conf['project_startdate'])) else ("end date", "less than", "end date")
                                                      )
    }


def convert_to_date(date_value):
    return date(date_value['year'], date_value['month'], date_value['day']) if date_value else None


def compare_start_end_date(dag_run):
    project_start_date = convert_to_date(dag_run.conf['project_startdate'])
    project_end_date = convert_to_date(dag_run.conf['project_enddate'])

    if (datetime.strptime(dag_run.conf['task_start_date'], INPUT_DATE_FORMAT).date() < project_start_date if project_start_date else False) \
            or (datetime.strptime(dag_run.conf['task_end_date'], INPUT_DATE_FORMAT).date() > project_end_date if project_end_date else False):
        return False
    return True


def can_update_task(dag_run):
    existing_tasks = dag_run.conf['existing_task']

    if ((existing_tasks['task_code'] == dag_run.conf['task_code']) or not dag_run.conf['task_code']) and \
        (convert_to_date(existing_tasks['task_start_date']).strftime("%d.%m.%Y") if existing_tasks['task_start_date']
            else None) == dag_run.conf['task_start_date'] and \
            (convert_to_date(existing_tasks['task_end_date']).strftime("%d.%m.%Y") if existing_tasks['task_end_date']
             else None) == dag_run.conf['task_end_date']:
        return False
    return True


def get_completion_log_severity():
    logs = rail.load_all_records(rail.result('create_log'))
    if any(filter(lambda e: e['severity'] == 'Error', logs)):
        return 'Error'
    if any(filter(lambda e: e['severity'] == 'Exception', logs)):
        return 'Exception'
    return 'Success'


def convert_input_data_to_task_data(item):
    def convert_date_dxc_to_str(date_value):
        return f'{date_value[6:8]}/{date_value[4:6]}/{date_value[0:4]}' if date_value else None

    if not item:
        return []
    res = {
        'wbs': item['wbs'],
        'taskname': item['task'],
        'taskcode': item['description'] if item['description'] else None,
        'startdate': convert_date_dxc_to_str(item['validto']),
        'enddate': convert_date_dxc_to_str(item['validfrom']),
    }
    return {k: v if v is not None else '' for k, v in res.items()}


def map_division_name_or_code(response):
    return response.json()['d']['name'] if response.json()['d']['name'] == 'IWO' \
        else (response.json()['d']['code'] if response.json()['d']['code'] else response.json()['d']['parent']['displayText'])
