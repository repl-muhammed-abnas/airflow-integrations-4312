import datetime
import rail
from dxctechnology.c1_task_import import request_payload


def get_log_missing_required_fields_msg(item):
    msg = []
    msg.append(
        "Start date is not available" if not item['startdate'] else None)
    msg.append(
        "End date is not available" if not item['enddate'] else None)
    return ", ".join([m for m in msg if m is not None])


def get_log_invalid_task_records_for_project(item):
    return {
        'wbs': item['wbs'],
        'task': item['taskname'],
        'status': 'Exception',
        'details': get_log_missing_required_fields_msg(item)
    }


def get_log_out_of_range(dag_run, item):
    # pylint: disable=consider-using-f-string
    return {
        'wbs': item['project_name'],
        'task': item['task_name'],
        'status': 'Skipped',
        'details': "Task %s is %s than project %s" % (("start date", "less than", "start date")
        if (datetime.datetime.strptime(dag_run.conf['start_date'], '%d/%m/%Y').date() < convert_to_date(dag_run.conf['project_startdate']))
        else ("end date", "less than", "end date"))
    }


def convert_to_date(date):
    return datetime.date(date['year'], date['month'], date['day']) if date else None


def compare_start_end_date(dag_run):
    project_start_date = convert_to_date(dag_run.conf['project_startdate'])
    project_end_date = convert_to_date(dag_run.conf['project_enddate'])

    if (datetime.datetime.strptime(dag_run.conf['start_date'], '%d/%m/%Y').date() < project_start_date if project_start_date else False) \
            or (datetime.datetime.strptime(dag_run.conf['end_date'], '%d/%m/%Y').date() > project_end_date if project_end_date else False):
        return False
    return True


def can_update_task(dag_run):
    existing_tasks = dag_run.conf['existing_tasks']

    if ((existing_tasks['code'] == dag_run.conf['task_code']) or not dag_run.conf['task_code']) and \
           ( convert_to_date(existing_tasks['timeEntryDateRange']['startDate']).strftime("%d/%m/%Y") if existing_tasks['timeEntryDateRange']['startDate']\
        else None) == dag_run.conf['start_date'] and \
            (convert_to_date(existing_tasks['timeEntryDateRange']['endDate']).strftime("%d/%m/%Y") if existing_tasks['timeEntryDateRange']['endDate']\
        else None) == dag_run.conf['end_date']:
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
    def convert_date_dxc_to_str(date):
        return f'{date[6:8]}/{date[4:6]}/{date[0:4]}' if date else None

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


def get_valid_wbs(response):
    response = response.json()['d']['rows']
    return list(
        filter(lambda x: x['parent_wbs_name'] == request_payload.get_conf()['wbs'],
               map(lambda item:
                   {
                       'child_wbs_uri': item['cells'][0]['uri'],
                       'child_wbs_name': item['cells'][0].get('textValue'),
                       'parent_wbs_name': item['cells'][1].get('textValue'),
                   }, response))
    )


def map_division_name_or_code(response):
    return response.json()['d']['name'] if response.json()['d']['name'] == 'IWO' \
        else (response.json()['d']['code'] if response.json()['d']['code'] else response.json()['d']['parent']['displayText'])
