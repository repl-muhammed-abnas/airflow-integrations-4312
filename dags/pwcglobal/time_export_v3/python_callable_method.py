from datetime import datetime, timedelta
import json
from airflow.models import DagRun, Variable
import rail
from rail.lib.artifact import new_artifact

null = None


object_types = {
    'timesheet_object_type': 'urn:replicon:object-type:timesheet',
    'user_object_type': 'urn:replicon:object-type:user'
}

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_dag_runs(dag_id, state=None):

    dag_runs = []
    state = state.lower() if state else None
    # TO BE CHECKED LATER
    # To add execution_start_date & execution_end_date in DagRun.find(...)
    # this will allow to query the data faster.
    for run in DagRun.find(dag_id=dag_id, state=state):
        dag_runs.append({
            'id': run.id,
            'run_id': run.run_id,
            'state': run.state,
            'dag_id': run.dag_id,
            'execution_date': run.execution_date.isoformat(),
            'conf': run.conf
        })

    return dag_runs


def get_last_child_location_dagrun(dag_ids, state):
    location_dag_runs = []
    for dag_id in dag_ids:
        dag_run_state = get_dag_runs(dag_id, state)
        location_dag_runs.extend(dag_run_state)
    return location_dag_runs[-1] if location_dag_runs else None


def get_export_entries(time_extract_config, uatkeyvalue):
    time_extract_mapper = json.loads(
        Variable.get(time_extract_config, default_var='{}'))
    if uatkeyvalue:
        return list(filter(lambda x: x['export_needed'].lower() == 'yes' and x['UAT'].lower() == uatkeyvalue, time_extract_mapper))
    return list(filter(lambda x: x['export_needed'].lower() == 'yes', time_extract_mapper))

def generate_dag_ids(locations, instance):
    return [f"pwc_time_export_child_location_{item['code'].lower()}_{instance}_v3" for item in locations]

# pylint: disable=unused-argument
def get_export_period(dag_ids, state, locations, instance):
    dag_ids_to_use = generate_dag_ids(rail.result(locations), instance)
    location_dag_runs = []
    for dag_id in dag_ids_to_use:
        dag_run_state = get_dag_runs(dag_id, state)
        location_dag_runs.extend(dag_run_state)
    export_period = location_dag_runs[-1]['conf'].get(
        'export_period') if location_dag_runs else None
    rail.set_result(key = "lookup_child_dag_id", val=dag_ids_to_use)
    return 'current' if export_period and export_period == 'past' else 'past'


def map_current_timesheet_period():
    response = rail.result('current_timesheet_period_replicon_before_usersearch') if rail.result(
        'current_timesheet_period_replicon_before_usersearch') else rail.result('current_ts_replicon_after_usersearch')
    current_timesheet_period = []
    if response and response['rows']:
        current_timesheet_period = list(map(lambda x: {
            'user': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', object_types['user_object_type'], 'textValue'),
            'user_uri': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', object_types['user_object_type'], 'uri'),
            'timesheet_uri': rail.find_first_by_attr_and_get_attr(x['cells'], 'objectType', object_types['timesheet_object_type'], 'uri'),
            'date_range': rail.find_first_by_attr_and_get_attr(x['cells'], 'dataType', 'urn:replicon:list-type:date-range', 'dateRangeValue')
        }, response['rows']))
    return current_timesheet_period


def get_date_minus_1_day(replicon_date):
    if not replicon_date:
        return null
    try:
        date = datetime.strptime(
            f"{replicon_date['year']}-{replicon_date['month']}-{replicon_date['day']}", '%Y-%m-%d')
        return (date - timedelta(days=1)).date()
    except:  # pylint: disable=bare-except
        return null


def map_twb_enddate_startdate(dag_run):
    date_range = rail.result('map_current_timesheet_period')[0]['date_range']
    start_date = date_range['startDate']
    if dag_run.conf['export_period'] == 'past':
        start_date_minus_1_day = get_date_minus_1_day(start_date)
        if dag_run.conf.get('custom_start_date') and dag_run.conf.get('custom_end_date'):
            return {
                'endDate': dag_run.conf['custom_end_date'],
                'startDate': dag_run.conf['custom_start_date']
            }
        return {
            'endDate': {
                'year': start_date_minus_1_day.year,
                'month': start_date_minus_1_day.month,
                'day': start_date_minus_1_day.day
            },
            'startDate': {
                'year': 2023,
                'month': 6,
                'day': 1
            }
        }
    return {
        'endDate': date_range['endDate'],
        'startDate': start_date
    }


def format_user_list_from_batch():
    user_list = []
    batch_result = rail.result('get_timedata_batch')['listData']['rows']
    if batch_result and len([x['cells'][0]['uri'] for x in batch_result if x['cells']]) > 0:
        user_list = list(map(lambda x: {
            'text_value': x['cells'][0]['textValue'],
            'uri': x['cells'][0]['uri']
        }, batch_result))
    return user_list


def get_api_payload():

    data = rail.load_all_records(
        rail.result("render_final_extract_data"))

    if not data:
        raise Exception(
            "No Export data found yet went to posting to endpoint")

    output_payload = []

    for item in data:
        output_payload.append({
            'TransactionDate': item['TransactionDate'] if item['TransactionDate'] else '',
            'TimeEntryId': item['TimeEntryId'] if item['TimeEntryId'] else '',
            'iwfrInternalPersonPartyId': item['iwfr\\InternalPerson\\PartyId'] if item['iwfr\\InternalPerson\\PartyId'] else '',
            'iwfrPwCLegalEntityPartyId': item['iwfr\\PwCLegalEntity\\PartyId'] if item['iwfr\\PwCLegalEntity\\PartyId'] else '',
            'TimesheetStartDate': item['Timesheet Start Date'] if item['Timesheet Start Date'] else '',
            'TimesheetEndDate': item['Timesheet End Date'] if item['Timesheet End Date'] else '',
            'HoursQuantity': item['HoursQuantity'] if item['HoursQuantity'] else '',
            'Comments': item['Comments'] if item['Comments'] else '',
            'WorkLocation': item['WorkLocation'] if item['WorkLocation'] else '',
            'WorkCategory': item['WorkCategory'] if item['WorkCategory'] else '',
            'ResourceRole': item['ResourceRole'] if item['ResourceRole'] else '',
            'ChargeCode': item['ChargeCode'] if item['ChargeCode'] else '',
            'WorkItemType': item['WorkItemType'] if item['WorkItemType'] else '',
            'TransactionID': item['TransactionID'] if item['TransactionID'] else ''
        })
    payload = {
        "postings": output_payload
    }
    with new_artifact(mode="w", encoding='utf-8') as payload_artifact:
        payload_artifact.file.write(json.dumps(payload))
        return payload_artifact.name


def gather_all_api_logs():
    log = []
    for index in range(0, len(rail.result('get_api_endpoint_details_for_location'))):
        log.append(rail.result(f"get_api_log_{index+1}"))
    return ";".join(message for message in log if message and message.strip())


def get_api_log_message(dag_run, task_id, get_endpoint_details_task_id):
    if get_task_state(task_id) == "success":
        return f"INFO API Posting Successful for endpoint {rail.result(get_endpoint_details_task_id)['endpoint_url']}"
    if get_task_state(task_id) == "failed":
        #pylint: disable=line-too-long
        return f"""INFO API posting not successful for endpoint {rail.result(get_endpoint_details_task_id)['endpoint_url']}, File {dag_run.conf['export_file_name']}.json is added to backup folder"""
    return None
