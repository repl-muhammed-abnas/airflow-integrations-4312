from datetime import datetime, timedelta
import json
from airflow.models import DagRun, Variable
import rail


null = None


object_types = {
    'timesheet_object_type': 'urn:replicon:object-type:timesheet',
    'user_object_type': 'urn:replicon:object-type:user'
}


def get_dag_runs(dag_id, state=None):

    dag_runs = []
    state = state.lower() if state else None
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


def get_export_period(dag_ids, state):
    location_dag_runs = []
    for dag_id in dag_ids:
        dag_run_state = get_dag_runs(dag_id, state)
        location_dag_runs.extend(dag_run_state)
    export_period = location_dag_runs[-1]['conf'].get(
        'export_period') if location_dag_runs else None
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
