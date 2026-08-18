from datetime import datetime, timedelta, timezone
from airflow.models import Variable, DagRun
import json
import rail

def check_client_data(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['clientcode'] == json.loads(rail.result('get_details_of_company_from_hubspot'))['id'], list(map(lambda item: {
        "clienturi": item['cells'][1]['uri'],
        "clientcode": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
    }, response['rows']))))

def get_pipeline_dealstage_name():
    pipeline_details = json.loads(rail.result('get_pipeline_details'))['results']
    pipeline_mapping = dict(map(dict.popitem, list(map(lambda x:{x['id']:x['label']}, pipeline_details))))
    stage_mapping_list = []
    list(map(lambda x:stage_mapping_list.extend(x['stages']), pipeline_details))
    stage_mapping = dict(map(dict.popitem, list(map(lambda x:{x['id']:x['label']}, stage_mapping_list))))
    return {
        'pipeline': pipeline_mapping[json.loads(rail.result('get_details_of_deal'))['properties']['pipeline']],
        'dealstage': stage_mapping[json.loads(rail.result('get_details_of_deal'))['properties']['dealstage']],
    }

def get_required_id_from_company():
    contact_id = ''
    owner_id = ''
    solution_consultant_id = ''
    company_details = json.loads(rail.result('get_details_of_company_from_hubspot'))
    if ('associations' in company_details) and ('contacts' in company_details['associations']):
        contact_id = company_details['associations']['contacts']['results'][0]['id']
    if company_details['properties']['hubspot_owner_id']:
        owner_id = company_details['properties']['hubspot_owner_id']
    if company_details['properties']['solutions_consultant']:
        solution_consultant_id = company_details['properties']['solutions_consultant']
    return {
        'contact_id': contact_id,
        'owner_id': owner_id,
        'solution_consultant_id': solution_consultant_id,
    }

def get_status_and_details_for_update():
    message = "Success"
    details = "Project processed successfully. "

    has_exception_message = rail.result('log_projectmanager_not_presentordisabled') if rail.result(
        'log_projectmanager_not_presentordisabled') else rail.result('log_project_manager_permission_not_available') if rail.result(
            'log_project_manager_permission_not_available') else rail.result('log_clientmanager_not_presentordisabled') if rail.result(
                'log_clientmanager_not_presentordisabled') else rail.result('log_client_manager_permission_not_available') if rail.result(
                    'log_client_manager_permission_not_available') else rail.result('log_client_not_present') if rail.result(
                        'log_client_not_present') else rail.result('log_company_name_absent_in_hubspot') if rail.result(
                            'log_company_name_absent_in_hubspot') else ''
    if has_exception_message:
        message = "Exception"
        details = " Project processed partially " + has_exception_message
    return {
        "deal_name": json.loads(rail.result('get_details_of_deal'))['properties']['dealname'],
        "pipeline": rail.result('get_pipeline_and_dealstage_name')['pipeline'],
        "deal_satge": rail.result('get_pipeline_and_dealstage_name')['dealstage'],
        "status": message,
        'details': details
    }

def get_status_and_details_for_project_to_update():
    message = "Success"
    details = "Project processed successfully. "

    if not bool(rail.result('search_project_with_code')[0].get('projectDetails')):
        message = "Exception"
        details = "Project not found in Replicon for update. "

    has_exception_message = rail.result('log_projectmanager_not_presentordisabled') if rail.result(
        'log_projectmanager_not_presentordisabled') else rail.result('log_project_manager_permission_not_available') if rail.result(
            'log_project_manager_permission_not_available') else rail.result('log_clientmanager_not_presentordisabled') if rail.result(
                'log_clientmanager_not_presentordisabled') else rail.result('log_client_manager_permission_not_available') if rail.result(
                    'log_client_manager_permission_not_available') else ''
    if has_exception_message:
        message = "Exception"
        details = " Project processed partially " + has_exception_message

    return {
        "deal_name": json.loads(rail.result('get_details_of_deal'))['properties']['dealname'],
        "pipeline": rail.result('get_pipeline_and_dealstage_name')['pipeline'],
        "deal_satge": rail.result('get_pipeline_and_dealstage_name')['dealstage'],
        "status": message,
        'details': details
    }

def get_project_uris_to_be_archived(response):
    response = response.json()['d']['rows']
    return list(map(lambda x:x['cells'][0]['uri'], response))

def get_presale_project_uri_to_be_archived(response):
    project_data = json.loads(rail.result('get_details_of_deal'))
    response = response['rows']
    return list(map(lambda x:x['cells'][0]['uri'] , list(filter(lambda d:d['cells'][0]['textValue'] == str(
        'Pre-Sales - ' + project_data['properties']['dealname']), response))))

def get_existing_user_detail(response):
    response = response['rows']
    if response:
        return {
            'uri' : response[0]['cells'][0].get('uri'),
            'enabled': response[0]['cells'][1].get('textValue')
        }
    return None

def get_error_properties():
    return {
        "deal_name": json.loads(rail.result('get_details_of_deal'))['properties'].get('dealname'),
        "pipeline": rail.result('get_pipeline_and_dealstage_name').get('pipeline'),
        "deal_satge": rail.result('get_pipeline_and_dealstage_name').get('dealstage'),
        "status": 'Error',
        'details': '{{ get_error_message() }}'
    }

def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_ids):

    current_time = datetime.now(timezone.utc)
    lookup_timestamp_value = Variable.get(
        lookup_log_timestamp_var, default_var=None)

    query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
        current_time - timedelta(hours=lookup_log_timestamp_hours))

    dag_runs = []
    execution_dates = []
    for dag_id in dag_ids:
        for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
            execution_dates.append(run.execution_date)
            dag_runs.append(run.id)
    if execution_dates:
        max_execution_date = max(execution_dates)
        Variable.set(lookup_log_timestamp_var,
                     (max_execution_date + timedelta(seconds=1)).isoformat())
    return dag_runs

def get_unique_log_artifacts_callable():
    logs = rail.result("get_project_logs")
    invalid_project_logs = rail.result("get_invalid_project_logs")
    errored_logs = rail.result("get_errored_project_logs")
    project_logs = []
    for log in logs:
        project_logs.append(log)
    for e_log in errored_logs:
        project_logs.append(e_log)
    for i_log in invalid_project_logs:
        project_logs.append(i_log)
    return list(set(project_logs))

def format_logs_callable():
    final_log_records = []
    logs = get_unique_log_artifacts_callable()
    for log in logs:
        final_log_records.extend(rail.load_all_records(log))
    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x['properties']['status'].lower() == 'exception', final_log_records))))
    return rail.write_json_artifact(final_log_records)

def check_valid_deals_data():
    pipeline_dealstage_data = rail.result('get_pipeline_and_dealstage_name')
    if (pipeline_dealstage_data['pipeline'] == 'Sales' and pipeline_dealstage_data['dealstage'].lower() in ['3. solution & demo', 'closed won']):
        return True
    if (pipeline_dealstage_data['pipeline'] == 'Services' and pipeline_dealstage_data['dealstage'].lower() in ['closed won']):
        return True
    if (pipeline_dealstage_data['pipeline'] == 'Customer Success' and pipeline_dealstage_data['dealstage'].lower() in ['closed won', 'closed lost']):
        return True
    return False

def get_invalid_pipeline_dealstage_log():
    return {
        "deal_name": json.loads(rail.result('get_details_of_deal'))['properties'].get('dealname'),
        "pipeline": rail.result('get_pipeline_and_dealstage_name').get('pipeline'),
        "deal_satge": rail.result('get_pipeline_and_dealstage_name').get('dealstage'),
        "status": 'Exception',
        'details': 'Project is not processed due to invalid Pipeline or Dealstage'
    }
