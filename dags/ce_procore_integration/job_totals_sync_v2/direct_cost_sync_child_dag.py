from datetime import datetime, timedelta, timezone
import json
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType


def create_dag_instance(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.direct_cost_sync_child_dag_id,
        description='Computerease to Procore Direct Cost Sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_project_exists',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_project_exists = rail.IfOperator(
            task_id='check_project_exists',
            test=lambda dag_run: dag_run.conf.get('procore_project_id') is not None,
            yes_task='fetch_direct_cost',
            no_task='log_project_not_found'
        )

        fetch_direct_cost = rail.ProcoreApiOperator(
            task_id='fetch_direct_cost',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/direct_costs',
            method='GET',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf['procore_company_id'],
                'filters[origin_id]': dag_run.conf['job_data']['direct_cost_fingerprint']
            },
            data_handler=lambda response: response[0] if response else None
        )

        should_create_direct_cost = rail.IfOperator(
            task_id='should_create_direct_cost',
            test=lambda: rail.result("fetch_direct_cost") is None or float(rail.result('fetch_direct_cost')['grand_total']) == 0,
            yes_task='build_direct_cost_payload',
            no_task='catch_error'
        )

        def build_payload(dag_run):
            raw = dag_run.conf['job_data']['direct_cost_line_items']
            direct_cost_line_items = json.loads(raw) if isinstance(raw, str) else raw
            wbs_codes_lookup = dag_run.conf.get('wbs_codes_lookup') or {}

            updates = []
            missing_flat_codes = []

            for item in direct_cost_line_items:
                flat_code = item.get('flat_code', '')
                wbs_lookup_value = wbs_codes_lookup.get(flat_code)
                if wbs_lookup_value and not isinstance(wbs_lookup_value, str):
                    updates.append({
                        'wbs_code_id': wbs_lookup_value,
                        'amount': float(item['amount'])
                    })
                else:
                    missing_flat_codes.append(flat_code)

            if missing_flat_codes:
                flat_codes = ' , '.join([flat_code for flat_code in missing_flat_codes])
                raise ValueError(f"WBS code creation failed for {flat_codes}")

            return { 'updates': updates }

        build_direct_cost_payload = rail.PythonOperator(
            task_id='build_direct_cost_payload',
            python_callable=build_payload
        )

        def extract_direct_cost_id(response):
            if response.get('errors'):
                raise ValueError(f"Could not sync direct cost: {response['errors']}")
            return response['entities'][0]['id']

        sync_direct_cost = rail.ProcoreApiOperator(
            task_id='sync_direct_cost',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/direct_costs/sync',
            method='PATCH',
            data=lambda dag_run: {
                'updates': [{
                    'status': config.direct_cost_status,
                    'direct_cost_type': config.direct_cost_type,
                    'origin_id': dag_run.conf['job_data']['direct_cost_fingerprint'],
                    'direct_cost_date': datetime.now(timezone.utc).strftime('%Y-%m-%d')
                }]
            },
            data_handler=extract_direct_cost_id
        )


        def handle_error(response):
            if response.get('errors'):
                raise ValueError(f"Could not sync direct cost line items: {response['errors']}")
            return response
        sync_direct_cost_line_items = rail.ProcoreApiOperator(
            task_id='sync_direct_cost_line_items',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/direct_costs/line_items/sync',
            method='PATCH',
            data=lambda: {
                'updates': [
                    {**update, 'direct_cost_id': rail.result('sync_direct_cost')}
                    for update in rail.result('build_direct_cost_payload')['updates']
                ]
            },
            data_handler=handle_error
        )

        log_project_not_found = rail.WriteLogOperator(
            task_id='log_project_not_found',
            message='Project not found in Procore, skipping direct cost sync',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf['job_data'].get('job_code', ''),
                'error_message': f'Direct cost not synced for job : {dag_run.conf["job_data"].get("job_code", "")}, since project doesn\'t exist in Procore',
                'sync_type': SyncType.DIRECT_COST,
                'reset_retry_count': dag_run.conf['job_data'].get('reset_retry_count', False)
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf['job_data'].get('job_code', ''),
                'error_message': f"Direct cost not synced for job - {dag_run.conf['job_data'].get('job_code', '')} due to error: {{{{ get_error_message() }}}}",
                'sync_type': SyncType.DIRECT_COST,
                'reset_retry_count': dag_run.conf['job_data'].get('reset_retry_count', False)
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error >> log_to_sumo
        batch_task >> check_project_exists

        check_project_exists >> rail.Label('No') >> log_project_not_found >> catch_error
        check_project_exists >> rail.Label('Yes') >> fetch_direct_cost >> should_create_direct_cost

        should_create_direct_cost >> rail.Label('No') >> catch_error
        should_create_direct_cost >> rail.Label('Yes') >> build_direct_cost_payload >> sync_direct_cost >> sync_direct_cost_line_items >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
