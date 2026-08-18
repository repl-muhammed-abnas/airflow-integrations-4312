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
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_procore_project',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fetch_procore_project = rail.ProcoreApiOperator(
            task_id='fetch_procore_project',
            endpoint='/projects',
            method='GET',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf['procore_company_id'],
                'filters[origin_id]': f'CE_{dag_run.conf["job_data"]["job_code"]}'
            },
            data_handler=lambda projects: projects[0].get(
                'id') if projects else None
        )

        check_project_exists = rail.IfOperator(
            task_id='check_project_exists',
            test='{{ result("fetch_procore_project") is not none }}',
            yes_task='fetch_direct_cost',
            no_task='log_project_not_found'
        )

        fetch_direct_cost = rail.ProcoreApiOperator(
            task_id='fetch_direct_cost',
            endpoint=lambda: f'/projects/{rail.result("fetch_procore_project")}/direct_costs',
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
            yes_task='fetch_existing_wbs_codes',
            no_task='catch_error'
        )

        fetch_existing_wbs_codes = rail.ProcoreApiOperator(
            task_id='fetch_existing_wbs_codes',
            endpoint=lambda: f'/projects/{rail.result("fetch_procore_project")}/work_breakdown_structure/wbs_codes',
            method='GET',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf['procore_company_id']
            }
        )

        def segregate_wbs_codes(dag_run):
            existing_wbs_codes = rail.result('fetch_existing_wbs_codes')
            raw = dag_run.conf['job_data']['direct_cost_line_items']
            direct_cost_items = json.loads(raw) if isinstance(raw, str) else raw

            # Build lookup from raw list: {flat_code: id}
            existing_flat_codes_lookup = {
                wbs_code['flat_code']: wbs_code['id']
                for wbs_code in existing_wbs_codes
                if wbs_code.get('flat_code')
            }

            # Compute flat_code for each direct cost item and find missing ones
            missing_wbs_codes = []
            for item in direct_cost_items:
                phase_code = item.get('phase_code', '')
                category_code = item.get('category_code', '')
                costtype_code = item.get('costtype_reference', '')
                flat_code = (
                    f"{phase_code}-{category_code}.{costtype_code}"
                    if phase_code and category_code
                    else f"{phase_code or category_code}.{costtype_code}"
                )
                if flat_code and flat_code not in existing_flat_codes_lookup:
                    missing_wbs_codes.append({
                        **item,
                        'flat_code': flat_code,
                        'cost_type': costtype_code
                    })

            return {
                'existing_flat_codes_lookup': existing_flat_codes_lookup,
                'missing_wbs_codes': missing_wbs_codes
            }

        identify_missing_wbs_codes = rail.PythonOperator(
            task_id='identify_missing_wbs_codes',
            python_callable=segregate_wbs_codes
        )

        if_wbs_codes_missing = rail.IfOperator(
            task_id='if_wbs_codes_missing',
            test='{{ result("identify_missing_wbs_codes")["missing_wbs_codes"] | length > 0 }}',
            yes_task='trigger_wbs_code_creation',
            no_task='build_direct_cost_payload'
        )

        trigger_wbs_code_creation = rail.TriggerDagRunOperator(
            task_id='trigger_wbs_code_creation',
            trigger_dag_id=config.wbs_code_creator_dag_id,
            conf=lambda: {
                'project_id': rail.result('fetch_procore_project'),
                'wbs_codes_to_create': rail.result('identify_missing_wbs_codes')['missing_wbs_codes']
            }
        )

        wait_for_wbs_code_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_wbs_code_creation',
            dag_runs='{{ result("trigger_wbs_code_creation") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_wbs_creation_results = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_wbs_creation_results',
            dag_runs='{{ result("trigger_wbs_code_creation") }}',
            dagrun_task_id='compile_results'
        )

        def build_payload(dag_run):
            raw = dag_run.conf['job_data']['direct_cost_line_items']
            direct_cost_line_items = json.loads(raw) if isinstance(raw, str) else raw
            wbs_codes_lookup = rail.result('identify_missing_wbs_codes')['existing_flat_codes_lookup'].copy()

            # Merge newly created WBS codes into lookup
            try:
                created_wbs_results = rail.result('gather_wbs_creation_results')
                for result in created_wbs_results:
                    if result and isinstance(result, dict):
                        wbs_codes_lookup.update(result)
            except:
                pass

            updates = []
            missin_flat_codes = []

            for item in direct_cost_line_items:
                phase_code = item.get('phase_code', '')
                category_code = item.get('category_code', '')
                costtype_code = item.get('costtype_reference', '')
                flat_code = (
                    f"{phase_code}-{category_code}.{costtype_code}"
                    if phase_code and category_code
                    else f"{phase_code or category_code}.{costtype_code}"
                )

                wbs_lookup_value = wbs_codes_lookup.get(flat_code)
                if wbs_lookup_value and not isinstance(wbs_lookup_value, str):
                    updates.append({
                        'wbs_code_id': wbs_lookup_value,
                        'amount': float(item['amount'])
                    })
                else:
                    missin_flat_codes.append(flat_code)

            if missin_flat_codes:
                flat_codes = ' , '.join([flat_code for flat_code in missin_flat_codes])
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
            endpoint=lambda: f'/projects/{rail.result("fetch_procore_project")}/direct_costs/sync',
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
            endpoint=lambda: f'/projects/{rail.result("fetch_procore_project")}/direct_costs/line_items/sync',
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
        batch_task >> fetch_procore_project >> check_project_exists

        check_project_exists >> rail.Label('No') >> log_project_not_found >> catch_error
        check_project_exists >> rail.Label('Yes') >> fetch_direct_cost >> should_create_direct_cost
        
        should_create_direct_cost >> rail.Label('No') >> catch_error
        should_create_direct_cost >> rail.Label('Yes') >> fetch_existing_wbs_codes >> identify_missing_wbs_codes >> if_wbs_codes_missing

        if_wbs_codes_missing >> rail.Label('Yes') >> trigger_wbs_code_creation >> wait_for_wbs_code_creation >> gather_wbs_creation_results >> build_direct_cost_payload
        if_wbs_codes_missing >> rail.Label('No') >> build_direct_cost_payload

        build_direct_cost_payload >> sync_direct_cost >> sync_direct_cost_line_items >> catch_error

        return dag


rail.for_each_instance(create_dag_instance)
