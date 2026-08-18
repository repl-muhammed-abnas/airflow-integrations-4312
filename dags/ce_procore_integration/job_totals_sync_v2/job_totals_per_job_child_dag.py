from datetime import timedelta
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_totals_per_job_child_dag_id,
        description='Computerease to Procore Job Totals Sync per-job orchestrator',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
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
            yes_task='fetch_existing_wbs_codes',
            no_task='check_has_budget'
        )

        fetch_existing_wbs_codes = rail.ProcoreApiOperator(
            task_id='fetch_existing_wbs_codes',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/work_breakdown_structure/wbs_codes',
            method='GET',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf['procore_company_id']
            },
            data_handler=lambda wbs_codes: {
                normalize_ce_identifier(wbs_code.get('flat_code')): wbs_code.get('id')
                for wbs_code in wbs_codes
                if wbs_code.get('flat_code') and wbs_code.get('id')
            }
        )

        def identify_required_wbs_codes(dag_run):
            existing = rail.result('fetch_existing_wbs_codes') or {}
            needed = []
            seen = set()

            def add_if_missing(item):
                flat_code = item.get('flat_code', '')
                if not flat_code or flat_code in seen or flat_code in existing:
                    return
                seen.add(flat_code)
                needed.append({
                    'flat_code': flat_code,
                    'phase_code': item.get('phase_code', ''),
                    'category_code': item.get('category_code', ''),
                    'cost_type': item.get('costtype_reference', '')
                })

            for sync_key in ('budget_data', 'contract_data', 'direct_cost_data'):
                sync_data = dag_run.conf.get(sync_key) or {}
                for item in sync_data.get('line_items', []):
                    add_if_missing(item)

            return needed

        identify_missing_wbs_codes = rail.PythonOperator(
            task_id='identify_missing_wbs_codes',
            python_callable=identify_required_wbs_codes
        )

        if_wbs_codes_missing = rail.IfOperator(
            task_id='if_wbs_codes_missing',
            test='{{ result("identify_missing_wbs_codes") | length > 0 }}',
            yes_task='trigger_wbs_code_creation',
            no_task='compile_wbs_lookup'
        )

        trigger_wbs_code_creation = rail.TriggerDagRunOperator(
            task_id='trigger_wbs_code_creation',
            trigger_dag_id=config.wbs_code_creator_dag_id,
            conf=lambda dag_run: {
                'project_id': dag_run.conf['procore_project_id'],
                'wbs_codes_to_create': rail.result('identify_missing_wbs_codes')
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

        def build_final_wbs_lookup():
            lookup = (rail.result('fetch_existing_wbs_codes') or {}).copy()
            try:
                created_wbs_results = rail.result('gather_wbs_creation_results')
                if created_wbs_results:
                    for result in created_wbs_results:
                        if result and isinstance(result, dict):
                            lookup.update(result)
            except Exception:  # pylint: disable=broad-except
                pass
            return lookup

        compile_wbs_lookup = rail.PythonOperator(
            task_id='compile_wbs_lookup',
            python_callable=build_final_wbs_lookup
        )

        check_has_budget = rail.IfOperator(
            task_id='check_has_budget',
            test=lambda dag_run: bool(dag_run.conf.get('budget_data')),
            yes_task='trigger_budget_sync',
            no_task='check_has_contract'
        )

        trigger_budget_sync = rail.TriggerDagRunOperator(
            task_id='trigger_budget_sync',
            trigger_dag_id=config.budget_sync_child_dag_id,
            conf=lambda dag_run: {
                'job_data': {
                    'job_code': dag_run.conf['job_code'],
                    'budget_line_items': dag_run.conf['budget_data']['line_items'],
                    'reset_retry_count': dag_run.conf['budget_data'].get('reset_retry_count', False)
                },
                'procore_company_id': dag_run.conf['procore_company_id'],
                'procore_project_id': dag_run.conf['procore_project_id'],
                'cost_code_segment_id': dag_run.conf['cost_code_segment_id'],
                'cost_type_segment_id': dag_run.conf['cost_type_segment_id'],
                'cost_type_segment_items': dag_run.conf['cost_type_segment_items'],
                'wbs_codes_lookup': rail.result('compile_wbs_lookup') or {}
            }
        )

        check_has_contract = rail.IfOperator(
            task_id='check_has_contract',
            test=lambda dag_run: bool(dag_run.conf.get('contract_data')),
            yes_task='trigger_contract_sync',
            no_task='check_has_direct_cost'
        )

        trigger_contract_sync = rail.TriggerDagRunOperator(
            task_id='trigger_contract_sync',
            trigger_dag_id=config.contract_line_items_sync_child_dag_id,
            conf=lambda dag_run: {
                'job_code': dag_run.conf['job_code'],
                'contract_line_items': dag_run.conf['contract_data']['line_items'],
                'procore_company_id': dag_run.conf['procore_company_id'],
                'procore_project_id': dag_run.conf['procore_project_id'],
                'reset_retry_count': dag_run.conf['contract_data'].get('reset_retry_count', False),
                'wbs_codes_lookup': rail.result('compile_wbs_lookup') or {}
            }
        )

        check_has_direct_cost = rail.IfOperator(
            task_id='check_has_direct_cost',
            test=lambda dag_run: bool(dag_run.conf.get('direct_cost_data')),
            yes_task='trigger_direct_cost_sync',
            no_task='collect_dag_runs'
        )

        trigger_direct_cost_sync = rail.TriggerDagRunOperator(
            task_id='trigger_direct_cost_sync',
            trigger_dag_id=config.direct_cost_sync_child_dag_id,
            conf=lambda dag_run: {
                'job_data': {
                    'job_code': dag_run.conf['job_code'],
                    'direct_cost_line_items': dag_run.conf['direct_cost_data']['line_items'],
                    'direct_cost_fingerprint': dag_run.conf['direct_cost_data'].get('fingerprint', ''),
                    'reset_retry_count': dag_run.conf['direct_cost_data'].get('reset_retry_count', False)
                },
                'procore_company_id': dag_run.conf['procore_company_id'],
                'procore_project_id': dag_run.conf['procore_project_id'],
                'cost_code_segment_id': dag_run.conf['cost_code_segment_id'],
                'cost_type_segment_items': dag_run.conf['cost_type_segment_items'],
                'wbs_codes_lookup': rail.result('compile_wbs_lookup') or {}
            }
        )

        def collect_triggered_dag_runs():
            runs = []
            for task_id in ('trigger_budget_sync', 'trigger_contract_sync', 'trigger_direct_cost_sync'):
                result = rail.result(task_id)
                if result is None:
                    continue
                if isinstance(result, list):
                    runs.extend(result)
                else:
                    runs.append(result)
            return runs

        collect_dag_runs = rail.PythonOperator(
            task_id='collect_dag_runs',
            python_callable=collect_triggered_dag_runs,
        )

        wait_for_dag_runs = rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_runs',
            dag_runs='{{ result("collect_dag_runs") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def build_wrapper_error_items(dag_run):
            job_code = dag_run.conf['job_code']
            items = []
            message = rail.render_template("{{get_error_message()}}")
            if dag_run.conf.get('budget_data'):
                items.append({
                    'entity_code': job_code,
                    'error_message': f"Budget sync did not complete for job {job_code} due to {message}",
                    'sync_type': SyncType.BUDGET,
                    'reset_retry_count': dag_run.conf['budget_data'].get('reset_retry_count', False)
                })
            if dag_run.conf.get('contract_data'):
                items.append({
                    'entity_code': job_code,
                    'error_message': f"Contract sync did not complete for job {job_code} due to {message}",
                    'sync_type': SyncType.CONTRACT,
                    'reset_retry_count': dag_run.conf['contract_data'].get('reset_retry_count', False)
                })
            if dag_run.conf.get('direct_cost_data'):
                items.append({
                    'entity_code': job_code,
                    'error_message': f"Direct cost sync did not complete for job {job_code} due to {message}",
                    'sync_type': SyncType.DIRECT_COST,
                    'reset_retry_count': dag_run.conf['direct_cost_data'].get('reset_retry_count', False)
                })
            return items

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            items=build_wrapper_error_items,
            properties=lambda item: item
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_project_exists

        check_project_exists >> rail.Label(
            'Yes') >> fetch_existing_wbs_codes >> identify_missing_wbs_codes >> if_wbs_codes_missing
        check_project_exists >> rail.Label('No') >> check_has_budget

        if_wbs_codes_missing >> rail.Label(
            'Yes') >> trigger_wbs_code_creation >> wait_for_wbs_code_creation >> gather_wbs_creation_results >> compile_wbs_lookup
        if_wbs_codes_missing >> rail.Label('No') >> compile_wbs_lookup

        compile_wbs_lookup >> check_has_budget

        check_has_budget >> rail.Label(
            'Yes') >> trigger_budget_sync >> check_has_contract
        check_has_budget >> rail.Label('No') >> check_has_contract

        check_has_contract >> rail.Label(
            'Yes') >> trigger_contract_sync >> check_has_direct_cost
        check_has_contract >> rail.Label('No') >> check_has_direct_cost

        check_has_direct_cost >> rail.Label(
            'Yes') >> trigger_direct_cost_sync >> collect_dag_runs
        check_has_direct_cost >> rail.Label('No') >> collect_dag_runs

        collect_dag_runs >> wait_for_dag_runs >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
