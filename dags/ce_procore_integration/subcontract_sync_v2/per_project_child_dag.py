from datetime import timedelta
import rail
from ce_procore_integration.subcontract_sync_v2.utils.util import build_flat_code
from ce_procore_integration.util_dags.utils import normalize_ce_identifier


def create_dag_instance(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.subcontract_per_project_child_dag_id,
        description='Computerease to Procore Subcontract Sync per-project wrapper',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id,
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
            yes_task='fetch_wbs_codes',
            no_task='log_project_not_found'
        )

        log_project_not_found = rail.WriteLogOperator(
            task_id='log_project_not_found',
            message='Project not found in Procore',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'subcontract_code': sub_entry.get('subcontract_data', {}).get('code', ''),
                    'vendor_code': sub_entry.get('subcontract_data', {}).get('vendor_code', ''),
                    'job_code': sub_entry.get('subcontract_data', {}).get('job_code', ''),
                    'error_message': f"Subcontract not synced because Project {sub_entry.get('subcontract_data', {}).get('job_code', '')} not found in Procore"
                }
                for sub_entry in dag_run.conf.get('subcontracts', [])
            ],
            properties=lambda item: item
        )

        fetch_wbs_codes = rail.ProcoreApiOperator(
            task_id='fetch_wbs_codes',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/work_breakdown_structure/wbs_codes',
            method='GET',
            data_handler=lambda wbs_codes: {
                normalize_ce_identifier(code.get('flat_code')): code.get('id')
                for code in (wbs_codes or [])
                if code.get('flat_code')
            }
        )
        
        def identify_required_wbs_codes(dag_run):
            existing = rail.result('fetch_wbs_codes') or {}
            subcontracts = dag_run.conf.get('subcontracts', [])
            ce_cost_type_map = dag_run.conf.get('ce_cost_type_map', {})

            needed = []
            seen = set()

            for sub_entry in subcontracts:
                subcontract = sub_entry.get('subcontract_data', {})
                for item in subcontract.get('subcontract_item', []):
                    phase_code = item.get('phase_code', '')
                    category_code = item.get('category_code', '')
                    cost_type_id = str(item.get('costtype', '')).strip()
                    cost_type = ce_cost_type_map.get(cost_type_id, '')

                    flat_code = build_flat_code(phase_code, category_code, cost_type)
                    if not flat_code or flat_code in seen or flat_code in existing:
                        continue
                    seen.add(flat_code)
                    needed.append({
                        'flat_code': flat_code,
                        'phase_code': phase_code.strip(),
                        'category_code': category_code.strip(),
                        'cost_type': cost_type
                    })

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
            lookup = (rail.result('fetch_wbs_codes') or {}).copy()
            try:
                created = rail.result('gather_wbs_creation_results')
                if created:
                    for result in created:
                        if result and isinstance(result, dict):
                            lookup.update(result)
            except Exception:  # pylint: disable=broad-except
                pass
            return lookup

        compile_wbs_lookup = rail.PythonOperator(
            task_id='compile_wbs_lookup',
            python_callable=build_final_wbs_lookup
        )

        def identify_vendors_to_assign_func(dag_run):
            procore_project_id = dag_run.conf.get('procore_project_id')
            subcontracts = dag_run.conf.get('subcontracts', [])

            vendors = []
            seen = set()
            for sub_entry in subcontracts:
                vendor = sub_entry.get('procore_vendor')
                if not vendor:
                    continue
                vendor_id = vendor.get('id')
                if not vendor_id or vendor_id in seen:
                    continue
                if procore_project_id in vendor.get('project_ids', []):
                    continue
                seen.add(vendor_id)
                vendors.append({'vendor_id': vendor_id})
            return vendors

        identify_vendors_to_assign = rail.PythonOperator(
            task_id='identify_vendors_to_assign',
            python_callable=identify_vendors_to_assign_func
        )

        if_vendors_to_assign = rail.IfOperator(
            task_id='if_vendors_to_assign',
            test='{{ result("identify_vendors_to_assign") | length > 0 }}',
            yes_task='trigger_vendor_assignments',
            no_task='trigger_subcontract_sync_child_dag'
        )

        trigger_vendor_assignments = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_vendor_assignments',
            items=lambda: rail.result('identify_vendors_to_assign'),
            trigger_dag_id=config.subcontract_vendor_assignment_child_dag_id,
            execution_timeout=timedelta(minutes=30),
            conf=lambda item, dag_run: {
                'vendor_id': item['vendor_id'],
                'project_id': dag_run.conf['procore_project_id'],
                'procore_company_id': dag_run.conf['procore_company_id'],
                'job_code': dag_run.conf.get('job_code', '')
            }
        )

        wait_for_vendor_assignments = rail.WaitForDagRunsSensor(
            task_id='wait_for_vendor_assignments',
            dag_runs='{{ result("trigger_vendor_assignments") }}',
            execution_timeout=timedelta(minutes=30)
        )

        def make_child_conf(item, dag_run):
            return {
                'subcontract_data': item['subcontract_data'],
                'procore_company_id': dag_run.conf['procore_company_id'],
                'ce_cost_type_map': dag_run.conf['ce_cost_type_map'],
                'procore_project_id': dag_run.conf.get('procore_project_id'),
                'procore_vendor': item.get('procore_vendor'),
                'wbs_codes_lookup': rail.result('compile_wbs_lookup') or {}
            }

        trigger_subcontract_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_subcontract_sync_child_dag',
            items=lambda dag_run: dag_run.conf.get('subcontracts', []),
            trigger_dag_id=config.subcontract_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=make_child_conf
        )

        wait_for_subcontract_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_subcontract_sync_completion',
            dag_runs='{{ result("trigger_subcontract_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def build_catch_error_properties(dag_run):
            job_code = dag_run.conf.get('job_code', '')
            return {
                'subcontract_code': '',
                'vendor_code': '',
                'job_code': job_code,
                'error_message': f"One or more Subcontracts not synced for the Project {job_code} due to {rail.render_template('{{ get_error_message() }}')}"
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=build_catch_error_properties
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Dependency chain
        batch_task >> catch_error
        batch_task >> check_project_exists

        # No project: log not-found for all subcontracts in this group; skip downstream sync work
        check_project_exists >> rail.Label('No') >> log_project_not_found >> catch_error

        # Project exists: fetch existing WBS, identify missing, optionally create
        check_project_exists >> rail.Label(
            'Yes') >> fetch_wbs_codes >> identify_missing_wbs_codes >> if_wbs_codes_missing

        if_wbs_codes_missing >> rail.Label(
            'Yes') >> trigger_wbs_code_creation >> wait_for_wbs_code_creation >> gather_wbs_creation_results >> compile_wbs_lookup
        if_wbs_codes_missing >> rail.Label('No') >> compile_wbs_lookup

        # Identify and assign vendors, then trigger children
        compile_wbs_lookup >> identify_vendors_to_assign >> if_vendors_to_assign

        if_vendors_to_assign >> rail.Label(
            'Yes') >> trigger_vendor_assignments >> wait_for_vendor_assignments >> trigger_subcontract_sync_child_dag
        if_vendors_to_assign >> rail.Label('No') >> trigger_subcontract_sync_child_dag

        trigger_subcontract_sync_child_dag >> wait_for_subcontract_sync_completion >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
