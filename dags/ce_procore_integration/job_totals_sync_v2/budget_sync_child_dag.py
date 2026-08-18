from datetime import timedelta
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType


def create_dag_instance(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.budget_sync_child_dag_id,
        description='Computerease to Procore Budget Items Sync Child DAG',
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
            yes_task='build_bulk_budget_payload',
            no_task='log_project_not_found'
        )

        def build_bulk_payload(dag_run):
            job_data = dag_run.conf['job_data']
            budget_line_items = job_data['budget_line_items']
            wbs_codes_lookup = dag_run.conf.get('wbs_codes_lookup') or {}

            updates = []
            errors = []

            for line_item in budget_line_items:
                flat_code = line_item.get('flat_code', '')
                wbs_lookup_value = wbs_codes_lookup.get(flat_code)

                if wbs_lookup_value and not isinstance(wbs_lookup_value, str):
                    updates.append({
                        'wbs_code_id': wbs_lookup_value,
                        'original_budget_amount': float(line_item['budget_amount']),
                        'uom': config.budget_uom,
                        'quantity': float(line_item['budget_hours']),
                        'calculation_strategy': config.calculation_strategy
                    })
                else:
                    if wbs_lookup_value and isinstance(wbs_lookup_value, str):
                        error_msg = wbs_lookup_value
                    else:
                        error_msg = f"WBS code not found for {flat_code}"
                    errors.append({
                        'flat_code': flat_code,
                        'phase_code': line_item.get('phase_code', ''),
                        'category_code': line_item.get('category_code', ''),
                        'costtype_reference': line_item.get('costtype_reference', ''),
                        'error_message': error_msg
                    })

            return {
                'updates': updates,
                'errors': errors
            }

        build_bulk_budget_payload = rail.PythonOperator(
            task_id='build_bulk_budget_payload',
            python_callable=build_bulk_payload
        )

        log_errors_for_unprocessable_budget_line_items = rail.WriteLogOperator(
            task_id='log_errors_for_unprocessable_budget_line_items',
            message='Budget line item validation errors',
            severity='Error/Exception',
            items=lambda dag_run: [
                {
                    'entity_code': dag_run.conf['job_data']['job_code'],
                    'error_message': f"Budget amount not added/updated for {err['phase_code']}.{err['category_code']}.{err['costtype_reference']} due to {err['error_message']}",
                    'sync_type': SyncType.BUDGET,
                    'reset_retry_count': dag_run.conf['job_data'].get('reset_retry_count', False)
                }
                for err in rail.result('build_bulk_budget_payload').get('errors', [])
            ],
            properties=lambda item: item
        )

        check_has_valid_items = rail.IfOperator(
            task_id='check_has_valid_items',
            test=lambda: len(rail.result('build_bulk_budget_payload')['updates']) > 0,
            yes_task='bulk_sync_budget_line_items',
            no_task='catch_error'
        )

        bulk_sync_budget_line_items = rail.ProcoreApiOperator(
            task_id='bulk_sync_budget_line_items',
            endpoint='/budget_line_items/sync',
            method='POST',
            data=lambda dag_run: {
                'project_id': dag_run.conf['procore_project_id'],
                'budget_line_items': rail.result('build_bulk_budget_payload')['updates']
            }
        )

        log_project_not_found = rail.WriteLogOperator(
            task_id='log_project_not_found',
            message='Project not found in Procore, skipping budget sync',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf['job_data'].get('job_code', ''),
                'error_message': f'Budget not synced for job : {dag_run.conf["job_data"].get("job_code", "")}, since project doesn\'t exist in Procore',
                'sync_type': SyncType.BUDGET,
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
                'error_message': f"Budget not synced for job - {dag_run.conf['job_data'].get('job_code', '')} due to error: {{{{ get_error_message() }}}}",
                'sync_type': SyncType.BUDGET,
                'reset_retry_count': dag_run.conf['job_data'].get('reset_retry_count', False)
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_project_exists

        check_project_exists >> rail.Label(
            'No') >> log_project_not_found >> catch_error
        check_project_exists >> rail.Label(
            'Yes') >> build_bulk_budget_payload >> log_errors_for_unprocessable_budget_line_items >> check_has_valid_items

        check_has_valid_items >> rail.Label(
            'Yes') >> bulk_sync_budget_line_items >> catch_error
        check_has_valid_items >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
