from datetime import timedelta
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType


def create_dag_instance(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.job_totals_child_dag_id,
        description='Computerease to Procore Job Totals Sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
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
            yes_task='fetch_cost_code_segment_items',
            no_task='log_project_not_found'
        )

        fetch_cost_code_segment_items = rail.ProcoreApiOperator(
            task_id='fetch_cost_code_segment_items',
            # pylint: disable=line-too-long
            endpoint=lambda dag_run: f'/projects/{rail.result("fetch_procore_project")}/work_breakdown_structure/segments/{dag_run.conf["cost_code_segment_id"]}/segment_items',
            method='GET',
            data_handler=lambda items: {
                item.get('path_code', ''): item.get('id') for item in items if item.get('path_code')
            }
        )

        def validate_and_prepare_budget_items(dag_run):
            job_data = dag_run.conf['job_data']
            budget_line_items = job_data['budget_line_items']

            cost_code_items = rail.result('fetch_cost_code_segment_items')
            cost_type_items = dag_run.conf['cost_type_segment_items']
            cost_code_segment_id = dag_run.conf['cost_code_segment_id']
            cost_type_segment_id = dag_run.conf['cost_type_segment_id']

            valid_items = []
            errors = []

            for line_item in budget_line_items:
                phase_code = line_item['phase_code']
                category_code = line_item['category_code']
                costtype_code = line_item['costtype_code']

                wbs_segment_items = []

                # Get Phase ID (if exists)
                if phase_code:
                    phase_item_id = cost_code_items.get(phase_code)
                    if not phase_item_id:
                        costtype_reference = line_item.get(
                            'costtype_reference', '')
                        full_path = f"{phase_code}.{category_code}.{costtype_reference}"
                        errors.append(
                            f"Budget amount not added/updated for {full_path} due to Phase {phase_code} not found in Procore")
                        continue
                    wbs_segment_items.append({
                        "segment_id": cost_code_segment_id,
                        "segment_item_id": phase_item_id
                    })

                # Get Category ID
                category_path = f"{phase_code}-{category_code}" if phase_code else category_code
                category_item_id = cost_code_items.get(category_path)
                if not category_item_id:
                    costtype_reference = line_item.get(
                        'costtype_reference', '')
                    full_path = f"{phase_code}.{category_code}.{costtype_reference}"
                    errors.append(
                        f"Budget amount not added/updated for {full_path} due to Category {category_code} not found in Procore")
                    continue
                wbs_segment_items.append({
                    "segment_id": cost_code_segment_id,
                    "segment_item_id": category_item_id
                })

                # Get Cost Type ID using the mapped reference
                costtype_reference = line_item.get('costtype_reference', '')
                cost_type_item_id = cost_type_items.get(costtype_reference)
                if not cost_type_item_id:
                    full_path = f"{phase_code}.{category_code}.{costtype_reference}"
                    errors.append(
                        f"Budget amount not added/updated for {full_path} due to Cost type {costtype_code} not found in Procore")
                    continue
                wbs_segment_items.append({
                    "segment_id": cost_type_segment_id,
                    "segment_item_id": cost_type_item_id
                })

                # Add to valid items with WBS payload ready
                valid_items.append({
                    **line_item,
                    'wbs_payload': {
                        "segment_items": wbs_segment_items
                    }
                })

            return {
                'valid_items': valid_items,
                'errors': errors
            }

        validate_budget_prerequisites = rail.PythonOperator(
            task_id='validate_budget_prerequisites',
            python_callable=validate_and_prepare_budget_items
        )

        def log_validation_errors(dag_run):
            validation_result = rail.result('validate_budget_prerequisites')
            errors = validation_result.get('errors', [])
            job_code = dag_run.conf['job_data']['job_code']

            return [{
                'entity_code': job_code,
                'error_message': error,
                'sync_type': SyncType.BUDGET,
                'reset_retry_count': dag_run.conf['job_data'].get('reset_retry_count', False)
            } for error in errors]

        log_errors_for_unprocessable_budget_line_items = rail.WriteLogOperator(
            task_id='log_errors_for_unprocessable_budget_line_items',
            message='Budget line item validation errors',
            severity='Error/Exception',
            items=log_validation_errors,
            properties=lambda item: item
        )

        check_has_valid_items = rail.IfOperator(
            task_id='check_has_valid_items',
            test=lambda: len(rail.result("validate_budget_prerequisites")[
                             "valid_items"]) > 0,
            yes_task='trigger_budget_line_item_sync',
            no_task='catch_error'
        )

        trigger_budget_line_item_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_budget_line_item_sync',
            items=lambda: rail.result('validate_budget_prerequisites')[
                'valid_items'],
            trigger_dag_id=config.budget_line_item_sync_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'budget_line_item': item,
                'job_code': dag_run.conf['job_data']['job_code'],
                'procore_project_id': rail.result('fetch_procore_project'),
                'reset_retry_count': dag_run.conf['job_data'].get('reset_retry_count', False)
            }
        )

        wait_for_budget_line_item_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_budget_line_item_sync_completion',
            dag_runs='{{ result("trigger_budget_line_item_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
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
        batch_task >> fetch_procore_project >> check_project_exists
        check_project_exists >> rail.Label(
            'Yes') >> fetch_cost_code_segment_items >> validate_budget_prerequisites >> log_errors_for_unprocessable_budget_line_items >> check_has_valid_items
        check_project_exists >> rail.Label(
            'No') >> log_project_not_found >> catch_error

        check_has_valid_items >> rail.Label(
            'Yes') >> trigger_budget_line_item_sync >> wait_for_budget_line_item_sync_completion >> catch_error
        check_has_valid_items >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
