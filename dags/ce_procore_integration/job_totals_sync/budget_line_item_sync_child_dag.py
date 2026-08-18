from datetime import timedelta
import rail
from ce_procore_integration.job_totals_sync.utils.constants import SyncType


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.budget_line_item_sync_child_dag_id,
        description='Computerease to Procore Budget Line Item Sync Child DAG',
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
            start_task='create_wbs_code',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def build_wbs_code_payload(dag_run):
            budget_line_item = dag_run.conf['budget_line_item']
            return budget_line_item['wbs_payload']

        create_wbs_code = rail.ProcoreApiOperator(
            task_id='create_wbs_code',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/work_breakdown_structure/wbs_codes',
            method='POST',
            data=build_wbs_code_payload,
            data_handler=lambda response: response.get('id')
        )

        def build_budget_line_item_payload(dag_run):
            budget_line_item = dag_run.conf['budget_line_item']
            wbs_code_id = rail.result('create_wbs_code')

            return {
                "project_id": dag_run.conf['procore_project_id'],
                "budget_line_items": [{
                    "wbs_code_id": wbs_code_id,
                    "original_budget_amount": float(budget_line_item['budget_amount']),
                    "uom": "hours",
                    "quantity": float(budget_line_item['budget_hours']),
                    "calculation_strategy": config.calculation_strategy
                }]
            }

        create_budget_line_item = rail.ProcoreApiOperator(
            task_id='create_budget_line_item',
            endpoint='/budget_line_items/sync',
            method='POST',
            data=build_budget_line_item_payload
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_code': dag_run.conf['job_code'],
                # pylint: disable=line-too-long
                'error_message': f"Budget amount not added/updated for {dag_run.conf['budget_line_item']['phase_code']}.{dag_run.conf['budget_line_item']['category_code']}.{dag_run.conf['budget_line_item'].get('costtype_reference', '')} due to error: {rail.render_template('{{ get_error_message() }}')}",
                'sync_type': SyncType.BUDGET,
                'reset_retry_count': dag_run.conf.get('reset_retry_count', False)
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> create_wbs_code >> create_budget_line_item >> catch_error
        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
