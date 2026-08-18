from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.subcontract_line_items_deletion_child_dag_id,
        description='Computerease to Procore Subcontract Line Item Deletion Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(minutes=10),
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='delete_line_item',
            end_task='catch_error',
            execution_timeout=timedelta(minutes=10)
        )

        delete_line_item = rail.ProcoreApiOperator(
            task_id='delete_line_item',
            endpoint=lambda dag_run: f'/work_order_contracts/{dag_run.conf["subcontract_id"]}/line_items/{dag_run.conf["line_item_id"]}',
            method='DELETE',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'subcontract_code': dag_run.conf.get('subcontract_code', ''),
                'vendor_code': dag_run.conf.get('vendor_code', ''),
                'job_code': dag_run.conf.get('job_code', ''),
                'line_item_id': dag_run.conf.get('line_item_id', ''),
                'error_message': "Line item deletion failed - {{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> catch_error
        batch_task >> delete_line_item >> catch_error
        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
