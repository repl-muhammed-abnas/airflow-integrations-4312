from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.subcontract_vendor_assignment_child_dag_id,
        description='Computerease to Procore Subcontract Sync - Vendor Assignment Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(minutes=30),
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='add_vendor_to_project',
            end_task='catch_error',
            execution_timeout=timedelta(minutes=30)
        )

        add_vendor_to_project = rail.ProcoreApiOperator(
            task_id='add_vendor_to_project',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["project_id"]}/vendors/{dag_run.conf["vendor_id"]}/actions/add',
            method='POST',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf['procore_company_id']
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: None
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> add_vendor_to_project >> catch_error
        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
