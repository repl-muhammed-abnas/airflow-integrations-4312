from datetime import timedelta
from pendulum import datetime
import rail
from pwcglobal.project_import_api_v1.custom_method import get_unique_sender_ids_by_companykey
from pwcglobal.project_import_api_v1.python_callable_method import get_dagruns_to_process

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api_v1/config.py


# pylint:disable = too-many-statements
def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_import_master_log_scheduled_{config.instance}_v1',
        description=f'Projectimport_dynamicwait_Loggeneration_Project-Scheduled {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_runs=config.master_scheduled_log_generation_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1),
    ) as dag:

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.lookup_log_timestamp_var,
                     config.lookup_log_timestamp_hours,
                     f'pwc_project_import_child_log_pregeneration_{config.instance}_v1']
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_project_logs',
            no_task='delete_this_dagrun'
        )

        get_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_project_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='format_logs',
            flatten=True
        )

        compose_project_logs = rail.CreateCollectionOperator(
            task_id='compose_project_logs',
            source=lambda: rail.result('get_project_logs')
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('compose_project_logs', 'length') > 0 }}",
            yes_task='process_logs_by_sender',
            no_task='delete_this_dagrun'
        )

        process_logs_by_sender = rail.TriggerDagRunForEachItemOperator(
            task_id='process_logs_by_sender',
            retries=0,
            items=get_unique_sender_ids_by_companykey,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwc_project_import_child_log_{config.instance}_v1',
            conf={
                'sender_id': "{{ item }}",
                'project_final_logs': "{{ result('compose_project_logs') }}"
            }
        )

        wait_for_process_logs_by_sender = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_logs_by_sender',
            dag_runs='{{ result("process_logs_by_sender") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_project_logs >> compose_project_logs >> has_any_data

        has_any_data >> rail.Label(
            "Yes") >> process_logs_by_sender >> wait_for_process_logs_by_sender

        has_any_data >> rail.Label(
            "No") >> delete_this_dagrun

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        return dag


rail.for_each_instance(create_log_airflow_dag)
