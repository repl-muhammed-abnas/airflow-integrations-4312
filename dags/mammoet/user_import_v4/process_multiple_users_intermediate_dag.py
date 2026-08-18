from datetime import timedelta
from pendulum import datetime
import rail
from mammoet.user_import_v4.utils.custom_methods \
    import process_each_user_conf_for_multiple_user_records_processing, get_all_triggered_child_for_task_id


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_process_multiple_users_child_dag_id,
        description="Mammoet User Import Process Each User",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_multiple_users_dag_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")


        query_records_for_employee_id = rail.QueryCollectionOperator(
            task_id = "query_records_for_employee_id",
            query="SELECT * FROM query_multiple_records_for_processing qmrfp WHERE qmrfp.emp_records_index == :employee_record_index",
            query_params = {
                "employee_record_index" : "{{ dag_run.conf.emp_records_index }}"
            }
        )

        process_user = rail.trigger_parallel_dagrun(
            task_id="process_user",
            items="{{result('query_records_for_employee_id')}}",
            parallel_count=10,
            trigger_dag_id=config.user_import_process_users_child_dag_id,
            conf=lambda dag_run, item: process_each_user_conf_for_multiple_user_records_processing(
                dag_run,
                item,
                config
            ),
            execution_timeout=timedelta(days=14)
        )

        get_all_process_user_ids = rail.PythonOperator(
            task_id = "get_all_process_user_ids",
            python_callable=lambda : get_all_triggered_child_for_task_id(config, "process_user")
        )

        get_process_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="get_process_user_logs",
            dag_runs="{{result('get_all_process_user_ids')}}",
            dagrun_task_id="create_user_log"
        )

        create_user_log = rail.PythonOperator(
            task_id = "create_user_log",
            python_callable=lambda: rail.result("get_process_user_logs")
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule="one_failed",
            severity="Error",
            items="{{ result('query_records_for_employee_id') }}",
            message="{{get_error_message()}}",
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": "{{ dag_run.conf.emp_records_index }}",
                "status": "Error",
                "action": "pre-check",
                "details": "{{get_error_message()}}"
            }
        )

        query_records_for_employee_id >> process_user >> get_all_process_user_ids \
        >> get_process_user_logs >> create_user_log >> rail.Label("On Error")>> catch_and_log_error

    return dag


rail.for_each_instance(create_main_dag)
