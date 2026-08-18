from datetime import timedelta
import itertools
from pendulum import datetime
import rail

from dxctechnology.workday_user_import.user_import.tasks.get_all_required_data import get_all_required_fields
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_usa_csc_user_process_conf
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_all_run_ids_callable

def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_usa_csc_data_child_dag,
        description="dxctechnology workday user sync - USA CSC",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        start_task, end_task = get_all_required_fields("usa_csc_get_all_records", config)

        trigger_process_user = rail.trigger_parallel_dagrun(
            task_id = "trigger_process_user",
            items=lambda dag_run:dag_run.conf['compass_usa_csc_data'],
            trigger_dag_id=config.usa_csc_process_users_child_dag_id,
            parallel_count=config.process_users_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf = lambda item, dag_run : get_usa_csc_user_process_conf(item, dag_run, config)
        )

        get_all_run_ids = rail.PythonOperator(
            task_id = "get_all_run_ids",
            python_callable = lambda: get_all_run_ids_callable('trigger_process_user', config.process_users_parallel_count),
        )

        gather_all_logs = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_all_logs",
            dagrun_task_id = "create_user_log",
            dag_runs="{{result('get_all_run_ids')}}"
        )

        end_task >> trigger_process_user >> get_all_run_ids >> gather_all_logs

        return dag
    
rail.for_each_instance(create_dag)
