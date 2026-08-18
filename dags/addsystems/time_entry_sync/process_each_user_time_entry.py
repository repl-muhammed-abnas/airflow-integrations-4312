from datetime import datetime, timedelta
import rail
null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"addsystems_time_data_process_each_user_time_record_child_{config.instance}",
        description=f"addsystems TimeSync for user Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        query_all_time_for_user_records = rail.QueryCollectionOperator(
            task_id='query_all_time_for_user_records',
            query="""SELECT *  FROM time_data Where UserInitials=:clientname""",
            query_params={
                'clientname': '{{ dag_run.conf.item.UserInitials }}'
            }
        )

        process_each_time_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_time_records',
            items="{{result('query_all_time_for_user_records')}}",
            trigger_dag_id=f"addsystems_time_data_process_each_time_record_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_each_time_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_all_time_for_user_records >> process_each_time_records >> wait_process_time_records

        

    return dag


rail.for_each_instance(create_child_dag)
