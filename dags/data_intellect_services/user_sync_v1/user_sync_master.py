from datetime import timedelta
from pendulum import datetime
from data_intellect_services.user_sync_v1.utils.python_callable import get_basic_updated_payload_for_collection
from data_intellect_services.user_sync_v1.mapper.update_fields_mapper_hibob import tenant_wide_log_columns
import rail
from airflow.models import Variable

null = None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"data_intellect_user_import_master_{config.instance}_v1",
        description=f"Data intellect services user sync master dag {config.instance} V1",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_user_sync_master_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_tenant_wide_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_tenant_wide_log',
            end_task='dagrun_log_to_sumo',
        )

        create_tenant_wide_log = rail.CreateLogOperator(
            task_id='create_tenant_wide_log',
            tenant_wide_name=config.user_sync_tenant_wide_log_name,
            existing_log_mode="append"
        )

        get_logged_data = rail.FilterLogEntriesOperator(
            task_id='get_logged_data',
            log='{{ result("create_tenant_wide_log") }}',
            remove_filtered_entries=True
        )

        has_any_entries = rail.IfOperator(
            task_id='has_any_entries',
            test='{{ result("get_logged_data") | load_all_records() | length > 0 }}',
            yes_task='create_user_data_collection',
            no_task='dagrun_log_to_sumo',
        )

        create_user_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_data_collection',
            columns=tenant_wide_log_columns,
            source=lambda: list(map(lambda log_data: log_data["properties"],
                filter(lambda log_data: log_data["properties"] is not null, rail.load_all_records(rail.result("get_logged_data"))))),
            name='user_sync_payload_collection'
        )

        create_basic_details_updated_collection = rail.CreateCollectionOperator(
            task_id='create_basic_details_updated_collection',
            columns=tenant_wide_log_columns,
            source=get_basic_updated_payload_for_collection,
            name='basic_details_updated_collection'
        )

        query_create_and_work_or_emp_type_details_updated_users = rail.QueryCollectionOperator(
            task_id='query_create_and_work_or_emp_type_details_updated_users',
            query="""SELECT * FROM user_sync_payload_collection WHERE action='Update'
                AND (type='Work Create Update' OR type='Contract Create Update')
                OR action='Create'""",
            name="create_user_and_work_or_emp_type_details_updated_users"
        )

        query_user_data_payloads = rail.QueryCollectionOperator(
            task_id='query_user_data_payloads',
            query="""SELECT * FROM
                (SELECT * FROM basic_details_updated_collection
                UNION ALL
                SELECT * FROM create_user_and_work_or_emp_type_details_updated_users)
                WHERE NULLIF(id, '') IS NOT NULL ORDER BY timestamp ASC"""
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
            tenant_wide_name=config.tenant_wide_log_name_for_logs,
            existing_log_mode="append"
        )

        trigger_process_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_user',
            items='{{ result("query_user_data_payloads") }}',
            trigger_dag_id=f"data_intellect_user_import_process_users_child_{config.instance}_v1",
            conf=lambda item: {
                "user_details": item,
                "log_artifact": rail.result("create_log")
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> create_tenant_wide_log >> get_logged_data >> has_any_entries
        has_any_entries >> rail.Label("Yes") >> create_user_data_collection >> create_basic_details_updated_collection \
            >> query_create_and_work_or_emp_type_details_updated_users >> query_user_data_payloads \
                >> create_log >> trigger_process_user >> dagrun_log_to_sumo
        has_any_entries >> rail.Label("No") >> dagrun_log_to_sumo

    return dag
rail.for_each_instance(create_dag)
