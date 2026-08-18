
from datetime import timedelta
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_check_and_delete_from_lookup_table_master_{config.instance}_v2',
        description=f'NTTDATABC Check and Delete from Lookup table {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timesheet_mapper'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_timesheet_mapper',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_timesheet_mapper = rail.CreateLogOperator(
            task_id = 'get_timesheet_mapper',
            tenant_wide_name="ntt_timesheet_mapper",
            existing_log_mode="append",
        )

        search_and_delete_today_entries=rail.FilterLogEntriesOperator(
            task_id='search_and_delete_today_entries',
            log="{{ result('get_timesheet_mapper') }}",
            properties={
                'check': "{{ current_time_in_specified_tz('America/Los_Angeles','%Y-%m-%d')}}"
            },
            remove_filtered_entries=True
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_timesheet_mapper >> search_and_delete_today_entries
        search_and_delete_today_entries >> finish >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
