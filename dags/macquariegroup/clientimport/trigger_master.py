from datetime import timedelta, date
import ast
from pendulum import datetime, now, today
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_clientimport_master_trigger_{config.instance}',
        description=f'Macquarie Client Import Master Trigger {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 7, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: now(
                config.timezone).strftime('%Y-%m-%d-%H%M%S')
        )

        def check_matching_date():
            schedule_dates = ast.literal_eval(
                Variable.get(config.master_trigger_schedules_var_name, default_var=[]))

            # Get the current date
            current_date = today(config.timezone).strftime('%Y-%m-%d')
            return [schedule_date for schedule_date in schedule_dates["dates"] if date(int(schedule_date.split(
                    '-')[0]), int(schedule_date.split('-')[1]), int(schedule_date.split('-')[2])) == date(int(current_date.split(
                        '-')[0]), int(current_date.split('-')[1]), int(current_date.split('-')[2]))]

        is_current_date_matching_schedule = rail.IfOperator(
            task_id='is_current_date_matching_schedule',
            test=check_matching_date,
            yes_task='trigger_macquarie_process_clientimport_master',
            no_task='finish'
        )

        trigger_macquarie_process_clientimport_master = rail.TriggerDagRunOperator(
            task_id='trigger_macquarie_process_clientimport_master',
            retries=0,
            trigger_dag_id=f'macquarie_ondemand_initiate_clientimport_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={}
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        log_current_time >> is_current_date_matching_schedule
        is_current_date_matching_schedule >> rail.Label(
            'Yes') >> trigger_macquarie_process_clientimport_master >> finish
        is_current_date_matching_schedule >> rail.Label(
            'No') >> finish >> log_dagrun_to_sumo
    return dag


rail.for_each_instance(create_dag)
