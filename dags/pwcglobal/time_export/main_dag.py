from pendulum import datetime
import rail
from pwcglobal.time_export.task.main_dag_task import main_dag_task_group


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/time_extract/config.py


# pylint:disable = too-many-statements
def create_main_airflow_dag(config):
    dag_id_postfix = f'_{config.instance}_{config.uat_postfix}' if config.enable_uatmain_dag else f'_{config.instance}'
    uat_key_value = 'yes' if config.enable_uatmain_dag else None

    with rail.create_airflow_dag(
        dag_id=f'pwc_time_export_master{dag_id_postfix}',
        description=f'Timeexport Master V4.0 {dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_schedule,
        start_date=datetime(2022, 1, 1, tz=config.pacific_timezone),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_tasks=config.dag_max_active_tasks,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag1:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        main_dag = main_dag_task_group(config, uat_key_value)

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            trigger_rule='all_done',
            extra_info={
                'exportperiod': "{{ result('get_export_period') }}"
            }
        )

        main_dag >> dagrun_log_to_sumo

    if config.enable_uatmain_dag:

        dag_id_postfix2 = f'_{config.instance}_{config.non_uat_postfix}' if config.enable_uatmain_dag else f'_{config.instance}'
        non_uat_key_value = 'no' if config.enable_uatmain_dag else None

        with rail.create_airflow_dag(
            dag_id=f'pwc_time_export_master{dag_id_postfix2}',
            description=f'Timeexport Master V4.0 {dag_id_postfix2}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            schedule_interval=config.nonuat_master_dag_schedule,
            start_date=datetime(2022, 1, 1, tz=config.pacific_timezone),
            default_args={
                'sftp_conn_id': config.sftp_conn_id
            },
            max_active_tasks=config.dag_max_active_tasks,
            max_active_runs=config.master_dag_max_active_runs
        ) as dag2:

            rail.ViewDagRunConfOperator(
                task_id="view_dagrun_config",
                extra_config=config)

            main_dag = main_dag_task_group(config, non_uat_key_value)

            dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='dagrun_log_to_sumo',
                sumo_conn_id=config.dagrun_log_sumo_conn_id,
                trigger_rule='all_done',
                extra_info={
                    'exportperiod': "{{ result('get_export_period') }}"
                }
            )

            main_dag >> dagrun_log_to_sumo

    return dag1, dag2 if config.enable_uatmain_dag else dag1


rail.for_each_instance(create_main_airflow_dag)
