from datetime import timedelta
import functools
from pendulum import now
import itertools
from pendulum import datetime
import rail
from wikwemikongboard.timeoffbalancetransfer.utils import request_payload
from wikwemikongboard.timeoffbalancetransfer.utils import python_callable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f"wikwemikongboard_timeoffbalancetransfer_Master - {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        get_all_enabled_users = rail.RepliconServiceOperator(
            task_id="get_all_enabled_users",
            endpoint="/services/UserService1.svc/GetEnabledUsers"
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=lambda: {
                'process_start_time': now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                'log_filename': 'wikwemikongboardlog_' + rail.render_template('{{ dag_run_ecid() | replace(":", "-") }}') + ".csv"
            }
        )

        create_user_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_data_collection',
            source=lambda: rail.load_all_records(
                rail.result('get_all_enabled_users')),
            name="user_data"
        )

        query_valid_user_records = rail.QueryCollectionOperator(
            task_id='query_valid_user_records',
            query="SELECT uri,loginName,displayText FROM user_data"
        )

        get_timeoff_balance = rail.trigger_parallel_dagrun(
            task_id="get_timeoff_balance",
            items=lambda: rail.result('query_valid_user_records'),
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.get_timeoff_child_dag_id,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda item:  item
        )

        get_process_each_timeoff_balance_ids =rail.PythonOperator(
            task_id= 'get_process_each_timeoff_balance_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'get_timeoff_balance_{x+1}'), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs= False
        )

        get_timeoff_balance_details = rail.GatherResultsFromDagRunsOperator(
            task_id='get_timeoff_balance_details',
            dag_runs="{{ result('get_process_each_timeoff_balance_ids') }}",
            dagrun_task_id='all_data',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        create_timeoff_data_collection = rail.CreateCollectionOperator(
            task_id='create_timeoff_data_collection',
            source=lambda: rail.result('get_timeoff_balance_details'),
            name="timeoff_data"
        )

        query_valid_timeoff_records = rail.QueryCollectionOperator(
            task_id='query_valid_timeoff_records',
            query="""SELECT DISTINCT uri,loginName,timeOffTemplate FROM timeoff_data Where uri IS NOT NULL AND timeOffTemplate IS NOT NULL"""
        )

        transfer_timeoff_balance = rail.trigger_parallel_dagrun(
            task_id="transfer_timeoff_balance",
            items=lambda: rail.result('query_valid_timeoff_records'),
            parallel_count=config.trigger_parallel_dagrun_count,
            trigger_dag_id=config.timeoff_child_dag_id,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=request_payload.process_transfer_timeoff_conf
        )

        get_transfer_timeoff_balance_dag_ids =rail.PythonOperator(
            task_id= 'get_transfer_timeoff_balance_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'transfer_timeoff_balance_{x+1}'), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs= False
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("get_transfer_timeoff_balance_dag_ids") }}',
            dagrun_task_id='create_log_artifact',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda:{
                "logs": rail.result('gather_logs'),
                "log_filename": rail.result('logging_details')['log_filename'],
                "start_time": rail.result('logging_details')['process_start_time']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_all_enabled_users >> logging_details >> create_user_data_collection >> query_valid_user_records >> get_timeoff_balance >> get_process_each_timeoff_balance_ids\
            >> get_timeoff_balance_details >> create_timeoff_data_collection >> query_valid_timeoff_records >> transfer_timeoff_balance\
            >> get_transfer_timeoff_balance_dag_ids >> gather_logs >> process_log_generation >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
