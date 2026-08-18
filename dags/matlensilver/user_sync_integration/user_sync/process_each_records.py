from datetime import timedelta
import rail
from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import response_filter


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlen_silver_user_sync_child_process_each_records_{config.instance}',
        description='Matlen_Silver User Sync Process Each Records',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_records,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_data_payload,
            response_filter=response_filter.get_filtered_user_data
        )

        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result('get_user_data')),
            yes_task='process_update_user',
            no_task='process_new_user'
        )

        process_new_user = rail.TriggerDagRunOperator(
            task_id='process_new_user',
            trigger_dag_id=f'matlen_silver_user_sync_child_process_new_user_{config.instance}',
            conf=lambda dag_run: request_payload.get_process_user_conf(
                dag_run, 'new_user'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_user',
            dag_runs='{{ result("process_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id='process_update_user',
            trigger_dag_id=f'matlen_silver_user_sync_child_process_update_user_{config.instance}',
            conf=lambda dag_run: request_payload.get_process_user_conf(
                dag_run, 'update_user'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_user_data >> is_user_available >> rail.Label(
            'No') >> process_new_user >> wait_for_process_new_user >> catch_and_log_errors
        is_user_available >> rail.Label(
            'Yes') >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag_wbs)
