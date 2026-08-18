from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import response_filter
from airflow.models import Variable


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_delete_future_time_off_for_user_{config.instance}_child',
        description='Avenu User Sync update_time_off_for_future_time_off',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_records,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_specfic_time_off_types"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_specfic_time_off_types',
            end_task="catch_and_log_errors",
        )

        get_specfic_time_off_types = rail.RepliconServiceOperator(
            task_id='get_specfic_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=response_filter.get_specfic_time_off_types
        )

        get_data_forall_timeoff_after_the_enddate = rail.RepliconServiceOperator(
            task_id='get_data_forall_timeoff_after_the_enddate',
            endpoint='/services/TimeOffListService1.svc/GetData',
            data=request_payload.get_data_for_all_future_timeoff_after_the_enddate,
            response_filter=response_filter.map_time_off_delete_uri
        )

        if_time_off_present = rail.IfOperator(
            task_id='if_time_off_present',
            test=lambda: bool(rail.result(
                'get_data_forall_timeoff_after_the_enddate')),
            yes_task='create_timeOff_delete_batch',
            no_task='catch_and_log_errors'
        )

        create_timeOff_delete_batch = rail.RepliconServiceOperator(
            task_id="create_timeOff_delete_batch",
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=request_payload.create_timeOff_delete_batch
        )

        execute_timeOff_delete_batch = rail.RepliconServiceOperator(
            task_id="execute_timeOff_delete_batch",
            endpoint="/services/TimeOffService1.svc/ExecuteTimeOffDeleteBatch",
            data=request_payload.execute_timeOff_delete_batch
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

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_specfic_time_off_types >> get_data_forall_timeoff_after_the_enddate >> if_time_off_present >> rail.Label(
            "Yes") >> create_timeOff_delete_batch >> execute_timeOff_delete_batch >> catch_and_log_errors >> log_to_sumo
        if_time_off_present >> rail.Label(
            "No") >> catch_and_log_errors
    return dag


rail.for_each_instance(create_child_dag_wbs)
