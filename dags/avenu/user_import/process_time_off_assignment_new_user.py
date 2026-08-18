from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import python_callable_method
from airflow.models import Variable


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_time_off_assignment_new_user_{config.instance}_child',
        description='Avenu User Sync Process Time off assignment New User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_assignment_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "time_off_types_to_assign"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='time_off_types_to_assign',
            end_task="catch_and_log_errors",
        )

        time_off_types_to_assign = rail.PythonOperator(
            task_id="time_off_types_to_assign",
            python_callable=lambda dag_run: python_callable_method.get_time_off_types_to_assign(
                dag_run, config),
        )

        is_time_off_types_to_assign_present = rail.IfOperator(
            task_id='is_time_off_types_to_assign_present',
            test=lambda: bool(rail.result('time_off_types_to_assign')),
            yes_task='get_all_time_off_types',
            no_task='no_timeoff_to_be_assigned'
        )

        no_timeoff_to_be_assigned = rail.EmptyOperator(
            task_id='no_timeoff_to_be_assigned'
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
        )

        get_timeoff_type_list = rail.PythonOperator(
            task_id='get_timeoff_type_list',
            python_callable=python_callable_method.get_timeoff_type_list
        )

        is_timeoff_available = rail.IfOperator(
            task_id='is_timeoff_available',
            test=request_payload.test_timeoff_availablity,
            yes_task='get_time_off_type_uris',
            no_task='log_timeoff_not_available'
        )

        log_timeoff_not_available = rail.WriteLogOperator(
            task_id='log_timeoff_not_available',
            log="{{ dag_run.conf.log_exception}}",
            message="Time off type is not available in replicon",
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        get_time_off_type_uris = rail.PythonOperator(
            task_id='get_time_off_type_uris',
            python_callable=python_callable_method.get_time_off_type_uris
        )

        is_time_off_uris_present = rail.IfOperator(
            task_id='is_time_off_uris_present',
            test=lambda: bool(rail.result('get_time_off_type_uris')),
            yes_task='put_timeoff_assignment_for_user',
            no_task='log_no_time_off_uri_available'
        )

        log_no_time_off_uri_available = rail.WriteLogOperator(
            task_id='log_no_time_off_uri_available',
            log="{{ dag_run.conf.log_exception}}",
            message="Time off type not available",
            severity='Exception',
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_user
        )

        process_time_off_policy_new_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_time_off_policy_new_user',
            items=lambda: rail.result('get_timeoff_type_list'),
            trigger_dag_id=f'avenu_user_sync_process_time_off_policy_new_user_{config.instance}_child',
            conf=request_payload.get_process_time_off_policy_new_user_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_policy_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_policy_new_user',
            dag_runs='{{ result("process_time_off_policy_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log_error}}",
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
        can_run_batch_task >> rail.Label("No") >> time_off_types_to_assign >> is_time_off_types_to_assign_present\
            >> rail.Label('Yes') >> get_all_time_off_types >> get_timeoff_type_list
        is_time_off_types_to_assign_present >> rail.Label(
            'No') >> no_timeoff_to_be_assigned >> catch_and_log_errors
        get_timeoff_type_list >> is_timeoff_available >> rail.Label(
            'Yes') >> get_time_off_type_uris
        is_timeoff_available >> rail.Label(
            'No') >> log_timeoff_not_available >> catch_and_log_errors
        get_time_off_type_uris >> is_time_off_uris_present >> rail.Label(
            'No') >> log_no_time_off_uri_available >> catch_and_log_errors
        is_time_off_uris_present >> rail.Label(
            'Yes') >> put_timeoff_assignment_for_user
        put_timeoff_assignment_for_user >> process_time_off_policy_new_user
        process_time_off_policy_new_user >> wait_for_process_time_off_policy_new_user >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
