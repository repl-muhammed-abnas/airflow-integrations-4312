from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import response_filter
from avenu.user_import.utils import python_callable_method
from airflow.models import Variable


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_time_off_assignment_update_{config.instance}_child',
        description='Avenu User Sync Process Time off assignment',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_assignment_update_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_user_time_off_policy_summary"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_time_off_policy_summary',
            end_task="catch_and_log_errors",
        )

        get_user_time_off_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_time_off_policy_summary,
            response_filter=response_filter.get_user_time_off_assigned
        )

        is_rehire_user = rail.IfOperator(
            task_id='is_rehire_user',
            test=request_payload.check_rehire_time_off_scenario,
            yes_task='process_time_off_policy_rehire_user',
            no_task='time_off_types_to_assign'
        )

        process_time_off_policy_rehire_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_time_off_policy_rehire_user',
            items=lambda dag_run: python_callable_method.get_timeoff_types_to_process_no_accrual_rehire(
                dag_run, get_user_time_off_policy_summary.task_id, config),
            trigger_dag_id=f'avenu_user_sync_process_time_off_policy_rehire_user_{config.instance}_child',
            conf=request_payload.get_process_time_off_policy_rehire_user,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_policy_rehire_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_policy_rehire_user',
            dag_runs='{{ result("process_time_off_policy_rehire_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        time_off_types_to_assign = rail.PythonOperator(
            task_id="time_off_types_to_assign",
            python_callable=lambda dag_run: python_callable_method.get_update_time_off_types_to_assign(
                dag_run, config),
        )

        time_off_types_to_disable = rail.PythonOperator(
            task_id="time_off_types_to_disable",
            python_callable=python_callable_method.get_update_time_off_types_to_delete,
            op_args=[config]
        )

        is_time_off_types_to_disable_present = rail.IfOperator(
            task_id='is_time_off_types_to_disable_present',
            test=lambda: bool(rail.result('time_off_types_to_disable')),
            yes_task='delete_future_time_off_scenario',
            no_task='is_time_off_types_to_assign_present'
        )

        delete_future_time_off_scenario = rail.TriggerDagRunForEachItemOperator(
            task_id='delete_future_time_off_scenario',
            items=lambda: rail.result('time_off_types_to_disable'),
            trigger_dag_id=f'avenu_user_sync_delete_future_time_off_for_user_{config.instance}_child',
            conf=request_payload.get_delete_future_time_off_scenario,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_delete_future_time_off_scenario = rail.WaitForDagRunsSensor(
            task_id='wait_for_delete_future_time_off_scenario',
            dag_runs='{{ result("delete_future_time_off_scenario") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_user_disable_time_off = rail.RepliconServiceOperator(
            task_id='get_user_disable_time_off',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            },
            response_filter=response_filter.get_user_future_time_off
        )

        update_future_time_off_for_no_aacural = rail.TriggerDagRunForEachItemOperator(
            task_id='update_future_time_off_for_no_aacural',
            items=lambda dag_run:python_callable_method.get_timeoff_types_to_process_no_accrual(dag_run, get_user_disable_time_off.task_id, config),
            trigger_dag_id=f'avenu_user_sync_update_time_off_for_no_aacural_{config.instance}_child',
            conf=request_payload.get_update_time_off_for_no_aacural,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_update_future_time_off_for_no_aacural = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_future_time_off_for_no_aacural',
            dag_runs='{{ result("update_future_time_off_for_no_aacural") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_time_off_types_to_assign_present = rail.IfOperator(
            task_id='is_time_off_types_to_assign_present',
            test=lambda: bool(rail.result('time_off_types_to_assign')),
            yes_task='get_all_time_off_types',
            no_task='no_time_off_to_be_assigned'
        )

        no_time_off_to_be_assigned = rail.EmptyOperator(
            task_id='no_time_off_to_be_assigned'
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
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastName'],
                'status': 'Exception',
            }
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_user
        )

        get_time_off_policy_to_assign = rail.PythonOperator(
            task_id="get_time_off_policy_to_assign",
            python_callable=lambda dag_run: python_callable_method.get_update_time_off_policy_to_assign(
                dag_run, config),
        )

        get_timeoff_policy_list = rail.PythonOperator(
            task_id='get_timeoff_policy_list',
            python_callable=python_callable_method.get_timeoff_policy_list
        )

        process_time_off_policy_update_rehire_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_time_off_policy_update_rehire_user',
            items=lambda: rail.result('get_timeoff_policy_list'),
            trigger_dag_id=f'avenu_user_sync_process_time_off_policy_update_rehire_user_{config.instance}_child',
            conf=request_payload.get_process_time_off_policy_update_rehire_user,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_policy_update_rehire_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_policy_update_rehire_user',
            dag_runs='{{ result("process_time_off_policy_update_rehire_user") }}',
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
        can_run_batch_task >> rail.Label("No") >> get_user_time_off_policy_summary >> is_rehire_user >> rail.Label(
            "No") >> time_off_types_to_assign >> time_off_types_to_disable >> is_time_off_types_to_disable_present
        is_time_off_types_to_disable_present >> rail.Label(
            "Yes") >> delete_future_time_off_scenario >> wait_for_delete_future_time_off_scenario >> get_user_disable_time_off
        get_user_disable_time_off >> update_future_time_off_for_no_aacural
        update_future_time_off_for_no_aacural >> wait_for_update_future_time_off_for_no_aacural >> is_time_off_types_to_assign_present
        is_time_off_types_to_disable_present >> rail.Label("No") >> is_time_off_types_to_assign_present >> rail.Label(
            'Yes') >> get_all_time_off_types >> get_timeoff_type_list
        is_rehire_user >> rail.Label(
            "Yes") >> process_time_off_policy_rehire_user >> wait_for_process_time_off_policy_rehire_user >> time_off_types_to_assign
        is_time_off_types_to_assign_present >> rail.Label(
            'No') >> no_time_off_to_be_assigned >> catch_and_log_errors
        get_timeoff_type_list >> is_timeoff_available >> rail.Label(
            'Yes') >> get_time_off_type_uris
        is_timeoff_available >> rail.Label(
            'No') >> log_timeoff_not_available >> catch_and_log_errors
        get_time_off_type_uris >> is_time_off_uris_present >> rail.Label(
            'No') >> log_no_time_off_uri_available >> catch_and_log_errors
        is_time_off_uris_present >> rail.Label(
            'Yes') >> put_timeoff_assignment_for_user >> get_time_off_policy_to_assign >> get_timeoff_policy_list >> process_time_off_policy_update_rehire_user
        process_time_off_policy_update_rehire_user >> wait_for_process_time_off_policy_update_rehire_user >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
