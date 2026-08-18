from datetime import timedelta
import rail

from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import python_callable_method


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlen_silver_user_sync_child_process_time_off_assignment_update_{config.instance}',
        description='Matlen_Silver User Sync Process Time off assignment',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_assignment_update_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        time_off_types_to_assign = rail.PythonOperator(
            task_id="time_off_types_to_assign",
            python_callable=lambda dag_run: python_callable_method.get_time_off_types_to_assign(
                dag_run, config),
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
            python_callable=lambda dag_run: python_callable_method.get_timeoff_type_list(
                dag_run, config)
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
            message=request_payload.log_timeoff_not_available,
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
            yes_task='get_user_time_off_policy_summary',
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

        get_user_time_off_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_time_off_policy_summary
        )

        assigned_time_offs = rail.PythonOperator(
            task_id='assigned_time_offs',
            python_callable=python_callable_method.assigned_time_offs
        )

        time_off_types_to_be_assigned = rail.PythonOperator(
            task_id='time_off_types_to_be_assigned',
            python_callable=python_callable_method.time_off_types_to_be_assigned
        )

        time_off_types_to_be_disabled = rail.PythonOperator(
            task_id='time_off_types_to_be_disabled',
            python_callable=python_callable_method.time_off_types_to_be_disabled
        )

        does_time_off_to_be_assigned = rail.IfOperator(
            task_id='does_time_off_to_be_assigned',
            test=lambda: rail.result('time_off_types_to_be_assigned'),
            yes_task='put_timeoff_assignment_for_user',
            no_task='no_time_off_to_assign'
        )

        no_time_off_to_assign = rail.EmptyOperator(
            task_id='no_time_off_to_assign'
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_user
        )

        process_time_off_policy_update_rehire_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_time_off_policy_update_rehire_user',
            items=lambda: rail.result('time_off_types_to_be_assigned'),
            trigger_dag_id=f'matlen_silver_user_sync_child_process_time_off_policy_update_rehire_user_{config.instance}',
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

        time_off_types_to_assign >> is_time_off_types_to_assign_present >> rail.Label(
            'Yes') >> get_all_time_off_types >> get_timeoff_type_list
        is_time_off_types_to_assign_present >> rail.Label(
            'No') >> no_time_off_to_be_assigned >> catch_and_log_errors
        get_timeoff_type_list >> is_timeoff_available >> rail.Label(
            'Yes') >> get_time_off_type_uris
        is_timeoff_available >> rail.Label(
            'No') >> log_timeoff_not_available >> catch_and_log_errors
        get_time_off_type_uris >> is_time_off_uris_present >> rail.Label(
            'No') >> log_no_time_off_uri_available >> catch_and_log_errors
        is_time_off_uris_present >> rail.Label(
            'Yes') >> get_user_time_off_policy_summary
        put_timeoff_assignment_for_user >> process_time_off_policy_update_rehire_user >> wait_for_process_time_off_policy_update_rehire_user
        wait_for_process_time_off_policy_update_rehire_user >> catch_and_log_errors
        get_user_time_off_policy_summary >> assigned_time_offs >> time_off_types_to_be_assigned
        time_off_types_to_be_assigned >> time_off_types_to_be_disabled >> does_time_off_to_be_assigned >> rail.Label(
            'Yes') >> put_timeoff_assignment_for_user
        does_time_off_to_be_assigned >> rail.Label(
            'No') >> no_time_off_to_assign >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
