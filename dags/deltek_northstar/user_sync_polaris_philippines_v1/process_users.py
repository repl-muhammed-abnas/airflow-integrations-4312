from datetime import timedelta
from airflow.models import Variable
import rail
from deltek_northstar.user_sync_polaris_philippines_v1.utils import request_payload, response_filter, python_callable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_users,
        description='Deltek Costpoint User Import- Process Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        get_user_by_empl_id = rail.RepliconServiceOperator(
            task_id="get_user_by_empl_id",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_payload,
            data_handler=response_filter.get_filtered_user_data
        )

        if_user_present_with_empl_id = rail.IfOperator(
            task_id ='if_user_present_with_empl_id',
            test = lambda: bool(rail.result('get_user_by_empl_id')),
            yes_task="get_user_data",
            no_task="get_user_by_loginname"
        )

        get_user_by_loginname = rail.RepliconServiceOperator(
            task_id="get_user_by_loginname",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_by_loginname_payload,
            data_handler=response_filter.get_filtered_user_data
        )

        get_user_data = rail.PythonOperator(
            task_id='get_user_data',
            python_callable=lambda: rail.result('get_user_by_empl_id') if rail.result('get_user_by_empl_id') else rail.result('get_user_by_loginname')
        )

        has_valid_data = rail.IfOperator(
            task_id ='has_valid_data',
            test = request_payload.test_valid_fields,
            yes_task="check_add_or_update_user",
            no_task="log_invalid_data"
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id = 'log_invalid_data',
            log = '{{ result("create_user_log") }}',
            message = request_payload.get_invalid_fields_message,
            severity='Exception',
            properties = lambda dag_run: {
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'useruri': '',
                'manager': dag_run.conf['mgr_empl_id'],
                'action': 'Validation',
                'status': 'Exception',
                'details': request_payload.get_invalid_fields_message(dag_run)
            }
        )

        check_add_or_update_user = rail.PythonOperator(
            task_id='check_add_or_update_user',
            python_callable=lambda dag_run: python_callable.check_add_or_update_user(dag_run, config.time_zone)
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=lambda response, dag_run:response_filter.get_available_timeoff_types(response, dag_run, config.TIMEOFF_TYPE_MAPPER)
        )

        if_mapper_timeoff_type_is_not_present_in_instance = rail.IfOperator(
            task_id ='if_mapper_timeoff_type_is_not_present_in_instance',
            test = lambda: bool(rail.result('get_all_time_off_types')['not_available_in_instance']),
            yes_task="log_timeoff_types_not_present",
            no_task="if_add_or_update_is_valid"
        )

        log_timeoff_types_not_present = rail.WriteLogOperator(
            task_id = 'log_timeoff_types_not_present',
            log = '{{ result("create_user_log") }}',
            message = "Timeoff types not present in replicon.",
            severity='Exception',
            properties = lambda dag_run: {
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'useruri': '',
                'manager': dag_run.conf['mgr_empl_id'],
                'action': 'Validation',
                'status': 'Exception',
                'details': f"{', '.join(rail.result('get_all_time_off_types')['not_available_in_instance'])} Timeoff types not present in replicon."
            }
        )

        if_add_or_update_is_valid = rail.IfOperator(
            task_id ='if_add_or_update_is_valid',
            test = lambda: rail.result('check_add_or_update_user')['setup_new_profile'] != "",
            yes_task="if_setup_new_profile_is_no",
            no_task="log_invalid_pers_act_cd"
        )

        log_invalid_pers_act_cd = rail.WriteLogOperator(
            task_id = 'log_invalid_pers_act_cd',
            log = '{{ result("create_user_log") }}',
            message = "Profile is disabled or not present in replicon.",
            severity='Exception',
            properties = lambda dag_run: {
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'useruri': '',
                'manager': dag_run.conf['mgr_empl_id'],
                'action': 'Validation',
                'status': 'Exception',
                'details': "Profile is disabled or not present in replicon."
            }
        )

        if_setup_new_profile_is_no = rail.IfOperator(
            task_id='if_setup_new_profile_is_no',
            test=lambda: rail.result('check_add_or_update_user')['setup_new_profile'] == "No",
            yes_task='get_timesheet_details',
            no_task='needs_to_update_the_existing_profile'
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: request_payload.get_timesheet_details(dag_run,config)
        )

        needs_to_update_the_existing_profile = rail.IfOperator(
            task_id='needs_to_update_the_existing_profile',
            test=lambda: rail.result('check_add_or_update_user')['setup_new_profile'] == "Yes" \
                and rail.result('check_add_or_update_user')['update_existing_profile'].get('loginName'),
            yes_task='update_existing_user_profile',
            no_task='process_new_user'
        )

        update_existing_user_profile = rail.RepliconServiceOperator(
            task_id='update_existing_user_profile',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run:request_payload.get_update_existing_user_profile(
                dag_run,
                config.time_zone,
                rail.result('get_user_data')[0]['userDetails'],
                rail.result('check_add_or_update_user')['update_existing_profile']
            )
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': "{{ result('get_user_data')[0].userDetails.uri }}",
            }
        )

        process_new_user = rail.TriggerDagRunOperator(
            task_id='process_new_user',
            trigger_dag_id=config.process_new_users,
            conf=request_payload.get_process_new_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_user',
            dag_runs='{{ result("process_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id='process_update_user',
            trigger_dag_id=config.process_update_users,
            conf=request_payload.get_process_update_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{result("create_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "lastname": "{{dag_run.conf.last_name}}",
                "firstname": "{{dag_run.conf.first_name}}",
                "loginname": "{{dag_run.conf.email_id}}",
                "employeeid": "{{dag_run.conf.empl_id}}",
                "useruri": "",
                "manager": "{{dag_run.conf.mgr_empl_id}}",
                "action": "Sync",
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> get_user_by_empl_id >> if_user_present_with_empl_id >> rail.Label('Yes') >> get_user_data
        if_user_present_with_empl_id >> rail.Label('No') >> get_user_by_loginname >> get_user_data >> has_valid_data
        has_valid_data >> rail.Label('No') >> log_invalid_data >> catch_and_log_errors
        has_valid_data >> rail.Label('Yes') >> check_add_or_update_user >> get_all_time_off_types >> if_mapper_timeoff_type_is_not_present_in_instance
        if_mapper_timeoff_type_is_not_present_in_instance >> rail.Label('Yes') >> log_timeoff_types_not_present >> if_add_or_update_is_valid
        if_mapper_timeoff_type_is_not_present_in_instance >> rail.Label('No') >> if_add_or_update_is_valid
        if_add_or_update_is_valid >> rail.Label('Yes') >> if_setup_new_profile_is_no
        if_add_or_update_is_valid >> rail.Label('No') >> log_invalid_pers_act_cd >> catch_and_log_errors
        if_setup_new_profile_is_no >> rail.Label('No') >> needs_to_update_the_existing_profile
        needs_to_update_the_existing_profile >> rail.Label('Yes') >> update_existing_user_profile >> disable_user >> process_new_user
        needs_to_update_the_existing_profile >> rail.Label('No') >> process_new_user
        process_new_user >> wait_for_process_new_user >> catch_and_log_errors
        if_setup_new_profile_is_no >> rail.Label('Yes') >> get_timesheet_details >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
