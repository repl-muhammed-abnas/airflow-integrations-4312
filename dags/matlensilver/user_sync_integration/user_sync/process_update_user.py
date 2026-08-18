from datetime import timedelta
import rail
from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import python_callable_method
from matlensilver.user_sync_integration.user_sync.tasks.process_supervisor import process_supervisor_assignment_task_group


def create_child_dag_wbs(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'matlen_silver_user_sync_child_process_update_user_{config.instance}',
        description='Matlen_Silver User Sync Process Update User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        user_uri = '{{ dag_run.conf.useruri }}'

        null = None

        has_valid_update_fields = rail.IfOperator(
            task_id ='has_valid_update_fields',
            test = request_payload.test_valid_fields,
            yes_task="update_user_exception_log",
            no_task="log_invalid_update_fields"
        )

        log_invalid_update_fields =rail.WriteLogOperator(
            task_id = 'log_invalid_update_fields',
            message = request_payload.get_invalid_fields_message,
            severity='Exception',
            properties = lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        update_user_exception_log = rail.CreateLogOperator(
            task_id='update_user_exception_log'
        )

        update_user_error_logs = rail.CreateLogOperator(
            task_id='update_user_error_logs'
        )

        update_user_success_logs = rail.CreateLogOperator(
            task_id='update_user_success_logs'
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": user_uri,
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        is_rehire_user = rail.IfOperator(
            task_id="is_rehire_user",
            test=lambda: request_payload.test_enddate and not rail.result(
                'get_user_info')['userDetails']['isEnabled'],
            yes_task="enable_login",
            no_task="get_current_custom_field_values"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": user_uri
            }
        )

        log_rehire_user = rail.WriteLogOperator(
            task_id='log_rehire_user',
            log="{{result('update_user_success_logs')}}",
            message="Rehired User",
            severity='Success',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Success',
            }
        )

        get_current_custom_field_values = rail.PythonOperator(
            task_id='get_current_custom_field_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['customFieldValues']
        )

        get_current_oef_values = rail.PythonOperator(
            task_id='get_current_oef_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['extensionFieldValues']
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications,
        )

        is_billing_rate_changed = rail.IfOperator(
            task_id='is_billing_rate_changed',
            test=lambda dag_run: dag_run.conf['netbillrate'] and request_payload.test_billing_rate(
                dag_run),
            yes_task='update_billing_rate',
            no_task='is_hourly_rate_changed'
        )

        update_billing_rate = rail.RepliconServiceOperator(
            task_id='update_billing_rate',
            endpoint='/services/ResourceService1.svc/InsertBillingRateIntoUserBillingRateSchedule',
            data=request_payload.update_billing_rate,
        )

        is_hourly_rate_changed = rail.IfOperator(
            task_id='is_hourly_rate_changed',
            test=lambda dag_run: dag_run.conf['hourlypayrate'] and request_payload.test_hourly_rate(
                dag_run),
            yes_task='update_hourly_rate',
            no_task='is_supervisor_in_feed_file'
        )

        update_hourly_rate = rail.RepliconServiceOperator(
            task_id='update_hourly_rate',
            endpoint='/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange',
            data=request_payload.update_hourly_rate,
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: dag_run.conf['supervisorname'] and dag_run.conf['supervisorcode'],
            yes_task='is_supervisor_same_as_user',
            no_task='log_supervisor_not_in_feedfile'
        )

        log_supervisor_not_in_feedfile = rail.WriteLogOperator(
            task_id='log_supervisor_not_in_feedfile',
            log="{{result('update_user_exception_log')}}",
            message="Supervisor details not present in feed file",
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        process_supervisor_task_entry, process_supervisor_task_exit = process_supervisor_assignment_task_group(
            'useruri', 'update_user')

        is_employee_type_changed = rail.IfOperator(
            task_id='is_employee_type_changed',
            test=lambda dag_run: dag_run.conf['employeetype'] and dag_run.conf['employeetypecode'] and request_payload.test_employeetypechange(
                dag_run, config),
            yes_task='log_employee_type_changed',
            no_task='is_zipcode_changed'
        )

        log_employee_type_changed = rail.WriteLogOperator(
            task_id='log_employee_type_changed',
            log="{{result('update_user_success_logs')}}",
            message="Employee type changed",
            severity='Success',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Success',
            }
        )

        is_zipcode_changed = rail.IfOperator(
            task_id='is_zipcode_changed',
            test=lambda dag_run: request_payload.test_zipcode_change(
                dag_run, config),
            yes_task='log_zipcode_changed',
            no_task='no_change_in_time_off'
        )

        log_zipcode_changed = rail.WriteLogOperator(
            task_id='log_zipcode_changed',
            log="{{result('update_user_success_logs')}}",
            message="Zip code changed",
            severity='Success',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Success',
            }
        )

        no_change_in_time_off = rail.EmptyOperator(
            task_id='no_change_in_time_off'
        )

        process_timeoff = rail.EmptyOperator(
            task_id="process_timeoff",
        )

        process_time_off_assignment = rail.TriggerDagRunOperator(
            task_id='process_time_off_assignment',
            trigger_dag_id=f'matlen_silver_user_sync_child_process_time_off_assignment_update_{config.instance}',
            conf=lambda dag_run: request_payload.get_process_time_off_assignment_conf(
                dag_run, 'update_user'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_assignment',
            dag_runs='{{ result("process_time_off_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_logs = rail.EmptyOperator(
            task_id='get_all_logs'
        )

        get_all_exception_logs = rail.PythonOperator(
            task_id='get_all_exception_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['update_user_exception_log']
        )

        get_all_error_logs = rail.PythonOperator(
            task_id='get_all_error_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['update_user_error_logs']
        )

        get_all_success_logs = rail.PythonOperator(
            task_id='get_all_success_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['update_user_success_logs']
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message=request_payload.get_update_completion_message,
            severity=request_payload.get_update_severity,
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'firstname': dag_run.conf['firstname'],
                'lastname': dag_run.conf['lastname'],
                'status': 'Error' if rail.result('get_all_error_logs') else ('Exception' if rail.result('get_all_exception_logs') else 'Success'),
            }
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

        has_valid_update_fields >> rail.Label('No') >> log_invalid_update_fields >> catch_and_log_errors
        has_valid_update_fields >> rail.Label('Yes') >> update_user_exception_log
        update_user_exception_log >> update_user_error_logs >> update_user_success_logs >> get_user_info >> is_rehire_user >> rail.Label(
            'Yes') >> enable_login >> log_rehire_user >> get_current_custom_field_values
        is_rehire_user >> rail.Label('No') >> get_current_custom_field_values
        get_current_custom_field_values >> get_current_oef_values >> apply_user_modifications
        apply_user_modifications >> is_billing_rate_changed >> rail.Label(
            'Yes') >> update_billing_rate >> is_hourly_rate_changed
        is_billing_rate_changed >> rail.Label('No') >> is_hourly_rate_changed >> rail.Label(
            'Yes') >> update_hourly_rate >> is_supervisor_in_feed_file
        is_hourly_rate_changed >> rail.Label(
            'No') >> is_supervisor_in_feed_file
        is_supervisor_in_feed_file >> rail.Label(
            'Yes') >> process_supervisor_task_entry
        is_supervisor_in_feed_file >> rail.Label(
            'No') >> log_supervisor_not_in_feedfile >> is_employee_type_changed
        process_supervisor_task_exit >> is_employee_type_changed >> rail.Label(
            'No') >> is_zipcode_changed >> rail.Label('No') >> no_change_in_time_off >> get_all_logs
        is_employee_type_changed >> rail.Label(
            'Yes') >> log_employee_type_changed >> process_time_off_assignment >> wait_for_process_time_off_assignment
        is_zipcode_changed >> rail.Label(
            'Yes') >> log_zipcode_changed >> process_timeoff >> process_time_off_assignment >> wait_for_process_time_off_assignment
        wait_for_process_time_off_assignment >> get_all_logs >> [
            get_all_exception_logs, get_all_error_logs, get_all_success_logs] >> log_completion >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
