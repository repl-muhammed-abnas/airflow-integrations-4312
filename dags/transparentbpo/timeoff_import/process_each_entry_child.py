import rail
from datetime import timedelta
from airflow.models import Variable
from transparentbpo.timeoff_import.utils import custom_methods, request_payload

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_timeoff_record_child_dag_id,
        description="TransparentBPO Timeoff import Processes each trigger record",
        company_key=config.company_key,
        max_active_runs=config.max_active_child_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        timeoff_type_is_enabled = rail.IfOperator(
            task_id="timeoff_type_is_enabled",
            test=lambda dag_run: dag_run.conf['type']['name'] in dag_run.conf['required_timeoff_types_in_replicon'],
            yes_task='get_employee_details',
            no_task='log_timeoff_type_is_not_enabled',
        )

        log_timeoff_type_is_not_enabled = rail.WriteLogOperator(
            task_id='log_timeoff_type_is_not_enabled',
            log="{{ result('create_log') }}",
            severity="Exception",
            message='Timeoff "{{dag_run.conf.type.name}}" is not available or enabled in Replicon',
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type.name }}",
                'booking_date': "{{ dag_run.conf.start }} to {{ dag_run.conf.end }}",
                'status': "Exception",
                'details': 'Timeoff "{{dag_run.conf.type.name}}" is not available or enabled in Replicon'
            }
        )

        get_employee_details = rail.BambooHROperator(
            task_id='get_employee_details',
            request_method='GET',
            company_domain="",
            endpoint="/employees/{{dag_run.conf.bamboohr_id}}?fields=employeeNumber",
            bamboohr_conn_id=config.bamboohr_conn_id,
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{ result('get_employee_details').employeeNumber }}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        user_exists_or_not = rail.IfOperator(
            task_id="user_exists_or_not",
            test=lambda: custom_methods.is_user_enabled(rail.result('get_user_data')),
            yes_task='get_user_scheduled_hours_in_date_range',
            no_task='log_user_is_not_available_in_replicon'
        )

        log_user_is_not_available_in_replicon = rail.WriteLogOperator(
            task_id='log_user_is_not_available_in_replicon',
            log="{{ result('create_log') }}",
            severity="Exception",
            message="User is not available in Replicon",
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ result('get_employee_details').employeeNumber }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type.name }}",
                'booking_date': "{{ dag_run.conf.start }} to {{ dag_run.conf.end }}",
                'status': "Exception",
                'details': "User is not available in Replicon"
            }
        )

        get_user_scheduled_hours_in_date_range = rail.RepliconServiceOperator(
            task_id="get_user_scheduled_hours_in_date_range",
            endpoint="/services/SchedulingService2.svc/GetScheduledHoursInDateRange",
            data=lambda dag_run: request_payload.get_user_scheduled_hours_in_date_range_payload(
                dag_run, config.DATE_DEFAULT_FORMAT
            ),
            data_handler=custom_methods.format_user_schedule_hrs_list
        )

        uri_of_assigned_timeoff_type_is_present = rail.IfOperator(
            task_id="uri_of_assigned_timeoff_type_is_present",
            test=lambda dag_run: custom_methods.get_timeoff_uri_from_user_data(
                rail.result('get_user_data'),
                dag_run.conf['type']['name']
            ),
            yes_task='add_timeoffs_to_bamboohr_output',
            no_task='log_uri_of_assigned_timeoff_type_is_not_present',
        )

        log_uri_of_assigned_timeoff_type_is_not_present = rail.WriteLogOperator(
            task_id='log_uri_of_assigned_timeoff_type_is_not_present',
            log="{{ result('create_log') }}",
            severity="Exception",
            message='Timeoff "{{ dag_run.conf.type.name }}" is not available or enabled in Replicon',
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ result('get_employee_details').employeeNumber }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type.name }}",
                'booking_date': "{{ dag_run.conf.start }} to {{ dag_run.conf.end }}",
                'status': "Exception",
                'details': 'Timeoff "{{ dag_run.conf.type.name }}" is not available or enabled in Replicon'
            }
        )

        add_timeoffs_to_bamboohr_output = rail.PythonOperator(
            task_id="add_timeoffs_to_bamboohr_output",
            python_callable=lambda dag_run: custom_methods.add_timeoffs_array(dag_run.conf)
        )

        process_timeoff_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_records',
            retries=0,
            items=lambda: rail.result("add_timeoffs_to_bamboohr_output")['time_offs'],
            conf=lambda item, dag_run: request_payload.process_each_timeoff_record(item, dag_run.conf),
            trigger_dag_id=config.process_timeoff_bookings_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_timeoff_records_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoff_records_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timeoff_records") }}'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            severity="Error",
            message="{{ get_error_message() }}",
            properties={
                'timeoff_id': "{{ dag_run.conf.timeoff_id }}",
                'bamboohr_id': "{{ dag_run.conf.bamboohr_id }}",
                'employee_id': "{{ result('get_employee_details').employeeNumber }}",
                'username': "{{ dag_run.conf.name }}",
                'timeoff_type': "{{ dag_run.conf.type.name }}",
                'booking_date': "{{ dag_run.conf.start }} to {{ dag_run.conf.end }}",
                'status': "Error",
                'details': "{{ get_error_message() }}"
            }
        )

    can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
    can_run_batch_task >> rail.Label("No") >> create_log

    create_log >> timeoff_type_is_enabled

    timeoff_type_is_enabled >> rail.Label(
        "Yes") >> get_employee_details
    timeoff_type_is_enabled >> rail.Label(
        "No") >> log_timeoff_type_is_not_enabled >> catch_and_log_errors

    get_employee_details >> get_user_data >> user_exists_or_not

    user_exists_or_not >> rail.Label(
        "Yes") >> get_user_scheduled_hours_in_date_range
    user_exists_or_not >> rail.Label(
        "No") >> log_user_is_not_available_in_replicon >> catch_and_log_errors

    get_user_scheduled_hours_in_date_range >> uri_of_assigned_timeoff_type_is_present

    uri_of_assigned_timeoff_type_is_present >> rail.Label(
        "Yes") >> add_timeoffs_to_bamboohr_output
    uri_of_assigned_timeoff_type_is_present >> rail.Label(
        "No") >> log_uri_of_assigned_timeoff_type_is_not_present >> catch_and_log_errors

    add_timeoffs_to_bamboohr_output >> process_timeoff_records >> wait_for_process_timeoff_records_child \
        >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
