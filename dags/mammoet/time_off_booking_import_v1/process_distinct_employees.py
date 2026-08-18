from datetime import timedelta
from airflow.models import Variable
import rail

from mammoet.time_off_booking_import_v1.utils import request_payload, response_filter

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_distinct_employees_dagid,
        description="Mammoet Time Off Booking Import Process Distinct Employees",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_process_distinct_employees,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_employee_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_employee_log',
            end_task='catch_and_log_errors',
        )

        create_employee_log =  rail.CreateLogOperator(
            task_id='create_employee_log',
        )

        query_employee_data = rail.QueryCollectionOperator(
            task_id="query_employee_data",
            name='employee_data',
            query="""SELECT DISTINCT * FROM valid_records WHERE employee_id='{{dag_run.conf.employee_id}}'"""
        )

        get_user_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_on_empid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_empid_payload,
            data_handler=response_filter.get_filtered_output_empid
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('get_user_on_empid')),
            yes_task="get_user_info",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{result("create_employee_log")}}',
            items='{{result("query_employee_data")}}',
            message="User with employeeid '{{dag_run.conf.employee_id}}' is not present/disabled in replicon",
            severity='Exception',
            properties={
                'sf_booking_id': "{{item.sf_booking_id}}",
                'employee_id': "{{item.employee_id}}",
                'time_off_type_description': "{{item.time_off_type_description}}",
                'action':'Validation',
                'status': 'Exception',
                'details': "User with employeeid '{{dag_run.conf.employee_id}}' is not present/disabled in replicon",
            }
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id="get_user_info",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('get_user_on_empid').0.uri}}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            data_handler=response_filter.get_filtered_output_user_info
        )

        is_timeoff_template_present = rail.IfOperator(
            task_id="is_timeoff_template_present",
            test=lambda: bool(rail.result('get_user_info')[0]['timeoff_template']),
            yes_task="get_all_assigned_time_off_type_for_user",
            no_task="log_timeoff_template_not_present"
        )

        log_timeoff_template_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_template_not_present',
            log='{{result("create_employee_log")}}',
            items='{{result("query_employee_data")}}',
            message='Time Off Template is not assigned to the User',
            severity='Exception',
            properties={
                'sf_booking_id': "{{item.sf_booking_id}}",
                'employee_id': "{{item.employee_id}}",
                'time_off_type_description': "{{item.time_off_type_description}}",
                'action':'Validation',
                'status': 'Exception',
                'details': 'Time Off Template is not assigned to the User'
            }
        )

        get_all_assigned_time_off_type_for_user = rail.RepliconServiceOperator(
            task_id='get_all_assigned_time_off_type_for_user',
            endpoint='/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser',
            data={
                "userUri": "{{result('get_user_on_empid').0.uri}}"
            },
            data_handler=lambda response: list(map(lambda row: row['uri'], response))
        )

        process_each_timeoff_entry= rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_entry',
            retries=0,
            items="{{ result('query_employee_data') }}",
            trigger_dag_id=config.process_each_time_off_entry_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_child_conf
        )

        wait_for_process_each_timeoff_entry = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff_entry',
            dag_runs='{{ result("process_each_timeoff_entry") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{result("create_employee_log")}}',
            items='{{result("query_employee_data")}}',\
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                 'sf_booking_id': "{{item.sf_booking_id}}",
                'employee_id': "{{item.employee_id}}",
                'time_off_type_description': "{{item.time_off_type_description}}",
                'action':'Sync',
                'status': 'Exception',
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_employee_log

        create_employee_log >> query_employee_data >> get_user_on_empid >> is_user_present
        is_user_present >> rail.Label(
            'No') >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label(
            'Yes') >> get_user_info >> is_timeoff_template_present
        is_timeoff_template_present >> rail.Label(
            'No') >> log_timeoff_template_not_present >> catch_and_log_errors
        is_timeoff_template_present >> rail.Label(
            'Yes') >> get_all_assigned_time_off_type_for_user >> process_each_timeoff_entry
        process_each_timeoff_entry >> wait_for_process_each_timeoff_entry >>  catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
