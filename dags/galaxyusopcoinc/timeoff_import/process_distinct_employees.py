from datetime import timedelta
from airflow.models import Variable
import rail
from galaxyusopcoinc.timeoff_import.utils import response_filter
from galaxyusopcoinc.timeoff_import.utils import request_payload
from galaxyusopcoinc.timeoff_import.utils import python_callable_method


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoff_import_child_process_employees_{config.instance}',
        description='Vialto Partners Timeoff Import Process Distinct Employees',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_employees,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_employee_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            start_task='query_employee_data',
            end_task='catch_and_log_errors',
        )

        query_employee_data = rail.QueryCollectionOperator(
            task_id="query_employee_data",
            name='employeedata',
            query="""SELECT DISTINCT * FROM validrecords WHERE employeeid='{{dag_run.conf.employeeid}}' """
        )

        get_user_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_on_empid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_empid_payload,
            response_filter=response_filter.get_filtered_output_empid
        )

        user_details = rail.PythonOperator(
            task_id='user_details',
            python_callable=python_callable_method.get_user_details,
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('user_details')['useruri']),
            yes_task="get_user_info",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            items='{{result("query_employee_data")}}',
            message='User with employeeid "{{dag_run.conf.employeeid}}" is not present/disabled in replicon',
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'referenceid':  item['referenceid'],
                'timeoffentryid': item['timeoffentryid'],
                'status': 'Exception',
            }
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id="get_user_info",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_info_payload,
            response_filter=response_filter.get_filtered_output_user_info
        )

        is_timeoff_template_present = rail.IfOperator(
            task_id="is_timeoff_template_present",
            test=lambda: bool(rail.result('get_user_info')
                              [0]['timeofftemplate']),
            yes_task="get_all_assigned_time_off_type_for_user",
            no_task="log_timeoff_template_not_present"
        )

        log_timeoff_template_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_template_not_present',
            items='{{result("query_employee_data")}}',
            message='Time Off Template is not assigned to the User',
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'referenceid':  item['referenceid'],
                'timeoffentryid': item['timeoffentryid'],
                'status': 'Exception',
            }
        )

        get_all_assigned_time_off_type_for_user = rail.RepliconServiceOperator(
            task_id='get_all_assigned_time_off_type_for_user',
            endpoint='/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser',
            data=request_payload.get_all_assigned_time_off_type_for_user_payload,
            response_filter=response_filter.get_assigned_time_off_uris
        )

        query_employee_data_non_rfl_flag = rail.QueryCollectionOperator(
            task_id="query_employee_data_non_rfl_flag",
            query="""SELECT * FROM employeedata WHERE flag!='RFL' and flag!='LOA-R'"""
        )

        process_each_timeoff_entry_non_rfl = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_entry_non_rfl',
            retries=0,
            items="{{ result('query_employee_data_non_rfl_flag') }}",
            trigger_dag_id=f'vialtopartners_timeoff_import_child_process_each_timeoff_entry_{config.instance}',
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            conf=request_payload.get_child_conf
        )

        wait_for_process_each_timeoff_entry_non_rfl = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff_entry_non_rfl',
            dag_runs='{{ result("process_each_timeoff_entry_non_rfl") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        query_employee_data_rfl_flag = rail.QueryCollectionOperator(
            task_id="query_employee_data_rfl_flag",
            query="""SELECT * FROM employeedata WHERE flag='RFL'"""
        )

        process_each_timeoff_entry_rfl = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_entry_rfl',
            retries=0,
            items="{{ result('query_employee_data_rfl_flag') }}",
            trigger_dag_id=f'vialtopartners_timeoff_import_child_process_each_timeoff_entry_{config.instance}',
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            conf=request_payload.get_child_conf
        )

        wait_for_process_each_timeoff_entry_rfl = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff_entry_rfl',
            dag_runs='{{ result("process_each_timeoff_entry_rfl") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        query_employee_data_delete_flag = rail.QueryCollectionOperator(
            task_id="query_employee_data_delete_flag",
            query="""SELECT * FROM employeedata WHERE flag='LOA-R'"""
        )

        process_each_timeoff_entry_delete = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_entry_delete',
            retries=0,
            items="{{ result('query_employee_data_delete_flag') }}",
            trigger_dag_id=f'vialtopartners_timeoff_import_child_process_each_timeoff_entry_{config.instance}',
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            conf=request_payload.get_child_conf
        )

        wait_for_process_each_timeoff_entry_delete = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff_entry_delete',
            dag_runs='{{ result("process_each_timeoff_entry_delete") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            items='{{result("query_employee_data")}}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'referenceid':  item['referenceid'],
                'timeoffentryid': item['timeoffentryid'],
                'status': 'Error',
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> query_employee_data

        query_employee_data >> get_user_on_empid >> user_details >> is_user_present
        is_user_present >> rail.Label(
            'No') >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label(
            'Yes') >> get_user_info >> is_timeoff_template_present
        is_timeoff_template_present >> rail.Label(
            'No') >> log_timeoff_template_not_present >> catch_and_log_errors
        is_timeoff_template_present >> rail.Label(
            'Yes') >> get_all_assigned_time_off_type_for_user >> query_employee_data_non_rfl_flag
        query_employee_data_non_rfl_flag >> process_each_timeoff_entry_non_rfl >> wait_for_process_each_timeoff_entry_non_rfl
        wait_for_process_each_timeoff_entry_non_rfl >> query_employee_data_rfl_flag >> process_each_timeoff_entry_rfl
        process_each_timeoff_entry_rfl >> wait_for_process_each_timeoff_entry_rfl >> query_employee_data_delete_flag
        query_employee_data_delete_flag >> process_each_timeoff_entry_delete >> wait_for_process_each_timeoff_entry_delete >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
