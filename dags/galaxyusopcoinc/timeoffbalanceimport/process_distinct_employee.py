from datetime import timedelta
from airflow.models import Variable
import rail
from galaxyusopcoinc.timeoffbalanceimport.utils import response_filter
from galaxyusopcoinc.timeoffbalanceimport.utils import request_payload
from galaxyusopcoinc.timeoffbalanceimport.utils import python_callable_method


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoffbalance_import_process_employee_child_{config.instance}',
        description='Vialto Partners Timeoff Balance Import Process Distinct Employee',
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
            no_task='create_employee_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout),
            start_task='create_employee_log',
            end_task='catch_and_log_errors',
        )

        create_employee_log = rail.CreateLogOperator(
            task_id='create_employee_log'
        )

        query_employee_data = rail.QueryCollectionOperator(
            task_id="query_employee_data",
            query="""SELECT * FROM query_valid_records WHERE employeeid='{{dag_run.conf.employeeid}}' """
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
            yes_task="get_user_time_off_types",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log = '{{result("create_employee_log")}}',
            items='{{result("query_employee_data")}}',
            message='User with employeeid "{{dag_run.conf.employeeid}}" is not present/disabled in replicon',
            severity='Exception',
            properties=lambda item: {
                'batchid': item['batchid'],
                'employeeid': item['employeeid'],
                'referenceid':  item['referenceid'],
                'status': 'Exception',
            }
        )

        get_user_time_off_types = rail.PythonOperator(
            task_id='get_user_time_off_types',
            python_callable=python_callable_method.get_user_time_off_types
        )

        is_time_off_enabled = rail.IfOperator(
            task_id="is_time_off_enabled",
            test=request_payload.is_time_off_enabled_test,
            yes_task="get_user_time_off_policy_summary",
            no_task="enable_time_off"
        )

        enable_time_off = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_time_off',
            endpoint='/services/TimeOffService1.svc/EnableTimeOffType',
            items=request_payload.get_timeoff_uri,
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            data=request_payload.get_enable_timeoff_payload(
                '{{item.timeoffuri}}')
        )

        get_user_time_off_policy_summary = rail.RepliconServiceOperator(
            task_id="get_user_time_off_policy_summary",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=request_payload.get_user_time_off_policy_summary_payload,
            response_filter=response_filter.get_filtered_timeoff_summary
        )

        is_all_time_off_policy_enabled = rail.IfOperator(
            task_id="is_all_time_off_policy_enabled",
            test=request_payload.is_all_timeoff_policy_enabled_test,
            yes_task="process_each_time_off_policy",
            no_task="get_all_enabled_timeoff_policy_uris"
        )

        get_all_enabled_timeoff_policy_uris = rail.PythonOperator(
            task_id='get_all_enabled_timeoff_policy_uris',
            python_callable=python_callable_method.get_enabled_timeoff_uris,
        )

        put_timeoff_type_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_timeoff_type_assignment_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_type_assignment_for_user_payload,
        )

        process_each_time_off_policy = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_time_off_policy',
            retries=0,
            items="{{ result('query_employee_data') }}",
            execution_timeout=timedelta(
                days=config.child_process_execution_timeout),
            trigger_dag_id=f'vialtopartners_timeoffbalance_import_process_time_off_policy_child_{config.instance}',
            conf=request_payload.get_each_time_off_policy_conf
        )

        wait_for_process_each_time_off_policy = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_time_off_policy',
            dag_runs='{{ result("process_each_time_off_policy") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{result("create_employee_log")}}',
            trigger_rule='one_failed',
            items='{{result("query_employee_data")}}',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties=lambda item: {
                'batchid': item['batchid'],
                'employeeid': item['employeeid'],
                'referenceid':  item['referenceid'],
                'status': 'Error',
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_employee_log

        create_employee_log >> query_employee_data >> get_user_on_empid >> user_details >> is_user_present
        is_user_present >> rail.Label(
            'No') >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label('Yes') >> get_user_time_off_types >> is_time_off_enabled >> rail.Label(
            'No') >> enable_time_off >> get_user_time_off_policy_summary
        is_time_off_enabled >> rail.Label(
            'Yes') >> get_user_time_off_policy_summary >> is_all_time_off_policy_enabled
        is_all_time_off_policy_enabled >> rail.Label(
            'Yes') >> process_each_time_off_policy
        is_all_time_off_policy_enabled >> rail.Label(
            'No') >> get_all_enabled_timeoff_policy_uris >> put_timeoff_type_assignment_for_user
        put_timeoff_type_assignment_for_user >> process_each_time_off_policy >> wait_for_process_each_time_off_policy >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
