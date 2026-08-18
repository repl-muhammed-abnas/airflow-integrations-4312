from datetime import timedelta
import rail
from avenu.user_import.utils import request_payload
from avenu.user_import.utils import python_callable_method
from avenu.user_import.utils import response_filter
from avenu.user_import.task.process_supervisor import process_supervisor_assignment_task_group
from airflow.models import Variable


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_new_user_{config.instance}_child',
        description='Avenu User Sync Process New User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "has_valid_add_fields"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_valid_add_fields',
            end_task="catch_and_log_errors",
        )

        has_valid_add_fields = rail.IfOperator(
            task_id='has_valid_add_fields',
            test=request_payload.test_valid_fields,
            yes_task="is_hiredate_present",
            no_task="log_invalid_add_fields"
        )

        is_hiredate_present = rail.IfOperator(
            task_id='is_hiredate_present',
            test=request_payload.is_hiredate_present,
            yes_task="add_user_exception_log",
            no_task="log_invalid_hiredate"
        )

        log_invalid_add_fields = rail.WriteLogOperator(
            task_id='log_invalid_add_fields',
            message=request_payload.get_invalid_fields_message,
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        log_invalid_hiredate = rail.WriteLogOperator(
            task_id='log_invalid_hiredate',
            message='Hire date is not present',
            severity='Exception',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                'status': 'Exception',
            }
        )

        add_user_exception_log = rail.CreateLogOperator(
            task_id='add_user_exception_log'
        )

        add_user_error_logs = rail.CreateLogOperator(
            task_id='add_user_error_logs'
        )

        is_status_leave = rail.IfOperator(
            task_id='is_status_leave',
            test=request_payload.user_position_status_check,
            yes_task='log_user_in_leave_status',
            no_task='add_new_user'
        )

        log_user_in_leave_status = rail.WriteLogOperator(
            task_id='log_user_in_leave_status',
            message="Position status is incorrect",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Exception',
            },
        )

        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_put_user_payload
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_remove_timeoff_payload
        )

        add_hourly_rate = rail.RepliconServiceOperator(
            task_id='add_hourly_rate',
            endpoint='/services/PayrollService1.svc/UpdateUserPayrollRateScheduleOverDateRange',
            data=request_payload.add_hourly_rate,
        )

        get_file_id_uri = rail.RepliconServiceOperator(
            task_id='get_file_id_uri',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data=request_payload.get_file_id_uri,
            response_filter=response_filter.get_file_id_uri
        )

        add_file_id = rail.RepliconServiceOperator(
            task_id='add_file_id',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data=request_payload.add_file_id,
        )

        is_employee_non_exmept = rail.IfOperator(
            task_id='is_employee_non_exmept',
            test=request_payload.test_non_exempt_employee_type,
            yes_task='assign_punch_entry_policy',
            no_task='update_cost_normalization_rule'
        )

        assign_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='assign_punch_entry_policy',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data=lambda dag_run: request_payload.assign_punch_entry_policy(
                dag_run, config),
        )

        update_cost_normalization_rule = rail.RepliconServiceOperator(
            task_id= "update_cost_normalization_rule",
            endpoint="/services/CostNormalizationRuleService1.svc/UpdateUserCostNormalizationRuleAssignmentScheduleOverDateRange",
            data=request_payload.get_cost_normalization_payload_add
        )

        process_supervisor_task_entry, process_supervisor_task_exit = process_supervisor_assignment_task_group(
            'add_new_user', 'new_user')

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: dag_run.conf['reportstoid'] and dag_run.conf['reportstoname'],
            yes_task=process_supervisor_task_entry.task_id,
            no_task='log_supervisor_not_in_feedfile'
        )

        log_supervisor_not_in_feedfile = rail.WriteLogOperator(
            task_id='log_supervisor_not_in_feedfile',
            message="Supervisor details not present in feed file",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Exception',
            },
        )


        process_time_off_assignment = rail.TriggerDagRunOperator(
            task_id='process_time_off_assignment',
            trigger_dag_id=f'avenu_user_sync_process_time_off_assignment_new_user_{config.instance}_child',
            conf=lambda dag_run: request_payload.get_process_time_off_assignment_conf(
                dag_run, 'new_user'),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_time_off_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_assignment',
            dag_runs='{{ result("process_time_off_assignment") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_exception_logs = rail.PythonOperator(
            task_id='get_all_exception_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['add_user_exception_log']
        )

        get_all_error_logs = rail.PythonOperator(
            task_id='get_all_error_logs',
            python_callable=python_callable_method.get_user_logs_by_status,
            op_args=['add_user_error_logs']
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message=request_payload.get_add_completion_message,
            severity=request_payload.get_add_severity,
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

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> has_valid_add_fields >> rail.Label(
            'No') >> log_invalid_add_fields >> catch_and_log_errors
        has_valid_add_fields >> rail.Label('Yes') >> is_hiredate_present >> rail.Label(
            "Yes") >> add_user_exception_log
        is_hiredate_present >> rail.Label(
            "No") >> log_invalid_hiredate >> catch_and_log_errors
        add_user_exception_log >> add_user_error_logs >> is_status_leave >> rail.Label(
            "Yes") >> add_new_user >> remove_timeoff_assignments >> add_hourly_rate >> get_file_id_uri >> add_file_id
        is_status_leave >> rail.Label(
            "No") >> log_user_in_leave_status >> catch_and_log_errors
        add_file_id >> is_employee_non_exmept >> rail.Label("Yes") >> assign_punch_entry_policy >> is_supervisor_in_feed_file >> rail.Label(
            'Yes') >> process_supervisor_task_entry
        is_employee_non_exmept >> rail.Label(
            "No") >> update_cost_normalization_rule >> is_supervisor_in_feed_file
        is_supervisor_in_feed_file >> rail.Label(
            'No') >> log_supervisor_not_in_feedfile >> process_time_off_assignment
        process_supervisor_task_exit >> process_time_off_assignment
        process_time_off_assignment >> wait_for_process_time_off_assignment >> \
            get_all_exception_logs >> get_all_error_logs >> log_completion >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag_wbs)
