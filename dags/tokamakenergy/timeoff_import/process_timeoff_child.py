from datetime import timedelta, datetime
import rail
from tokamakenergy.timeoff_import.utils import python_callable
from tokamakenergy.timeoff_import.utils import request_payload
from tokamakenergy.timeoff_import.utils import response_filter
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_child,
        description=f'Tokamak Timeoff Sync Process Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_timeoff_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_process_timeoff_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_process_timeoff_log',
            end_task='catch_and_log_errors',
        )

        create_process_timeoff_log = rail.CreateLogOperator(
            task_id='create_process_timeoff_log'
        )

        def get_endpoint_detail(dag_run):
            endpoint = f"/employees/{dag_run.conf['employeeId']}/?fields=firstName,lastName,employeeNumber&onlyCurrent=true"
            return endpoint
        
        get_endpoint = rail.PythonOperator(
            task_id='get_endpoint',
            python_callable=get_endpoint_detail
        )

        #https://api.bamboohr.com/api/gateway.php/tokamakenergytest/v1/employees/1370/?fields=firstName,lastName,employeeNumber&onlyCurrent=true
        get_employee_number = rail.BambooHROperator(
            task_id='get_employee_number',
            company_domain=config.company_domain,
            request_method='GET',
            endpoint="{{result('get_endpoint')}}",
            bamboohr_conn_id=config.bamboohr_conn_id
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_bulk_users_payload,
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_user_present_and_enabled = rail.IfOperator(
            task_id='is_user_present_and_enabled',
            test='{{ result("get_user_info") | is_truthy and result("get_user_info").userDetails.isEnabled | is_truthy }}',
            yes_task='is_timeoff_type_present',
            no_task='log_user_not_present'
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ result("create_process_timeoff_log") }}',
            message="User not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": rail.result("get_employee_number")['employeeNumber'],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Skipped",
                "details": "User " + str(rail.result("get_employee_number")['employeeNumber']) + " not present in Replicon OR in Disabled State.",
            }
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test='{{ dag_run.conf.timeoff_type_uri | is_truthy }}',
            yes_task='is_timeoff_type_assigned_to_user',
            no_task='log_timeoff_type_not_present'
        )

        log_timeoff_type_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_present',
            log='{{ result("create_process_timeoff_log") }}',
            message='Time Off type {{ dag_run.conf.timeoff_name }} not available in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": rail.result("get_employee_number")['employeeNumber'],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Skipped",
                "details": "Time Off type " + str(dag_run.conf['timeoff_name']) + " not available in Replicon",
            }
        )

        is_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id='is_timeoff_type_assigned_to_user',
            test=python_callable.check_timeoff_type_assigned_to_user,
            yes_task='get_time_off_details_on_booking_id',
            no_task='log_timeoff_type_not_assigned_to_user'
        )

        log_timeoff_type_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_assigned_to_user',
            log='{{ result("create_process_timeoff_log") }}',
            message='Time Off type {{ dag_run.conf.timeoff_name }} is not assigned to user in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": rail.result("get_employee_number")['employeeNumber'],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Skipped",
                "details": "Time Off type " + str(dag_run.conf['timeoff_name']) + " is not assigned to user in Replicon",
            }
        )

        get_time_off_details_on_booking_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_booking_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_booking_id,
            data_handler=response_filter.get_filtered_time_off_details_on_booking_id
        )

        is_timeoff_present_in_instance = rail.IfOperator(
            task_id='is_timeoff_present_in_instance',
            test="{{ result('get_time_off_details_on_booking_id') | is_truthy }}",
            yes_task='process_timeoff_update_delete',
            no_task='is_timeoff_status_canceled'
        )

        process_timeoff_update_delete = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_update_delete',
            items=['1'],
            trigger_dag_id=config.timeoff_booking_update_delete_child,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                **dag_run.conf,
                'user_uri': rail.result("get_user_info")["userDetails"]["uri"],
                'timeoff_uri': rail.result('get_time_off_details_on_booking_id')[0]['timeoff_uri'],
                'approval_status': rail.result('get_time_off_details_on_booking_id')[0]['approval_status'],
                'create_log': rail.result('create_process_timeoff_log'),
                'employeeNumber': rail.result("get_employee_number")['employeeNumber']
            }
        )

        wait_for_process_update_delete = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_delete',
            dag_runs='{{ result("process_timeoff_update_delete") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        is_timeoff_status_canceled = rail.IfOperator(
            task_id='is_timeoff_status_canceled',
            test=lambda dag_run: dag_run.conf['status']['status'] == "canceled" or dag_run.conf['status']['status'] == 'superceded',
            yes_task='log_canceled_timeoff_not_present',
            no_task='process_timeoff_add'
        )

        log_canceled_timeoff_not_present = rail.WriteLogOperator(
            task_id='log_canceled_timeoff_not_present',
            log='{{ result("create_process_timeoff_log") }}',
            message='Time Off status is Canceled/Superceded and not present in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": rail.result("get_employee_number")['employeeNumber'],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Skipped",
                "details": "Time Off status is Canceled/Superceded and not present in Replicon",
            }
        )

        process_timeoff_add = rail.TriggerDagRunOperator(
            task_id='process_timeoff_add',
            trigger_dag_id=config.timeoff_add_child,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                **dag_run.conf,
                'user_uri': rail.result("get_user_info")["userDetails"]["uri"],
                'create_log': rail.result('create_process_timeoff_log'),
                'employeeNumber': rail.result("get_employee_number")['employeeNumber']
            }
        )

        wait_for_process_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add',
            dag_runs='{{ result("process_timeoff_add") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_process_timeoff_log") }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "employee_id": "{{ result('get_employee_number').employeeNumber }}",
                "booking_id": "{{ dag_run.conf.id }}",
                "start_date": "{{ dag_run.conf.start }}",
                "end_date": "{{ dag_run.conf.end }}",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_process_timeoff_log
        create_process_timeoff_log >> get_endpoint >> get_employee_number >> get_user_info >> is_user_present_and_enabled
        is_user_present_and_enabled >> rail.Label("Yes") >> is_timeoff_type_present
        is_timeoff_type_present >> rail.Label("Yes") >> is_timeoff_type_assigned_to_user
        is_timeoff_type_present >> rail.Label("No") >> log_timeoff_type_not_present >> catch_and_log_errors
        is_user_present_and_enabled >> rail.Label("No") >> log_user_not_present >> catch_and_log_errors

        is_timeoff_type_assigned_to_user >> rail.Label("Yes") >> get_time_off_details_on_booking_id >> \
        is_timeoff_present_in_instance >> rail.Label("Yes") >> process_timeoff_update_delete >> wait_for_process_update_delete >> catch_and_log_errors
        is_timeoff_present_in_instance >> rail.Label("No") >> is_timeoff_status_canceled
        is_timeoff_status_canceled >> rail.Label("Yes") >> log_canceled_timeoff_not_present >> catch_and_log_errors
        is_timeoff_status_canceled >> rail.Label("No") >> process_timeoff_add
        process_timeoff_add >> wait_for_process_add >> catch_and_log_errors
        is_timeoff_type_assigned_to_user >> rail.Label("No") >> log_timeoff_type_not_assigned_to_user >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
