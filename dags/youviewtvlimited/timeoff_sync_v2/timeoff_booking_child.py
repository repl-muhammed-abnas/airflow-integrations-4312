from datetime import timedelta
import rail
from youviewtvlimited.timeoff_sync_v2.utils import custom_methods
from youviewtvlimited.timeoff_sync_v2.utils import request_payload
from youviewtvlimited.timeoff_sync_v2.utils import response_filters
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_booking_child,
        description=f'Youview TV Timeoff Sync Booking Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_booking_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_info',
            end_task='catch_and_log_errors',
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_bulk_users_payload,
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else null
        )

        is_user_present = rail.IfOperator(
            task_id='is_user_present',
            test='{{ result("get_user_info") | is_truthy }}',
            yes_task='if_timeoff_type_in_nonsync',
            no_task='log_user_not_present'
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ dag_run.conf.log_artifact}}',
            message="User not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run: {
                "username": dag_run.conf["booking_data"]["employeeDisplayName"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Skipped",
                "comments": "User not present in Replicon",
                "dag_run_id": get_dagrun_ecid(dag_run)
            }
        )

        if_timeoff_type_in_nonsync = rail.IfOperator(
            task_id= "if_timeoff_type_in_nonsync",
            test=lambda dag_run: dag_run.conf["replicon_timeoff_type_name"] is null,
            yes_task="log_timeoff_type_in_not_sync_list",
            no_task="is_timeoff_type_present"
        )

        log_timeoff_type_in_not_sync_list = rail.WriteLogOperator(
            task_id='log_timeoff_type_in_not_sync_list',
            log='{{ dag_run.conf.log_artifact}}',
            message="Timeoff type {{ dag_run.conf.booking_data.policyTypeDisplayName }} is Non-Out Off Office, hence skipped",
            severity='Skipped',
            properties=lambda dag_run: {
                "username": dag_run.conf["booking_data"]["employeeDisplayName"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Skipped",
                "comments": 'Time Off type "'+ dag_run.conf["booking_data"]["policyTypeDisplayName"] + '" is Non-Out Off Office, hence skipped',
                "dag_run_id": get_dagrun_ecid(dag_run)
            }
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test='{{ dag_run.conf.get_absense_time_off_type | is_truthy }}',
            yes_task='is_timeoff_type_assigned_to_user',
            no_task='log_timeoff_type_not_present'
        )

        log_timeoff_type_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_present',
            log='{{ dag_run.conf.log_artifact}}',
            message='"{{ dag_run.conf.replicon_timeoff_type_name }}" Time Off type not available in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Skipped",
                "comments": '"'+ dag_run.conf["replicon_timeoff_type_name"] + '" Time Off type not available in Replicon',
                "dag_run_id": get_dagrun_ecid(dag_run)
            }
        )

        is_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id='is_timeoff_type_assigned_to_user',
            test=custom_methods.check_timeoff_type_assigned_to_user,
            yes_task='put_and_submit_timeoff_booking_for_user',
            no_task='log_timeoff_type_not_assigned_to_user'
        )

        log_timeoff_type_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_assigned_to_user',
            log='{{ dag_run.conf.log_artifact}}',
            message='Time Off type {{ dag_run.conf.booking_data.policyTypeDisplayName }} is not assigned to user in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Skipped",
                "comments": 'Time Off type "'+ dag_run.conf["replicon_timeoff_type_name"] + '" is not assigned to user in Replicon',
                "dag_run_id": get_dagrun_ecid(dag_run)
            }
        )

        put_and_submit_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='put_and_submit_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_put_and_submit_timeoff_payload
        )

        get_time_off_approval_status = rail.RepliconServiceOperator(
            task_id="get_time_off_approval_status",
            endpoint="/services/TimeOffApprovalService1.svc/GetApprovalHistoryDetails",
            data={
                "timeOffUri": "{{ result('put_and_submit_timeoff_booking_for_user').uri }}"
            },
            data_handler = lambda response: response['approvalStatus']['displayText']
        )

        is_timeoff_approved = rail.IfOperator(
            task_id="is_timeoff_approved",
            test=lambda: rail.result('get_time_off_approval_status') == 'Approved',
            yes_task='log_booking_successful',
            no_task='approve_timeoff_booking_for_user'
        )

        approve_timeoff_booking_for_user = rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking_for_user',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=request_payload.get_approve_holiday_booking_payload
        )

        log_booking_successful = rail.WriteLogOperator(
            task_id='log_booking_successful',
            log='{{ dag_run.conf.log_artifact }}',
            message='Timeoff Synced Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "username": rail.result("get_user_info")["userDetails"]["displayText"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Success",
                "comments": 'Timeoff "'+ dag_run.conf["booking_data"]["policyTypeDisplayName"] + '" Synced Successfully',
                "dag_run_id": get_dagrun_ecid(dag_run)
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log_artifact}}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "username": '{{ result("get_user_info").userDetails.displayText }}',
                "employee_email": '{{ dag_run.conf.booking_data.employeeEmail }}',
                "unique_id": '{{ dag_run.conf.booking_data.requestId }}',
                "booking_start_date": '{{ dag_run.conf.booking_data.startDate if "startDate" in dag_run.conf.booking_data else dag_run.conf.booking_data.date }}',
                "booking_end_date": '{{ dag_run.conf.booking_data.endDate if "endDate" in dag_run.conf.booking_data else dag_run.conf.booking_data.date }}',
                "status": "Error",
                "comments": '{{ get_error_message() }}',
                "dag_run_id": '{{ dag_run_ecid() }}'
            }
        )
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_user_info
        get_user_info >> is_user_present
        is_user_present >> rail.Label("Yes") >> if_timeoff_type_in_nonsync
        if_timeoff_type_in_nonsync >> rail.Label("Yes") >> log_timeoff_type_in_not_sync_list >> catch_and_log_errors
        if_timeoff_type_in_nonsync >> rail.Label("No") >> is_timeoff_type_present
        is_timeoff_type_present >> rail.Label("Yes") >> is_timeoff_type_assigned_to_user
        is_timeoff_type_present >> rail.Label("No") >> log_timeoff_type_not_present >> catch_and_log_errors
        is_user_present >> rail.Label("No") >> log_user_not_present >> catch_and_log_errors

        is_timeoff_type_assigned_to_user >> rail.Label("Yes") >> put_and_submit_timeoff_booking_for_user >> \
        get_time_off_approval_status >> is_timeoff_approved >> rail.Label("Yes") >> log_booking_successful
        is_timeoff_approved >> rail.Label("No") >> approve_timeoff_booking_for_user
        approve_timeoff_booking_for_user >> log_booking_successful >> catch_and_log_errors
        is_timeoff_type_assigned_to_user >> rail.Label("No") >> log_timeoff_type_not_assigned_to_user >> catch_and_log_errors


    return dag

rail.for_each_instance(create_child_dag)
