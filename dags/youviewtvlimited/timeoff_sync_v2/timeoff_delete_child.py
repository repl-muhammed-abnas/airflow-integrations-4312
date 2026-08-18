from datetime import timedelta
import rail
from youviewtvlimited.timeoff_sync_v2.utils import request_payload
from youviewtvlimited.timeoff_sync_v2.utils import response_filters
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_delete_child,
        description=f'Youview TV Timeoff Sync Booking Canceled Deleted Child {config.instance}',
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
            no_task='get_time_off_details_on_booking_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_time_off_details_on_booking_id',
            end_task='catch_and_log_errors',
        )

        get_time_off_details_on_booking_id = rail.RepliconServiceOperator(
            task_id="get_time_off_details_on_booking_id",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_details_on_booking_id,
            data_handler=response_filters.get_filtered_time_off_details_on_booking_id
        )

        is_timeoff_present = rail.IfOperator(
            task_id='is_timeoff_present',
            test='{{ result("get_time_off_details_on_booking_id") | is_truthy }}',
            yes_task='delete_time_off',
            no_task='log_timeoff_not_present'
        )

        log_timeoff_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_not_present',
            log='{{ dag_run.conf.log_artifact}}',
            message="TimeOff not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run: {
                "username": dag_run.conf["booking_data"]["employeeDisplayName"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Skipped",
                "comments": 'TimeOff "'+ dag_run.conf["booking_data"]["policyTypeDisplayName"] + '" is not present in Replicon',
                "dag_run_id": get_dagrun_ecid(dag_run)
            }
        )

        delete_time_off = rail.RepliconServiceOperator(
            task_id='delete_time_off',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('get_time_off_details_on_booking_id')[0].timeoff_uri }}"
            }
        )

        log_booking_successful_deleted = rail.WriteLogOperator(
            task_id='log_booking_successful_deleted',
            log='{{ dag_run.conf.log_artifact }}',
            message='Timeoff Deleted Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "username": dag_run.conf["booking_data"]["employeeDisplayName"],
                "employee_email": dag_run.conf["booking_data"]["employeeEmail"],
                "unique_id": dag_run.conf["booking_data"]["requestId"],
                "booking_start_date": dag_run.conf["booking_data"].get("startDate") or dag_run.conf["booking_data"].get("date"),
                "booking_end_date": dag_run.conf["booking_data"].get("endDate") or dag_run.conf["booking_data"].get("date"),
                "status": "Success",
                "comments": 'Timeoff "'+ dag_run.conf["booking_data"]["policyTypeDisplayName"] + '" Deleted Successfully',
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
                "username": '{{ dag_run.conf.booking_data.employeeDisplayName }}',
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
        can_run_batch_task >> rail.Label("No") >> get_time_off_details_on_booking_id
        get_time_off_details_on_booking_id >> is_timeoff_present
        is_timeoff_present >> rail.Label("Yes") >> delete_time_off >> log_booking_successful_deleted >> catch_and_log_errors
        is_timeoff_present >> rail.Label("No") >> log_timeoff_not_present >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
