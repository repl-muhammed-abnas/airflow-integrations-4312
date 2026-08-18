from datetime import timedelta
import uuid
import rail
from airflow.models import Variable
from tokamakenergy.timeoff_import.utils import request_payload, response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_booking_update_delete_child,
        description=f'Tokamak Timeoff Sync Update Delete Child {config.instance}',
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
            no_task='is_timeoff_status_canceled_or_superceded'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_timeoff_status_canceled_or_superceded',
            end_task='catch_and_log_errors',
        )

        is_timeoff_status_canceled_or_superceded = rail.IfOperator(
            task_id='is_timeoff_status_canceled_or_superceded',
            test=lambda dag_run: (dag_run.conf['status']['status'] == "canceled" or dag_run.conf['status']['status'] == 'superceded'),
            yes_task='delete_time_off',
            no_task='get_time_off_details_to_update'
        )

        get_time_off_details_to_update = rail.RepliconServiceOperator(
            task_id='get_time_off_details_to_update',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetails2",
            data=lambda dag_run: {
                "timeOffUri": dag_run.conf['timeoff_uri']
            },
            data_handler=response_filter.get_time_off_details_to_update
        )

        is_timeoff_hours_mismatch = rail.IfOperator(
            task_id='is_timeoff_hours_mismatch',
            test=lambda: rail.result("get_time_off_details_to_update"),
            yes_task='is_timeoff_not_open',
            no_task='log_no_change_in_timeoff'
        )

        log_no_change_in_timeoff = rail.WriteLogOperator(
            task_id='log_no_change_in_timeoff',
            log='{{ dag_run.conf.create_log }}',
            message='No Change in Timeoff',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employeeNumber"],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Success",
                "details": "No Change in Timeoff.",
            }
        )

        is_timeoff_not_open = rail.IfOperator(
            task_id='is_timeoff_not_open',
            test=lambda dag_run: dag_run.conf['approval_status'] != "Not Submitted",
            yes_task='reopen_timeoff_booking',
            no_task='update_timeoff'
        )

        reopen_timeoff_booking = rail.RepliconServiceOperator(
            task_id='reopen_timeoff_booking',
            endpoint="/services/TimeOffApprovalService1.svc/Reopen",
            data={
                "timeOffUri": "{{ dag_run.conf.timeoff_uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopened by Replicon Integration"
            }
        )

        update_timeoff = rail.RepliconServiceOperator(
            task_id='update_timeoff',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=lambda dag_run: request_payload.get_put_and_submit_timeoff_payload(dag_run, 'update')
        )

        log_booking_update_successful = rail.WriteLogOperator(
            task_id='log_booking_update_successful',
            log='{{ dag_run.conf.create_log }}',
            message='Timeoff Synced Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employeeNumber"],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Success",
                "details": "Time Off {{ dag_run.conf.timeoff_name }} is updated successfully.",
            }
        )

        delete_time_off = rail.RepliconServiceOperator(
            task_id='delete_time_off',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ dag_run.conf.timeoff_uri }}"
            }
        )

        log_booking_successful_deleted = rail.WriteLogOperator(
            task_id='log_booking_successful_deleted',
            log='{{ dag_run.conf.create_log }}',
            message='Timeoff Deleted Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employeeNumber"],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Success",
                "details": "Time Off {{ dag_run.conf.timeoff_name }} is deleted successfully. Because timeoff status was 'canceled/superceded'.",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.create_log}}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "employee_id": "{{ dag_run.conf.employeeNumber }}",
                "booking_id": "{{ dag_run.conf.id }}",
                "start_date": "{{ dag_run.conf.start }}",
                "end_date": "{{ dag_run.conf.end }}",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> is_timeoff_status_canceled_or_superceded
        is_timeoff_status_canceled_or_superceded >> rail.Label("Yes") >> delete_time_off
        is_timeoff_status_canceled_or_superceded >> rail.Label("No") >> get_time_off_details_to_update
        get_time_off_details_to_update >> is_timeoff_hours_mismatch
        is_timeoff_hours_mismatch >> rail.Label("Yes") >> is_timeoff_not_open
        is_timeoff_hours_mismatch >> rail.Label("No") >> log_no_change_in_timeoff >> catch_and_log_errors
        is_timeoff_not_open >> rail.Label("Yes") >> reopen_timeoff_booking >> update_timeoff >> \
            log_booking_update_successful >> catch_and_log_errors
        is_timeoff_not_open >> rail.Label("No") >> update_timeoff
        delete_time_off >> log_booking_successful_deleted >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
