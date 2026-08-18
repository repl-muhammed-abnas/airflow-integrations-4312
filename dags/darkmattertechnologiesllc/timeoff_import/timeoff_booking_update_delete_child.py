from datetime import timedelta
import uuid
import rail
from darkmattertechnologiesllc.timeoff_import.utils import request_payload
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_booking_update_delete_child,
        description=f'Dark Matter Timeoff Sync Update Delete Child {config.instance}',
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
            no_task='get_total_hours'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_total_hours',
            end_task='catch_and_log_errors',
        )

        get_total_hours = rail.PythonOperator(
            task_id='get_total_hours',
            python_callable=lambda dag_run: float(dag_run.conf['total_units']) + float(dag_run.conf['hours'])
        )

        is_total_hours_positive = rail.IfOperator(
            task_id='is_total_hours_positive',
            test=lambda: rail.result('get_total_hours') > 0.0,
            yes_task='is_timeoff_not_open',
            no_task='delete_time_off'
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
                "comments": "Reopened by Integration"
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
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Success",
                "details": "Time Off {{ dag_run.conf.time_off_type }} is updated successfully.",
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
                "employee_id": dag_run.conf["employee_id"],
                "unique_id": dag_run.conf["unique_id"],
                "time_off_date": dag_run.conf["time_off_date"],
                "status": "Success",
                "details": "Time Off {{ dag_run.conf.time_off_type }} is deleted successfully. Because total timeoff hours is {{ result('get_total_hours') }}.",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.create_log}}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "employee_id": '{{ dag_run.conf.employee_id }}',
                "unique_id": '{{ dag_run.conf.unique_id }}',
                "time_off_date": '{{ dag_run.conf.time_off_date }}',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_total_hours
        get_total_hours >> is_total_hours_positive
        is_total_hours_positive >> rail.Label("Yes") >> is_timeoff_not_open >> rail.Label("Yes") >> reopen_timeoff_booking >> update_timeoff >> \
            log_booking_update_successful >> catch_and_log_errors
        is_timeoff_not_open >> rail.Label("Yes") >> update_timeoff
        is_total_hours_positive >> rail.Label("No") >> delete_time_off >> log_booking_successful_deleted >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
