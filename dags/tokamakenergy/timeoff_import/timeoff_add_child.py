from datetime import timedelta
import rail
from tokamakenergy.timeoff_import.utils import request_payload
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timeoff_add_child,
        description=f'Tokamak Timeoff Sync Add Child {config.instance}',
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
            no_task='is_total_hours_positive'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_total_hours_positive',
            end_task='catch_and_log_errors',
        )

        is_total_hours_positive = rail.IfOperator(
            task_id='is_total_hours_positive',
            test=lambda dag_run: float(dag_run.conf['amount']['amount']) > 0.0,
            yes_task='add_timeoff',
            no_task='log_timeoff_is_not_valid'
        )

        add_timeoff = rail.RepliconServiceOperator(
            task_id='add_timeoff',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=lambda dag_run: request_payload.get_put_and_submit_timeoff_payload(dag_run, 'add')
        )

        log_booking_add_successful = rail.WriteLogOperator(
            task_id='log_booking_add_successful',
            log='{{ dag_run.conf.create_log }}',
            message='Timeoff Synced Successfully',
            severity='Success',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employeeNumber"],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Success",
                "details": "Time Off {{ dag_run.conf.timeoff_name }} is added successfully.",
            }
        )

        log_timeoff_is_not_valid = rail.WriteLogOperator(
            task_id='log_timeoff_is_not_valid',
            log='{{ dag_run.conf.create_log }}',
            message='Timeoff Add Skipped',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employeeNumber"],
                "booking_id": dag_run.conf["id"],
                "start_date": dag_run.conf["start"],
                "end_date": dag_run.conf["end"],
                "status": "Skipped",
                "details": "Time Off {{ dag_run.conf.timeoff_name }} is Invalid because total timeoff hours is {{ dag_run.conf.amount.amount }}.",
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
        can_run_batch_task >> rail.Label("No") >> is_total_hours_positive
        is_total_hours_positive >> rail.Label("Yes") >> add_timeoff >> \
            log_booking_add_successful >> catch_and_log_errors
        is_total_hours_positive >> rail.Label("No") >> log_timeoff_is_not_valid >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
