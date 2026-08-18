from datetime import timedelta
import rail
from galaxyusopcoinc.timeoff_hours_booking_import.utils import request_payload, response_filter
from galaxyusopcoinc.timeoff_hours_booking_import.tasks.update_timeoff_booking import update_timeoff_booking
from galaxyusopcoinc.timeoff_hours_booking_import.tasks.add_timeoff_booking import add_timeoff_booking
from airflow.models import Variable

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_timeoff_booking,
        description=f'Vialto Timeoff Booking Sync Add Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_timeoff_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='false').lower() == 'true',
            yes_task='batch_task',
            no_task='is_timeoff_type_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_timeoff_type_present',
            end_task='catch_and_log_errors',
        )

        is_timeoff_type_present = rail.IfOperator(
            task_id='is_timeoff_type_present',
            test='{{ dag_run.conf.timeoff_uri | is_truthy }}',
            yes_task='is_timeoff_type_assigned_to_user',
            no_task='log_timeoff_type_not_present'
        )

        log_timeoff_type_not_present = rail.WriteLogOperator(
            task_id='log_timeoff_type_not_present',
            log='{{ dag_run.conf.log }}',
            message='Time Off type {{ dag_run.conf.plan_ref_id }} is not available in Replicon',
            severity='Skipped',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                "wd_event_id": dag_run.conf["wd_event_id"],
                "status": "Skipped",
                "details": "Time Off Type " + str(dag_run.conf['plan_ref_id']) + " is not present/disabled in Replicon",
            }
        )

        is_timeoff_type_assigned_to_user = rail.IfOperator(
            task_id='is_timeoff_type_assigned_to_user',
            test=lambda dag_run: bool(
                dag_run.conf['timeoff_uri'] in dag_run.conf['available_timeoff_uris']),
            yes_task='get_time_off_booking_details',
            no_task='assign_timeoff_type_to_user'
        )

        assign_timeoff_type_to_user = rail.RepliconServiceOperator(
            task_id='assign_timeoff_type_to_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_timeoff_add_payload
        )

        get_time_off_booking_details = rail.RepliconServiceOperator(
            task_id="get_time_off_booking_details",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_payload.get_time_off_booking_details,
            data_handler=response_filter.get_filtered_time_off_details_on_sf_booking_id
        )

        is_timeoff_booking_available = rail.IfOperator(
            task_id='is_timeoff_booking_available',
            test='{{ result("get_time_off_booking_details") | is_truthy and dag_run.conf.request_type != "Absence Request" }}',
            yes_task='start_update_timeoff_task',
            no_task='start_add_timeoff_task'
        )

        start_add_timeoff_task = rail.EmptyOperator(
            task_id='start_add_timeoff_task')

        add_timeoff = add_timeoff_booking()

        end_add_timeoff_task = rail.EmptyOperator(
            task_id='end_add_timeoff_task')

        start_update_timeoff_task = rail.EmptyOperator(
            task_id='start_update_timeoff_task')

        update_timeoff = update_timeoff_booking()

        end_update_timeoff_task = rail.EmptyOperator(
            task_id='end_update_timeoff_task')

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": dag_run.conf["plan_ref_id"],
                "entry_date": dag_run.conf["timeoff_date"],
                "hours": dag_run.conf["hours"],
                "wd_event_id": dag_run.conf["wd_event_id"],
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            "No") >> is_timeoff_type_present

        is_timeoff_type_present >> rail.Label(
            "Yes") >> is_timeoff_type_assigned_to_user

        is_timeoff_type_present >> rail.Label(
            "No") >> log_timeoff_type_not_present >> catch_and_log_errors

        is_timeoff_type_assigned_to_user >> rail.Label(
            "Yes") >> get_time_off_booking_details

        is_timeoff_type_assigned_to_user >> rail.Label(
            "No") >> assign_timeoff_type_to_user >> \
            get_time_off_booking_details >> is_timeoff_booking_available

        is_timeoff_booking_available >> rail.Label(
            "Yes") >> start_update_timeoff_task >> update_timeoff >> end_update_timeoff_task >> catch_and_log_errors

        is_timeoff_booking_available >> rail.Label(
            "No") >> start_add_timeoff_task >> add_timeoff >> end_add_timeoff_task >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
