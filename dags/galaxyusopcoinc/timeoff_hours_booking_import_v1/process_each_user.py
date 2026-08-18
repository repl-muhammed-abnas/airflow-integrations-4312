from datetime import timedelta
import rail
from galaxyusopcoinc.timeoff_hours_booking_import_v1.utils import request_payload
from galaxyusopcoinc.timeoff_hours_booking_import_v1.utils import response_filter,custom_methods
from airflow.models import Variable

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_dag_id,
        description=f'Vialto Timeoff Booking Sync Process Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_user
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_data_from_query'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_data_from_query',
            end_task='catch_and_log_errors',
        )

        get_user_data_from_query = rail.QueryCollectionOperator(
            task_id='get_user_data_from_query',
            query="""SELECT * from uniquedata WHERE employee_id == :Employee_id""",
            query_params={
                'Employee_id': '{{ dag_run.conf.employee_id }}'
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                "users": [{"employeeId": dag_run.conf["employee_id"]}]
            },
            data_handler=response_filter.get_filtered_output_user_info
        )

        is_user_present = rail.IfOperator(
            task_id='is_user_present',
            test='{{ result("get_user_details") | is_truthy }}',
            yes_task='check_if_timeoff_template_assigned',
            no_task='log_user_not_present'
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ dag_run.conf.log }}',
            items='{{ result("get_user_data_from_query") }}',
            message="User not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run, item: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": item["plan_ref_id"],
                "entry_date": item["timeoff_date"],
                "hours": item["hours"],
                "wd_event_id": item["wd_event_id"],
                "status": "Skipped",
                "details": "User " + str(dag_run.conf['employee_id']) + " not present in Replicon",
            }
        )

        check_if_timeoff_template_assigned = rail.IfOperator(
            task_id='check_if_timeoff_template_assigned',
            test=lambda: bool(rail.result('get_user_details')
                              ['timeoff_template']),
            yes_task='get_min_max_dates_from_query',
            no_task='log_timeoff_template_not_assigned'
        )

        log_timeoff_template_not_assigned = rail.WriteLogOperator(
            task_id='log_timeoff_template_not_assigned',
            log='{{ dag_run.conf.log }}',
            items='{{ result("get_user_data_from_query") }}',
            message='Time Off Template is not assigned to the User',
            severity='Skipped',
            properties=lambda dag_run, item: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": item["plan_ref_id"],
                "entry_date": item["timeoff_date"],
                "hours": item["hours"],
                "wd_event_id": item["wd_event_id"],
                "status": "Skipped",
                "details": "Time Off Template is not assigned to the User",
            }
        )

        get_min_max_dates_from_query = rail.QueryCollectionOperator(
            task_id='get_min_max_dates_from_query',
            query="""SELECT MIN(timeoff_date) as start_date, MAX(timeoff_date) as end_date from get_user_data_from_query """,
        )

        get_user_timesheet_details = rail.RepliconServiceOperator(
            task_id="get_user_timesheet_details",
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=request_payload.get_all_timesheet_for_user,
            data_handler=response_filter.get_timesheet_details
        )

        get_all_assigned_time_off_type_for_user = rail.RepliconServiceOperator(
            task_id='get_all_assigned_time_off_type_for_user',
            endpoint='/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser',
            data={
                "userUri": "{{ result('get_user_details').uri }}"
            },
            data_handler=lambda response: list(
                map(lambda row: row['uri'], response))
        )

        process_each_timeoff_per_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_per_user',
            items="{{ result('get_user_data_from_query') }}",
            trigger_dag_id=config.process_each_timeoff_booking,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_child_conf
        )

        wait_for_process_each_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff',
            dag_runs='{{ result("process_each_timeoff_per_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_user_timesheet_details_after_processing = rail.RepliconServiceOperator(
            task_id="get_user_timesheet_details_after_processing",
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=request_payload.get_all_timesheet_for_user,
            data_handler=response_filter.get_timesheet_details_after_process
        )

        is_any_timesheet_reopened = rail.IfOperator(
            task_id='is_any_timesheet_reopened',
            test='{{ result("get_user_timesheet_details_after_processing") | is_truthy }}',
            yes_task='get_email_body',
            no_task='catch_and_log_errors'
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file='templates/emails/employee_email.html',
            target='result',
        )

        send_email_to_user = rail.RepliconServiceOperator(
            task_id='send_email_to_user',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data= custom_methods.get_final_payload_sendemail
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            items='{{ result("get_user_data_from_query") }}',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run, item: {
                "employee_id": dag_run.conf["employee_id"],
                "timeoff_type": item["plan_ref_id"],
                "entry_date": item["timeoff_date"],
                "hours": item["hours"],
                "wd_event_id": item["wd_event_id"],
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            "No") >> get_user_data_from_query >> get_user_details >> is_user_present

        is_user_present >> rail.Label(
            "Yes") >> check_if_timeoff_template_assigned

        is_user_present >> rail.Label(
            "No") >> log_user_not_present >> catch_and_log_errors

        check_if_timeoff_template_assigned >> rail.Label(
            "Yes") >> get_min_max_dates_from_query >> get_user_timesheet_details >> get_all_assigned_time_off_type_for_user >>\
            process_each_timeoff_per_user >> wait_for_process_each_timeoff >> \
                get_user_timesheet_details_after_processing >> is_any_timesheet_reopened

        is_any_timesheet_reopened >> rail.Label(
            "Yes") >> get_email_body >> send_email_to_user >> catch_and_log_errors

        is_any_timesheet_reopened >> rail.Label(
            "No") >> catch_and_log_errors

        check_if_timeoff_template_assigned >> rail.Label(
            "No") >> log_timeoff_template_not_assigned >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
