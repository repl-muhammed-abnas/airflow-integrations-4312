from datetime import timedelta
import rail
from bearingpoint.sap_h4s4_timeoff_booking_import.utils import request_payload, response_filter, custom_methods
from airflow.models import Variable

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_dag_id,
        description=f'Bearingpoint Timeoff Booking Sync Process Child {config.instance}',
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
            query="""SELECT * from valid_records WHERE employee_id == :Employee_id""",
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
            yes_task='is_user_enabled',
            no_task='log_user_not_present'
        )

        is_user_enabled = rail.IfOperator(
            task_id = 'is_user_enabled',
            test = lambda: rail.result("get_user_details")['enabled'],
            yes_task= 'query_invalid_date_records',
            no_task= 'log_user_disabled'
        )

        log_user_disabled = rail.WriteLogOperator(
            task_id='log_user_disabled',
            log='{{ dag_run.conf.log }}',
            items='{{ result("get_user_data_from_query") }}',
            message="User disabled in Replicon",
            severity='Skipped',
            properties=lambda dag_run, item: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": item["timeofftype"],
                "startdate": item["startdate"],
                "enddate": item["enddate"],
                "hours": item["hours"],
                "booking_id": item["booking_id"],
                'action': 'Validation',
                "status": "Skipped",
                "details": "User - " + str(dag_run.conf['employee_id']) + " disabled in Replicon",
            }
        )

        query_invalid_date_records = rail.QueryCollectionOperator(
            task_id='query_invalid_date_records',
            query="""SELECT *, CASE
                        WHEN (startdate > enddate) THEN 'invalid_dates'
                        WHEN (:start_date IS NOT NULL AND startdate < :start_date) THEN 'start_date'
                        WHEN (:end_date IS NOT NULL AND enddate > :end_date) THEN 'end_date'
                        ELSE 'Valid'
                    END AS validation
                FROM get_user_data_from_query
                WHERE (startdate > enddate) OR
                    (:start_date IS NOT NULL AND startdate < :start_date)
                    OR
                    (:end_date IS NOT NULL AND enddate > :end_date)""",
            query_params={
                'start_date': '{{ result("get_user_details").start_date }}',
                'end_date': '{{ result("get_user_details").end_date }}',
            }
        )

        has_invalid_date_records = rail.IfOperator(
            task_id='has_invalid_date_records',
            test='{{ result("query_invalid_date_records", "length") > 0 }}',
            yes_task='log_invalid_timeoff_date_records',
            no_task='query_valid_date_records'
        )

        log_invalid_timeoff_date_records = rail.WriteLogOperator(
            task_id='log_invalid_timeoff_date_records',
            items='{{result("query_invalid_date_records")}}',
            log="{{ dag_run.conf.log }}",
            message='Timeoff Startdate/Enddates are invalid',
            severity='Exception',
            properties=lambda dag_run,item: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": item["timeofftype"],
                "startdate": item["startdate"],
                "enddate": item["enddate"],
                "hours": item["hours"],
                "booking_id": item["booking_id"],
                'action': 'Validation',
                "status": "Skipped",
                'details': custom_methods.get_timeoff_dates_exception_message(item)
            }
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ dag_run.conf.log }}',
            items='{{ result("get_user_data_from_query") }}',
            message="User not present in Replicon",
            severity='Skipped',
            properties=lambda dag_run, item: {
                "employee_id": dag_run.conf["employee_id"],
                "timeofftype": item["timeofftype"],
                "startdate": item["startdate"],
                "enddate": item["enddate"],
                "hours": item["hours"],
                "booking_id": item["booking_id"],
                'action': 'Validation',
                "status": "Skipped",
                "details": "User - " + str(dag_run.conf['employee_id']) + " not present in Replicon",
            }
        )

        query_valid_date_records = rail.QueryCollectionOperator(
            task_id='query_valid_date_records',
            query="""SELECT * FROM get_user_data_from_query WHERE
                        (:start_date IS NULL OR startdate >= :start_date) AND
                        (:end_date IS NULL OR enddate <= :end_date) AND
                        (startdate <= enddate) """,
            query_params={
                'start_date': '{{ result("get_user_details").start_date }}',
                'end_date': '{{ result("get_user_details").end_date }}',
            }
        )

        has_valid_date_records = rail.IfOperator(
            task_id='has_valid_date_records',
            test='{{ result("query_valid_date_records", "length") > 0 }}',
            yes_task='check_if_timeoff_template_assigned',
            no_task='catch_and_log_errors'
        )

        check_if_timeoff_template_assigned = rail.IfOperator(
            task_id='check_if_timeoff_template_assigned',
            test=lambda: bool(rail.result('get_user_details')
                              ['timeoff_template']),
            yes_task='get_user_timesheet_details',
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
                "timeofftype": item["timeofftype"],
                "startdate": item["startdate"],
                "enddate": item["enddate"],
                "hours": item["hours"],
                "booking_id": item["booking_id"],
                'action': 'Validation',
                "status": "Skipped",
                "details": "Time Off Template is not assigned to the User",
            }
        )

        get_user_timesheet_details = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_timesheet_details",
            endpoint="/services/TimesheetListService1.svc/GetData",
            items='{{ result("query_valid_date_records") }}',
            data=request_payload.get_all_timesheet_for_user,
            data_handler=response_filter.get_timesheet_details
        )

        map_timesheet_with_user_data = rail.PythonOperator(
            task_id="map_timesheet_with_user_data",
            python_callable=custom_methods.map_timesheet_with_user_data,
            op_args=[query_valid_date_records.task_id,
                     get_user_timesheet_details.task_id],
            show_return_value_in_logs=False
        )

        reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id="reopen_timesheets",
            items="{{result('map_timesheet_with_user_data', 'timesheet_to_reopen') | to_json}}",
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ item.ts_uri }}",
                "unitOfWorkId": "{{ item.unit_of_work_id }}",
                "comments": "Timesheet is reopened by Integration (Time Data Import)"
            }
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

        get_all_timeoff_description_details = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_description_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {
                "timeOffTypeUris": rail.result("get_all_assigned_time_off_type_for_user")
            },
            data_handler=lambda resp: list(map(lambda item: {
                'description': item['description'],
                'uri': item['uri']
            }, resp))
        )

        process_each_timeoff_per_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff_per_user',
            items="{{ result('query_valid_date_records') }}",
            trigger_dag_id=config.process_each_timeoff_booking,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_child_conf
        )

        wait_for_process_each_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff',
            dag_runs='{{ result("process_each_timeoff_per_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_any_timesheet_reopened = rail.IfOperator(
            task_id='is_any_timesheet_reopened',
            test="{{ result('map_timesheet_with_user_data', 'timesheet_to_reopen') | is_truthy }}",
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
            data=custom_methods.get_final_payload_sendemail
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
                "timeofftype": item["timeofftype"],
                "startdate": item["startdate"],
                "enddate": item["enddate"],
                "hours": item["hours"],
                "booking_id": item["booking_id"],
                'action': 'Add',
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            "No") >> get_user_data_from_query >> get_user_details >> is_user_present

        is_user_present >> rail.Label(
            "Yes") >> is_user_enabled >> rail.Label(
                "Yes") >> query_invalid_date_records

        is_user_enabled >> rail.Label(
            "No") >> log_user_disabled >> catch_and_log_errors

        query_invalid_date_records >> has_invalid_date_records

        is_user_present >> rail.Label(
            "No") >> log_user_not_present >> catch_and_log_errors

        has_invalid_date_records >> rail.Label(
            "Yes") >> log_invalid_timeoff_date_records >> query_valid_date_records

        has_invalid_date_records >> rail.Label(
            "No") >> query_valid_date_records >> has_valid_date_records

        has_valid_date_records >> rail.Label(
            "Yes") >> check_if_timeoff_template_assigned

        has_valid_date_records >> rail.Label(
            "No") >> catch_and_log_errors

        check_if_timeoff_template_assigned >> rail.Label(
            "Yes") >> get_user_timesheet_details >> map_timesheet_with_user_data >> reopen_timesheets >>\
                get_all_assigned_time_off_type_for_user >> get_all_timeoff_description_details >>\
                    process_each_timeoff_per_user >> wait_for_process_each_timeoff >> is_any_timesheet_reopened

        is_any_timesheet_reopened >> rail.Label(
            "Yes") >> get_email_body >> send_email_to_user >> catch_and_log_errors

        is_any_timesheet_reopened >> rail.Label(
            "No") >> catch_and_log_errors

        check_if_timeoff_template_assigned >> rail.Label(
            "No") >> log_timeoff_template_not_assigned >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
