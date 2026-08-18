from datetime import timedelta
import pendulum
import rail
from cie_crl.auto_approve_ts_and_to.utils import request_payload, python_callable_method


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f"{dag_id_prefix}{config.company_key}_timesheet_auto_approval_{location}master{dag_id_postfix}".lower(),
        description=f"Auto_Approval {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1
    ) as dag:

        holiday_calender = rail.RepliconServiceOperator(
            task_id='holiday_calender',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.holiday_calender_name)
        )

        get_current_date = rail.PythonOperator(
            task_id='get_current_date',
            python_callable=lambda: pendulum.now(config.time_zone).strftime(config.date_format),
        )

        get_holidays_for_current_date = rail.RepliconServiceOperator(
            task_id="get_holidays_for_current_date",
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data=lambda: request_payload.get_holiday_payload(config)
        )

        is_it_a_holiday = rail.IfOperator(
            task_id='is_it_a_holiday',
            test="{{ result('get_holidays_for_current_date') | length > 0 }}",
            yes_task='get_ts_report_details',
            no_task='no_payroll_day'
        )

        no_payroll_day = rail.EmptyOperator(
            task_id='no_payroll_day'
        )

        get_ts_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_ts_report_details',
            report_name=config.ts_report_name
        )

        run_report_for_waiting_ts_entry, run_report_for_waiting_ts_exit = rail.run_report(
            group_id='run_report_for_waiting_ts',
            report_params=lambda: request_payload.get_report_filter_uris(config),
        )

        waiting_report_has_data = rail.IfOperator(
            task_id='waiting_report_has_data',
            test="{{ result('run_report_for_waiting_ts.get_report_result', 'has_data') }}",
            yes_task='get_to_report_details',
            no_task='finish'
        )

        get_to_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_to_report_details',
            report_name=config.to_report_name,
        )

        run_report_for_rejected_to= rail.run_report2(
            group_id='run_to_report',
            report_params=lambda: request_payload.get_to_report_filter_uris(config),
            target='result',
            replicon_conn_id=config.replicon_conn_id
        )

        get_timesheet_waiting_for_approval = rail.PythonOperator(
            task_id="get_timesheet_waiting_for_approval",
            python_callable=python_callable_method.get_timesheet_uri_data,
            op_args=[config]

        )

        process_timesheet_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_child',
            items=lambda: rail.result('get_timesheet_waiting_for_approval'),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{location}child{dag_id_postfix}'.lower(
            ),
            conf=lambda item: {
                "company_key": config.company_key,
                "connection_id": config.replicon_conn_id,
                "item":item
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_timesheet_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_child',
            dag_runs='{{ result("process_timesheet_child") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_entry_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_entry_child_data',
            dag_runs="{{ result('process_timesheet_child') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        get_merged_entries_logs = rail.PythonOperator(
            task_id='get_merged_entries_logs',
            python_callable=python_callable_method.get_error_logs
        )

        merged_entries_logs_has_data = rail.IfOperator(
            task_id='merged_entries_logs_has_data',
            test="{{ result('get_merged_entries_logs') | length > 0 }}",
            yes_task='get_final_process_data_for_email',
            no_task='finish',
        )

        get_final_process_data_for_email = rail.PythonOperator(
            task_id='get_final_process_data_for_email',
            python_callable=python_callable_method.get_user_time_data
        )

        process_email_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_email_child',
            items=lambda: rail.result('get_final_process_data_for_email'),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_email_chunk_{location}child{dag_id_postfix}'.lower(
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_email_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_email_child',
            dag_runs='{{ result("process_email_child") }}',
            execution_timeout=timedelta(days=14),
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.email_distro_list,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Timesheet Approval - Run Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/email_for_success_format.html",
        )

        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Timesheet Approval - failed to Approve Timehseet - {{ current_time_in_specified_tz() }}",
            html_content="templates/email/failure_email.html",
            params={
                'dag_id': f'{config.company_key}_timesheet_approval_master{dag_id_postfix}'.lower()
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )


        holiday_calender >> get_current_date >> get_holidays_for_current_date >> is_it_a_holiday >> rail.Label("Yes") >> get_ts_report_details \
            >> run_report_for_waiting_ts_entry >> run_report_for_waiting_ts_exit >> waiting_report_has_data >> \
                rail.Label("Yes") >> get_to_report_details >> run_report_for_rejected_to \
                    >> get_timesheet_waiting_for_approval >> \
                    process_timesheet_child >> wait_for_process_timesheet_child >> gather_entry_child_data >> get_merged_entries_logs\
                    >> merged_entries_logs_has_data >> rail.Label("Yes") >> \
                    get_final_process_data_for_email >> process_email_child >> wait_for_process_email_child >>  finish

        finish >> send_task_completion_email >> send_task_failure_email >> log_to_sumo >> final_status


        is_it_a_holiday >> rail.Label("No") >> no_payroll_day
        waiting_report_has_data >> rail.Label("No") >> finish
        merged_entries_logs_has_data >> rail.Label("No") >> finish


        return dag

rail.for_each_instance(create_dag)
