from datetime import timedelta
import pendulum
import rail
from cie_wipro.ksa_absconding_employee_notification.utils import request_payload, python_callable_method


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f"{dag_id_prefix}{config.company_key}_employee_absconding_to_{location}master{dag_id_postfix}".lower(),
        description=f"Auto_Approval {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1
    ) as dag:

        current_datetime = python_callable_method.get_timenow(config)
        # current_datetime = python_callable_method.get_timenow(config) - timedelta(days=122)
        # current_date = current_datetime.strftime(config.report_date_format)
        start_datetime = current_datetime - timedelta(days=config.days)
        # start_date = start_datetime.strftime(config.report_date_format)

        get_to_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_to_report_details',
            report_name=config.to_report_name,
        )

        run_report_to = rail.run_report2(
            group_id='run_to_report',
            report_params=lambda: request_payload.get_to_report_filter_uris(
                config, start_datetime, current_datetime),
            target='result',
            replicon_conn_id=config.replicon_conn_id
        )

        get_holiday_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_holiday_report_details',
            report_name=config.hol_report_name,
        )

        run_report_holiday = rail.run_report2(
            group_id='run_holiday_report',
            report_params=lambda: request_payload.get_holiday_report_filter_uris(
                config, start_datetime, current_datetime),
            target='result',
            replicon_conn_id=config.replicon_conn_id
        )

        get_timeoff_report_data = rail.PythonOperator(
            task_id="get_timeoff_report_data",
            python_callable=python_callable_method.get_timeoff_report_data,
            op_args=[config]
        )

        final_list_has_data = rail.IfOperator(
            task_id='final_list_has_data',
            test="{{ result('get_timeoff_report_data') | length > 0 }}",
            yes_task='gpo_and_hr_manager_empid',
            no_task='finish'
        )

        gpo_and_hr_manager_empid = rail.PythonOperator(
            task_id="gpo_and_hr_manager_empid",
            python_callable=python_callable_method.get_gpo_and_hrs_empid,
        )

        get_gpo_and_hr_manager_data = rail.RepliconServiceOperator(
            task_id="get_gpo_and_hr_manager_data",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=request_payload.bulk_get_user2,
            response_filter=lambda response: python_callable_method.get_gpo_and_hrs_uri(
                response)
        )

        get_gpo_and_manager_details = rail.RepliconServiceOperator(
            task_id="get_gpo_and_manager_details",
            endpoint="/services/UserService1.svc/BulkGetUserDetails",
            data=request_payload.bulk_get_user_details,
            response_filter=lambda response: python_callable_method.map_empid_to_uri(
                 response)
        )

        update_timeoff_report_data = rail.PythonOperator(
            task_id='update_timeoff_report_data',
            python_callable=python_callable_method.update_timeoff_report_data
        )

        group_data_by_notification_step = rail.PythonOperator(
            task_id='group_data_by_notification_step',
            python_callable=python_callable_method.group_data_by_notification_step,
            # op_args=[config]
        )

        process_first_notification_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_first_notification_batch',
            items=lambda: [0] if config.firstReminder in rail.result(
                "group_data_by_notification_step") else [],
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_send_1st_notification_{config.location}{dag_id_postfix}_child_v1'.lower(
            ),
            conf=lambda item: {
                'user_list': rail.result("group_data_by_notification_step").get(config.firstReminder),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_first_notification_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_first_notification_batch',
            dag_runs='{{ result("process_first_notification_batch") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_first_notification_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_first_notification_data',
            dag_runs="{{ result('process_first_notification_batch') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        process_second_notification_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_second_notification_batch',
            items=lambda: [0] if config.secondReminder in rail.result(
                "group_data_by_notification_step") else [],
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_send_2nd_notification_{config.location}{dag_id_postfix}_child_v1'.lower(
            ),
            conf=lambda item: {
                'user_list': rail.result("group_data_by_notification_step").get(config.secondReminder),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_second_notification_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_second_notification_batch',
            dag_runs='{{ result("process_second_notification_batch") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_second_notification_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_second_notification_data',
            dag_runs="{{ result('process_second_notification_batch') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        process_third_notification_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_third_notification_batch',
            items=lambda: [0] if config.thirdReminder in rail.result(
                "group_data_by_notification_step") else [],
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_send_3rd_notification_{config.location}{dag_id_postfix}_child_v1'.lower(
            ),
            conf=lambda item: {
                'user_list': rail.result("group_data_by_notification_step").get(config.thirdReminder),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_third_notification_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_third_notification_batch',
            dag_runs='{{ result("process_third_notification_batch") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_third_notification_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_third_notification_data',
            dag_runs="{{ result('process_third_notification_batch') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        process_forth_notification_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_forth_notification_batch',
            items=lambda: [0] if config.forthReminder in rail.result(
                "group_data_by_notification_step") else [],
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_send_4th_notification_{config.location}{dag_id_postfix}_child_v1'.lower(
            ),
            conf=lambda item: {
                'user_list': rail.result("group_data_by_notification_step").get(config.forthReminder),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_forth_notification_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_forth_notification_batch',
            dag_runs='{{ result("process_forth_notification_batch") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_forth_notification_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_forth_notification_data',
            dag_runs="{{ result('process_forth_notification_batch') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        generate_merged_log_data = rail.PythonOperator(
            task_id='generate_merged_log_data',
            execution_timeout=timedelta(days=14),
            python_callable=python_callable_method.get_merged_logs_data,
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.email_distro_list,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Absconding Employee - Run Successfully - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/email_for_success_format.html",
        )

        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Absconding Employee - failed to run - {{ current_time_in_specified_tz() }}",
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

        get_to_report_details >> run_report_to >> get_holiday_report_details >> run_report_holiday >> get_timeoff_report_data >> final_list_has_data >> rail.Label("Yes") >> gpo_and_hr_manager_empid >>\
            get_gpo_and_hr_manager_data >> get_gpo_and_manager_details >> update_timeoff_report_data >> group_data_by_notification_step >>\
            process_first_notification_batch >> wait_for_process_first_notification_batch >> gather_first_notification_data >>\
            process_second_notification_batch >> wait_for_process_second_notification_batch >> gather_second_notification_data >>\
            process_third_notification_batch >> wait_for_process_third_notification_batch >> gather_third_notification_data >>\
            process_forth_notification_batch >> wait_for_process_forth_notification_batch >> gather_forth_notification_data >>\
            generate_merged_log_data >> send_task_completion_email >> send_task_failure_email >> finish >> log_to_sumo >> final_status

        final_list_has_data >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
