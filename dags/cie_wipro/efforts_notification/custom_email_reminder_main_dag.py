# pylint: disable=unnecessary-lambda,line-too-long,too-many-statements
# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dags/cie_wipro/efforts_notification/config.py
from datetime import timedelta
import pendulum
import rail
from cie_wipro.efforts_notification.utils import python_callable


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_efforts_custom_email_notification_{config.country}{dag_id_postfix}_master_v1'.lower(),
        description=f'Custom email notification for Efforts - {dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=timedelta(minutes=5),
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.instance_tz),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        current_datetime = python_callable.get_eastern_timenow(config)
        current_date = current_datetime.strftime('%b %d, %Y')
        start_datetime = current_datetime - timedelta(days=30)
        start_date = start_datetime.strftime('%b %d, %Y')

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: python_callable.findItemByDisplayText(
                response, config.report_name)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )
        get_schedule_report_in_batch = rail.run_report2(
            group_id='get_schedule_report_in_batch',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details').get('uri'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                                "value": None,
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                                "value": start_date,
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                                "value": current_date,
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        filter_report_data = rail.PythonOperator(
            task_id='filter_report_data',
            python_callable=python_callable.get_report_data,
            op_args=[config, [config.trigger_1, config.final_trigger],
                     start_datetime, current_datetime]
        )

        group_data_by_notification_step = rail.PythonOperator(
            task_id='group_data_by_notification_step',
            python_callable=python_callable.group_data_by_notification_step,
            # op_args=[config]
        )

        process_first_notification_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_first_notification_batch',
            items=lambda: [0] if f"{config.trigger_1}" in rail.result(
                "group_data_by_notification_step") else [],
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_send_first_notification_{config.country}{dag_id_postfix}_child_v1'.lower(
            ),
            conf=lambda item: {
                'user_list': rail.result("group_data_by_notification_step").get(f"{config.trigger_1}"),
                'booking_date': config.booking_date,
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_for_process_first_notification_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_first_notification_batch',
            dag_runs='{{ result("process_first_notification_batch") }}',
            execution_timeout=timedelta(days=14),
        )
        gather_first_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_first_child_data',
            dag_runs="{{ result('process_first_notification_batch') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )
        process_final_notification_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_final_notification_batch',
            items=lambda: [0] if f"{config.final_trigger}" in rail.result(
                "group_data_by_notification_step") else [],
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_send_final_notification_{config.country}{dag_id_postfix}_child_v1'.lower(
            ),
            conf=lambda item: {
                'user_list': rail.result("group_data_by_notification_step").get(f"{config.final_trigger}"),
                'booking_date': config.booking_date,
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_for_process_final_notification_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_final_notification_batch',
            dag_runs='{{ result("process_final_notification_batch") }}',
            execution_timeout=timedelta(days=14),
        )
        gather_final_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_final_child_data',
            dag_runs="{{ result('process_final_notification_batch') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )
        generate_merged_log_data = rail.PythonOperator(
            task_id='generate_merged_log_data',
            execution_timeout=timedelta(days=14),
            python_callable=python_callable.get_merged_logs_data,
        )
        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Custom Efforts Notification - Run Successfully - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content="templates/emails/email_reminder_complete.html",
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Custom Efforts Notification - failed to Send Reminder - {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/failure_email.html',
            params={
                'dag_id': f'{dag_id_prefix}{config.company_key}efforts_custom_email_notification_{config.country}{dag_id_postfix}_master_v1'.lower()
            }
        )

        def final_status(config, msg, **kwargs):
            if config.send_failure_alerts:
                python_callable.send_failure_alert(config, msg)
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
            op_args=[config, "Airflow Dag: " + dag_id_prefix+config.company_key +
                     dag_id_postfix + " has failed with the Error - {{ get_error_message() }}"],
            retries=0,
        )

        get_all_report >> get_report_details >> get_schedule_report_in_batch >> filter_report_data >> group_data_by_notification_step >> process_first_notification_batch >> wait_for_process_first_notification_batch >> gather_first_child_data >> process_final_notification_batch >> wait_for_process_final_notification_batch >> gather_final_child_data >> generate_merged_log_data >> send_task_completion_email >> send_task_failure_email >> final_status
    return dag


rail.for_each_instance(create_dag)
