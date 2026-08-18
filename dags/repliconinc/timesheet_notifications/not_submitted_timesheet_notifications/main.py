from pendulum import datetime as dt
import rail
from airflow.utils.email import send_mime_email, build_mime_message
from repliconinc.timesheet_notifications.not_submitted_timesheet_notifications.utils.custom_methods import get_timesheet_report_payload

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Inhouse - Not Submitted Timesheet Notifications',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 11, 1, tz=config.ist_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master,
    ) as dag:

        rail.ViewDagRunScheduleOperator(task_id='view_dagrun_config')

        generate_timesheet_report_details = rail.RepliconServiceOperator(
            task_id='generate_timesheet_report_details',
            endpoint='/services/reportService1.svc/GenerateReport',
            data=lambda: get_timesheet_report_payload(
                config.not_submitted_timesheets_report_uri,
                config.timesheet_period_filter_uri,
                config.company_key,
                config.period_start_date
            )
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ result('generate_timesheet_report_details').error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ result('generate_timesheet_report_details').error  }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test=lambda: rail.result('generate_timesheet_report_details')['payload']  != "No Data\r\n",
            yes_task='load_timesheet_report_data',
            no_task='no_report_data_found'
        )

        no_report_data_found = rail.EmptyOperator(
            task_id='no_report_data_found',
        )

        load_timesheet_report_data = rail.LoadCSVFileOperator(
            task_id='load_timesheet_report_data',
            document="{{ result('generate_timesheet_report_details').payload }}",
            headers=['timesheet_period', 'user']
        )

        render_email_template = rail.RenderTemplateOperator(
            task_id="render_email_template",
            template_file="email_template.html",
            target='result'
        )

        def send_standard_response_callable():
            subject = """Action Required: """ + config.company_identifier + """ | Unsubmitted Timesheets - {{ current_time_in_specified_tz() }}"""

            msg, recipients = build_mime_message(
                    mail_from=config.FROM_EMAIL_ADDR,
                    to=config.TO_EMAIL_ADDR,
                    cc=config.CC_EMAIL_ADDR,
                    subject=rail.render_template(subject),
                    html_content=rail.result(f"render_email_template")
                )

            send_mime_email(e_from=config.FROM_EMAIL_ADDR, e_to=recipients, mime_msg=msg)

        send_timesheet_approval_notification = rail.PythonOperator(
            task_id="send_timesheet_approval_notification",
            python_callable=send_standard_response_callable
        )

        generate_timesheet_report_details >> is_report_failed >> rail.Label('Yes') >> fail_report_generation
        is_report_failed >> rail.Label('No') >> report_has_data >> rail.Label('No') >> no_report_data_found
        report_has_data >> rail.Label('Yes') >> load_timesheet_report_data >> render_email_template >> send_timesheet_approval_notification

    return dag

rail.for_each_instance(create_main_dag)
