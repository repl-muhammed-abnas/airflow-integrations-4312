
from datetime import datetime
import rail
from adtalem.custom_email_notification.utils import python_callable, request_payload
null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_custom_email_notification_individual_timesheet_reminder_for_regular_child_{config.instance}',
        description=f'Live|Adtalem_call recipe to send individual custom email notification for Timesheet Reminder Regular {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
    ) as dag:

        html_body = "templates/emails/individual/email_for_regular_timesheet.html"

        get_email_details = rail.PythonOperator(
            task_id='get_email_details',
            python_callable=python_callable.get_required_details_for_email
        )

        check_for_useremail = rail.IfOperator(
            task_id='check_for_useremail',
            test='''{{ dag_run.conf.get('useremail') | is_truthy and '@' in dag_run.conf.get('useremail') }}''',
            yes_task="get_email_body",
            no_task="write_log_for_skipped_entries",
        )
        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file=html_body,
            target='result',
        )

        send_reminder_email = rail.RepliconServiceOperator(
            task_id='send_reminder_email',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data=request_payload.get_regular_payload_sendemail
        )

        write_log_for_skipped_entries = rail.WriteLogOperator(
            task_id='write_log_for_skipped_entries',
            log="{{ dag_run.conf['logid'] }}",
            message="Email Not Available",
            severity="skipped",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "Type": "Timesheet Reminder - {{ dag_run.conf['type'] }}",
                "User Name | Supervisor Name": "{{ dag_run.conf.user }} | {{ dag_run.conf.supervisor }}",
                "Status": "Skipped - User Email Not Available"
            }
        )
        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log="{{ dag_run.conf['logid'] }}",
            message="Reminder email send",
            severity="success",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "Type": "Timesheet Reminder - Regular",
                "User Name | Supervisor Name": "{{ dag_run.conf.user }} | {{ dag_run.conf.supervisor }}",
                "Status": "{{ dag_run_ecid() }} - Email Sent Successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf['logid'] }}",
            severity="Failed",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "Type": "Timesheet Reminder - Regular",
                "User Name | Supervisor Name": "{{ dag_run.conf.user }} | {{ dag_run.conf.supervisor }}",
                "Status": "{{ dag_run_ecid() }} - Email Not Sent Successfully."
            }
        )
        get_email_details >> check_for_useremail
        check_for_useremail >> rail.Label(
            'Yes') >> get_email_body >> send_reminder_email >> log_success >> catch_and_log_errors
        check_for_useremail >> rail.Label(
            'No') >> write_log_for_skipped_entries >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
