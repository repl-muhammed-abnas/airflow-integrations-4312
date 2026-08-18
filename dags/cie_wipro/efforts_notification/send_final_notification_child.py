# pylint: disable=line-too-long, too-many-statements trailing-whitespace
from datetime import datetime
import rail
from cie_wipro.efforts_notification.utils import request_payload
null = None


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_send_final_notification_{config.country}{dag_id_postfix}_child_v1'.lower(),
        description=f'{dag_id_prefix}_send_final_notification_child_{config.country}{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run,
    ) as dag:

        employee_html_body = "templates/emails/email_for_efforts_final_reminder.html"

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )
        get_all_entries = rail.PythonOperator(
            task_id='get_all_entries',
            python_callable=lambda dag_run: dag_run.conf['user_list']
        )

        foreach_reminder_entry = rail.ForEachOperator(
            task_id='foreach_reminder_entry',
            items=lambda: rail.result('get_all_entries'),
            start_task='get_email_subject',
            end_task='foreach_reminder_entry_end'
        )
        foreach_reminder_entry_end = rail.EmptyOperator(
            task_id='foreach_reminder_entry_end'
        )

        get_email_subject = rail.PythonOperator(
            task_id='get_email_subject',
            python_callable=lambda dag_run: f"Final Reminder: Missing Efforts Entry for {rail.result('foreach_reminder_entry')['Entry Date']}"
        )

        check_for_useremail = rail.IfOperator(
            task_id='check_for_useremail',
            test='''{{ result('foreach_reminder_entry')['User Email'] | is_truthy and '@' in result('foreach_reminder_entry')['User Email'] }}''',
            yes_task="get_email_body",
            no_task="write_log_for_skipped_entries",
        )
        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file=employee_html_body,
            target='result',
        )

        send_reminder_email = rail.RepliconServiceOperator(
            task_id='send_reminder_email',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data=request_payload.get_final_payload_sendemail
        )

        write_log_for_skipped_entries = rail.WriteLogOperator(
            task_id='write_log_for_skipped_entries',
            log="{{ result('create_log') }}",
            message="Email Not Available",
            severity="skipped",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "Type": "Efforts not entered for on {{ result('foreach_reminder_entry')['Entry Date'] }}",
                "User Name": "{{ result('foreach_reminder_entry')['User Name'] }}",
                "Status": "Skipped - User Email Not Available",
                "Reminder No": "Final"
            }
        )
        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log="{{ result('create_log') }}",
            message="Reminder email send",
            severity="success",
            properties={
                "JobID": "{{ dag_run_ecid() }}",
                "Date": datetime.now().strftime("%d/%m/%Y"),
                "Type": "Efforts not entered for on {{ result('foreach_reminder_entry')['Entry Date'] }}",
                "User Name": "{{ result('foreach_reminder_entry')['User Name'] }}",
                "Status": "{{ dag_run_ecid() }} - Email Sent Successfully",
                "Reminder No": "Final"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        create_log >> get_all_entries >> foreach_reminder_entry >> get_email_subject >> check_for_useremail >> get_email_body
        foreach_reminder_entry >> foreach_reminder_entry_end
        check_for_useremail >> rail.Label(
            'Yes') >> get_email_body
        get_email_body >> send_reminder_email >> log_success >> foreach_reminder_entry_end
        check_for_useremail >> rail.Label(
            'No') >> write_log_for_skipped_entries >> foreach_reminder_entry_end
        foreach_reminder_entry_end >> finish
    return dag


rail.for_each_instance(create_dag)
