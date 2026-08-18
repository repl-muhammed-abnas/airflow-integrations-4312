from datetime import datetime as dt
import pytz
from pendulum import datetime
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_sftp_custom_notification_master_{config.instance}',
        description=f'Macquarie User Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_schedule_interval,
        start_date= datetime(2023, 1, 1, tz=config.timezone),
        max_active_runs=config.master_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        list_input_dir = rail.SFTPListFilesOperator(
            task_id = "list_input_dir",
            paths=[config.input_filepath]
        )

        has_any_file_found = rail.IfOperator(
            task_id = "has_any_file_found",
            test=lambda: rail.result('list_input_dir') and len(rail.result('list_input_dir')[config.input_filepath]) > 0,
            yes_task= "end",
            no_task="list_archive_dir"
        )

        list_archive_dir = rail.SFTPListFilesOperator(
            task_id = "list_archive_dir",
            paths=[config.archive_filepath]
        )

        has_any_archive_files = rail.IfOperator(
            task_id = "has_any_archive_files",
            test=lambda: rail.result('list_archive_dir') and len(rail.result('list_archive_dir')[config.archive_filepath]) > 0,
            yes_task= "is_current_month_file_processed",
            no_task="send_email_notification"
        )

        def bool_is_current_month_file_processed():
            today = dt.now(tz = pytz.timezone(config.timezone))
            # the file list is in the ascending order of modification date
            last_archived_file = rail.result("list_archive_dir")[config.archive_filepath][-1]

            #file modify date format: 20221128060459
            last_archived_file_date = dt.strptime(last_archived_file['modify'], "%Y%m%d%H%M%S")

            if last_archived_file_date.year == today.year and last_archived_file_date.month == today.month:
                return True

            return False

        is_current_month_file_processed = rail.IfOperator(
            task_id ="is_current_month_file_processed",
            test=bool_is_current_month_file_processed,
            yes_task= "end", # can be marked for delete
            no_task="can_trigger_email_notification"
        )

        end = rail.EmptyOperator(
            task_id = "end"
        )

        def bool_can_trigger_email_notification():
            today = dt.now()
            if today.day >=10 and today.day <=15:
                return True
            return False

        can_trigger_email_notification = rail.IfOperator(
            task_id = "can_trigger_email_notification",
            test= bool_can_trigger_email_notification,
            yes_task="send_email_notification",
            no_task="end"
        )

        send_email_notification = rail.EmailOperator(
            task_id="send_email_notification",
            to= config.tenant_email,
            subject="{{ get_company_key()}} | Alert Active Department Feed file is not available for processing - {{current_time_in_specified_tz()}}",
            html_content="templates/email/alert_notification.html",
            params = {
                "input_file_path": config.input_filepath
            }
        )

        list_input_dir >> has_any_file_found >> rail.Label("Yes") >> end
        has_any_file_found >> rail.Label("No") >> list_archive_dir >> has_any_archive_files >> rail.Label("Yes")\
            >> is_current_month_file_processed
        has_any_archive_files >> rail.Label("No") >> send_email_notification >> end
        is_current_month_file_processed >> rail.Label("Yes") >> end
        is_current_month_file_processed >> rail.Label("No") >> \
            can_trigger_email_notification >> rail.Label("Yes") >> send_email_notification

        can_trigger_email_notification >> rail.Label("No") >> end

    return dag

rail.for_each_instance(create_main_dag)
