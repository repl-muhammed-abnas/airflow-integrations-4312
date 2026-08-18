# pylint: disable=wildcard-import unused-wildcard-import
from nrdc.custom_email_notification.config import *

instance = "production"
environment = 'production'

company_key = 'NRDC'
replicon_conn_id = 'nrdc_replicon_admin'
can_run_batch_task_var_name = f'nrdc_custom_email_notification_can_run_batch_task_{instance}'


execution_timeout_days = 14
child_dag_max_active_runs = 10


overdue_send_mail_c3_dagid = f'nrdc_custom_email_notification_overdue_sendmail_c3'
overdue_send_mail_c4_dagid = f'nrdc_custom_email_notification_overdue_sendmail_c4'
due_notification_send_mail_c3_dagid = f'nrdc_custom_email_notification_due_notification_sendmail_c3'
due_notification_send_mail_c4_dagid = f'nrdc_custom_email_notification_due_notification_sendmail_c4'
due_today_send_mail_c3_dagid = f'nrdc_custom_email_notification_due_today_sendmail_c3'
due_today_send_mail_c4_dagid = f'nrdc_custom_email_notification_due_today_sendmail_c4'
