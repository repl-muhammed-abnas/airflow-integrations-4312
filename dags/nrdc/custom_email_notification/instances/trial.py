# pylint: disable=wildcard-import unused-wildcard-import
from nrdc.custom_email_notification.config import *

instance = "trial"
environment = 'pre-production'

company_key = 'nrdctrial01'
replicon_conn_id = 'nrdctrial01_admin'
can_run_batch_task_var_name = f'nrdc_custom_email_notification_can_run_batch_task_{instance}'


execution_timeout_days = 14
child_dag_max_active_runs = 10

user_list_for_notification_report = "**User List For Email Notification Test**"


overdue_send_mail_c3_dagid = f'nrdc_custom_email_notification_overdue_sendmail_c3'
overdue_send_mail_c4_dagid = f'nrdc_custom_email_notification_overdue_sendmail_c4'
due_notification_send_mail_c3_dagid = f'nrdc_custom_email_notification_due_notification_sendmail_c3'
due_notification_send_mail_c4_dagid = f'nrdc_custom_email_notification_due_notification_sendmail_c4'
due_today_send_mail_c3_dagid = f'nrdc_custom_email_notification_due_today_sendmail_c3'
due_today_send_mail_c4_dagid = f'nrdc_custom_email_notification_due_today_sendmail_c4'

disabled=True
