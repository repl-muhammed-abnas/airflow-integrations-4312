import rail
from macquariegroup.recovery_reconciliation.utils.request_payload import get_final_payload_sendemail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_recovery_reconciliation_send_recovery_enabled_emails_child_{config.instance}",
        description=f"Macquarie Recovery Reconciliation Send recovery enabled emails to users {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        def bool_has_exception_message(dag_run):
            if dag_run.conf['fmg_exception_message'] or dag_run.conf['rmg_exception_message']:
                if dag_run.conf['fmg_exception_message'] and dag_run.conf['employee_type'].lower() == "fmg/mod/bis/ccir":
                    return True
                if dag_run.conf['rmg_exception_message'] and dag_run.conf['employee_type'].lower() in ['rmg', 'fmg (non-gfs)']:
                    return True

            return False
        has_exception_message = rail.IfOperator(
            task_id = "has_exception_message",
            test= bool_has_exception_message,
            yes_task= "log_exception",
            no_task= "get_email_template_filepath"
        )

        log_exception = rail.WriteLogOperator(
            task_id = "log_exception",
            severity="Exception",
            log="{{dag_run.conf.log}}",
            message="The timesheet Doesn't have any Due date",
            properties={
                "user_login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "group": "{{dag_run.conf.groups}}",
                "derived_due_date": "na",
                "status": "Exception",
                "details": "The timesheet Doesn't have any Due date"
            }
        )

        def get_email_template_filepath_callable(dag_run):

            if dag_run.conf['employee_type'].lower() == "rmg":
                return {
                    "template_file": "templates/emails/rmg_notification_body.html",
                    "subject_line": f"RMG General Notification Timesheet Submission Due on {dag_run.conf['custom_due_date']['rmg_timesheet_end_date']}"
                }

            if dag_run.conf['employee_type'].lower() == "fmg/mod/bis/ccir":
                return {
                    "template_file": "templates/emails/gfs_email_notification_body.html",
                    "subject_line": f"GFS General Notification Timesheet Submission Due on {dag_run.conf['custom_due_date']['fmg_timesheet_end_date']}"
                }

            if dag_run.conf['employee_type'].lower() == "fmg (non-gfs)":
                return {
                    "template_file": "templates/emails/non_gfs_notification_body.html",
                    "subject_line": f"Non-GFS General Notification Timesheet Submission Due on {dag_run.conf['custom_due_date']['rmg_timesheet_end_date']}"
                }

            raise Exception(
                f"Employee Type assigned for user is different {dag_run.conf['employee_type']}")

        get_email_template_filepath = rail.PythonOperator(
            task_id="get_email_template_filepath",
            python_callable=get_email_template_filepath_callable
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file='{{result("get_email_template_filepath").template_file}}',
            target='result',
        )

        get_final_payload = rail.PythonOperator(
            task_id='get_final_payload',
            python_callable=get_final_payload_sendemail,
            op_args=[
                "{{dag_run.conf.user_uri}}",
                "{{ result('get_email_body') }}",
                "{{ result('get_email_template_filepath').subject_line}}"
            ]
        )

        send_email_user = rail.RepliconServiceOperator(
            task_id='send_email_user',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data='{{ result("get_final_payload") }}'
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            severity="Success",
            log="{{dag_run.conf.log}}",
            message="Email Triggered Successfully",
            properties={
                "user_login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "group": "{{dag_run.conf.groups}}",
                # pylint: disable=line-too-long
                "derived_due_date": "{{dag_run.conf.custom_due_date.rmg_timesheet_end_date if dag_run.conf.employee_type | lower() != 'fmg/mod/bis/ccir' else dag_run.conf.custom_due_date.fmg_timesheet_end_date}}",
                "status": "Success",
                "details": "Email Triggered"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            log="{{dag_run.conf.log}}",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "user_login_name": "{{dag_run.conf.login_name}}",
                "employee_type": "{{dag_run.conf.employee_type}}",
                "group": "{{dag_run.conf.groups}}",
                # pylint: disable=line-too-long
                "derived_due_date": "{{dag_run.conf.custom_due_date.rmg_timesheet_end_date if dag_run.conf.employee_type != 'fmg/mod/bis/ccir' else dag_run.conf.custom_due_date.fmg_timesheet_end_date}}",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        has_exception_message >> rail.Label("Yes") >> get_email_template_filepath >> get_email_body >> get_final_payload >> send_email_user\
            >> log_success >> rail.Label("On Error") >> catch_and_log_error
        has_exception_message >> rail.Label("No") >> log_exception >> rail.Label("On Error") >> catch_and_log_error
    return dag


rail.for_each_instance(create_child_dag)
