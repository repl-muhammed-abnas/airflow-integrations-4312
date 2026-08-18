import pendulum
import rail
from ge.timesheet_email_notification_poland.utils import custom_methods
from ge.timesheet_email_notification_poland.tasks.send_notification_email import send_notification

def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_timesheet_email_notification_poland_send_email_notification_child_{config.instance}',
        description=f'Live| GE_Poland Send Email Notification Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_supervisor_last_name = rail.PythonOperator(
            task_id='get_supervisor_last_name',
            python_callable=lambda dag_run: dag_run.conf["supervisor_data"]["supervisor_name"].split(",")[-1]
        )

        users_timesheet_data = rail.PythonOperator(
            task_id='users_timesheet_data',
            python_callable=custom_methods.get_users_timesheets_data
        )

        get_users_timesheets_length = rail.PythonOperator(
            task_id='get_users_timesheets_length',
            python_callable=lambda: len(rail.result("users_timesheet_data"))
        )

        users_locations = rail.PythonOperator(
            task_id='users_locations',
            python_callable=custom_methods.get_users_locations
        )

        poland_locations = rail.PythonOperator(
            task_id='poland_locations',
            python_callable=custom_methods.get_poland_locations
        )

        render_users_html_table = rail.RenderTemplateOperator(
            task_id='render_users_html_table',
            target='result',
            template_file='templates/emails/users_table.html',
            dataset="{{ result('users_timesheet_data') | to_json }}"
        )

        is_poland_location_exists = rail.IfOperator(
            task_id='is_poland_location_exists',
            test=lambda: len(rail.result("poland_locations")) > 0,
            yes_task='are_all_locations_poland',
            no_task='process_not_poland_email'
        )

        are_all_locations_poland = rail.IfOperator(
            task_id='are_all_locations_poland',
            test=lambda: len(rail.result("poland_locations")) == len(rail.result("users_locations")),
            yes_task='process_poland_email',
            no_task='get_no_of_poland_timesheets'
        )

        get_no_of_poland_timesheets = rail.PythonOperator(
            task_id='get_no_of_poland_timesheets',
            python_callable=lambda: len(rail.result("users_locations")) - len(rail.result("poland_locations"))
        )

        process_poland_email = rail.EmptyOperator(
            task_id='process_poland_email'
        )

        send_poland_email = send_notification(config, "poland")

        process_other_locations_email = rail.EmptyOperator(
            task_id='process_other_locations_email'
        )

        send_all_locations_email = send_notification(config, "all_locations")

        process_not_poland_email = rail.EmptyOperator(
            task_id='process_not_poland_email'
        )

        send_not_poland_email = send_notification(config, "not_poland")

        log_mail_sent_success = rail.WriteLogOperator(
            task_id='log_mail_sent_success',
            message='Sent',
            severity='Success',
            properties=lambda dag_run: {
                "Parentjobid": dag_run.conf["parent_dag_run_ecid"],
                "username": dag_run.conf["supervisor_data"]["supervisor_name"],
                "emailid": dag_run.conf["supervisor_data"]["supervisor_email"],
                "status": "Sent",
                "reason": "",
                "childjobid": '{{ dag_run_ecid() }}',
                "date": pendulum.now().strftime("%m/%d/%Y")
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "Parentjobid": dag_run.conf["parent_dag_run_ecid"],
                "username": dag_run.conf["supervisor_data"]["supervisor_name"],
                "emailid": dag_run.conf["supervisor_data"]["supervisor_email"],
                "status": "Error",
                "reason": '{{ get_error_message() }}',
                "childjobid": '{{ dag_run_ecid() }}',
                "date": pendulum.now().strftime("%m/%d/%Y")
            }
        )

        get_supervisor_last_name >> users_timesheet_data >> get_users_timesheets_length >> users_locations \
            >> poland_locations >> render_users_html_table >> is_poland_location_exists

        is_poland_location_exists >> rail.Label("Yes") >> are_all_locations_poland
        is_poland_location_exists >> rail.Label("No") >> process_not_poland_email >> send_not_poland_email >> log_mail_sent_success

        are_all_locations_poland >> rail.Label("Yes") >> process_poland_email >> send_poland_email >> log_mail_sent_success
        are_all_locations_poland >> rail.Label("No") >> get_no_of_poland_timesheets >> process_other_locations_email \
            >> send_all_locations_email >> log_mail_sent_success

        log_mail_sent_success >> catch_and_log_errors

        return dag

rail.for_each_instance(create_child_dag)
