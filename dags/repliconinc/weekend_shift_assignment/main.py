from datetime import datetime, timedelta
import rail
from repliconinc.weekend_shift_assignment.utils import response_payload, request_payload
from airflow.utils.email import send_mime_email, build_mime_message
from pendulum import datetime as dt


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description='Weekend Shift Assignment',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2022, 1, 1, tz=config.ist_timezone),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=lambda: request_payload.logging_details()
        )

        # Get shift assignments from dag_run conf (triggered by webhook)
        get_shift_assignments = rail.PythonOperator(
            task_id='get_shift_assignments',
            python_callable=request_payload.get_shift_assignments_from_conf
        )

        is_shift_assignments_empty = rail.IfOperator(
            task_id='is_shift_assignments_empty',
            test=lambda: bool(rail.result('get_shift_assignments')),
            yes_task='get_all_user_details',
            no_task='render_email__no_data_template'
        )

        get_all_user_details = rail.RepliconServiceOperator(
            task_id="get_all_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_all_user_details,
            data_handler=response_payload.get_user_details
        )

        render_email_template = rail.RenderTemplateOperator(
            task_id="render_email_template",
            template_file="templates/emails/completion.html",
            target='result'
        )

        def send_standard_response_callable():
            subject = """Weekend Shift Assignment Roaster For {{ result("get_logging_details")["weekend_shift_start_date"] }} - {{ result("get_logging_details")["weekend_shift_end_date"] }}"""

            msg, recipients = build_mime_message(
                mail_from=config.FROM_EMAIL_ADDR,
                to=config.TO_EMAIL_ADDR,
                cc=config.CC_EMAIL_ADDR,
                subject=rail.render_template(subject),
                html_content=rail.result(f"render_email_template")
            )

            send_mime_email(e_from=config.FROM_EMAIL_ADDR, e_to=recipients, mime_msg=msg)

        send_mail_success = rail.PythonOperator(
            task_id="send_mail_success",
            python_callable=send_standard_response_callable
        )

        render_email__no_data_template = rail.RenderTemplateOperator(
            task_id="render_email__no_data_template",
            template_file="templates/emails/no_data.html",
            target='result'
        )

        def send_standard_response__no_datacallable():
            subject = """Weekend Shift Assignment Roaster For {{ result("get_logging_details")["weekend_shift_start_date"] }} to {{ result("get_logging_details")["weekend_shift_end_date"] }} is not Configured"""

            msg, recipients = build_mime_message(
                mail_from=config.FROM_EMAIL_ADDR,
                to=config.TO_EMAIL_ADDR,
                cc=config.CC_EMAIL_ADDR,
                subject=rail.render_template(subject),
                html_content=rail.result(f"render_email_template")
            )

            send_mime_email(e_from=config.FROM_EMAIL_ADDR, e_to=recipients, mime_msg=msg)

        send_no_data_mail = rail.PythonOperator(
            task_id="send_no_data_mail",
            python_callable=send_standard_response__no_datacallable
        )

        is_currentday_is_friday = rail.IfOperator(
            task_id='is_currentday_is_friday',
            test=lambda: datetime.today().weekday() == 4,
            yes_task='process_weekend_shift_assignment',
            no_task='stop_execution'
        )

        stop_execution = rail.EmptyOperator(
            task_id='stop_execution'
        )

        process_weekend_shift_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_weekend_shift_assignment',
            items=lambda: rail.result('get_shift_assignments'),
            trigger_dag_id=config.process_weekend_shift_records_child_dag,
            conf=lambda item: {**dict(item.items()), "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_details'), 'employeeid', item['EmployeeID'], 'uri'), "name": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_details'), 'employeeid', item['EmployeeID'], 'name'), "emailaddress": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_details'), 'employeeid', item['EmployeeID'], 'emailaddress')},
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_process_shift_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_shift_records",
            dag_runs="{{result('process_weekend_shift_assignment')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_logging_details >> get_shift_assignments >> is_shift_assignments_empty
        is_shift_assignments_empty >> rail.Label("Yes") >> get_all_user_details >> render_email_template >> send_mail_success >> is_currentday_is_friday
        is_currentday_is_friday >> rail.Label("Yes") >> process_weekend_shift_assignment >> wait_process_shift_records >> log_to_sumo >> can_fail_dag >> fail_dagrun
        is_currentday_is_friday >> rail.Label("No") >> stop_execution

        is_shift_assignments_empty >> rail.Label("No") >> render_email__no_data_template >> send_no_data_mail

        return dag


rail.for_each_instance(create_main_airflow_dag)
