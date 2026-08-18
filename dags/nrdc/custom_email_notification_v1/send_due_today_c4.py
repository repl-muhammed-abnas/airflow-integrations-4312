import rail
from nrdc.custom_email_notification_v1.utils import custom_methods
from datetime import datetime

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.due_today_send_mail_c4_dagid,
        description=f'NRDC Custom Email Notification Due Send mail c3{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)
        
        get_email_subject = rail.PythonOperator(
            task_id='get_email_subject',
            python_callable=lambda dag_run: "Reminder: Your " + datetime.strptime(dag_run.conf["timesheetperiod"].replace(" ", "").split("-")[0], "%m/%d/%Y").strftime("%B") + " Action Fund timesheet is due TODAY"
        )

        get_timsheet_button_url = rail.PythonOperator(
            task_id = 'get_timsheet_button_url',
            python_callable=lambda dag_run: custom_methods.get_timsheet_button_url(dag_run, config)
        )

        get_template_name = rail.PythonOperator(
            task_id='get_template_name',
            python_callable=lambda dag_run: f"templates/due_today/c4_notification_email_{'9am' if dag_run.conf.get('scheduled_time') == 9 else '4pm'}.html"
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file="{{result('get_template_name')}}",
            target='result',
        )

        send_email_to_user = rail.EmailOperator(
            task_id='send_email_to_user',
            to="{{dag_run.conf.emailnotification}}",
            subject="{{result('get_email_subject')}}",
            html_content="{{result('get_email_body')}}"
        )

        get_email_subject >> get_timsheet_button_url >> get_template_name >> get_email_body >> send_email_to_user
    return dag

rail.for_each_instance(create_dag)
