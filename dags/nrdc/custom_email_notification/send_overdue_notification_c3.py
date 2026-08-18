import rail
from nrdc.custom_email_notification.utils import request_payload,custom_methods
from datetime import datetime

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.overdue_send_mail_c3_dagid,
        description=f'NRDC Custom Email Notification Overdue Send mail c3{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)
        
        get_email_subject = rail.PythonOperator(
            task_id='get_email_subject',
            python_callable=lambda dag_run: "Reminder: Your " + datetime.strptime(dag_run.conf["timesheetperiod"].replace(" ", "").split("-")[0], "%m/%d/%Y").strftime("%B") + " lobbying timesheet is now OVERDUE"
        )

        get_timsheet_button_url = rail.PythonOperator(
            task_id = 'get_timsheet_button_url',
            python_callable=lambda dag_run: custom_methods.get_timsheet_button_url(dag_run, config)
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file='templates/overdue_notification/c3_notification_email.html',
            target='result',
        )


        send_email_to_user = rail.EmailOperator(
            task_id='send_email_to_user',
            to="{{dag_run.conf.emailnotification}}",
            subject="{{result('get_email_subject')}}",
            html_content="{{result('get_email_body')}}"
        )

        get_email_subject >> get_timsheet_button_url >>get_email_body >> send_email_to_user
    
    return dag

rail.for_each_instance(create_dag)
