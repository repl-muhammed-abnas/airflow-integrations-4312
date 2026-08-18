import rail
from eisner_amper.time_entry_overdue_notification.utils import request_payload

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'eisneramper_time_entry_overdue_notification_user_vise_child_{config.instance}',
        description=f'eisneramper_time_entry_overdue_notification_user_vise_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_user_data = rail.QueryCollectionOperator(
            task_id = "get_user_data",
            query= "SELECT * FROM valid_users_data_collection WHERE userUri = :user_uri",
            query_params={
                "user_uri": "{{dag_run.conf.userUri}}"
            }
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file='send_email.html',
            target='result',
            dataset="{{ result('get_user_data') }}",
        )

        get_final_payload = rail.PythonOperator(
            task_id='get_final_payload',
            python_callable=request_payload.get_final_payload_sendemail,
            op_args=[
                "{{ dag_run.conf.user_first_name }}",
                "{{dag_run.conf.userUri}}",
                "{{ result('get_email_body') }}"]
        )

        send_email_user = rail.RepliconServiceOperator(
            task_id='send_email_user',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data='{{ result("get_final_payload") }}'
        )

        get_user_data >> get_email_body >> get_final_payload >> send_email_user
    return dag

rail.for_each_instance(create_child_dag)
