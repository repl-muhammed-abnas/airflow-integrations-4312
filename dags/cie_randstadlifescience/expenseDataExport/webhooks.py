# pylint: disable=line-too-long
from datetime import datetime
import rail
from cie_randstadlifescience.expenseDataExport.utils import upload_to_s3, data_formatting
# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}randstadlifescience_expenseDataExport_webhooks{dag_id_postfix}',
        description=f'Randstad Expense Webhook receiver {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 1, 1),
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=f'{dag_id_prefix}randstadlifescience_expenseDataExport_webhook{dag_id_postfix}_expenseapproved_secret'),
        ]
    ) as dag:

        # rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        start = rail.EmptyOperator(task_id='start')
        finish = rail.EmptyOperator(task_id='finish')

        get_expense_details = rail.PythonOperator(
            task_id="get_expense_details",
            python_callable=data_formatting.extract_data,
            op_args=['{{ dag_run.conf.webhook.data | tojson }}']
        )

        data_exists = rail.IfOperator(
            task_id="data_exists",
            test="{{  result('get_expense_details') is not none  }}",
            yes_task="upload_expense_data",
        )

        upload_expense_data = upload_to_s3.UploadCsvOperator(
            task_id='upload_expense_data',
            source="{{ result('get_expense_details') | tojson }}",
            bucket_name=config.bucket_name,
            file_path=config.file_path,
            file_name=config.file_name.format(
                datetime.now().strftime("%m%d%Y%H%M%S%f")),  # rail.result('get_expense_details').get('expenseuri').split(':')[-1],
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject='{{ get_company_key() }} | Expense Webhook - failed to create/upload file - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content='templates/webhook_failure_email.html',
            params={
                'dag_id': f'{dag_id_prefix}randstadlifescience_expenseDataExport_webhooks{dag_id_postfix}'.lower()
            }
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )
        start >> get_expense_details >> data_exists >> rail.Label(
            "Yes") >> upload_expense_data >> finish
        start >> get_expense_details >> data_exists >> rail.Label(
            "No") >> finish
        finish >> send_task_failure_email >> final_status
    return dag


rail.for_each_instance(create_dag)
