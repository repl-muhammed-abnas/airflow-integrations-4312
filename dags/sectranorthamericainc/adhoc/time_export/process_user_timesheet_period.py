from datetime import timedelta
from pendulum import datetime
import rail
from sectranorthamericainc.adhoc.time_export.utils.custom_methods import get_sas_token, create_json_payload_callable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_export_process_export_dag_id,
        description="sectranorthamericainc Time Export Child dag",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.post_time_export_data_max_active_run,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        query_employee_data = rail.QueryCollectionOperator(
            task_id="query_employee_data",
            query="""SELECT * FROM final_data_to_post frtd
                    WHERE frtd.employee_id = :employee_id AND frtd.timesheet_period = :timesheet_period""",
            query_params={
                "employee_id": "{{dag_run.conf.employee_id}}",
                "timesheet_period": "{{dag_run.conf.timesheet_period}}"
            }
        )

        create_json_payload = rail.PythonOperator(
            task_id="create_json_payload",
            python_callable=create_json_payload_callable,
            op_args=[query_employee_data.task_id],
        )

        generate_token = rail.PythonOperator(
            task_id="generate_token",
            python_callable=get_sas_token,
            op_args=(config.AZURE_API_ENDPOINT,
                     "Replicon",
                     config.client_secrete_var_name,
                     timedelta(days=config.generate_token_ttl_days))
        )

        post_to_api_endpoint = rail.HTTPUploadFileOperator(
            task_id="post_to_api_endpoint",
            http_conn_id=config.http_conn_id,
            content="{{result('create_json_payload')}}",
            headers={
                "Content-Type": 'application/json',
                "Authorization": "{{result('generate_token')}}"
            }
        )

        query_employee_data >> create_json_payload >> generate_token >> post_to_api_endpoint

    return dag


rail.for_each_instance(create_main_dag)
