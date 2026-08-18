import json
import rail
from wipro.efforts_submit.custom_http_operator.CustomSimpleHttpOperator import CustomSimpleHttpOperator

null = None
# dag run conf
# {
#   "requesttype": "POST",
#   "requestURL": "/CapgeminiDev/services/TimeOffValidationScriptAdministrationService1",
#   "contenttype": "application/json",
#   "authorization": "Bearer <token>",
#   "requestpayload": "",
#   "http_conn_id":""
# }

def create_airflow_master(config):
    with rail.create_airflow_dag(
        dag_id=config.adhoc_http_test_master,
        description="Adhoc http test",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        process_data = rail.PythonOperator(
            task_id="process_data",
            python_callable=lambda dag_run:json.dumps(dag_run.conf["requestpayload"], ensure_ascii=False)
        )

        # method is not templated hence it is not configurable yet.
        # need to complete dev of the same
        http_submit_data_to_endpoint=CustomSimpleHttpOperator(
            task_id="http_submit_data_to_endpoint",
            http_conn_id='wipro_http_effort_submit',
            endpoint='{{dag_run.conf.requestURL}}',
            method='POST',
            auth_type=None,
            headers={
                "Authorization": '{{dag_run.conf.authorization}}',
                'Content-Type': '{{dag_run.conf.contenttype}}',
                "sourceSystemId": "REPLICON",
            },
            data='{{result("process_data")}}',
            extra_options= {
                "check_response": False
            },
            log_response=True
        )

        process_data >> http_submit_data_to_endpoint

        return dag


rail.for_each_instance(create_airflow_master)
