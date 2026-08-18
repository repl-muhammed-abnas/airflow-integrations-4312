from datetime import datetime
import time
from airflow.models import Variable
import rail


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"custom_dummy_dag_ondemand_testing_{config.region.replace('-', '_')}_{config.instance}",
        description=f'Custom Dummy Dag For On Demand Connector Testing {config.region} {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_secret),
        start_date=datetime(2022, 1, 1),
        max_active_runs = config.max_active_runs
    ) as dag:

        def get_config_value(dag_run):
            time.sleep(6)
            if rail.result('get_connector_name')['fail']:
                print(dag_run.conf['hello'])
            print(dag_run.conf)

        show_dag_run_config = rail.PythonOperator(
            task_id = 'show_dag_run_config',
            python_callable = get_config_value
        )

        def get_connector_name_and_worflow():
            connector_workflow = Variable.get('test_xero_connection_id', default_var='a:b')
            c_w = connector_workflow.split(':')
            return {
                'connector': c_w[0],
                'workflow': c_w[1],
                'fail': c_w[2] == 'true'
            }


        get_connector_name = rail.PythonOperator(
            task_id = 'get_connector_name',
            python_callable = get_connector_name_and_worflow
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id="log_dagrun_details_to_table",
            trigger_rule='all_done',
            required_configs={
                "airflow_connector_ui_connid": config.airflow_connector_ui_connid,
                "hmac_secret_var": config.webhook_secret,
            },
            company_key="{{ dag_run.conf.company_key }}",
            connector_name="{{result('get_connector_name').connector}}",
            integration_type="{{result('get_connector_name').workflow}}",
        )

        get_connector_name >> show_dag_run_config >> log_dagrun_details_to_table

    return dag


rail.for_each_instance(create_airflow_dag)
