import rail
from dxctechnology.c1_labour_types_import_v1 import request_payload

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_labour_types_import_v1/config.py


def create_child_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_labour_types_child_create_billing_rate{dag_id_postfix}_v1',
        description=f'DXC_C1_Labour Types-child_create billing rate_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_create_billing_rate_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        put_company_billing_rate = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_company_billing_rate',
            endpoint='/services/BillingRateService1.svc/PutCompanyBillingRate',
            items=['|Billable', '|Non-Billable'],
            data=request_payload.get_create_billing_rates_param
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        put_company_billing_rate >> finish

    return dag


rail.for_each_instance(create_child_dag)
