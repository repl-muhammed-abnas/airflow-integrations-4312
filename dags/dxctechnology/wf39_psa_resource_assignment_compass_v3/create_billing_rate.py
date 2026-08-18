import rail
from dxctechnology.wf39_psa_resource_assignment_compass_v3.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.billing_rate_child_dagid,
        description=f'DXC_WF39 PSA Resource Assignment create billing rate {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.create_billing_rate_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        put_company_billing_rate = rail.RepliconServiceOperator(
            task_id='put_company_billing_rate',
            endpoint='/services/BillingRateService1.svc/PutCompanyBillingRate',
            data=request_payload.get_create_billing_rates_param
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        put_company_billing_rate >> finish

    return dag


rail.for_each_instance(create_child_dag)
