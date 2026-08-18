import rail
from dxctechnology.c1_iwo_leanstaffing import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_iwo_leanstaffing_child_create_billing_rate_{config.instance}',
        description=f'DXC_C1_Lean Staffing_Automation- child_create billing rate_V3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_create_billing_rate_max_active_runs,
    ) as dag:

        name = "{{ dag_run.conf.name }}"
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        put_company_billing_rate_with_billable_extension = rail.RepliconServiceOperator(
            task_id='put_company_billing_rate_with_billable_extension',
            endpoint='/services/BillingRateService1.svc/PutCompanyBillingRate',
            data=request_payload.get_create_billable_non_billable_billing_rates_param(
                name, "billable")
        )

        put_company_billing_rate_with_non_billable_extension = rail.RepliconServiceOperator(
            task_id='put_company_billing_rate_with_non_billable_extension',
            endpoint='/services/BillingRateService1.svc/PutCompanyBillingRate',
            data=request_payload.get_create_billable_non_billable_billing_rates_param(
                name, "non-billable")
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        [put_company_billing_rate_with_billable_extension,
         put_company_billing_rate_with_non_billable_extension] >> finish
    return dag


rail.for_each_instance(create_dag)
