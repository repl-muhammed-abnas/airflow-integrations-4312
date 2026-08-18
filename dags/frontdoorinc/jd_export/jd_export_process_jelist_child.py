import rail
from frontdoorinc.jd_export.utils import python_callable, request_payload


def create_child_dag(config):
    # pylint: disable=too-many-statements unnecessary-lambda line-too-long
    with rail.create_airflow_dag(
        dag_id=config.jd_export_process_jelist_child,
        description=f"Frontdoorinc_JDEIntegration Process JE List child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        get_amount_financedept = rail.PythonOperator(
            task_id="get_amount_financedept",
            python_callable=python_callable.get_amount_finance_dept
        )

        add_item_to_create_summary_list = rail.WriteLogOperator(
            task_id='add_item_to_create_summary_list',
            log="{{ dag_run.conf.summary_list_lookup_table }}",
            message="na",
            severity="Success",
            properties=request_payload.get_summary_list
        )

        get_amount_financedept >> add_item_to_create_summary_list

    return dag


rail.for_each_instance(create_child_dag)
