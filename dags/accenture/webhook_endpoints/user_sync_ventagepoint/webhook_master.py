from datetime import timedelta
import rail
from accenture.webhook_endpoints.user_sync_ventagepoint.utils.python_callable_methods import (
    filter_unprocessed_rows,
    make_get_child_conf,
    build_parent_processed_payload,
)


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dagid,
        description='Accenture User Sync MRDR Webhook Master',
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.company_key,
        replicon_conn_id=None,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var=config.basic_auth_username_accenture_mrdr,
            basic_auth_password_var=config.basic_auth_password_accenture_mrdr
        ),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dag_run_conf')

        fetch_grid_rows = rail.VantagepointHubDataTablesOperator(
            task_id='fetch_grid_rows',
            vp_conn_id=config.vantagepoint_conn_id,
            request_method='GET',
            hub=config.vantagepoint_hub,
            hub_key='{{ dag_run.conf.webhook.data.UID }}',
            associated_table=config.employee_integration_table,
        )

        filter_unprocessed = rail.PythonOperator(
            task_id='filter_unprocessed',
            python_callable=filter_unprocessed_rows,
        )

        trigger_employee_child = rail.trigger_parallel_dagrun(
            task_id='trigger_employee_child',
            items=lambda: rail.result('filter_unprocessed'),
            parallel_count=config.parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_employee_child_dag_id,
            conf=make_get_child_conf(config.vantagepoint_conn_id),
        )

        mark_parent_processed = rail.VantagepointHubDataTablesOperator(
            task_id='mark_parent_processed',
            vp_conn_id=config.vantagepoint_conn_id,
            request_method='PUT',
            endpoint=f'/{config.vantagepoint_hub}' + "/{{ dag_run.conf.webhook.data.UID }}",
            request_body=build_parent_processed_payload,
        )

        fetch_grid_rows >> filter_unprocessed >> trigger_employee_child >> mark_parent_processed

        return dag


rail.for_each_instance(create_main_dag)
