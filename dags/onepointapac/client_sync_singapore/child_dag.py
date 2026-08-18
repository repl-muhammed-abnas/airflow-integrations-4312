import rail
from onepointapac.client_sync_singapore.utils import request_payload, response_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'Xero Contact to Replicon Client Sync - Singapore Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config)

        is_new_client = rail.IfOperator(
            task_id='is_new_client',
            test=lambda dag_run: dag_run.conf.get('is_new_client', False),
            yes_task='create_client',
            no_task='finish'
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_create_client_param
        )

        update_currency = rail.RepliconServiceOperator(
            task_id='update_currency',
            endpoint='/services/ClientService1.svc/UpdateDefaultBillingCurrency',
            data=request_payload.get_update_currency_payload
        )

        catch_client_error = rail.PythonOperator(
            task_id='catch_client_error',
            trigger_rule='one_failed',
            python_callable=response_filter.get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.client_name }}',
                     '{{ get_error_message() }}']
        )

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        is_new_client >> rail.Label('Yes') >> create_client >> update_currency >> finish
        is_new_client >> rail.Label('No') >> finish
        create_client >> rail.Label('on Error') >> catch_client_error >> finish
        update_currency >> rail.Label('on Error') >> catch_client_error

    return dag


rail.for_each_instance(create_child_dag)
