from datetime import timedelta
from airflow.models import Variable
import rail
from technicolorg3.ceta_project_client_data.utils import request_payload
from technicolorg3.ceta_project_client_data.utils import response_filter

null = None

# pylint: disable=too-many-statements


def create_create_client_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_project_client_details_create_client_{config.instance}',
        description=f'Technicolor CETA Create Client {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='search_clients'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_clients',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            end_task='catch_and_log_errors'
        )

        search_clients = rail.RepliconServiceOperator(
            task_id='search_clients',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.search_client_data,
            data_handler=response_filter.get_client_uri
        )

        is_client_present = rail.IfOperator(
            task_id='is_client_uri_present',
            test=lambda: bool(rail.result('search_clients')),
            yes_task='client_uri',
            no_task='create_client'
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.create_client_payload,
            data_handler=lambda resp: resp['uri']
        )

        client_uri = rail.PythonOperator(
            task_id='client_uri',
            python_callable=lambda: rail.result('search_clients') if rail.result(
                'search_clients') else rail.result('create_client')

        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'db': '',
                'client': '',
                'project': '',
                'status': 'Exception',
                'action': 'Create Client',
                'details': {config.error_template},
                'reference': '',
                'exported': 'No'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'clientname ': '{{ dag_run.conf.clientname }}',
                'clientcode': '{{ dag_run.conf.clientcode }}',
                'status': '\
                    {%- if result("search_clients") | is_truthy -%} \
                         Already present \
                    {%- else -%} \
                         Created\
                    {%- endif -%}',
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> search_clients

        search_clients >> is_client_present
        is_client_present >> rail.Label('Yes') >> client_uri >> finish
        is_client_present >> rail.Label('No') >> create_client >> client_uri

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_create_client_child_dag)
