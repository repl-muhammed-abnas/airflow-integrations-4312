from datetime import timedelta
from airflow.models import Variable
import rail
from pwcglobal.project_import_ury_and_arg.utils import request_payload, response_filter

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_clients,
        description='PwC Project Import- Process Clients',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_clients,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_client_data_from_query'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_client_data_from_query',
            end_task='catch_and_log_errors',
        )

        get_client_data_from_query = rail.QueryCollectionOperator(
            task_id='get_client_data_from_query',
            query="""SELECT * from inputdata WHERE clientcode == :client_code LIMIT 1""",
            query_params = {
                'client_code':  '{{ dag_run.conf.clientcode }}'
            }
        )

        get_query_data = rail.PythonOperator(
            task_id = 'get_query_data',
            python_callable= lambda: rail.load_all_records(rail.result("get_client_data_from_query"))[0]
        )

        get_clients_in_replicon = rail.RepliconServiceOperator(
            task_id = 'get_clients_in_replicon',
            endpoint="/services/ClientListService1.svc/GetData",
            data = request_payload.get_client_data,
            data_handler=response_filter.get_data_from_list_service
        )

        if_client_uri_present = rail.IfOperator(
            task_id="if_client_uri_present",
            test='{{result("get_clients_in_replicon") | is_truthy}}',
            yes_task="update_client_in_replicon",
            no_task="create_client_in_replicon"
        )

        update_client_in_replicon = rail.RepliconServiceOperator(
            task_id='update_client_in_replicon',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_update_client_payload
        )

        create_client_in_replicon = rail.RepliconServiceOperator(
            task_id='create_client_in_replicon',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_create_client_payload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.exception_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                    'projectcode': '{{ result("get_query_data").projectcode}}',
                    'projectname': '{{ result("get_query_data").projectname}}',
                    'clientcode': '{{ result("get_query_data").clientcode}}',
                    'taskcode': '{{ result("get_query_data").taskcode}}',
                    'taskname': '{{ result("get_query_data").taskname}}',
                    'action': 'Add',
                    "status": "Error",
                    'details': '{{ get_error_message() }}'
                }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_client_data_from_query >> get_query_data >> get_clients_in_replicon >> \
        if_client_uri_present >> rail.Label('Yes') >> update_client_in_replicon >> catch_and_log_errors
        if_client_uri_present >> rail.Label('No') >> create_client_in_replicon >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
