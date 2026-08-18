from datetime import timedelta
import rail
from mammoet.project_import_v1.utils import response_filter,request_payload
from airflow.models import Variable

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.client_child_dag_id,
        description='Mammoet Process Clients Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
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

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_client_data_from_query = rail.QueryCollectionOperator(
            task_id='get_client_data_from_query',
            query="""SELECT * from validwbsdata WHERE clientcode == :client_code LIMIT 1""",
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
            endpoint="/services/ClientService1.svc/GetActiveClients",
            data_handler=response_filter.get_client_data
        )

        check_client_is_available = rail.IfOperator(
            task_id='check_client_is_available',
            test='{{ result("get_clients_in_replicon") | is_truthy }}',
            yes_task='update_client_name',
            no_task='create_client'
        )

        update_client_name = rail.RepliconServiceOperator(
            task_id='update_client_name',
            endpoint='/services/ClientService1.svc/UpdateName',
            data={
                    "clientUri": '{{ result("get_clients_in_replicon") }}',
                    "name": '{{ result("get_query_data").clientname }}'
                }
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_create_client_payload
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log= '{{ dag_run.conf.project_log }}',
            message='{{ get_error_message() }}',
            severity= 'Error',
            items= '{{ result("get_query_data") }}',
            properties={
                'projectcode': '{{ item.projectcode}}',
                'projectname(code)': '{{ item.projectname(code)}}',
                'projectname(name)': '{{ item.projectname(name)}}',
                'programcode': '{{ item.programcode}}',
                'programname(code)': '{{ item.programname(code)}}',
                'programname(name)': '{{ item.programname(name)}}',
                'clientname': '{{ item.clientname}}',
                'clientcode': '{{ item.clientcode}}',
                'projecttype': '{{ item.projecttype}}',
                'details': '{{ get_error_message() }}',
                'status': "error"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_client_data_from_query

        get_client_data_from_query >> get_query_data >> get_clients_in_replicon >>\
                check_client_is_available

        check_client_is_available >> rail.Label(
            "Yes") >> update_client_name >> catch_and_log_errors

        check_client_is_available >> rail.Label(
            "No") >> create_client >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
