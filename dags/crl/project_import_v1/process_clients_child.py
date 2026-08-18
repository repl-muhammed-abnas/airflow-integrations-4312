from datetime import timedelta
from airflow.models import Variable
import rail
from crl.project_import_v1.utils import response_filter,request_payload

def create_child_dag_wbs(config):

    add_dags = []

    for idx in range(0, config.CLIENT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.client_child_dag_id}{get_postfix}",
            description='CRL Process Clients Child',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
                endpoint="/services/ClientListService1.svc/GetData",
                data = request_payload.get_client_data,
                data_handler=response_filter.get_client_data_from_list_service
            )

            check_client_is_available = rail.IfOperator(
                task_id='check_client_is_available',
                test='{{ result("get_clients_in_replicon") | is_truthy }}',
                yes_task='check_client_name_is_not_same',
                no_task='create_client'
            )

            check_client_name_is_not_same = rail.IfOperator(
                task_id='check_client_name_is_not_same',
                test='{{ result("get_clients_in_replicon").name != result("get_query_data").clientname }}',
                yes_task='update_client_name',
                no_task='catch_and_log_errors'
            )

            update_client_name = rail.RepliconServiceOperator(
                task_id='update_client_name',
                endpoint='/services/ClientService1.svc/UpdateName',
                data={
                        "clientUri": '{{ result("get_clients_in_replicon").uri }}',
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
                log= '{{ dag_run.conf.exception_log }}',
                message='{{ get_error_message() }}',
                severity= 'Error',
                properties={
                    'projectcode': '{{ result("get_query_data").projectcode}}',
                    'projectname': '{{ result("get_query_data").projectname}}',
                    'clientcode': '{{ result("get_query_data").clientcode}}',
                    'taskcode': '{{ result("get_query_data").taskcode}}',
                    'taskname': '{{ result("get_query_data").taskname}}',
                    'action': 'Add',
                    'Status': "error",
                    'details': '{{ get_error_message() }}'
                }
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_client_data_from_query

            get_client_data_from_query >> get_query_data >> get_clients_in_replicon >>\
                    check_client_is_available

            check_client_is_available >> rail.Label(
                "Yes") >> check_client_name_is_not_same

            check_client_is_available >> rail.Label(
                "No") >> create_client >> catch_and_log_errors >> log_to_sumo

            check_client_name_is_not_same >> rail.Label(
                "Yes") >> update_client_name >> catch_and_log_errors

            check_client_name_is_not_same >> rail.Label(
                "No") >> catch_and_log_errors

        add_dags.append(dag)

    return add_dags

rail.for_each_instance(create_child_dag_wbs)
