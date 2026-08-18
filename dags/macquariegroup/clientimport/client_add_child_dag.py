
import rail
from rail.lib.ecid import get_dagrun_ecid
from macquariegroup.clientimport.utils import request_payload
from macquariegroup.clientimport.utils import python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_client_add_child_{config.instance}',
        description=f'Macquarie Client_Add_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        create_log_per_client_add = rail.CreateLogOperator(
            task_id='create_log_per_client_add'
        )

        filter_clients = rail.PythonOperator(
            task_id='filter_clients',
            python_callable=python_callable_method.filter_clients
        )

        log_client_name_required = rail.WriteLogOperator(
            task_id='log_client_name_required',
            log='{{ result("create_log_per_client_add") }}',
            items='{{ result("filter_clients") | attr_or_default("invalid_clients") | to_json }}',
            message='Client not Added - Client name is required',
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": item['group'] if item['group'] else '',
                "division": item['division'] if item['division'] else '',
                "locationname": item['locationname'] if item['locationname'] else '',
                "businessunit": item['businessunitname'] if item['businessunitname'] else '',
                "status": "Skipped",
                "details": "Client not Added - Client name is required",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        create_client = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_client',
            items='{{ result("filter_clients") | attr_or_default("valid_clients") | to_json }}',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_put_client_param
        )

        log_client_added = rail.WriteLogOperator(
            task_id='log_client_added',
            log='{{ result("create_log_per_client_add") }}',
            items='{{ result("filter_clients") | attr_or_default("valid_clients") | to_json }}',
            message="Client Added",
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": item['group'] if item['group'] else '',
                "division": item['division'] if item['division'] else '',
                "locationname": item['locationname'] if item['locationname'] else '',
                "businessunit": item['businessunitname'] if item['businessunitname'] else '',
                "status": "Success",
                "details": "Client Added",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log_per_client_add") }}',
            items='{{ result("filter_clients") | attr_or_default("valid_clients") | to_json }}',
            trigger_rule='one_failed',
            severity='Error',
            message=f"Error adding Client - {config.error_template}",
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": item['group'] if item['group'] else '',
                "division": item['division'] if item['division'] else '',
                "locationname": item['locationname'] if item['locationname'] else '',
                "businessunit": item['businessunitname'] if item['businessunitname'] else '',
                "status": "Error",
                "details": f"Error adding Client - {config.error_template}",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_log_per_client_add >> filter_clients >> log_client_name_required >> create_client >> log_client_added >> finish
        finish >> catch_and_log_errors >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
