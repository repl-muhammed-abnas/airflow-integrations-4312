
import rail
from rail.lib.ecid import get_dagrun_ecid
from macquariegroup.clientimport.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_client_update_child_{config.instance}',
        description=f'Macquarie Client_Update_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        create_log_per_client_update = rail.CreateLogOperator(
            task_id='create_log_per_client_update'
        )

        update_client_service = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_client_service',
            items='{{ dag_run.conf.client_input | to_json }}',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_put_client_param
        )

        log_client_update_success = rail.WriteLogOperator(
            task_id='log_client_update_success',
            log='{{ result("create_log_per_client_update") }}',
            items='{{ dag_run.conf.client_input | to_json }}',
            message="Client Updated",
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": item['group'] if item['group'] else '',
                "division": item['division'] if item['division'] else '',
                "locationname": item['locationname'] if item['locationname'] else '',
                "businessunit": item['businessunitname'] if item['businessunitname'] else '',
                "status": "Success",
                "details": "Client Updated",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log_per_client_update") }}',
            items='{{ dag_run.conf.client_input | to_json }}',
            trigger_rule='one_failed',
            message=f"Error updating Client - {config.error_template}",
            severity="Error",
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": item['group'] if item['group'] else '',
                "division": item['division'] if item['division'] else '',
                "locationname": item['locationname'] if item['locationname'] else '',
                "businessunit": item['businessunitname'] if item['businessunitname'] else '',
                "status": "Error",
                "details": f"Error updating Client - {config.error_template}",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_log_per_client_update >> update_client_service >> log_client_update_success >> finish >> catch_and_log_errors >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
