
import rail
from rail.lib.ecid import get_dagrun_ecid
from macquariegroup.clientimport.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_client_disable_child_{config.instance}',
        description=f'Macquarie Client_Disable_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        create_log_per_client_disable = rail.CreateLogOperator(
            task_id='create_log_per_client_disable'
        )

        disable_client_service = rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_client_service',
            items='{{ dag_run.conf.client_input | to_json }}',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.client_disable_payload
        )

        log_client_disable_success = rail.WriteLogOperator(
            task_id='log_client_disable_success',
            log='{{ result("create_log_per_client_disable") }}',
            items='{{ dag_run.conf.client_input | to_json }}',
            message="Client Disabled",
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": '',
                "division": '',
                "locationname": '',
                "businessunit": '',
                "status": "Success",
                "details": "Client Disabled",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log_per_client_disable") }}',
            items='{{ dag_run.conf.client_input | to_json }}',
            trigger_rule='one_failed',
            message=f"Error disabling Client - {config.error_template}",
            severity="Error",
            properties=lambda item: {
                "client": item['clientname'] if item['clientname'] else '',
                "code": item['clientcode'] if item['clientcode'] else '',
                "location": item['location'] if item['location'] else '',
                "group": '',
                "division": '',
                "locationname": '',
                "businessunit": '',
                "status": "Error",
                "details": f"Error disabling Client - {config.error_template}",
                "childjobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_log_per_client_disable >> disable_client_service >> log_client_disable_success >> finish >> catch_and_log_errors >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
