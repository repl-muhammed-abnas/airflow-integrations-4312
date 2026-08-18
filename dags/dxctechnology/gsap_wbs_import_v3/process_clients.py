from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v3.utils import request_payload
from dxctechnology.gsap_wbs_import_v3.utils import response_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_clients_dagid,
        description='DXC_GSAP_WBS_Automation Process Client',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_clients,
    ) as dag:

        clientname = "{{ dag_run.conf.clientname }}"

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_client_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='search_client_in_replicon',
            end_task='catch_and_log_errors',
        )

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param(clientname),
            data_handler=response_filter.get_filtered_client_data
        )

        does_client_exist = rail.IfOperator(
            task_id="does_client_exist",
            test=lambda: bool(rail.result('search_client_in_replicon')),
            yes_task="finish",
            no_task="create_client"
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_put_client_param(clientname)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'clientname': clientname,
                'status': 'Error',
            },
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task= "fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id = "fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> search_client_in_replicon

        search_client_in_replicon >> does_client_exist
        does_client_exist >> rail.Label("Yes") >> finish
        does_client_exist >> rail.Label("No") >> create_client >> finish
        finish >> catch_and_log_errors >> log_to_sumo >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag)
