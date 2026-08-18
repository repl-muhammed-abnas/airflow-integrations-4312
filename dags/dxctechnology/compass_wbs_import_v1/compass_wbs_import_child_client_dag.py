import rail
from dxctechnology.compass_wbs_import_v1 import request_payload
from dxctechnology.compass_wbs_import_v1 import response_filter

def create_child_import_client_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_client_dagid,
        description='DXC_Compass_WBS_Automation Client Child V1.0',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.client_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        client_name = "{{ dag_run.conf.client }}"
        client_code = "{{ dag_run.conf.client_code }}"

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param,
            response_filter= response_filter.map_client_name
        )

        does_client_exist = rail.IfOperator(
            task_id="does_client_exist",
            test="{{ result('search_client_in_replicon') is not none }}",
            yes_task="finish",
            no_task="create_client"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data= request_payload.get_put_client_param
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'client_name': client_name,
                'client_code': client_code,
                'status': 'Error',
            },
        )

        search_client_in_replicon >> does_client_exist
        does_client_exist >> rail.Label("Yes") >> finish
        does_client_exist >> rail.Label("No") >> create_client >> finish
        finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_import_client_dag)
