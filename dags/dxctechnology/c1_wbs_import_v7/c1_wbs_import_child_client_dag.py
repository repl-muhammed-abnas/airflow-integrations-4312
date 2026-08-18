import rail
from dxctechnology.c1_wbs_import_v7.utils import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import_v7/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id_client,
        description=f'DXC_C1_WBS_Automation Client Child V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.client_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        client_name = "{{ dag_run.conf.client_name }}"
        client_code = "{{ dag_run.conf.client_code }}"

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param(client_name)
        )

        does_client_exist = rail.IfOperator(
            task_id="does_client_exist",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                list(
                    map(
                        lambda x: x['cells'][0],
                        rail.result('search_client_in_replicon')['rows'])),
                'textValue',
                request_payload.get_dag_run_conf()['client_name'])),
            yes_task="finish",
            no_task="create_client"
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_put_client_param(client_name, client_code)
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

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        search_client_in_replicon >> does_client_exist
        does_client_exist >> rail.Label("Yes") >> finish
        does_client_exist >> rail.Label("No") >> create_client >> finish
        finish >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
