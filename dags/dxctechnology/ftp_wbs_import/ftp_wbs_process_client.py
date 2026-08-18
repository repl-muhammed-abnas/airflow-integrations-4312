import rail
from dxctechnology.ftp_wbs_import.utils import request_payload
from dxctechnology.ftp_wbs_import.utils import python_callable_method


# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ftp_wbs_import_child_process_client_{config.instance}',
        description=f'DXC_FTP_WBS_Automation Client Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs
    ) as dag:

        client = "{{ dag_run.conf.client }}"

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_client_in_replicon = rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param(client)
        )

        does_client_exist = rail.IfOperator(
            task_id="does_client_exist",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                list(
                    map(
                        lambda x: x['cells'][0],
                        rail.result('search_client_in_replicon')['rows'])),
                'textValue',
                python_callable_method.get_dag_run_conf()['client'])),
            yes_task="finish",
            no_task="create_client"
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.get_put_client_param(client)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'client_name': client,
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
