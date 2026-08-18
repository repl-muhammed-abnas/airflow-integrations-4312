import rail
from dxctechnology.c1_wbs_import_v6.utils import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import_v6/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id_program,
        description=f'DXC_C1_WBS_Automation Program Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.program_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        program_name = "{{ dag_run.conf.program_name }}"

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_program_in_replicon = rail.RepliconServiceOperator(
            task_id='search_program_in_replicon',
            endpoint='/services/ProgramListService1.svc/GetData',
            data=request_payload.get_program_list_search_param(program_name)
        )

        does_program_exist = rail.IfOperator(
            task_id="does_program_exist",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                list(
                    map(
                        lambda x: x['cells'][0],
                        rail.result('search_program_in_replicon')['rows'])),
                'textValue',
                request_payload.get_dag_run_conf()['program_name'])),
            yes_task="finish",
            no_task="create_program"
        )

        create_program = rail.RepliconServiceOperator(
            task_id='create_program',
            endpoint='/services/ProgramService1.svc/PutProgram',
            data=request_payload.get_put_program_param(program_name)
        )

        finish = rail.EmptyOperator(
            task_id='finish')

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'program_name': program_name,
                'status': 'Error',
            },
        )

        search_program_in_replicon >> does_program_exist
        does_program_exist >> rail.Label("Yes") >> finish
        does_program_exist >> rail.Label("No") >> create_program >> finish
        finish >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
