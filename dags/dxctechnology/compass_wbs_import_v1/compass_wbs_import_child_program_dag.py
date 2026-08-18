import rail
from dxctechnology.compass_wbs_import_v1 import request_payload
from dxctechnology.compass_wbs_import_v1 import response_filter

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import/config.py
def create_import_child_program_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_program_dagid,
        description='DXC_Compass_WBS_Automation Program Child V1.0',
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
            data=request_payload.get_program_list_search_param(program_name),
            response_filter= response_filter.map_program_name
        )

        does_program_exist = rail.IfOperator(
            task_id="does_program_exist",
            test="{{ result('search_program_in_replicon') is not none }}",
            yes_task="finish",
            no_task="create_program"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        create_program = rail.RepliconServiceOperator(
            task_id='create_program',
            endpoint='/services/ProgramService1.svc/PutProgram',
            data=request_payload.get_put_program_param(program_name)
        )

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

rail.for_each_instance(create_import_child_program_airflow_dag)
