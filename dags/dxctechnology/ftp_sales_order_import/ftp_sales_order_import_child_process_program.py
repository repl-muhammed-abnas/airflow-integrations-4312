import rail
from dxctechnology.ftp_sales_order_import import request_payload
from dxctechnology.ftp_sales_order_import import response_filter

# config
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ftp_sales_order_import/config.py

def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_ftp_sales_order_import_child_process_program{dag_id_postfix}',
        description = 'DXC_FTP_SalesOrder Program Child V1.1',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        max_active_runs = config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_config")

        program_name = "{{ dag_run.conf.programname }}"

        search_program = rail.RepliconServiceOperator(
            task_id='search_program',
            endpoint='/services/ProgramListService1.svc/GetData',
            data=request_payload.search_programs(program_name),
            response_filter=lambda response: response_filter.program_filter(response, request_payload.get_dag_run_conf()['programname'])
        )

        has_program = rail.IfOperator(
            task_id ='has_program',
            test = lambda : rail.result('search_program')!=[],
            yes_task="finish",
            no_task="create_program"
        )

        create_program = rail.RepliconServiceOperator(
            task_id='create_program',
            endpoint='/services/ProgramService1.svc/PutProgram',
            data=request_payload.get_put_program(program_name)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity = 'Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'program_name': program_name,
                'status': 'Error',
            },
        )

        search_program >> has_program
        has_program >> rail.Label("Yes") >> finish
        has_program >> rail.Label("No") >> create_program >> finish
        finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_wbs)
