from datetime import timedelta
import pendulum
from capefoxcorporation.project_sync.utils import request_payload, response_filters, custom_methods
import itertools
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements, line-too-long
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Capefoxcorporation Deltek Costpoint Project Sync Main DAG {config.instance}',
        schedule_interval=timedelta(minutes=config.master_dag_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_costpoint_conn_id,
        }
    ) as dag:

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=custom_methods.do_get_last_run_date,
            op_args=[config.last_run_date_var_name, config.time_zone],
        )

        can_load_data_in_chunks = rail.IfOperator(
            task_id='can_load_data_in_chunks',
            test=lambda: Variable.get(
                    config.get_data_in_chunk_var_name, default_var='false').lower() == 'true',
            yes_task='get_modified_projects_in_chunks',
            no_task='get_modified_projects'
        )

        get_modified_projects_in_chunks = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='get_modified_projects_in_chunks',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_costpoint_company_ids,
            items=lambda: custom_methods.get_project_filter_items(config.costpoint_time_zone),
            data=lambda item: request_payload.get_modified_projects_chunk_payload(item),
            # Filter projects by Owning Organization (ORG_ID starting with 1.02 or 3.01)
            data_handler=lambda data: [
                row for row in data['document']['rows'] 
                if row['row']['data'].get('ORG_ID', '').startswith(('1.02', '3.01'))
            ],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        get_modified_projects = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_projects',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_costpoint_company_ids,
            data=lambda: request_payload.get_modified_projects_payload(config.costpoint_time_zone),
            # Filter projects by Owning Organization (ORG_ID starting with 1.02 or 3.01)
            data_handler=lambda data: [
                row for row in data['document']['rows'] 
                if row['row']['data'].get('ORG_ID', '').startswith(('1.02', '3.01'))
            ],
        )

        has_project_data = rail.IfOperator(
            task_id='has_project_data',
            test=lambda: bool((rail.result('get_modified_projects') or rail.result(
                'get_modified_projects_in_chunks'))),
            yes_task='group_data_by_root_project',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        group_data_by_root_project = rail.PythonOperator(
            task_id='group_data_by_root_project',
            python_callable=lambda: [{'root_project_id': k, 'data': list(g)} for k, g in itertools.groupby(
                (rail.result('get_modified_projects') or rail.result('get_modified_projects_in_chunks')),
                    lambda x: x['row']['data']['PROJ_ID'].split(".")[0])]
        )

        get_all_clients_from_replicon = rail.RepliconServiceOperator(
            task_id='get_all_clients_from_replicon',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.get_all_clients_payload,
            data_handler=lambda data: list(
                map(lambda x: x['cells'][0]['textValue'], data['rows']))
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
        )

        get_costpoint_plcs = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_plcs',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_costpoint_company_ids,
            data=request_payload.get_costpoint_plcs_payload,
            data_handler=lambda data: list(map(lambda x:
                {
                    'name': x['row']['data']['UDT07_NAME'][0:50],
                    'code': x['row']['data']['UDT07_ID']
                }, data['document']['rows'])),
        )

        get_project_udfs = rail.RepliconServiceOperator(
            task_id='get_project_udfs',
            endpoint="/services/ProjectCustomFieldListService1.svc/GetData",
            data=request_payload.get_project_udfs_payload,
            data_handler=lambda data: list(
                map(lambda x: x['cells'][0], data['rows']))
        )

        process_clients = rail.RepliconServiceCallForEachItemOperator(
            task_id='process_clients',
            endpoint="/services/ClientService1.svc/PutClient",
            items=custom_methods.get_new_client_names,
            data=request_payload.process_clients_payload
        )

        process_each_root_project = rail.trigger_parallel_dagrun(
            task_id='process_each_root_project',
            items=lambda: rail.result('group_data_by_root_project'),
            trigger_dag_id=config.process_each_root_project_child_dag_id,
            parallel_count=config.trigger_parallel_dagrun_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'item': {**item},
                'permission_sets': rail.result('get_all_permission_sets'),
                'project_udfs': rail.result('get_project_udfs'),
                'plc_data': rail.result('get_costpoint_plcs')
            }
        )

        process_dag_runs_logs = rail.EmptyOperator(
            task_id='process_dag_runs_logs'
        )

        get_process_each_root_project_dag_ids = rail.PythonOperator(
            task_id='get_process_each_root_project_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_each_root_project_{x+1}') if rail.result(
                    f'process_each_root_project_{x+1}') else []), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("get_process_each_root_project_dag_ids") }}',
            dagrun_task_id='create_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs'))))))
        )

        update_last_run_date = rail.PythonOperator(
            task_id='update_last_run_date',
            python_callable=lambda: Variable.set(config.last_run_date_var_name, rail.result('get_last_run_date', 'current_time'))
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        get_last_run_date >> can_load_data_in_chunks
        can_load_data_in_chunks >> rail.Label(
            'Yes') >> get_modified_projects_in_chunks >> has_project_data
        can_load_data_in_chunks >> rail.Label(
            'No') >> get_modified_projects >> has_project_data
        has_project_data >> rail.Label('Yes') >> group_data_by_root_project
        has_project_data >> rail.Label(
            'No') >> delete_this_dagrun >> update_last_run_date >> finish
        group_data_by_root_project >> get_all_clients_from_replicon >> get_costpoint_plcs >> \
            get_project_udfs >> get_all_permission_sets >> process_clients >> process_each_root_project >> \
            process_dag_runs_logs >> get_process_each_root_project_dag_ids >> gather_child_logs >> \
            format_logs >> update_last_run_date >> finish

        return dag


rail.for_each_instance(create_dag)
