from datetime import timedelta
from elevatebiomanagement.time_export.utils import response_filter
from airflow.models import Variable
import rail

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'elevatebio_management_time_export_child_{config.instance}',
        description=f'Elevate Bio Management Time Export Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='load_time_data_csv'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_time_data_csv',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        load_time_data_csv = rail.PythonOperator(
            task_id='load_time_data_csv',
            python_callable=response_filter.load_csv,
            op_args=[config.export_columns]
        )

        get_list_file = rail.SimpleHttpOperator(
            task_id='get_list_file',
            method='GET',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/files",
            headers={
                "Content-Type": 'application/json',
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            response_filter=response_filter.get_extract_file
        )

        upload_file = rail.SimpleHttpOperator(
            task_id='upload_file',
            method='PUT',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/files/{{ result('get_list_file')[0].id }}",
            headers={
                "Content-Type": 'application/octet-stream',
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            data='{{ result("load_time_data_csv") }}'
        )

        mark_upload_as_complete = rail.SimpleHttpOperator(
            task_id='mark_upload_as_complete',
            method='POST',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/files/{{ result('get_list_file')[0].id }}/complete",
            headers={
                "Content-Type": 'application/json',
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            data='{"chunckCount": -1}',
            response_filter=response_filter.filter_response
        )

        get_import_ids = rail.SimpleHttpOperator(
            task_id='get_import_ids',
            method='GET',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/imports",
            headers={
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            response_filter=response_filter.filter_response
        )

        import_tasks_list = rail.SimpleHttpOperator(
            task_id='import_tasks_list',
            method='POST',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/imports/112000000066/tasks",
            headers={
                "Content-Type": 'application/json',
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            data='{"localeName": "en_US"}',
            response_filter=response_filter.filter_response
        )

        import_tasks_module = rail.SimpleHttpOperator(
            task_id='import_tasks_module',
            method='POST',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/imports/112000000067/tasks",
            headers={
                "Content-Type": 'application/json',
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            data='{"localeName": "en_US"}',
            response_filter=response_filter.filter_response
        )

        get_import_actions = rail.SimpleHttpOperator(
            task_id='get_import_actions',
            method='GET',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/" +
                "{{ dag_run.conf.modelid }}/imports/112000000067/tasks/{{ result('import_tasks_module').task.taskId }}",
            headers={
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            response_filter=response_filter.filter_response
        )

        get_process_id = rail.SimpleHttpOperator(
            task_id='get_process_id',
            method='GET',
            http_conn_id=config.default_http_conn_id,
            endpoint="api.anaplan.com/2/0/workspaces/{{ dag_run.conf.workspaceid }}/models/{{ dag_run.conf.modelid }}/processes",
            headers={
                "Authorization": "AnaplanAuthToken {{ dag_run.conf.tokenvalue }}"
            },
            response_filter=response_filter.filter_response
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> load_time_data_csv

        load_time_data_csv >> get_list_file >> upload_file >> mark_upload_as_complete >> get_import_ids
        get_import_ids >> import_tasks_list >> import_tasks_module >> get_import_actions >> get_process_id >> log_to_sumo

        return dag


rail.for_each_instance(create_child_dag_wbs)
