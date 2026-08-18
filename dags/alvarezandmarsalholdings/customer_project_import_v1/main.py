from datetime import datetime, timedelta
from pendulum import now
import itertools
import rail
from alvarezandmarsalholdings.customer_project_import_v1.utils import python_callable, request_payload, response_filter
from alvarezandmarsalholdings.customer_project_import_v1.task.get_project_prereqs import get_project_prereqs_task_group

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'{config.company_key} Customer Project Import - Master Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test=lambda dag_run: bool(dag_run.conf['payload']),
            yes_task="current_date_and_log_file_details"
        )

        current_date_and_log_file_details = rail.PythonOperator(
            task_id='current_date_and_log_file_details',
            python_callable=lambda: {
                'current_date': now().strftime("%Y-%m-%d"),
                'log_filename': "log_"+ rail.render_template('{{ dag_run_ecid() | replace(":", "-") }}') + "_customer_project_import_" + \
                    now().strftime("%Y%m%dT%H%M%S") + ".csv"
            }
        )

        create_exception_log = rail.CreateLogOperator(
            task_id = "create_exception_log"
        )

        dummy_get_project_prereqs, get_project_prereqs = get_project_prereqs_task_group(config)

        dummy_process_projects = rail.EmptyOperator(
            task_id='dummy_process_projects'
        )

        update_record_id_for_batch =rail.PythonOperator(
            task_id= 'update_record_id_for_batch',
            python_callable= python_callable.get_payload_with_record_id,
            show_return_value_in_logs=False
        )

        def get_process_process_trigger_id(item):
            modulo = int(item.get('record_id', 0)) % config.PROJECT_BATCH_COUNT
            if modulo == 0:
                return config.process_projects
            return f"{config.process_projects}_batch_{str(modulo)}"

        process_projects = rail.trigger_parallel_dagrun(
            task_id='process_projects',
            items=lambda: rail.result('update_record_id_for_batch'),
            parallel_count=config.trigger_parallel_dagrun_count_process_projects,
            trigger_dag_id=get_process_process_trigger_id,
            conf=lambda item: request_payload.get_process_project_conf(
                item, config
                ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_projects_dag_ids =rail.PythonOperator(
            task_id= 'get_process_projects_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_projects_{x+1}'), range(config.trigger_parallel_dagrun_count_process_projects))))),
            show_return_value_in_logs= False
        )

        gather_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_logs',
            dag_runs='{{ result("get_process_projects_dag_ids") }}',
            dagrun_task_id='create_project_log',
            execution_timeout=timedelta(
                hours=config.gather_project_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'projectlogs': "{{result('gather_project_logs')}}",
                'otherlogs': "{{result('create_exception_log')}}",
                'log_filename': '{{ result("current_date_and_log_file_details").log_filename }}'
            }
        )

        is_data_available >> rail.Label(
            'Yes') >> current_date_and_log_file_details >> create_exception_log

        create_exception_log >> dummy_get_project_prereqs
        get_project_prereqs >> dummy_process_projects >> update_record_id_for_batch >> process_projects >> get_process_projects_dag_ids >> \
        gather_project_logs >> process_log_generation

    return dag


rail.for_each_instance(create_main_dag)
