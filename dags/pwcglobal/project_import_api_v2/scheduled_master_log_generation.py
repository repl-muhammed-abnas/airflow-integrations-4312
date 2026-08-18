from datetime import datetime as py_datetime, timedelta, timezone
from pendulum import datetime
import rail
from pwcglobal.project_import_api_v2.custom_method import get_unique_sender_ids_by_companykey
from airflow.models import Variable, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.session import NEW_SESSION, provide_session
# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api_v2/config.py


# pylint:disable = too-many-statements
def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_import_master_log_scheduled_{config.instance}_v2',
        description=f'Projectimport_dynamicwait_Loggeneration_Project-Scheduled {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_runs=config.master_scheduled_log_generation_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1),
    ) as dag:

        @provide_session
        def get_dagruns_to_process(session=NEW_SESSION):

            current_time = py_datetime.now(timezone.utc)
            lookup_timestamp_value = Variable.get(
                config.lookup_log_timestamp_var, default_var=None)

            query_end_date = py_datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
                current_time - timedelta(hours=config.lookup_log_timestamp_hours))

            Variable.set(config.lookup_log_timestamp_var,
                         current_time.isoformat())

            dag_runs_to_filter = (
                session.query(DagRun.id, DagRun.dag_id,
                              DagRun.state, DagRun.end_date)
                .select_from(DagRun)
                .filter(
                    DagRun.dag_id == f'pwc_project_import_child_log_pregeneration_{config.instance}_v2', DagRun.state.in_(
                        [DagRunState.SUCCESS]), (DagRun.end_date >= query_end_date))
                .group_by(DagRun.id, DagRun.dag_id, DagRun.state, DagRun.end_date)
                .all()
            )
            dag_runs = [item[0]
                        for item in dag_runs_to_filter] if dag_runs_to_filter else []

            return dag_runs
        
        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_project_logs',
            no_task='delete_this_dagrun'
        )

        get_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_project_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='format_logs',
            flatten=True
        )

        compose_project_logs = rail.CreateCollectionOperator(
            task_id='compose_project_logs',
            source=lambda: rail.result('get_project_logs')
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('compose_project_logs', 'length') > 0 }}",
            yes_task='process_logs_by_sender',
            no_task='delete_this_dagrun'
        )

        process_logs_by_sender = rail.TriggerDagRunForEachItemOperator(
            task_id='process_logs_by_sender',
            retries=0,
            items=get_unique_sender_ids_by_companykey,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwc_project_import_child_log_{config.instance}_v2',
            conf={
                'sender_id': "{{ item }}",
                'project_final_logs': "{{ result('compose_project_logs') }}"
            }
        )

        wait_for_process_logs_by_sender = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_logs_by_sender',
            dag_runs='{{ result("process_logs_by_sender") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_project_logs >> compose_project_logs >> has_any_data

        has_any_data >> rail.Label(
            "Yes") >> process_logs_by_sender >> wait_for_process_logs_by_sender

        has_any_data >> rail.Label(
            "No") >> delete_this_dagrun

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        return dag


rail.for_each_instance(create_log_airflow_dag)
