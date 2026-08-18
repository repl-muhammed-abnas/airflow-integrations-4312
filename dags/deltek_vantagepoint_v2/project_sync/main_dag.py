from airflow.models import Variable
from datetime import timedelta
import rail

from deltek_vantagepoint_v2.project_sync.utils import python_callable_method


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_sync_main_dag_id,
        description=f'{config.company_key} Deltek Vantagepoint project and client sync',
        schedule_interval=None,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_full_sync'
        )


        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_full_sync',
            end_task='should_log_history',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )


        def check_if_full_sync(dag_run):
            company_key = dag_run.conf.get('company_key', config.company_key)
            key = f'{config.is_project_full_sync_var}_{company_key}'
            value = Variable.get(key, default_var='true').lower() == 'true'
            if value:
                Variable.set(key, 'false')
            return value
        
        is_full_sync = rail.IfOperator(
            task_id='is_full_sync',
            test=check_if_full_sync,
            yes_task='fetch_all_vp_projects',
            no_task='is_webhook_invalid'
        )

        is_webhook_invalid = rail.IfOperator(
            task_id='is_webhook_invalid',
            test=lambda dag_run: python_callable_method.get_invalidity_reason(dag_run, config),
            yes_task='should_log_history',
            no_task='is_delete_action'
        )


        is_delete_action = rail.IfOperator(
            task_id='is_delete_action',
            test=lambda dag_run: dag_run.conf['webhook']['data']['Action'] == config.WEBHOOK_ACTION['DELETE'],
            yes_task='is_phase_or_task',
            no_task='fetch_project_hierarchy'
        )

        is_phase_or_task = rail.IfOperator(
            task_id='is_phase_or_task',
            test="{{ dag_run.conf.webhook.data.WBS2 != ' ' }}",
            yes_task='fetch_project_hierarchy',
            no_task='should_log_history'
        )

        fetch_project_hierarchy = rail.VantagepointAPIOperator(
            task_id='fetch_project_hierarchy',
            endpoint='/project/hierarchy/{{ dag_run.conf.webhook.data.WBS1 }}',
            filters=f'?fieldFilter={config.PROJECT_FIELDS}',
            request_method='GET',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )


        fetch_all_vp_projects = rail.VantagepointAPIOperator(
            task_id = 'fetch_all_vp_projects',
            endpoint='/project',
            filters=f'?fieldFilter={config.PROJECT_FIELDS}',
            request_method='GET',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )


        is_project_exists = rail.IfOperator(
            task_id='is_project_exists',
            test='{{ get_error_message() | is_falsy }}',
            yes_task='filtered_projects',
            no_task='should_log_history',
            trigger_rule='all_done'
        )


        filtered_projects = rail.PythonOperator(
            task_id='filtered_projects',
            python_callable=lambda dag_run: python_callable_method.get_filtered_projects(dag_run, config)
        )

        trigger_project_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_sync_child_dag',
            retries=0,
            items=lambda dag_run: python_callable_method.get_child_dag_confs(dag_run, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.project_sync_child_dag_id,
            reset_count=50000,
            thread_pool_size=4,
            conf=lambda item: item
        )

        wait_for_project_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_project_sync_completion',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_project_sync_child_dag") }}'
        )


        gather_child_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_errors',
            dag_runs="{{ result('trigger_project_sync_child_dag') }}",
            dagrun_task_id='catch_error',
            flatten=True
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ result('gather_child_dag_errors') | is_truthy }}",
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun',
            trigger_rule='all_done'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow,
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task
        can_run_batch_task >> rail.Label('No') >> is_full_sync

        batch_task >> should_log_history
        is_full_sync >> rail.Label('Yes') >> fetch_all_vp_projects >> filtered_projects
        is_full_sync >> rail.Label('No') >> is_webhook_invalid

        is_webhook_invalid >> rail.Label('Yes') >> should_log_history
        is_webhook_invalid >> rail.Label('No') >> is_delete_action

        is_delete_action >> rail.Label('Yes') >> is_phase_or_task
        is_delete_action >> rail.Label('No') >> fetch_project_hierarchy

        is_phase_or_task >> rail.Label('No') >> should_log_history
        is_phase_or_task >> rail.Label('Yes') >> fetch_project_hierarchy >> is_project_exists

        is_project_exists >> rail.Label('Yes') >> filtered_projects >> trigger_project_sync_child_dag
        is_project_exists >> rail.Label('No') >> should_log_history

        trigger_project_sync_child_dag >> wait_for_project_sync_completion >> gather_child_dag_errors >> should_log_history

        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_dag)
