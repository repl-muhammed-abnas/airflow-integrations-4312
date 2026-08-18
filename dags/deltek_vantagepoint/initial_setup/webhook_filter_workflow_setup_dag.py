from datetime import timedelta
from airflow.models import Variable
from deltek_vantagepoint.initial_setup import utils
import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_webhook_filter_workflow_setup_{config.instance}',
        description='Setup filtered webhook workflow for integrations with Deltek Vantagepoint',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existing_workflow_events'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existing_workflow_events',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_existing_workflow_events = rail.VantagepointAPIOperator(
            task_id='get_existing_workflow_events',
            endpoint="/Workflow/WorkflowEvents/{{dag_run.conf['application_name']}}",
            request_method='GET',
            filters=lambda dag_run: f"?WorkflowAPP=I&ApplicationName={dag_run.conf['application_name']}"
        )

        existing_workflow_event_ids = rail.PythonOperator(
            task_id='existing_workflow_event_ids',
            python_callable=utils.extract_airflow_event_ids
        )

        get_existing_workflow_actions = rail.VantagepointAPIOperator(
            task_id='get_existing_workflow_actions',
            endpoint="/Workflow/WorkflowActions/{{dag_run.conf['application_name']}}",
            request_method='GET',
            filters=lambda dag_run: f"?WorkflowAPP=I&ApplicationName={dag_run.conf['application_name']}"
        )

        existing_webhook_actions_for_airflow = rail.PythonOperator(
            task_id='existing_webhook_actions_for_airflow',
            python_callable=utils.get_existing_webhook_actions_for_airflow
        )

        is_existing_webhook_action_found = rail.IfOperator(
            task_id='is_existing_webhook_action_found',
            test="{{ result('existing_webhook_actions_for_airflow') | length > 0 }}",
            yes_task='get_existing_filter_conditions',
            no_task='log_to_sumo'
        )

        get_existing_filter_conditions = rail.VantagepointAPIOperator(
            task_id='get_existing_filter_conditions',
            endpoint='/Workflow/dlgWorkflowConditions',
            request_method='GET',
            filters=lambda dag_run: f"?WorkflowAPP=I&ApplicationName={dag_run.conf['application_name']}"
        )

        manage_workflow_conditions = rail.ForEachOperator(
            task_id='manage_workflow_conditions',
            items=utils.get_conditions_to_update_or_create,
            start_task='update_or_create_single_condition',
            end_task='log_to_sumo'
        )

        update_or_create_single_condition = rail.VantagepointAPIOperator(
            task_id='update_or_create_single_condition',
            endpoint="{{ result('manage_workflow_conditions')['_endpoint'] }}",
            request_method="{{ result('manage_workflow_conditions')['_method'] }}",
            request_body=lambda: {
                k: v for k, v in rail.result("manage_workflow_conditions").items()
                if not k.startswith('_')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_existing_workflow_events

        get_existing_workflow_events >> existing_workflow_event_ids >> get_existing_workflow_actions >> existing_webhook_actions_for_airflow >> is_existing_webhook_action_found

        is_existing_webhook_action_found >> rail.Label(
            'Yes') >> get_existing_filter_conditions >> manage_workflow_conditions
        manage_workflow_conditions >> update_or_create_single_condition >> log_to_sumo
        manage_workflow_conditions >> log_to_sumo
        is_existing_webhook_action_found >> rail.Label(
            'No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
