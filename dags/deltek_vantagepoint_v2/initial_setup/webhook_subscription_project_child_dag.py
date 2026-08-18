from datetime import timedelta
import rail
from deltek_vantagepoint_v2.initial_setup import utils


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_project_child_dag_id,
        description=f'{config.company_key} Vantagepoint Project Webhook Creation',
        company_key=config.company_key,
        max_active_runs=10,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_webhook_existing',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_webhook_existing = rail.IfOperator(
            task_id='is_webhook_existing',
            test=lambda dag_run: dag_run.conf.get('existing_workflow') is not None,
            yes_task='get_webhook_event_id',
            no_task='create_project_workflow_event'
        )

        def get_event_payload(dag_run):
            company_key = dag_run.conf['company_key']
            event_type = dag_run.conf['event_type']
            payload_key = f'workflow_event_payload_project_{event_type}'
            payload = getattr(config, payload_key).copy()
            payload['Description'] = f"{payload['Description']} - {company_key}"
            return payload

        create_workflow_event = rail.VantagepointAPIOperator(
            task_id='create_project_workflow_event',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/Workflow/Workflow',
            request_method='POST',
            request_body=get_event_payload
        )

        def extract_webhook_event_id(dag_run):
            existing_workflow = dag_run.conf.get('existing_workflow')
            if existing_workflow:
                return existing_workflow['EventID']
            result = rail.result('create_project_workflow_event')
            return result[0]['EventID'] if isinstance(result, list) else result['EventID']

        get_webhook_event_id = rail.PythonOperator(
            task_id='get_webhook_event_id',
            python_callable=extract_webhook_event_id,
            trigger_rule='one_success'
        )

        is_action_created = rail.IfOperator(
            task_id='is_action_created',
            test=lambda dag_run: dag_run.conf.get('existing_action') is not None,
            yes_task='link_project_workflow_action',
            no_task='create_project_webhook_action'
        )

        def get_webhook_action_payload():
            event_id = rail.result('get_webhook_event_id')
            return {
                'ActionID': event_id,
                'WebhookURL': config.project_webhook_url,
                'AuthUsername': config.webhook_username,
                'AuthPassword': config.webhook_password,
                'RetryCount': 0,
                'Timeout': 5,
                'RunAfterSave': 'Y'
            }

        create_webhook_action = rail.VantagepointAPIOperator(
            task_id='create_project_webhook_action',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/Workflow/dlgWorkflowActionWebhook',
            request_method='POST',
            request_body=get_webhook_action_payload
        )

        def get_link_params(dag_run):
            event_id = rail.result('get_webhook_event_id')
            company_key = dag_run.conf['company_key']
            event_type = dag_run.conf['event_type']
            payload_key = f'workflow_event_payload_project_{event_type}'
            payload = getattr(config, payload_key)
            return {
                'WorkflowActions': [
                    {
                        'EventID': event_id,
                        'ActionID': event_id,
                        'Active': 'Y',
                        'ActionType': 'Webhook',
                        'PRLevel': 0,
                        'Description': f"{payload['Description']} - {company_key}"
                    }
                ]
            }

        link_workflow_action = rail.VantagepointAPIOperator(
            task_id='link_project_workflow_action',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/Workflow',
            request_method='PUT',
            request_body=get_link_params,
            trigger_rule='one_success'
        )

        get_existing_args_for_action = rail.VantagepointAPIOperator(
            task_id='get_existing_args_for_action',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/Workflow/dlgWorkflowActionWebhookArgs',
            request_method='GET'
        )


        manage_webhook_args = rail.ForEachOperator(
            task_id='manage_webhook_args',
            items=utils.get_missing_project_args_for_child(config),
            start_task='create_missing_arg',
            end_task='foreach_args_end'
        )

        create_missing_arg = rail.VantagepointAPIOperator(
            task_id='create_missing_arg',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/Workflow/dlgWorkflowActionWebhookArgs',
            request_method='POST',
            request_body=lambda: rail.result('manage_webhook_args')
        )

        foreach_args_end = rail.EmptyOperator(
            task_id='foreach_args_end'
        )

        def get_downstreamtasks_error(error_message):
            return {'error': f'Error in project webhook creation - {error_message}'}

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> is_webhook_existing

        is_webhook_existing >> rail.Label('Yes') >> get_webhook_event_id >> is_action_created
        is_webhook_existing >> rail.Label('No') >> create_workflow_event >> get_webhook_event_id >> is_action_created

        is_action_created >> rail.Label('Yes') >> link_workflow_action
        is_action_created >> rail.Label('No') >> create_webhook_action >> link_workflow_action

        link_workflow_action >> get_existing_args_for_action >> manage_webhook_args
        manage_webhook_args >> create_missing_arg >> foreach_args_end
        manage_webhook_args >> foreach_args_end >> catch_error

        return dag


rail.for_each_instance(create_dag)
