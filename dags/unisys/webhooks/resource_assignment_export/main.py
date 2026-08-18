from pendulum import datetime
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_master_dag_id,
        description=f'Unisys Resource Assignment Webhook Receiver {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 1, 1),
        max_active_runs=config.max_active_runs,
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=f'unisys_resource_assignment_created_webhook_secret_{config.instance}',
            ),
            rail.WebhookConf(
                hmac_secret_var=f'unisys_resource_assignment_modified_webhook_secret_{config.instance}',
            ),
            rail.WebhookConf(
                hmac_secret_var=f'unisys_resource_assignment_deleted_webhook_secret_{config.instance}',
            )
        ],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_webhook_payload")

        # Validate webhook event type
        is_valid_event_type = rail.IfOperator(
            task_id="is_valid_event_type",
            test="{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] in [" +
                 "'ProjectPolarisTeamMemberAllocationCreated', 'ProjectPolarisTeamMemberAllocationModified', " +
                 "'ProjectPolarisTeamMemberAllocationDeleted'] }}",
            yes_task="get_webhook_log",
            no_task="fail_invalid_event_type"
        )

        # Get or create tenant-wide log
        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name=config.webhook_log_name,
            existing_log_mode="append"
        )

        # Store webhook event in tenant-wide log
        store_webhook_event = rail.WriteLogOperator(
            task_id="store_webhook_event",
            log="{{ result('get_webhook_log') }}",
            message="{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] }}",
            properties=lambda dag_run: {
                'resource_uri': dag_run.conf['webhook']['data'].get('id', ''),
                'event_type': dag_run.conf['webhook']['headers']['X-Replicon-Webhook-Event-Type'].replace('ProjectPolarisTeamMemberAllocation', ''),
                'project_uri': dag_run.conf['webhook']['data'].get('project', {}).get('uri', ''),
                'user_uri': dag_run.conf['webhook']['data'].get('user', {}).get('uri', ''),
                'modified_date': dag_run.conf['webhook'].get('received_at', '')
            }
        )

        # Fail for invalid event types
        fail_invalid_event_type = rail.FailOperator(
            task_id="fail_invalid_event_type",
            message="Received invalid webhook event type: '{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] }}'"
        )

        # Task flow
        is_valid_event_type >> rail.Label("Yes") >> get_webhook_log >> store_webhook_event
        is_valid_event_type >> rail.Label("No") >> fail_invalid_event_type

    return dag

rail.for_each_instance(create_dag)
