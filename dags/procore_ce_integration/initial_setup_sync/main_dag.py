from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.initial_setup_main_dag_id,
        description='Procore Initial Setup - Webhook Subscription Management',
        schedule_interval=None,  # Manual trigger only
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.instance,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        # One multi-table create per collection (each lands in that
        # integration's per-tenant collection db).
        create_collection_tasks = [
            rail.S3CreateMultiTableCollectionOperator(
                task_id=f"create_{collection['integration']}_collection",
                integration=collection['integration'],
                customer=config.instance,
                tables=collection['tables']
            )
            for collection in config.collections_to_create
        ]

        # Collections are created after webhook setup; both branches converge here.
        collections_start = (create_collection_tasks[0].task_id
                             if create_collection_tasks else 'log_to_sumo')

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_webhooks_to_create',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        has_webhooks_to_create = rail.IfOperator(
            task_id='has_webhooks_to_create',
            test=lambda: len(config.webhook_subscriptions) > 0,
            yes_task='trigger_webhook_child_dag',
            no_task=collections_start
        )

        trigger_webhook_child_dag = rail.TriggerDagRunOperator(
            task_id='trigger_webhook_child_dag',
            trigger_dag_id=config.webhook_subscribing_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'webhook_subscriptions': config.webhook_subscriptions,
                'procore_company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs='{{ result("trigger_webhook_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> has_webhooks_to_create
        batch_task >> log_to_sumo

        if create_collection_tasks:
            has_webhooks_to_create >> rail.Label(
                'Yes') >> trigger_webhook_child_dag >> wait_for_child_completion >> create_collection_tasks[0]
            has_webhooks_to_create >> rail.Label('No') >> create_collection_tasks[0]

            prev = create_collection_tasks[0]
            for task in create_collection_tasks[1:]:
                prev >> task
                prev = task
            prev >> log_to_sumo
        else:
            has_webhooks_to_create >> rail.Label(
                'Yes') >> trigger_webhook_child_dag >> wait_for_child_completion >> log_to_sumo
            has_webhooks_to_create >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
