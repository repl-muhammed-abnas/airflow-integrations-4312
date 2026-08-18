from datetime import timedelta
from airflow.models import Variable
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_subscribing_child_dag_id,
        description='Procore Initial Setup - Webhook Creation Child DAG',
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.instance,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_delete_hook',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        if_delete_hook = rail.IfOperator(
            task_id='if_delete_hook',
            test=lambda dag_run: bool(dag_run.conf.get('hook_id')),
            yes_task='delete_hook',
            no_task='check_s3_files_exist'
        )

        delete_hook = rail.ProcoreApiOperator(
            task_id='delete_hook',
            endpoint=lambda dag_run: f"/webhooks/hooks/{dag_run.conf['hook_id']}",
            method='DELETE',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf.get('procore_company_id')
            }
        )

        check_s3_files_exist = rail.S3ListKeysOperator(
            task_id='check_s3_files_exist',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            prefix=config.webhooks_s3_directory
        )

        for_each_webhook_to_subscribe = rail.ForEachOperator(
            task_id='for_each_webhook_to_subscribe',
            items=config.webhook_subscriptions,
            start_task='get_hooks',
            end_task='end_of_webhook_subscribing'
        )

        get_hooks = rail.ProcoreApiOperator(
            task_id='get_hooks',
            endpoint='/webhooks/hooks',
            method='GET',
            query_params=lambda dag_run: {
                "company_id": dag_run.conf.get('procore_company_id'),
                # Procore rejects uppercase characters in webhook namespaces,
                # so normalise to lowercase to match the value used on create.
                "namespace": rail.result('for_each_webhook_to_subscribe')['namespace'].lower()
            }
        )

        does_hook_exist = rail.IfOperator(
            task_id='does_hook_exist',
            test=lambda: len(rail.result('get_hooks')) > 0,
            yes_task='delete_existing_hook',
            no_task='create_hook'
        )

        delete_existing_hook = rail.ProcoreApiOperator(
            task_id='delete_existing_hook',
            endpoint=lambda: f"/webhooks/hooks/{rail.result('get_hooks')[0]['id']}",
            method='DELETE',
            query_params=lambda dag_run: {
                "company_id": dag_run.conf.get('procore_company_id')
            }
        )


        def build_hook_payload(dag_run):
            procore_company_id = dag_run.conf.get('procore_company_id')
            webhook_config = rail.result('for_each_webhook_to_subscribe')
            return {
                "company_id": int(procore_company_id),
                "hook": {
                    "api_version": webhook_config['api_version'],
                    "destination_url": webhook_config['destination_url'],
                    # Procore rejects uppercase characters in webhook namespaces.
                    "namespace": webhook_config['namespace'].lower(),
                    "destination_headers": {
                        "Authorization": f"Bearer {Variable.get(config.bearer_token_var)}"
                    }
                }
            }

        create_hook = rail.ProcoreApiOperator(
            task_id='create_hook',
            endpoint='/webhooks/hooks',
            method='POST',
            data=build_hook_payload
        )

        def build_triggers_payload(dag_run):
            procore_company_id = dag_run.conf.get('procore_company_id')
            webhook_config = rail.result('for_each_webhook_to_subscribe')
            return {
                "company_id": procore_company_id,
                "api_version": webhook_config['api_version'],
                "triggers": webhook_config['triggers']
            }

        add_triggers = rail.ProcoreApiOperator(
            task_id='add_triggers',
            endpoint=lambda: f"/webhooks/hooks/{rail.result('create_hook')['id']}/triggers/bulk",
            method='POST',
            data=build_triggers_payload
        )

        for_each_s3_files_to_create = rail.ForEachOperator(
            task_id='for_each_s3_files_to_create',
            items=lambda: rail.result('for_each_webhook_to_subscribe').get('s3_files', []),
            start_task='should_create_s3_file',
            end_task='end_of_s3_files_to_create'
        )

        should_create_s3_file = rail.IfOperator(
            task_id='should_create_s3_file',
            test=lambda: rail.result('for_each_s3_files_to_create') not in rail.result('check_s3_files_exist'),
            yes_task='create_s3_file',
            no_task='end_of_s3_files_to_create'
        )

        create_s3_file = rail.S3UploadFileOperator(
            task_id='create_s3_file',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name='{{ result("for_each_s3_files_to_create") }}',
            replace=False,
            source='{}'
        )

        end_of_s3_files_to_create = rail.EmptyOperator(
            task_id='end_of_s3_files_to_create'
        )

        end_of_webhook_subscribing = rail.EmptyOperator(
            task_id='end_of_webhook_subscribing'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> if_delete_hook
        if_delete_hook >> rail.Label('Yes') >> delete_hook >> log_to_sumo
        if_delete_hook >> rail.Label('No') >> check_s3_files_exist >> for_each_webhook_to_subscribe
        for_each_webhook_to_subscribe >> get_hooks >> does_hook_exist

        does_hook_exist >> rail.Label('Yes') >> delete_existing_hook >> create_hook
        does_hook_exist >> rail.Label('No') >> create_hook

        create_hook >> add_triggers >> for_each_s3_files_to_create >> end_of_s3_files_to_create
        for_each_s3_files_to_create >> should_create_s3_file

        should_create_s3_file >> rail.Label('Yes') >> create_s3_file >> end_of_s3_files_to_create
        should_create_s3_file >> rail.Label('No') >> end_of_s3_files_to_create >> end_of_webhook_subscribing

        for_each_webhook_to_subscribe >> end_of_webhook_subscribing >> log_to_sumo
        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
