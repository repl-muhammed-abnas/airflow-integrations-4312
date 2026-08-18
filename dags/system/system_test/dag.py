import rail
from system.system_test import config

with rail.create_airflow_dag(
    dag_id='system_test',
    description='Validate dags are running',
    company_key=config.company_key,
    replicon_conn_id=config.replicon_conn_id,
    max_active_runs=10,
    webhook_conf={
        rail.WebhookConf(bearer_token_var="system-test-webhook-secret")
    },
) as dag:
    generate_content = rail.RenderTemplateOperator(
        task_id='generate_content',
        target='result',
        template='''
            {
                "version": "{{var.value.dag_build_version}}",
                "platform-version": "{{var.value.get('airflow_platform_build_version', 'none')}}"
            }
        ''',
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun')
    generate_content >> delete_this_dagrun
