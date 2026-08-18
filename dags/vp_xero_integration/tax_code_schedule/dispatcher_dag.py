"""
Dispatcher DAG for VP -> Xero Tax Code Schedule.
"""
# pylint:disable=too-many-statements,line-too-long,pointless-statement
# pylint:disable=expression-not-assigned
from datetime import timedelta
import rail
from vp_xero_integration.tax_code_schedule import config as sync_config


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'{sync_config.dispatcher_dag_id_prefix}_{config.instance}',
        description=sync_config.dispatcher_dag_description,
        integration_type=sync_config.integration_type,
        company_key=config.company_key,
        multi_tenant=True,
        schedule_interval=None,
        max_active_runs=config.max_active_runs,
        tags=sync_config.dispatcher_dag_tags,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        trigger_tax_code_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_tax_code_sync',
            items=lambda: [rail.get_current_context()['dag_run'].conf],
            trigger_dag_id=(
                f'{sync_config.processor_dag_id_prefix}_{config.instance}'
            ),
            conf=lambda item: item,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_tax_code_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_tax_code_sync',
            dag_runs="{{ result('trigger_tax_code_sync') }}",
            allowed_states=['success', 'failed', 'upstream_failed', 'removed'],
            failed_states=[],
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_tax_code_sync_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_tax_code_sync_errors',
            dag_runs="{{ result('trigger_tax_code_sync') }}",
            dagrun_task_id='catch_tax_code_sync_error',
            flatten=True,
        )

        has_sync_errors = rail.IfOperator(
            task_id='has_sync_errors',
            test=lambda: bool(
                [e for e in (rail.result('gather_tax_code_sync_errors') or []) if e]
            ),
            yes_task='fail_tax_code_schedule',
            no_task='log_sync_complete',
        )

        fail_tax_code_schedule = rail.FailOperator(
            task_id='fail_tax_code_schedule',
            message=(
                "{{ result('gather_tax_code_sync_errors')"
                " | map_to_attr('error') | join(' | ') }}"
            ),
        )

        log_sync_complete = rail.PythonOperator(
            task_id='log_sync_complete',
            python_callable=lambda: None,
        )

        post_dag_run_details = rail.PostDagRunDetailsToMiddlewareApiOperator(
            task_id='post_dag_run_details',
            middleware_api_base_url=(
                f"{{{{ var.value.get('{sync_config.middleware_api_base_url_variable_key}', '') }}}}"
            ),
            trigger_rule='all_done',
        )

        (
            trigger_tax_code_sync
            >> wait_for_tax_code_sync
            >> gather_tax_code_sync_errors
            >> has_sync_errors
        )
        has_sync_errors >> rail.Label('Yes') >> fail_tax_code_schedule
        has_sync_errors >> rail.Label('No') >> log_sync_complete
        fail_tax_code_schedule >> post_dag_run_details
        log_sync_complete >> post_dag_run_details

        return dag


rail.for_each_instance(create_dag)
