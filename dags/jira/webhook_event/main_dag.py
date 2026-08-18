from datetime import datetime, timedelta
import rail
from airflow.models import Variable

null = None
# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/jira/main_dag/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"airflow_event_dag_{config.region.replace('-', '_')}_{config.instance}",
        description=f'Airflow Event DAG {config.region} {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_runs,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.hmac_secret),
        start_date=datetime(2024, 1, 1),
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_new_replicon_user_event'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_new_replicon_user_event',
            end_task='should_delete_dagrun',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_new_replicon_user_event = rail.IfOperator(
            task_id='is_new_replicon_user_event',
            test=lambda dag_run: bool(
                dag_run.conf['webhook']['data']['connector_details'] and dag_run.conf['webhook']['data']['event_details']['user']),
            yes_task='get_dagid_to_trigger',
            no_task='should_delete_dagrun'
        )

        def get_dag_id_to_trigger(dag_run):
            dag_settings = dag_run.conf['webhook']['data']['connector_details'] and dag_run.conf[
                'webhook']['data']['connector_details']['dag_settings']
            custom_workflow = dag_settings.get('isCustom')
            custom_workflow = custom_workflow if custom_workflow  and custom_workflow != dag_settings.get('workflowId') else None
            return custom_workflow or f"standard_jira_{config.region.replace('-', '_')}_user_export_child_dag_{config.instance}"

        get_dagid_to_trigger = rail.PythonOperator(
            task_id='get_dagid_to_trigger',
            python_callable=get_dag_id_to_trigger
        )

        trigger_jira_user_export = rail.TriggerDagRunOperator(
            task_id='trigger_jira_user_export',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id="{{result('get_dagid_to_trigger')}}",
            conf=lambda dag_run: {
                **{
                    'data': dag_run.conf['webhook']['data']
                },
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_jira_user_export') == 'skipped' }}",
            trigger_rule='all_done',
            yes_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_delete_dagrun
        can_run_batch_task >> rail.Label(
            'No') >> is_new_replicon_user_event
        is_new_replicon_user_event >> rail.Label(
            'Yes') >> get_dagid_to_trigger >> trigger_jira_user_export >> should_delete_dagrun
        is_new_replicon_user_event >> rail.Label(
            'No') >> should_delete_dagrun
        should_delete_dagrun >> rail.Label(
            'Yes') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
