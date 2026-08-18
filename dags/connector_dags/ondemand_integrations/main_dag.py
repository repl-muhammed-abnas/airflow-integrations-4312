from datetime import datetime, timedelta
import rail
from airflow import settings
from airflow.models import DagRun, DagModel
from airflow.models import Variable


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"airflow_ondemand_connector_dag_{config.region.replace('-', '_')}_{config.instance}",
        description=f'Airflow On Demand Connector DAG {config.region} {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_secret),
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_for_invalid_run'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_for_invalid_run',
            end_task='update_dagrun_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def check_if_paused_or_duplicate_run(dag_run):
            child_dag_id = dag_run.conf['webhook']['data']['dagIdToTrigger']
            session = settings.Session()
            dag = session.query(DagModel).filter(
                DagModel.dag_id == child_dag_id).first()
            dag_paused = not dag or dag.is_paused

            running_dags = session.query(DagRun).filter(
                DagRun.dag_id == child_dag_id,
                DagRun.state.in_(['queued', 'running'])
            ).count()
            is_duplicate_run = running_dags > 0
            return {
                'warning_message': config.duplicate_job_message if is_duplicate_run else (config.paused_dag_message if dag_paused else ''),
                'is_invalid': is_duplicate_run or dag_paused
            }

        check_for_invalid_run = rail.PythonOperator(
            task_id='check_for_invalid_run',
            python_callable=check_if_paused_or_duplicate_run
        )

        is_it_invalid_run = rail.IfOperator(
            task_id='is_it_invalid_run',
            test=lambda: rail.result('check_for_invalid_run')['is_invalid'],
            yes_task='update_dagrun_history',
            no_task='trigger_related_dag'
        )

        trigger_related_dag = rail.TriggerDagRunOperator(
            task_id='trigger_related_dag',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id="{{dag_run.conf.webhook.data.dagIdToTrigger}}",
            conf=lambda dag_run: dag_run.conf['webhook']['data']['customSettings']
        )

        wait_for_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dag',
            dag_runs="{{result('trigger_related_dag')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_dagrun_history = rail.PostDagRunDetailsToRepliconOperator(
            task_id='update_dagrun_history',
            trigger_rule='all_done',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.webhook_secret
            },
            company_key='{{ dag_run.conf.webhook.data.companyKey}}',
            connector_name='replicon',
            integration_type='{{ dag_run.conf.webhook.data.dagIdToTrigger}}',
            message=lambda: rail.result('check_for_invalid_run')[
                'warning_message'] if rail.result('check_for_invalid_run') else ''
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> update_dagrun_history

        can_run_batch_task >> rail.Label(
            'No') >> check_for_invalid_run >> is_it_invalid_run
        is_it_invalid_run >> rail.Label(
            'No') >> trigger_related_dag >> wait_for_child_dag >> update_dagrun_history
        is_it_invalid_run >> rail.Label(
            'Yes') >> update_dagrun_history
    return dag


rail.for_each_instance(create_airflow_dag)
