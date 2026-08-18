from datetime import timedelta
from airflow.models import Variable
import rail
from rosterfy.hubspot_polaris_psa_integration.utils import python_callable
from rosterfy.hubspot_polaris_psa_integration.tasks.process_project import process_project
from rosterfy.hubspot_polaris_psa_integration.tasks.process_task import add_default_tasks
from rosterfy.hubspot_polaris_psa_integration.tasks.process_deal_and_company_details import get_details_of_deals_and_company

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.services_master_dag,
        description=f'rosterfy_hubspot_polaris_psa_integration_service_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.services_master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.webhook_service_shared_secrete)
        ],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_project_data',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_project_data = rail.PythonOperator(
            task_id='get_project_data',
            python_callable=lambda dag_run: dag_run.conf['webhook']['data']
        )

        get_details_deal_and_company = get_details_of_deals_and_company(config)

        process_service_project = process_project(config, 'add')

        add_default_non_billable_task = add_default_tasks(
            config.service_tasks_nonbillable, 'non-billable', config, 'service')

        add_default_billable_task = add_default_tasks(
            config.service_tasks_billable, 'billable', config, 'service')

        log_project_process = rail.WriteLogOperator(
            task_id="log_project_process",
            log = "{{ result('logger') }}",
            message='Success',
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = "{{ result('logger') }}",
            severity="Error",
            trigger_rule="one_failed",
            message='Error',
            properties=python_callable.get_error_properties
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_project_data

        get_project_data >> get_details_deal_and_company >> process_service_project >> add_default_non_billable_task >> \
            add_default_billable_task >> log_project_process >> catch_and_log_error
        
        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
