from datetime import timedelta
from airflow.models import Variable
import rail
from rosterfy.hubspot_polaris_psa_integration.utils import request_payload, python_callable
from rosterfy.hubspot_polaris_psa_integration.tasks.process_project import process_project
from rosterfy.hubspot_polaris_psa_integration.tasks.process_task import add_default_tasks
from rosterfy.hubspot_polaris_psa_integration.tasks.process_deal_and_company_details import get_details_of_deals_and_company
from rosterfy.hubspot_polaris_psa_integration.tasks.create_client import client_process

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sales_master_dag,
        description=f'rosterfy_hubspot_polaris_psa_integration_sales_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.sales_master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.webhook_sales_shared_secrete)
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

        get_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_client_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data = {
                "objectUri": "urn:replicon:object-type:client"
            }
        )

        process_client = client_process(config)

        project_process_start = rail.EmptyOperator(
            task_id = "project_process_start"
        )

        process_sales_project = process_project(config, 'add')

        if_dealstage_is_solution_and_demo = rail.IfOperator(
            task_id="if_dealstage_is_solution_and_demo",
            test=lambda : bool(rail.result('get_pipeline_and_dealstage_name')['dealstage'] == '3. Solution & Demo'),
            yes_task="presale_task_start",
            no_task="if_dealstage_is_closed_won"
        )

        presale_task_start = rail.EmptyOperator(
            task_id = "presale_task_start"
        )

        add_presales_tasks = add_default_tasks(
            config.presales_tasks_nonbillable, 'non-billable', config, 'presales')

        if_dealstage_is_closed_won = rail.IfOperator(
            task_id="if_dealstage_is_closed_won",
            test=lambda : bool(rail.result('get_pipeline_and_dealstage_name')['dealstage'] == 'Closed won'),
            yes_task="get_existing_client_data",
            no_task="log_project_process"
        )

        get_existing_client_data = rail.RepliconServiceOperator(
            task_id='get_existing_client_data',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.get_existing_client_data,
            response_filter=python_callable.check_client_data
        )

        get_projects_for_the_client = rail.RepliconServiceOperator(
            task_id='get_projects_for_the_client',
            endpoint="/services/ProjectListService1.svc/GetData",
            data=request_payload.get_project_data_for_client_data,
            data_handler=python_callable.get_presale_project_uri_to_be_archived
        )

        archive_projects = rail.RepliconServiceCallForEachItemOperator(
            task_id='archive_projects',
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            items=lambda: [x for x in rail.result('get_projects_for_the_client') if x],
            data={
                "projectUri": "{{ item }}",
                "projectStatusUri": "urn:replicon:project-status-type:archived"
            }
        )

        add_default_non_billable_task = add_default_tasks(
            config.sales_tasks_nonbillable, 'non-billable', config, 'sales')

        add_default_billable_task = add_default_tasks(
            config.sales_tasks_billable, 'billable', config, 'sales')

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

        get_project_data >> get_details_deal_and_company >> get_client_custom_fields >> process_client >> project_process_start

        project_process_start >> process_sales_project >> if_dealstage_is_solution_and_demo

        if_dealstage_is_solution_and_demo >> rail.Label('Yes') >> presale_task_start >> add_presales_tasks >> log_project_process
        if_dealstage_is_solution_and_demo >> rail.Label('No') >> if_dealstage_is_closed_won

        if_dealstage_is_closed_won >> rail.Label('Yes') >> get_existing_client_data >> get_projects_for_the_client >> archive_projects >> \
            add_default_non_billable_task >> add_default_billable_task >> log_project_process
        if_dealstage_is_closed_won >> rail.Label('No') >> log_project_process

        log_project_process >> catch_and_log_error
        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
