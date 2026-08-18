from datetime import timedelta
import time
from airflow.models import Variable
import json
import rail
from rosterfy.hubspot_polaris_psa_integration.utils import python_callable, request_payload
from rosterfy.hubspot_polaris_psa_integration.tasks.process_project import process_project
from rosterfy.hubspot_polaris_psa_integration.tasks.process_deal_and_company_details import get_details_of_deals_and_company
from rosterfy.hubspot_polaris_psa_integration.tasks.create_client import client_process

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_deal_master_dag,
        description=f'rosterfy_hubspot_polaris_psa_integration_update_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.update_project_master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.webhook_update_shared_secrete)
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

        wait_task = rail.PythonOperator(
            task_id = "wait_task",
            python_callable = lambda: time.sleep(20) 
        )

        get_details_deal_and_company = get_details_of_deals_and_company(config)

        if_valid_pipeline_and_dealstage= rail.IfOperator(
            task_id="if_valid_pipeline_and_dealstage",
            test=lambda : bool(python_callable.check_valid_deals_data() == True),
            yes_task="if_pipeline_is_sales_and_dealstage_is_solution_and_demo",
            no_task="log_invalid_pipeline_or_dealstage"
        )

        log_invalid_pipeline_or_dealstage = rail.WriteLogOperator(
            task_id = "log_invalid_pipeline_or_dealstage",
            log = "{{ result('logger') }}",
            message='Exception',
            properties=python_callable.get_invalid_pipeline_dealstage_log
        )

        if_pipeline_is_sales_and_dealstage_is_solution_and_demo = rail.IfOperator(
            task_id="if_pipeline_is_sales_and_dealstage_is_solution_and_demo",
            test=lambda : bool((rail.result(
                'get_pipeline_and_dealstage_name')['pipeline'] == 'Sales') and (rail.result(
                    'get_pipeline_and_dealstage_name')['dealstage'] == '3. Solution & Demo') and (
                        json.loads(rail.result('get_details_of_deal')).get('associations')) and (
                            json.loads(rail.result('get_details_of_deal'))['associations'].get('companies'))),
            yes_task="get_client_custom_fields",
            no_task="search_project_with_code"
        )

        get_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_client_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data = {
                "objectUri": "urn:replicon:object-type:client"
            }
        )

        process_client = client_process(config)

        search_project_with_code = rail.RepliconServiceOperator(
            task_id='search_project_with_code',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data = request_payload.search_project_with_code
        )

        is_project_found = rail.IfOperator(
            task_id="is_project_found",
            test=lambda: bool(rail.result('search_project_with_code')[0].get('projectDetails')),
            yes_task="process_project_start",
            no_task="log_project_process"
        )

        process_project_start = rail.EmptyOperator(
            task_id = "process_project_start"
        )

        process_update_project = process_project(config, 'update')

        log_project_process = rail.WriteLogOperator(
            task_id="log_project_process",
            log = "{{ result('logger') }}",
            message=lambda : 'Success' if bool(rail.result('search_project_with_code')[0].get('projectDetails')) else 'Exception',
            properties=python_callable.get_status_and_details_for_project_to_update
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

        get_project_data >> wait_task >> get_details_deal_and_company >> if_valid_pipeline_and_dealstage

        if_valid_pipeline_and_dealstage >> rail.Label('Yes') >> if_pipeline_is_sales_and_dealstage_is_solution_and_demo
        if_valid_pipeline_and_dealstage >> rail.Label('No') >> log_invalid_pipeline_or_dealstage >> catch_and_log_error

        if_pipeline_is_sales_and_dealstage_is_solution_and_demo >> rail.Label('Yes') >> get_client_custom_fields >> process_client >> search_project_with_code
        if_pipeline_is_sales_and_dealstage_is_solution_and_demo >> rail.Label('No') >> search_project_with_code

        search_project_with_code >> is_project_found

        is_project_found >> rail.Label('Yes') >> process_project_start >> process_update_project >> log_project_process
        is_project_found >> rail.Label('No') >> log_project_process

        log_project_process >> catch_and_log_error
        
        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
