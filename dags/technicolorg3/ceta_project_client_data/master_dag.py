from datetime import datetime, timedelta
from airflow.models import Variable
import rail

from technicolorg3.ceta_project_client_data.utils import request_payload
from technicolorg3.ceta_project_client_data.utils import response_filter
from technicolorg3.ceta_project_client_data.utils import python_callable_method


def create_master_project_client_data_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_project_client_details_{config.instance}',
        description=f'Technicolor CETA Project Client data Webhook_Master V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config)

        can_redirect_to_workato = rail.IfOperator(
            task_id='can_redirect_to_workato',
            test=lambda: Variable.get(
                config.can_redirect_to_workato_var_name, default_var='').lower() == 'true',
            yes_task='post_to_workato',
            no_task='can_run_batch_task',
        )

        post_to_workato = rail.SimpleHttpOperator(
            task_id='post_to_workato',
            method='POST',
            http_conn_id=config.workato_api_endpoint,
            headers={
                'Content-Type': 'application/json; charset=utf-8',
                'API-TOKEN': "{{ var.value." + config.workato_api_token_var_name + " }}"
            },
            data='{{ dag_run.conf.webhook.data | to_json }}',
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='was_triggered_by_technicolor'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='was_triggered_by_technicolor',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            end_task='catch_and_log_errors',
        )

        was_triggered_by_technicolor = rail.EmptyOperator(
            task_id='was_triggered_by_technicolor')

        client_project_logs = rail.CreateLogOperator(
            task_id='client_project_logs',
            tenant_wide_name=f'{config.client_project_logs}',
            existing_log_mode='append',
        )

        client_project_message_to_log = rail.PythonOperator(
            task_id='client_project_message_to_log',
            python_callable=python_callable_method.get_client_project_message_to_log
        )

        should_process_client_project_data = rail.IfOperator(
            task_id='should_process_client_project_data',
            test=lambda: not bool(rail.result(
                'client_project_message_to_log')),
            yes_task='get_all_countries',
            no_task='log_client_project_fields_missing'
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries'
        )

        log_client_project_fields_missing = rail.WriteLogOperator(
            task_id='log_client_project_fields_missing',
            log='{{ result("client_project_logs") }}',
            message='{{ result("client_project_message_to_log") }}',
            properties={
                'db': '{{ dag_run.conf.webhook.data.mill_mpc }}',
                'client': '{{ dag_run.conf.webhook.data.Client_Name }}',
                'project': '{{ dag_run.conf.webhook.data.Product_Name }}',
                'status': 'Exception',
                'action': 'Skipped',
                'details': '{{ result("client_project_message_to_log") }}',
                'reference': '{{ dag_run_ecid() }}',
                'exported': 'No',
            }
        )

        process_create_client = rail.TriggerDagRunOperator(
            task_id='process_create_client',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_create_client_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_create_client_payload
        )

        wait_for_process_create_client = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_create_client',
            dag_runs='{{ result("process_create_client") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_client_uri_to_process = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_uri_to_process',
            dag_runs='{{ result("process_create_client") }}',
            dagrun_task_id='client_uri',
            flatten=True
        )

        search_projects = rail.RepliconServiceOperator(
            task_id='search_projects',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.search_projects_payload,
            data_handler=response_filter.get_project_lists
        )

        check_project_present = rail.IfOperator(
            task_id='check_project_present',
            test=lambda: bool(rail.result('search_projects')),
            yes_task='process_update_project',
            no_task='process_add_project'
        )

        process_update_project = rail.TriggerDagRunOperator(
            task_id='process_update_project',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_udpate_project_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_project_payload
        )

        wait_for_process_update_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_project',
            dag_runs='{{ result("process_update_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_add_project = rail.TriggerDagRunOperator(
            task_id='process_add_project',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_add_project_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_project_payload
        )

        wait_for_process_add_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_project',
            dag_runs='{{ result("process_add_project") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("client_project_logs") }}',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'db': '{{ dag_run.conf.webhook.data.mill_mpc }}',
                'client': '{{ dag_run.conf.webhook.data.Client_Name }}',
                'project': '{{ dag_run.conf.webhook.data.Product_Name }}',
                'status': 'Error',
                'action': 'Validation',
                'details': {config.error_template},
                'reference': '{{ dag_run.conf.webhook.data.reference }} - {{ dag_run_ecid() }}',
                'exported': 'No'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'referenceid': '{{ dag_run_ecid() }}'
            }
        )

        can_redirect_to_workato >> rail.Label('Yes') >> post_to_workato
        can_redirect_to_workato >> rail.Label('No') >> can_run_batch_task

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> was_triggered_by_technicolor

        was_triggered_by_technicolor >> client_project_logs >> client_project_message_to_log >> should_process_client_project_data

        should_process_client_project_data >> rail.Label(
            'Yes') >> get_all_countries >> process_create_client >> wait_for_process_create_client\
            >> gather_client_uri_to_process >> search_projects >> check_project_present
        should_process_client_project_data >> rail.Label(
            'No') >> log_client_project_fields_missing >> finish

        check_project_present >> rail.Label(
            'Yes') >> process_update_project >> wait_for_process_update_project >> finish
        check_project_present >> rail.Label(
            'No') >> process_add_project >> wait_for_process_add_project >> finish

        finish >> catch_and_log_errors >> log_to_sumo

        return dag


rail.for_each_instance(create_master_project_client_data_dag)
