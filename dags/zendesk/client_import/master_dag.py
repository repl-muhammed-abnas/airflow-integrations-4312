import rail
from datetime import timedelta, datetime
from airflow.models import Variable
from zendesk.client_import.utils.custom_method import get_delta_records,get_dag_conf

null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_zendesk_connector_{config.region.replace('-', '_')}_create_updated_client_import_master_{config.instance}",
        description=f"Zendesk Online {config.region} Create/Update Client {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_lastsync_time = rail.GetLastSyncTimeOperator(
            task_id="get_lastsync_time",
            workflow_name=config.workflow,
            provider=config.provider,
            date_format="%Y-%m-%dT%H:%M:%S",
            initial_sync_time=lambda: (datetime.now() - timedelta(minutes=60)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        )

        get_created_organizations = rail.ZendeskAPIOperator2(
            task_id="get_created_organizations",
            zendesk_conn_id="{{ dag_run.conf.zendesk_conn_id }}",
            endpoint="/api/v2/search/export?filter[type]=organization&query=created>{{result('get_lastsync_time').last_synctime}}Z",
            request_method="GET",
            pagination=True,
        )

        get_updated_organizations = rail.ZendeskAPIOperator2(
            task_id="get_updated_organizations",
            zendesk_conn_id="{{ dag_run.conf.zendesk_conn_id }}",
            endpoint="/api/v2/search/export?filter[type]=organization&query=updated>{{result('get_lastsync_time').last_synctime}}Z",
            request_method="GET",
            pagination=True,
        )

        get_all_organizations = rail.PythonOperator(
            task_id="get_all_organizations",
            python_callable=lambda: get_delta_records(
                rail.result("get_created_organizations")["results"],
                rail.result("get_updated_organizations")["results"],
            )
        )

        trigger_dag_run_client_child = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_dag_run_client_child",
            retries=0,
            items=lambda: rail.result("get_all_organizations"),
            trigger_dag_id=f"standard_zendesk_connector_{config.region.replace('-', '_')}_create_updated_client_import_child{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item:
                {
                **get_dag_conf(dag_run),
                "client_items": item
                }
        )

        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_child",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_client_child") }}',
        )

        gather_client_error = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_client_error",
            dag_runs="{{ result('trigger_dag_run_client_child') }}",
            dagrun_task_id="catch_client_error",
            flatten=True,
        )

        is_client_error = rail.IfOperator(
            task_id="is_client_error",
            test="{{ result('gather_client_error') | length > 0 }}",
            yes_task="fail_client_error",
            no_task="should_log_history",
        )

        fail_client_error = rail.FailOperator(
            task_id="fail_client_error",
            message="{{ result('gather_client_error') | map_to_attr('error') | join('|') }}",
        )

        should_log_history = rail.IfOperator(
            task_id="should_log_history",
            test="{{ not(get_task_state('get_all_organizations') == 'success' and result('get_all_organizations') | length == 0)}}",
            trigger_rule="all_done",
            yes_task="log_dagrun_details_to_table",
            no_task="delete_this_dagrun",
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id="log_dagrun_details_to_table",
            required_configs={
                "airflow_connector_ui_connid": config.airflow_connector_ui_connid,
                "hmac_secret_var": config.hmac_secret,
            },
            company_key="{{ dag_run.conf.company_key }}",
            connector_name="zendesk",
            integration_type="client_import",
        )

        def get_modified_time():
            current_time = datetime.strptime(rail.result("get_lastsync_time")[
                                             "current_time"], "%Y-%m-%d %H:%M:%S")
            modified_time = current_time - timedelta(minutes=3)
            datetime_filter = modified_time.strftime("%Y-%m-%d %H:%M:%S")
            return datetime_filter

        get_lastsynced_time = rail.PythonOperator(
            task_id="get_lastsynced_time",
            trigger_rule="all_done",
            python_callable=get_modified_time,
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id="update_lastsync_time",
            provider=config.provider,
            workflow_name=config.workflow,
            value_to_set='{{result("get_lastsynced_time")}}',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time
        get_lastsync_time >> get_created_organizations >> get_updated_organizations
        get_updated_organizations >> get_lastsynced_time >> update_lastsync_time >> get_all_organizations
        get_all_organizations >> trigger_dag_run_client_child >> wait_for_process_child
        wait_for_process_child >> gather_client_error >> is_client_error
        is_client_error >> rail.Label(
            'Yes') >> fail_client_error >> should_log_history
        is_client_error >> rail.Label(
            'No') >> should_log_history
        (
            should_log_history
            >> rail.Label("Yes")
            >> log_dagrun_details_to_table
        )
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
