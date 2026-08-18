from datetime import timedelta, datetime
import rail
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from procore_ce_integration.initial_setup_sync.shared_utils import get_tenant_email


# config:
# https://github.com/replicon/airflow-integrations/blob/main/dags/procore_ce_integration/change_order_sync/config.py


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Procore to ComputerEase Subcontract Change Order Sync - Main DAG',
        schedule_interval=timedelta(seconds=config.schedule_in_seconds),
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(
                days=config.execution_timeout_days),
            'aws_conn_id': config.aws_conn_id,
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                date_format=config.procore_webhook_fmt,
                initial_sync_time='1900-01-01T00:00:00.000Z',
                reset_after_threshold=False,
                use_param_date_format=True
            )
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        fetch_webhook_events = rail.S3DownloadFileOperator(
            task_id='fetch_webhook_events',
            bucket_name=config.s3_bucket_name,
            key_name=config.change_order_events_key
        )

        def parse_webhook_events():
            events_data = rail.load_json_artifact(
                rail.result('fetch_webhook_events'))
            if not events_data:
                return {'projects_to_sync': [],
                        'max_processed_timestamp': None}

            def _parse_webhook_timestamp(timestamp_str, default_year=1900):
                try:
                    return datetime.strptime(timestamp_str, config.procore_webhook_fmt)
                except:
                    return datetime(default_year, 1, 1)

            last_processed_timestamp = rail.result(
                'get_last_sync_time')['last_synctime']
            last_processed_dt = _parse_webhook_timestamp(
                last_processed_timestamp)

            projects_to_sync = []
            max_processed_timestamp = last_processed_dt

            for project_id, project_data in events_data.items():
                if project_id == '_metadata':
                    continue
                try:
                    last_updated = project_data.get('last_updated', '')
                    event_dt = _parse_webhook_timestamp(last_updated)

                    if event_dt > last_processed_dt:
                        if event_dt > max_processed_timestamp:
                            max_processed_timestamp = event_dt

                        projects_to_sync.append({
                            'project_id': int(project_id),
                            'cop_ids': project_data['cop_ids'],
                            'last_updated': last_updated
                        })
                except (ValueError, AttributeError):
                    continue

            return {
                'projects_to_sync': projects_to_sync,
                'max_processed_timestamp': max_processed_timestamp.strftime(
                    config.procore_webhook_fmt) if projects_to_sync else None
            }

        process_webhook_events = rail.PythonOperator(
            task_id='process_webhook_events',
            python_callable=parse_webhook_events
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=rail.result('process_webhook_events')[
                    'max_processed_timestamp'] if rail.result(
                        'process_webhook_events') and rail.result('process_webhook_events')[
                            'max_processed_timestamp'] else rail.result('get_last_sync_time')['current_time']
            )
        )

        has_cops_to_sync = rail.IfOperator(
            task_id='has_cops_to_sync',
            test="{{ result('process_webhook_events').projects_to_sync | length > 0 }}",
            yes_task='trigger_project_child_dags',
            no_task='delete_this_dagrun'
        )

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"

        trigger_project_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_child_dags',
            trigger_dag_id=config.project_child_dag_id,
            items=lambda: rail.result('process_webhook_events')[
                'projects_to_sync'],
            conf=lambda item: {
                'project_id': item['project_id'],
                'cop_ids': item['cop_ids'],
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_project_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_project_completion',
            dag_runs="{{ result('trigger_project_child_dags') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_validated_cops = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_validated_cops',
            dag_runs="{{ result('trigger_project_child_dags') }}",
            dagrun_task_id='valid_cops',
            flatten=True
        )

        has_valid_cops = rail.IfOperator(
            task_id='has_valid_cops',
            test="{{ result('gather_validated_cops') | length > 0 }}",
            yes_task='fetch_cost_types',
            no_task='set_last_sync_time'
        )

        fetch_cost_types = rail.ComputereaseAPIOperator(
            task_id='fetch_cost_types',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda resp: {
                item['reference']: item['code']
                for item in resp.get('data', [])
                if item.get('reference') and item.get('code')
            }
        )

        trigger_cop_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_cop_child_dags',
            trigger_dag_id=config.cop_child_dag_id,
            items=lambda: rail.result('gather_validated_cops'),
            conf=lambda item: {
                'cop_id': item['id'],
                'job_code': item['job_code'],
                'project_id': item['project_id'],
                'wbs_type': item['wbs_type'],
                'company_id': rail.render_template(procore_company_id_template),
                'cost_type_map': rail.result('fetch_cost_types')
            }
        )

        wait_for_cop_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_cop_completion',
            dag_runs='{{ result("trigger_cop_child_dags") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def cleanup_processed_events():

            # Check if fetch_webhook_events task was executed
            events_data = rail.load_json_artifact(
                rail.result('fetch_webhook_events')) if rail.render_template(
                    "{{ get_task_state('fetch_webhook_events') }}") == 'success' else {}

            if not events_data:
                return {
                    'cleaned': False
                }

            projects_synced = rail.result('process_webhook_events')[
                'projects_to_sync']
            if not projects_synced:
                return {
                    'cleaned': False
                }

            project_ids_synced = {str(p['project_id'])
                                  for p in projects_synced}

            # Remove processed project entries
            for project_id in project_ids_synced:
                if project_id in events_data:
                    del events_data[project_id]

            return {
                'cleaned': True,
                'events_data': events_data,
                'removed_projects': list(project_ids_synced)
            }

        cleanup_events = rail.PythonOperator(
            task_id='cleanup_events',
            python_callable=cleanup_processed_events
        )

        should_update_after_sync = rail.IfOperator(
            task_id='should_update_after_sync',
            test="{{ result('cleanup_events').cleaned }}",
            yes_task='upload_cleaned_after_sync',
            no_task='search_logs'
        )

        upload_cleaned_after_sync = rail.S3UploadFileOperator(
            task_id='upload_cleaned_after_sync',
            bucket_name=config.s3_bucket_name,
            key_name=config.change_order_events_key,
            source='{{ result("cleanup_events").events_data | tojson }}',
            replace=True
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        has_errors = rail.IfOperator(
            task_id='has_errors',
            test="{{ result('search_logs', 'length') > 0 }}",
            yes_task='write_errors_to_csv',
            no_task='log_to_sumo'
        )

        write_errors_to_csv = rail.WriteCSVFileOperator(
            task_id='write_errors_to_csv',
            source='{{ result("search_logs") }}'
        )

        generate_error_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_error_download_link',
            artifact_name='{{ result("write_errors_to_csv") }}',
            output_file_name='ProcoreComputerease_ChangeOrderSyncErrors_{{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_error_email = rail.EmailOperator(
            task_id='send_error_email',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Subcontract Change Order Sync completed with errors - {{ current_time() }}',
            html_content='email_templates/change_order_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_last_sync_time >> fetch_webhook_events >> process_webhook_events >> has_cops_to_sync

        has_cops_to_sync >> rail.Label(
            'Yes') >> trigger_project_child_dags >> wait_for_project_completion >> gather_validated_cops >> has_valid_cops

        has_valid_cops >> rail.Label(
            'Yes') >> fetch_cost_types >> trigger_cop_child_dags >> wait_for_cop_completion >> set_last_sync_time

        has_valid_cops >> rail.Label(
            'No') >> set_last_sync_time

        has_cops_to_sync >> rail.Label(
            'No') >> delete_this_dagrun

        delete_this_dagrun >> set_last_sync_time

        set_last_sync_time >> cleanup_events >> should_update_after_sync

        should_update_after_sync >> rail.Label(
            'Yes') >> upload_cleaned_after_sync >> search_logs
        should_update_after_sync >> rail.Label(
            'No') >> search_logs

        search_logs >> has_errors

        has_errors >> rail.Label(
            'Yes') >> write_errors_to_csv >> \
            generate_error_download_link >> send_error_email >> log_to_sumo
        has_errors >> rail.Label(
            'No') >> log_to_sumo

        batch_task >> log_to_sumo

    return dag


rail.for_each_instance(create_dag_instance)
