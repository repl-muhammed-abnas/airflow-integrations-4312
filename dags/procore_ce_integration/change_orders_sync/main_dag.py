import rail
from datetime import timedelta
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable

from procore_ce_integration.change_orders_sync.utils.constants import APPROVED, RESOURCE_BUDGET_CHANGES
from procore_ce_integration.change_orders_sync.utils.util import (
    extract_ce_code,
    is_resource_ready,
    parse_webhook_timestamp
)
from procore_ce_integration.initial_setup_sync.shared_utils import normalize_ce_identifier, get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Procore to ComputerEase Change Order Sync - Load events to sync',
        schedule_interval=timedelta(seconds=config.schedule_in_seconds),
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_main_dag,
        default_args={
            'aws_conn_id': config.aws_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"


        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_bulk_sync',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Trigger bulk sync payload:
        # {   
        #     "is_bulk_sync": true,
        #     "project_ids": "123, 456", if not provided, fetches all active synced projects
        #     "bulk_sync_start_time": "2026-07-31T13:19:45.387Z" # if not provided, uses config.initial_sync_time
        # }
        is_bulk_sync = rail.IfOperator(
            task_id='is_bulk_sync',
            test=lambda dag_run: bool(dag_run.conf.get('is_bulk_sync')),
            yes_task='fetch_cost_types_for_bulk_sync',
            no_task='get_last_sync_time'
        )

        fetch_cost_types_for_bulk_sync = rail.ComputereaseAPIOperator(
            task_id='fetch_cost_types_for_bulk_sync',
            endpoint='/catalog/cost-type',
            request_method='GET',
            data_handler=lambda resp: {
                item['reference']: item['code']
                for item in resp.get('data', [])
                if item.get('reference') and item.get('code')
            }
        )

        fetch_custom_field = rail.ProcoreApiOperator(
            task_id='fetch_custom_field',
            endpoint=f'/companies/{procore_company_id_template}/custom_field_definitions',
            method='GET',
            version='1.1',
            query_params={
                'filters[with_label]': config.SYNC_CUSTOM_FIELD_LABEL
            },
            data_handler=lambda res: f"custom_field_{res[0]['id']}" if res else None
        )

        def get_projects_by_ids(response):
            return {
                x['id']: {
                    'origin_id': x['origin_id'],
                    'project_number': x['project_number'],
                    'job_code': normalize_ce_identifier(
                        extract_ce_code(x['origin_id']) if x['origin_id'] else x['project_number']
                    )
                } for x in response
            } if response else {}
        fetch_all_projects = rail.ProcoreApiOperator(
            task_id='fetch_all_projects',
            endpoint='/projects',
            method='GET',
            version='1.1',
            query_params=lambda dag_run: {
                'view': 'normal',
                'filters[synced]': True,
                'filters[by_status]': 'Active',
                'company_id': rail.render_template(procore_company_id_template),
                **(
                    {
                        'filters[id]': f"[{','.join(list(map(str.strip, dag_run.conf['project_ids'].split(','))))}]"
                    } if dag_run.conf.get('project_ids') else {}
                )
            },
            data_handler=get_projects_by_ids
        )

        
        trigger_bulk_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_bulk_sync',
            trigger_dag_id=config.bulk_sync_dag_id,
            items=lambda: [
                {'project_id': pid, 'job_code': pdata.get('job_code')}
                for pid, pdata in (rail.result('fetch_all_projects') or {}).items()
            ],
            conf=lambda item, dag_run: {
                'project_id': item['project_id'],
                'job_code': item['job_code'],
                'custom_field_key': rail.result('fetch_custom_field'),
                'cost_type_mapping': rail.result('fetch_cost_types_for_bulk_sync'),
                'bulk_sync_start_time': dag_run.conf.get('bulk_sync_start_time', config.initial_sync_time)
            }
        )

        wait_for_bulk_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_bulk_sync',
            dag_runs='{{ result("trigger_bulk_sync") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                date_format=config.procore_webhook_fmt,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False,
                use_param_date_format=True
            )
        )

        fetch_state_file = rail.S3DownloadFileOperator(
            task_id='fetch_state_file',
            bucket_name=config.s3_bucket_name,
            key_name=config.budget_revision_events_key
        )

        load_state_file = rail.PythonOperator(
            task_id='load_state_file',
            python_callable=lambda: rail.load_json_artifact(rail.result('fetch_state_file'))
        )

        def parse_ready_events():
            """Parse state file and expand into individual events."""
            state_data = rail.result('load_state_file')
            if not state_data:
                return {
                    'events_to_sync': [],
                    'max_processed_timestamp': None
                }
            last_sync_time = rail.result('get_last_sync_time')['last_synctime']
            last_processed_at = parse_webhook_timestamp(last_sync_time)

            events_to_sync = []
            max_processed_timestamp = last_processed_at

            events = state_data.get('events', {})
            for event_id, event_data in events.items():
                if event_data.get('status', '') != APPROVED or event_data.get('origin_id'):
                    continue

                last_updated = event_data.get('last_updated', '')
                event_dt = parse_webhook_timestamp(last_updated)

                if event_dt > last_processed_at:
                    if event_dt > max_processed_timestamp:
                        max_processed_timestamp = event_dt

                    should_sync_budget = any(
                        is_resource_ready(li.get(RESOURCE_BUDGET_CHANGES, {}))
                        for li in event_data.get('line_items', [])
                    )
                    events_to_sync.append({
                        'event_id': int(event_id),
                        'project_id': int(event_data['project_id']),
                        'custom_field_key': event_data['custom_field_key'],
                        'should_sync_budget': should_sync_budget
                    })
            latest_timestamp = max_processed_timestamp.strftime(config.procore_webhook_fmt) if events_to_sync else None
            return {
                'events_to_sync': events_to_sync,
                'max_processed_timestamp': latest_timestamp
            }

        process_ready_events = rail.PythonOperator(
            task_id='process_ready_events',
            python_callable=parse_ready_events
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.webhook_events_last_sync_time_var,
                value_to_set=rail.result('process_ready_events')['max_processed_timestamp'] \
                    if rail.result('process_ready_events').get('max_processed_timestamp')
                    else rail.result('get_last_sync_time')['current_time']
            )
        )

        has_events_to_sync = rail.IfOperator(
            task_id='has_events_to_sync',
            test="{{ result('process_ready_events').events_to_sync | length > 0 }}",
            yes_task='fetch_cost_types',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
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

        fetch_projects = rail.ProcoreApiOperator(
            task_id='fetch_projects',
            endpoint='/projects',
            method='GET',
            version='1.1',
            query_params={
                'view': 'normal',
                'company_id': procore_company_id_template,
                'filters[id]': "{{ result('process_ready_events').events_to_sync | map(attribute='project_id') | list | tojson }}"
            },
            data_handler=get_projects_by_ids
        )


        trigger_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_dags',
            trigger_dag_id=config.child_dag_id,
            items=lambda: rail.result('process_ready_events')['events_to_sync'],
            conf=lambda item: {
                'event_id': item['event_id'],
                'project_id': item['project_id'],
                'custom_field_key': item['custom_field_key'],
                'should_sync_budget': item['should_sync_budget'],
                'cost_type_mapping': rail.result('fetch_cost_types'),
                'company_id': rail.render_template(procore_company_id_template),
                'job_code': rail.result('fetch_projects').get(str(item['project_id']), {}).get('job_code')
            }
        )

        wait_for_child_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_completion',
            dag_runs="{{ result('trigger_child_dags') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
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
            output_file_name='ProcoreComputerease_BudgetRevisionSyncErrors_{{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_error_email = rail.EmailOperator(
            task_id='send_error_email',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Change Order Sync completed with errors - {{ current_time() }}',
            html_content='email_templates/budget_revision_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> is_bulk_sync
        is_bulk_sync >> rail.Label('Yes') >> fetch_cost_types_for_bulk_sync >> fetch_custom_field >> fetch_all_projects >> trigger_bulk_sync >> wait_for_bulk_sync >> search_logs
        is_bulk_sync >> rail.Label('No') >> get_last_sync_time >> fetch_state_file >> load_state_file >> process_ready_events >> set_last_sync_time >> has_events_to_sync

        has_events_to_sync >> rail.Label('Yes') >> fetch_cost_types >> fetch_projects >> trigger_child_dags >> wait_for_child_completion >> search_logs >> has_errors
        has_events_to_sync >> rail.Label('No') >> delete_this_dagrun

        has_errors >> rail.Label('Yes') >> write_errors_to_csv >> generate_error_download_link >> send_error_email >> log_to_sumo
        has_errors >> rail.Label('No') >> log_to_sumo

        batch_task >> log_to_sumo

    return dag


rail.for_each_instance(create_dag_instance)
