from collections import defaultdict
from datetime import timedelta
import rail

from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):

    with rail.create_airflow_dag(
        dag_id=config.vendor_main_dag_id,
        description='Computerease to Procore Vendor Sync MAIN DAG',
        schedule_interval=timedelta(
            minutes=config.vendor_sync_interval_minutes),
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id,
            'procore_conn_id': config.procore_conn_id,
        }
    ) as dag:

        procore_company_id_template = "{{conn." + \
            config.procore_conn_id + ".extra_dejson.company_id}}"

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.vendor_last_sync_time_var,
                date_format=config.ce_time_format,
                initial_sync_time='1970-01-01T00:00:00Z',
                reset_after_threshold=False
            )
        )

        def filter_vendors_by_timestamp(response_data):
            """
            Note: CE vendor endpoint doesn't support server-side filtering
            (gt~updated_at not available). Once they start supporting it,
            we can remove this function and directly pass the filter in query params.
            """
            last_sync_time = rail.result('get_last_sync_time')['last_synctime']
            filtered_vendors = []
            for vendor in response_data:
                vendor_updated_at = vendor.get('updated_at', '')
                if vendor_updated_at > last_sync_time:
                    filtered_vendors.append(vendor)

            return filtered_vendors

        fetch_computerease_vendors = rail.ComputereaseAPIOperator(
            task_id='fetch_computerease_vendors',
            endpoint='/catalog/vendor',
            request_method='GET',
            data_handler=lambda vendors: filter_vendors_by_timestamp(
                vendors['data'])
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.vendor_last_sync_time_var,
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )
        )

        has_vendors_to_sync = rail.IfOperator(
            task_id='has_vendors_to_sync',
            test='{{ result("fetch_computerease_vendors") | length > 0 }}',
            yes_task='fetch_procore_vendors',
            no_task='log_to_sumo'
        )


        def group_vendors_by_vendor_code(response):
            matching_vendors = defaultdict(list)
            for vendor in response:
                if not vendor['abbreviated_name']:
                    continue
                matching_vendors[vendor['abbreviated_name']].append({
                    'id': vendor['id'],
                    'origin_id': vendor['origin_id']
                })
            return dict(matching_vendors)

        fetch_procore_vendors = rail.ProcoreApiOperator(
            task_id='fetch_procore_vendors',
            endpoint='/vendors',
            method='GET',
            query_params=lambda: {
                'view': 'normal',
                'company_id': rail.render_template(procore_company_id_template),
            },
            data_handler=group_vendors_by_vendor_code
        )

        def parse_computerease_vendors():
            data = rail.result('fetch_computerease_vendors')
            matching_vendors = rail.result('fetch_procore_vendors')
            vendors = []
            for vendor in data:
                # Only concatenate address_2 if it's not already in address_1
                addr1, addr2 = (vendor.get('address_1') or '').strip(
                ), (vendor.get('address_2') or '').strip()
                address = ' '.join(
                    filter(None, [addr1, addr2])) if addr2 and addr2 not in addr1 else addr1

                vendors.append({
                    'code': vendor.get('code', ''),
                    'name': vendor.get('name', ''),
                    'address': address,
                    'city': vendor.get('city', ''),
                    'state': vendor.get('state', ''),
                    'zip': vendor.get('zip', ''),
                    'phone': vendor.get('phone', ''),
                    'fax': vendor.get('fax', ''),
                    'email': vendor.get('email', ''),
                    'website': vendor.get('web', ''),
                    'is_active': vendor.get('vendor_status', '').upper() == 'ACTIVE',
                    'matching_vendors': matching_vendors.get(vendor['code'], None),
                })
            return vendors

        parse_vendors = rail.PythonOperator(
            task_id='parse_vendors',
            python_callable=parse_computerease_vendors
        )

        def chunk_vendors():
            vendors = rail.result('parse_vendors') or []
            # Below the threshold, keep per-vendor granularity (chunk_size=1).
            # Above it, batch into larger chunks to collapse PATCH /vendors/sync calls.
            if len(vendors) <= config.vendor_sync_low_volume_threshold:
                chunk_size = 1
            else:
                chunk_size = config.vendor_sync_chunk_size
            return [vendors[i:i + chunk_size] for i in range(0, len(vendors), chunk_size)]

        build_vendor_chunks = rail.PythonOperator(
            task_id='build_vendor_chunks',
            python_callable=chunk_vendors
        )

        trigger_vendor_sync_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_vendor_sync_child_dag',
            items=lambda: rail.result('build_vendor_chunks'),
            trigger_dag_id=config.vendor_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'vendors': item,
                'procore_company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_vendor_sync_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_vendor_sync_completion',
            dag_runs='{{ result("trigger_vendor_sync_child_dag") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='{{ result("search_logs", "length") > 0 }}',
            yes_task='write_logs_into_csv',
            no_task='log_to_sumo'
        )

        write_logs_into_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_into_csv',
            source='{{ result("search_logs") }}',
            header=['Vendor Code', 'Vendor Name', 'Error Message', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('entity_code','') }}",
                "{{ item.properties | attr_or_default('entity_name','') }}",
                "{{ item.properties | attr_or_default('error_message','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_logs_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_VendorSyncLogs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60
        )

        send_notification_with_logs = rail.EmailOperator(
            task_id='send_notification_with_logs',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Vendor Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/vendor_sync_logs_notification.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> get_last_sync_time >> fetch_computerease_vendors >> set_last_sync_time
        batch_task >> log_to_sumo

        set_last_sync_time >> has_vendors_to_sync

        has_vendors_to_sync >> rail.Label(
            'Yes') >> fetch_procore_vendors >> parse_vendors >> build_vendor_chunks >> trigger_vendor_sync_child_dag
        has_vendors_to_sync >> rail.Label('No') >> log_to_sumo

        trigger_vendor_sync_child_dag >> wait_for_vendor_sync_completion >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_logs_download_link >> send_notification_with_logs >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
