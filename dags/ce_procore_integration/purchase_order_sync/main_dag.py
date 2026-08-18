import rail
from datetime import timedelta
from ce_procore_integration.purchase_order_sync.utils.constants import ProcorePurchaseOrderStatus, CE_Fields, InputSource
from ce_procore_integration.purchase_order_sync.utils.util import clean_currency
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore Purchase Order Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval='0 0 * * *' if config.input_source == InputSource.EMAIL else timedelta(
            minutes=config.purchase_order_sync_interval_minutes),
        default_args={
            'imap_conn_id': config.imap_conn_id,
            'sftp_conn_id': config.sftp_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"

        if config.input_source == InputSource.EMAIL:  # Email flow
            def extract_csv_from_email(response):
                if not response:
                    return {
                        'po_file': '',
                        'po_file_artifact': '',
                        'is_file_present': False
                    }

                po_file = ''
                po_file_artifact = ''

                for email in response:
                    if email.get('attachments'):
                        for attachment in email['attachments']:
                            filename = attachment['filename']
                            # Look for CSV attachment (adjust pattern if needed)
                            if filename == f'{config.po_report_filename}.csv':
                                po_file = filename
                                po_file_artifact = attachment['artifact']
                                break

                return {
                    'po_file': po_file,
                    'po_file_artifact': po_file_artifact,
                    'is_file_present': bool(po_file_artifact)
                }

            read_emails_from_inbox = rail.ReadEmailOperator(
                task_id='read_emails_from_inbox',
                subject_pattern=config.email_subject_pattern,
                limit=config.email_limit,
                max_emails_to_check=config.max_emails_to_check,
                data_handler=extract_csv_from_email
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                start_task='is_email_found_with_csv',
                end_task='log_to_sumo',
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            is_email_found_with_csv = rail.IfOperator(
                task_id='is_email_found_with_csv',
                test=lambda: rail.result('read_emails_from_inbox').get(
                    'is_file_present', False) if rail.result('read_emails_from_inbox') else False,
                yes_task='load_csv',
                no_task='send_missing_file_notification'
            )

            send_missing_file_notification = rail.EmailOperator(
                task_id='send_missing_file_notification',
                to=get_tenant_email(config),
                bcc=config.internal_email,
                subject='Computerease-Procore Integration: Purchase Order Sync - No File Found - {{ current_time() }}',
                html_content='/email_templates/purchase_order_missing_file.html'
            )

        else:  # SFTP flow
            new_file_sensor = rail.SFTPAnyFileSensor(
                task_id='new_file_sensor',
                path=config.file_path,
                soft_fail_timeout=timedelta(minutes=10)
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                start_task='download_artifact',
                end_task='log_to_sumo',
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            download_artifact = rail.SFTPDownloadFileOperator(
                task_id='download_artifact',
                remote_filepath="{{ result('new_file_sensor') }}"
            )

            is_new_file_found = rail.IfOperator(
                task_id='is_new_file_found',
                trigger_rule='all_done',
                test='{{ get_task_state("new_file_sensor") == "success" }}',
                yes_task='archive_file',
                no_task='delete_this_dagrun'
            )

            archive_file = rail.SFTPMoveFileOperator(
                task_id='archive_file',
                existing_filename="{{ result('new_file_sensor') }}",
                new_filename=config.archive_file_path +
                "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        # Conditional document source based on input type
        csv_document = "{{ result('read_emails_from_inbox').po_file_artifact }}" if config.input_source == InputSource.EMAIL else "{{ result('download_artifact') }}"

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document=csv_document
        )

        def get_computerease_purchase_order_data():
            csv_data = rail.load_all_records(rail.result('load_csv'))
            purchase_orders = [
                {
                    'number': item.get(CE_Fields.PO_NUMBER, ''),
                    'contract_date': item.get(CE_Fields.PO_DATE, ''),
                    'vendor_number': item.get(CE_Fields.VENDOR_NUMBER, ''),
                    'buyer': item.get(CE_Fields.BUYER, ''),
                    'item_number': item.get(CE_Fields.ITEM_NUMBER, ''),
                    'item_name': item.get(CE_Fields.ITEM_NAME, ''),
                    'unit_cost': clean_currency(item.get(CE_Fields.UNIT_PRICE, '0.00')),
                    'quantity': item.get(CE_Fields.ORDERED, '0'),
                    'amount': clean_currency(item.get(CE_Fields.VALUE_ORDERED, '0.00')),
                    'issued_on_date': item.get(CE_Fields.RECEIPT_DATE, ''),
                    'job_code': item.get(CE_Fields.JOB_CODE, ''),
                    'phase_code': item.get(CE_Fields.PHASE_CODE, ''),
                    'category_code': item.get(CE_Fields.CATEGORY_CODE, ''),
                    'cost_type': item.get(CE_Fields.COST_TYPE, ''),
                    'equipment_number': item.get(CE_Fields.EQUIPMENT_NUMBER, ''),
                    'equipment_code': item.get(CE_Fields.EQUIPMENT_CODE, ''),
                    'approved': item.get(CE_Fields.APPROVED, ''),
                    'delivery_date': item.get(CE_Fields.DATE_REQUIRED, ''),
                    'bill_to_address': item.get(CE_Fields.LOCATION, '')
                } for item in csv_data if item.get(CE_Fields.PO_NUMBER, '').strip() != ''
            ]
            return purchase_orders

        computerease_purchase_orders = rail.PythonOperator(
            task_id='computerease_purchase_orders',
            python_callable=get_computerease_purchase_order_data
        )

        has_po_to_sync = rail.IfOperator(
            task_id='has_po_to_sync',
            test='{{ result("computerease_purchase_orders") | length > 0 }}',
            yes_task='fetch_procore_projects',
            no_task='log_to_sumo'
        )

        fetch_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda projects: {
                x['origin_id']: x['id'] for x in projects if x['origin_id']} if projects else {}
        )

        # Fetch Procore vendors for matching with CE vendor data
        fetch_procore_vendors = rail.ProcoreApiOperator(
            task_id='fetch_procore_vendors',
            endpoint='/vendors',
            method='GET',
            query_params=lambda: {'company_id': rail.render_template(
                procore_company_id_template)},
            data_handler=lambda vendors: {
                x['origin_id']: {
                    'id': x['id'],
                    'project_ids': x['project_ids']
                } for x in vendors if x['origin_id']
            } if vendors else {}
        )

        def create_purchase_orders_payload():
            """Group CE data by job code and create batches for child DAG processing."""
            computerease_purchase_orders = rail.result(
                'computerease_purchase_orders')
            procore_vendors = rail.result('fetch_procore_vendors')

            missing_job_pos = set()
            invalid_purchase_orders = []
            # Two-level grouping: job_code -> po_number -> [line_items]
            purchase_orders_dict = {}

            for line_item in computerease_purchase_orders:
                job_code = line_item['job_code']
                po_number = line_item['number']

                if not job_code.strip():
                    if po_number not in missing_job_pos:
                        missing_job_pos.add(po_number)
                        invalid_purchase_orders.append({
                            'po_number': po_number,
                            'job_code': job_code,
                            'reason': 'Job Code is missing'
                        })
                    continue

                # Initialize job_code dict if not exists
                if job_code not in purchase_orders_dict:
                    purchase_orders_dict[job_code] = {}

                # Initialize po_number list if not exists
                if po_number not in purchase_orders_dict[job_code]:
                    purchase_orders_dict[job_code][po_number] = []

                purchase_orders_dict[job_code][po_number].append(line_item)

            purchase_orders = []
            # Iterate through job codes, then through PO numbers within each job
            for job_code, po_line_items in purchase_orders_dict.items():
                for po_number, line_items in po_line_items.items():
                    po = line_items[0]
                    vendor_origin_id = f"CE_{po['vendor_number']}"
                    vendor_id = procore_vendors.get(vendor_origin_id, {}).get('id')
                    status = ProcorePurchaseOrderStatus.APPROVED if po[
                        'approved'].upper() == 'TRUE' else ProcorePurchaseOrderStatus.DRAFT

                    po_item = {
                        'number': po_number,
                        'title': f"Purchase Order: {po_number} - Job: {job_code}",
                        'description': f"Items: {', '.join(list(set(item['item_name'] for item in line_items)))}"[:500],
                        'status': status,
                        'vendor_id': vendor_id,
                        'vendor_origin_id': vendor_origin_id,
                        'job_code': job_code,
                        'bill_to_address': po['bill_to_address'],
                        'issued_on_date': po['issued_on_date'],
                        'delivery_date': po['delivery_date'],
                        'contract_date': po['contract_date'],
                        'buyer': po['buyer'],
                        'line_items': list(map(lambda item: {
                            'item_number': item['item_number'],
                            'description': item['item_name'],
                            'amount': float(item['amount']),
                            'quantity': float(item['quantity']),
                            'unit_cost': float(item['unit_cost']),
                            'cost_type': item['cost_type'],
                            'phase_code': item['phase_code'],
                            'category_code': item['category_code'],
                            'equipment_code': item['equipment_code'],
                            'equipment_number': item['equipment_number']
                        }, line_items))
                    }

                    purchase_orders.append(po_item)

            return purchase_orders, invalid_purchase_orders

        def create_po_batch_grouped_by_job():
            """Group purchase orders by job code for batch processing"""
            purchase_orders, invalid_purchase_orders = create_purchase_orders_payload()
            procore_vendors = rail.result('fetch_procore_vendors')
            procore_project_ids = rail.result('fetch_procore_projects')

            # Group purchase orders by job code
            job_code_groups = {}
            for po in purchase_orders:
                po_number = po['number']
                job_code = po['job_code']
                vendor_id = po['vendor_id']

                if procore_project_ids.get(f'CE_{job_code}') is None:
                    invalid_purchase_orders.append({
                        'po_number': po_number,
                        'job_code': job_code,
                        'reason': 'Project not found in Procore'
                    })
                    continue

                if not vendor_id:
                    invalid_purchase_orders.append({
                        'po_number': po_number,
                        'job_code': job_code,
                        'reason': 'Vendor not found in Procore'
                    })
                    continue

                is_vendor_assigned_to_job = False
                vendor_associated_projects = procore_vendors.get(
                    po['vendor_origin_id'], {}).get('project_ids', [])
                if procore_project_ids.get(f'CE_{po["job_code"]}', '') not in vendor_associated_projects:
                    is_vendor_assigned_to_job = True
                po['should_assign_contractor_to_project'] = is_vendor_assigned_to_job

                if job_code not in job_code_groups:
                    job_code_groups[job_code] = []
                job_code_groups[job_code].append(po)

            # Convert groups into batches with project_id
            valid_batches = []
            for job_code, pos in job_code_groups.items():
                batch = {
                    'job_code': job_code,
                    'project_id': procore_project_ids[f'CE_{job_code}'],
                    'purchase_orders': pos
                }
                valid_batches.append(batch)

            return {
                'valid_batches': valid_batches,
                'invalid_purchase_orders': invalid_purchase_orders
            }

        group_po_by_job = rail.PythonOperator(
            task_id='group_po_by_job',
            python_callable=create_po_batch_grouped_by_job
        )

        has_po_exceptions = rail.IfOperator(
            task_id='has_po_exceptions',
            test=lambda: len(rail.result('group_po_by_job').get(
                'invalid_purchase_orders', [])) > 0,
            yes_task='write_exception',
            no_task='check_has_valid_batches'
        )

        write_exception = rail.WriteLogOperator(
            task_id='write_exception',
            message='PO Exception',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda: [
                {
                    'code': item.get('po_number', ''),
                    'job_code': item.get('job_code', ''),
                    'company_id': rail.render_template(procore_company_id_template),
                    'status': 'Exception',
                    'reason': item.get('reason', '')
                } for item in rail.result('group_po_by_job')['invalid_purchase_orders']
            ]
        )

        check_has_valid_batches = rail.IfOperator(
            task_id='check_has_valid_batches',
            test='{{ result("group_po_by_job").valid_batches | length > 0 }}',
            yes_task='trigger_purchase_order_sync',
            no_task='search_logs'
        )

        trigger_purchase_order_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_purchase_order_sync',
            items=lambda: rail.result('group_po_by_job')['valid_batches'],
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'purchase_order_batch': item,
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_purchase_order_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_purchase_order_sync',
            dag_runs='{{ result("trigger_purchase_order_sync") }}',
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
            header=['Code', 'Job Code', 'Company Id',
                    'Status', 'Reason', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('code','') }}",
                "{{ item.properties | attr_or_default('job_code','') }}",
                "{{ item.properties | attr_or_default('company_id','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_PurchaseOrderSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Purchase Order Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/purchase_order_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        if config.input_source == InputSource.EMAIL:

            read_emails_from_inbox >> batch_task >> is_email_found_with_csv

            is_email_found_with_csv >> rail.Label('Yes') >> load_csv
            is_email_found_with_csv >> rail.Label(
                'No') >> send_missing_file_notification >> delete_this_dagrun

        else:  # SFTP flow
            new_file_sensor >> batch_task >> download_artifact >> is_new_file_found

            is_new_file_found >> rail.Label('Yes') >> archive_file >> load_csv
            is_new_file_found >> rail.Label('No') >> delete_this_dagrun

        batch_task >> log_to_sumo
        load_csv >> computerease_purchase_orders >> has_po_to_sync

        has_po_to_sync >> rail.Label('No') >> log_to_sumo
        has_po_to_sync >> rail.Label(
            'Yes') >> fetch_procore_projects >> fetch_procore_vendors >> group_po_by_job >> has_po_exceptions

        has_po_exceptions >> rail.Label(
            'Yes') >> write_exception >> check_has_valid_batches
        has_po_exceptions >> rail.Label('No') >> check_has_valid_batches

        check_has_valid_batches >> rail.Label(
            'Yes') >> trigger_purchase_order_sync >> wait_for_purchase_order_sync >> search_logs >> if_logs_present
        check_has_valid_batches >> rail.Label(
            'No') >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
