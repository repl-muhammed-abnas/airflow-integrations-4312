import rail
from datetime import timedelta
from collections import defaultdict
from ce_procore_integration.ar_invoice_sync.utils.constants import CE_ITEMS, InputSource
from ce_procore_integration.ar_invoice_sync.utils.util import convert_date, clean_currency
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore AR Invoice Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval='0 0 * * *' if config.input_source == InputSource.EMAIL else timedelta(
            minutes=config.ar_invoices_sync_interval_minutes),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'imap_conn_id': config.imap_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"

        if config.input_source == InputSource.EMAIL:  # Email flow
            def extract_csv_from_email(response):
                if not response:
                    return {
                        'ar_invoice_file': '',
                        'ar_invoice_file_artifact': '',
                        'is_file_present': False
                    }

                ar_invoice_file = ''
                ar_invoice_file_artifact = ''

                for email in response:
                    if email.get('attachments'):
                        for attachment in email['attachments']:
                            filename = attachment['filename']
                            # Look for CSV attachment
                            if filename == f'{config.ar_invoice_report_filename}.csv':
                                ar_invoice_file = filename
                                ar_invoice_file_artifact = attachment['artifact']
                                break

                return {
                    'ar_invoice_file': ar_invoice_file,
                    'ar_invoice_file_artifact': ar_invoice_file_artifact,
                    'is_file_present': bool(ar_invoice_file_artifact)
                }

            read_emails_from_inbox = rail.ReadEmailOperator(
                task_id='read_emails_from_inbox',
                subject_pattern=config.email_subject_pattern,
                limit=config.email_limit,
                max_emails_to_check=config.email_max_to_check,
                data_handler=extract_csv_from_email
            )

            is_email_found_with_csv = rail.IfOperator(
                task_id='is_email_found_with_csv',
                test=lambda: rail.result('read_emails_from_inbox').get('is_file_present', False) if rail.result('read_emails_from_inbox') else False,
                yes_task='load_csv',
                no_task='send_missing_file_notification'
            )

            send_missing_file_notification = rail.EmailOperator(
                task_id='send_missing_file_notification',
                to=get_tenant_email(config),
                bcc=config.internal_email,
                subject="Computerease-Procore Integration: AR Invoice Sync - Missing Required File - {{ current_time() }}",
                html_content='/email_templates/missing_file_failure.html'
            )

        else:
            new_file_sensor = rail.SFTPAnyFileSensor(
                task_id='new_file_sensor',
                path=config.file_path,
                sftp_conn_id=config.sftp_conn_id,
                soft_fail_timeout=timedelta(minutes=10)
            )

            download_artifact = rail.SFTPDownloadFileOperator(
                task_id='download_artifact',
                sftp_conn_id=config.sftp_conn_id,
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

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='computerease_ar_invoicess',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        ar_invoice_doc = "{{ result('read_emails_from_inbox').ar_invoice_file_artifact }}" if config.input_source == InputSource.EMAIL else "{{ result('download_artifact') }}"
        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            encoding="utf-8-sig",
            document=ar_invoice_doc
        )

        def get_computerease_ar_invoices_data():
            csv_data = rail.load_all_records(rail.result('load_csv'))

            # Group line items by invoice number
            invoice_groups = defaultdict(list)
            for item in csv_data:
                invoice_number = item.get(CE_ITEMS.INVOICE, '')
                if invoice_number:
                    invoice_groups[invoice_number].append(item)

            # Process each invoice group
            ar_invoices = []
            for invoice_number, line_items in invoice_groups.items():
                # Use first line item for invoice-level fields
                first_item = line_items[0]

                total_billed = sum(clean_currency(
                    item.get(CE_ITEMS.BILLED, '0')) for item in line_items)

                # Build line items array with budget codes for SOV mapping
                sov_line_items = []
                for item in line_items:
                    phase = item.get(CE_ITEMS.PHASE, '')
                    category = item.get(CE_ITEMS.CATEGORY, '')
                    cost_type = config.default_cost_type

                    if phase and category:
                        budget_code = f"{phase}-{category}.{cost_type}"
                    elif phase or category:
                        budget_code = f"{phase or category}.{cost_type}"
                    else:
                        budget_code = ''

                    sov_line_items.append({
                        'phase': phase,
                        'phase_name': item.get(CE_ITEMS.PHASE_NAME, ''),
                        'category': category,
                        'category_name': item.get(CE_ITEMS.CATEGORY_NAME, ''),
                        'cost_type': cost_type,
                        'budget_code': budget_code,
                        'billed_amount': clean_currency(item.get(CE_ITEMS.BILLED, '0'))
                    })

                invoice_obj = {
                    'invoice_number': invoice_number,
                    'customer_code': first_item.get(CE_ITEMS.CUSTOMER, ''),
                    'job_code': first_item.get(CE_ITEMS.JOB, ''),
                    'invoice_date': convert_date(first_item.get(CE_ITEMS.DATE, '')),
                    'period': first_item.get(CE_ITEMS.PERIOD, ''),
                    'total': total_billed,
                    'line_items': sov_line_items
                }

                ar_invoices.append(invoice_obj)

            return ar_invoices

        computerease_ar_invoicess = rail.PythonOperator(
            task_id='computerease_ar_invoicess',
            python_callable=get_computerease_ar_invoices_data
        )

        has_invoices_to_sync = rail.IfOperator(
            task_id='has_invoices_to_sync',
            test='{{ result("computerease_ar_invoicess") | length > 0 }}',
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

        def create_ar_invoices_payload():
            computerease_ar_invoices = rail.result('computerease_ar_invoicess')
            invalid_invoices = []
            valid_invoices = []

            for invoice in computerease_ar_invoices:
                invoice_number = invoice['invoice_number']
                job_code = invoice['job_code']
                customer_code = invoice['customer_code']

                if not job_code:
                    invalid_invoices.append({
                        'invoice_number': invoice_number,
                        'job_code': job_code,
                        'customer_code': customer_code,
                        'reason': 'Job Code is missing'
                    })
                    continue
                if not customer_code:
                    invalid_invoices.append({
                        'invoice_number': invoice_number,
                        'job_code': job_code,
                        'customer_code': customer_code,
                        'reason': 'Customer Code is missing'
                    })
                    continue

                invoice_item = {
                    'invoice_number': invoice_number,
                    'job_code': job_code,
                    'customer_code': customer_code,
                    'invoice_date': invoice['invoice_date'],
                    'period': invoice['period'],
                    'total': invoice['total'],
                    'line_items': invoice['line_items']
                }
                valid_invoices.append(invoice_item)

            return valid_invoices, invalid_invoices

        def create_invoice_batch_grouped_by_job():
            valid_invoices, invalid_invoices = create_ar_invoices_payload()
            procore_project_ids = rail.result('fetch_procore_projects')
            computerease_ar_invoices = rail.result('computerease_ar_invoicess')

            # Create lookup for full invoice data (with line_items)
            invoice_lookup = {inv['invoice_number']: inv for inv in computerease_ar_invoices}

            project_customer_groups = {}

            for invoice in valid_invoices:
                job_code = invoice.get('job_code', '')
                customer_code = invoice.get('customer_code', '')
                invoice_number = invoice.get('invoice_number', '')
                project_origin_id = f'CE_{job_code}'

                if not job_code or procore_project_ids.get(project_origin_id) is None:
                    invalid_invoices.append({
                        'invoice_number': invoice_number,
                        'job_code': job_code,
                        'customer_code': customer_code,
                        'reason': 'Either this is not a freeform invoice or Project is not synced in Procore'
                    })
                    continue

                group_key = (job_code, customer_code)
                if group_key not in project_customer_groups:
                    project_customer_groups[group_key] = []
                project_customer_groups[group_key].append(invoice)

            valid_batches = []
            for (job_code, customer_code), invoices in project_customer_groups.items():
                invoices_with_budget_codes = []

                for invoice in invoices:
                    invoice_number = invoice.get('invoice_number', '')
                    full_invoice = invoice_lookup.get(invoice_number, {})
                    line_items = full_invoice.get('line_items', [])

                    # Aggregate line items within this invoice by Phase-Category-CostType
                    invoice_budget_totals = defaultdict(lambda: {
                        'net_amount': 0.0,
                        'phase': '',
                        'phase_name': '',
                        'category': '',
                        'category_name': '',
                        'cost_type': ''
                    })

                    for line_item in line_items:
                        phase = line_item.get('phase', '')
                        category = line_item.get('category', '')
                        cost_type = line_item.get(
                            'cost_type', config.default_cost_type)

                        # Create unique key including cost_type for proper aggregation
                        if phase and category:
                            budget_key = f"{phase}-{category}.{cost_type}"
                        elif phase or category:
                            budget_key = f"{phase or category}.{cost_type}"
                        else:
                            budget_key = ''

                        # Always aggregate, even for empty budget_key (per invoice)
                        invoice_budget_totals[budget_key]['net_amount'] += line_item.get(
                            'billed_amount', 0.0)
                        invoice_budget_totals[budget_key]['phase'] = phase
                        invoice_budget_totals[budget_key]['phase_name'] = line_item.get(
                            'phase_name', '')
                        invoice_budget_totals[budget_key]['category'] = category
                        invoice_budget_totals[budget_key]['category_name'] = line_item.get(
                            'category_name', '')
                        invoice_budget_totals[budget_key]['cost_type'] = cost_type

                    # Build budget codes array for this specific invoice
                    invoice_budget_codes = []
                    for bc, data in invoice_budget_totals.items():
                        if data['net_amount'] != 0:
                            invoice_budget_codes.append({
                                'budget_code': bc,
                                'net_amount': data['net_amount'],
                                'phase': data['phase'],
                                'phase_name': data['phase_name'],
                                'category': data['category'],
                                'category_name': data['category_name'],
                                'cost_type': data['cost_type']
                            })

                    # Attach budget_codes to this invoice
                    invoice_with_budget_codes = invoice.copy()
                    invoice_with_budget_codes['budget_codes'] = invoice_budget_codes
                    invoices_with_budget_codes.append(
                        invoice_with_budget_codes)

                batch = {
                    'job_code': job_code,
                    'customer_code': customer_code,
                    'project_id': procore_project_ids[f'CE_{job_code}'],
                    'ar_invoices': invoices_with_budget_codes
                }
                valid_batches.append(batch)

            return {
                'valid_batches': valid_batches,
                'invalid_invoices': invalid_invoices
            }

        group_invoices_by_job = rail.PythonOperator(
            task_id='group_invoices_by_job',
            python_callable=create_invoice_batch_grouped_by_job
        )

        has_invoice_exceptions = rail.IfOperator(
            task_id='has_invoice_exceptions',
            test=lambda: len(rail.result('group_invoices_by_job').get(
                'invalid_invoices', [])) > 0,
            yes_task='write_exception',
            no_task='check_has_valid_batches'
        )

        write_exception = rail.WriteLogOperator(
            task_id='write_exception',
            message='Invoice Exception',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda: [
                {
                    'code': item.get('invoice_number', ''),
                    'job_code': item.get('job_code', ''),
                    'customer_code': item.get('customer_code', ''),
                    'company_id': rail.render_template(procore_company_id_template),
                    'status': 'Exception',
                    'reason': item.get('reason', '')
                } for item in rail.result('group_invoices_by_job')['invalid_invoices']
            ]
        )

        check_has_valid_batches = rail.IfOperator(
            task_id='check_has_valid_batches',
            test='{{ result("group_invoices_by_job").valid_batches | length > 0 }}',
            yes_task='trigger_ar_invoices_sync',
            no_task='search_logs'
        )

        trigger_ar_invoices_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_ar_invoices_sync',
            items=lambda: rail.result('group_invoices_by_job')[
                'valid_batches'],
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'batch': item,
                'company_id': rail.render_template(procore_company_id_template)
            }
        )

        wait_for_ar_invoices_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_ar_invoices_sync',
            dag_runs='{{ result("trigger_ar_invoices_sync") }}',
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
            header=['Invoice Number', 'Job Code', 'Customer Code',
                    'Company Id', 'Status', 'Reason', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('code','') }}",
                "{{ item.properties | attr_or_default('job_code','') }}",
                "{{ item.properties | attr_or_default('customer_code','') }}",
                "{{ item.properties | attr_or_default('company_id','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_ARInvoiceSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: AR Invoice Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/ar_invoices_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Conditional branching based on input_source
        if config.input_source == InputSource.EMAIL:
            # Email flow
            read_emails_from_inbox >> is_email_found_with_csv

            is_email_found_with_csv >> rail.Label('Yes') >> load_csv
            is_email_found_with_csv >> rail.Label('No') >> send_missing_file_notification >> delete_this_dagrun

        else:
            # SFTP flow
            new_file_sensor >> download_artifact >> load_csv
            download_artifact >> rail.Label(
                'Always') >> is_new_file_found

            is_new_file_found >> rail.Label('Yes') >> archive_file 
            is_new_file_found >> rail.Label('No') >> delete_this_dagrun

        load_csv >> batch_task >> computerease_ar_invoicess >> has_invoices_to_sync 
        has_invoices_to_sync >> rail.Label('No') >> log_to_sumo
        has_invoices_to_sync >> rail.Label(
            'Yes') >> fetch_procore_projects >> group_invoices_by_job >> has_invoice_exceptions

        batch_task >> log_to_sumo

        has_invoice_exceptions >> rail.Label(
            'Yes') >> write_exception >> check_has_valid_batches
        has_invoice_exceptions >> rail.Label('No') >> check_has_valid_batches

        check_has_valid_batches >> rail.Label(
            'Yes') >> trigger_ar_invoices_sync >> wait_for_ar_invoices_sync >> search_logs >> if_logs_present
        check_has_valid_batches >> rail.Label(
            'No') >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
