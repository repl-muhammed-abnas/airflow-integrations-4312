import rail
from datetime import timedelta
from ce_procore_integration.ap_invoice_sync.utils.constants import CE_Fields
from ce_procore_integration.util_dags.constants import InputSource
from ce_procore_integration.util_dags.utils import get_tenant_email

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore AP Invoice Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval='0 0 * * *' if config.input_source == InputSource.EMAIL else timedelta(
            minutes=config.invoice_sync_interval_minutes),
        tags=['computerease_procore', 'ap_invoice_sync'],
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'imap_conn_id': config.imap_conn_id,
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + \
            config.procore_conn_id + ".extra_dejson.company_id }}"

        if config.input_source == InputSource.EMAIL:
            def extract_csv_attachments_func(response):
                if not response:
                    return

                ap_invoice_file = ''
                ap_invoice_file_artifact = ''

                for email in response:
                    if email.get('attachments'):
                        for attachment in email['attachments']:
                            filename = attachment['filename']

                            if filename == f'{config.ap_invoice_report_filename}.csv':
                                ap_invoice_file = filename
                                ap_invoice_file_artifact = attachment['artifact']

                return {
                    'ap_invoice_file': ap_invoice_file,
                    'ap_invoice_file_artifact': ap_invoice_file_artifact,
                    'is_files_present': True if ap_invoice_file else False
                }

            read_emails_from_inbox = rail.ReadEmailOperator(
                task_id='read_emails_from_inbox',
                subject_pattern=config.email_subject_pattern,
                limit=config.email_limit,
                max_emails_to_check=config.email_max_to_check,
                data_handler=extract_csv_attachments_func
            )

            is_matching_email_found = rail.IfOperator(
                task_id='is_matching_email_found',
                test='{{ result("read_emails_from_inbox") | sn | is_truthy }}',
                yes_task='has_report',
                no_task='delete_this_dagrun'
            )

        else:
            new_file_sensor = rail.SFTPAnyFileSensor(
                task_id='new_file_sensor',
                path=config.file_path,
                soft_fail_timeout=timedelta(
                    minutes=config.sftp_sensor_timeout_minutes)
            )

            list_all_files = rail.SFTPListFilesOperator(
                task_id='list_all_files',
                paths=[config.file_path]
            )

            def find_report_files_func():
                all_files = rail.result('list_all_files')[config.file_path]
                ap_invoice_file = ''

                for fileattributes in all_files:
                    if f'{config.ap_invoice_report_filename}.csv' == fileattributes['name']:
                        ap_invoice_file = f"{config.file_path}/{fileattributes['name']}"
                        break

                return {
                    'ap_invoice_file': ap_invoice_file,
                    'is_files_present': True if ap_invoice_file else False
                }

            find_report_files = rail.PythonOperator(
                task_id='find_report_files',
                python_callable=find_report_files_func
            )

            download_ap_invoice_file = rail.SFTPDownloadFileOperator(
                task_id='download_ap_invoice_file',
                remote_filepath="{{ result('find_report_files').ap_invoice_file }}"
            )

            was_file_found = rail.IfOperator(
                task_id='was_file_found',
                test='{{ get_task_state("download_ap_invoice_file") == "success" }}',
                yes_task='archive_ap_invoice_file',
                no_task='delete_this_dagrun'
            )

            archive_ap_invoice_file = rail.SFTPMoveFileOperator(
                task_id='archive_ap_invoice_file',
                existing_filename="{{ result('new_file_sensor') }}",
                new_filename=config.archive_filepath +
                '/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}'
            )

        report_test_templated = "{{ result('read_emails_from_inbox').is_files_present | sn | is_truthy }}" if config.input_source == InputSource.EMAIL else "{{ result('find_report_files').is_files_present | sn | is_truthy }}"
        has_report = rail.IfOperator(
            task_id='has_report',
            test=report_test_templated,
            yes_task='load_ap_invoice_csv' if config.input_source == InputSource.EMAIL else 'download_ap_invoice_file',
            no_task='send_missing_files_notification'
        )

        send_missing_files_notification = rail.EmailOperator(
            task_id='send_missing_files_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject="Computerease-Procore Integration: AP Invoice Sync - Missing Required Files - {{ current_time() }}",
            html_content='/email_templates/missing_file_failure.html',
            params={
                'expected_filename': f'{config.ap_invoice_report_filename}.csv',
                'file_result_task': 'read_emails_from_inbox' if config.input_source == InputSource.EMAIL else 'find_report_files'
            }
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        ap_invoice_doc = "{{ result('read_emails_from_inbox').ap_invoice_file_artifact }}" if config.input_source == InputSource.EMAIL else "{{ result('download_ap_invoice_file') }}"
        load_ap_invoice_csv = rail.LoadCSVFileOperator(
            task_id='load_ap_invoice_csv',
            document=ap_invoice_doc
        )

        def get_computerease_ap_invoices_data():
            csv_data = rail.load_all_records(
                rail.result('load_ap_invoice_csv'))
            invoice_numbers = []
            ap_invoices = {}
            for item in csv_data:
                if item.get(CE_Fields.Job, '').strip() != '':
                    invoice_number = item.get(CE_Fields.Invoice_Number, '')
                    invoice_amount = float(
                        item.get(CE_Fields.Amount, '').replace(',', ''))
                    if config.skip_zero_amount_invoice == 'yes' and invoice_amount == 0:
                        continue
                    data = {
                        'Due_Date': item.get(CE_Fields.Due_Date, ''),
                        'Description': item.get(CE_Fields.Description, ''),
                        'PO_Number': item.get(CE_Fields.PO_Number, '').strip(),
                        'Invoice_Number': item.get(CE_Fields.Invoice_Number, ''),
                        'Status': item.get(CE_Fields.Status, '').strip(),
                        'Invoice_Date': item.get(CE_Fields.Invoice_Date, ''),
                        'Invoice_Period': item.get(CE_Fields.Invoice_Period, '0'),
                        'Amount': item.get(CE_Fields.Amount, '').replace(',', ''),
                        'Job': item.get(CE_Fields.Job, '').strip(),
                        'Job_Name': item.get(CE_Fields.Job_Name, '').strip(),
                        'Phase': item.get(CE_Fields.Phase, '').strip(),
                        'Category': item.get(CE_Fields.Category, '').strip(),
                        'Cost_Type': item.get(CE_Fields.Cost_Type, '').strip(),
                        'Retention_Pct': item.get(CE_Fields.Retention_Pct, '').rstrip('%'),
                        'Retention_Amount': item.get(CE_Fields.Retention_Amount, '').replace(',', ''),
                        'Balance_Payable': item.get(CE_Fields.Balance_Payable, '').replace(',', ''),
                        'Quantity': item.get(CE_Fields.Quantity, ''),
                        'Item_Number': item.get(CE_Fields.Item_Number, '').strip(),
                        'Item_Name': item.get(CE_Fields.Item_Name, '').strip(),
                        'Distribution_Description': item.get(CE_Fields.Distribution_Description, '').strip()
                    }
                    if invoice_number in invoice_numbers:
                        ap_invoices[invoice_number].append(data)
                    else:
                        ap_invoices[invoice_number] = [data]
                        invoice_numbers.append(invoice_number)
            return [{'key': k, 'value': v} for k, v in ap_invoices.items()]

        computerease_ap_invoices = rail.PythonOperator(
            task_id='computerease_ap_invoices',
            python_callable=get_computerease_ap_invoices_data
        )

        has_invoices_to_sync = rail.IfOperator(
            task_id='has_invoices_to_sync',
            test='{{ result("computerease_ap_invoices") | length > 0 }}',
            yes_task='batch_task',
            no_task='log_to_sumo'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_procore_projects',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        fetch_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda projects: {
                x['origin_id']: x['id'] for x in projects if x['active']} if projects else {}
        )

        def get_project_id(ap_invoice):
            projects = rail.result('fetch_procore_projects')
            origin_id = f"CE_{ap_invoice['value'][0]['Job']}"
            return projects[origin_id] if origin_id in projects else None

        trigger_ap_invoice_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_ap_invoice_sync',
            items=lambda: rail.result('computerease_ap_invoices'),
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'ap_invoice': item,
                'company_id': rail.render_template(procore_company_id_template),
                'project_id': get_project_id(item)
            }
        )

        wait_for_ap_invoice_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_ap_invoice_sync',
            dag_runs='{{ result("trigger_ap_invoice_sync") }}',
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
            header=['Invoice Number', 'Project Id', 'Commitment COntract Id',
                    'Billing Period id', 'Payload', 'Status', 'Reason', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('invoice_number','') }}",
                "{{ item.properties | attr_or_default('project_id','') }}",
                "{{ item.properties | attr_or_default('commitment_contract_id','') }}",
                "{{ item.properties | attr_or_default('billing_period_id','') }}",
                "{{ item.properties | attr_or_default('payload','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_APInvoiceSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: AP Invoice Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/ap_invoice_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Conditional branching based on input_source
        if config.input_source == InputSource.EMAIL:
            # Email flow
            read_emails_from_inbox >> is_matching_email_found

            is_matching_email_found >> rail.Label(
                'Yes') >> has_report

            has_report >> rail.Label(
                'Yes') >> load_ap_invoice_csv
            has_report >> rail.Label(
                'No') >> send_missing_files_notification >> delete_this_dagrun

            is_matching_email_found >> rail.Label(
                'No') >> delete_this_dagrun

        else:
            # SFTP flow
            new_file_sensor >> list_all_files >> find_report_files >> has_report

            has_report >> rail.Label(
                'Yes') >> download_ap_invoice_file >> was_file_found
            has_report >> rail.Label(
                'No') >> send_missing_files_notification >> delete_this_dagrun

            was_file_found >> rail.Label(
                'Yes') >> archive_ap_invoice_file >> load_ap_invoice_csv
            was_file_found >> rail.Label(
                'No') >> delete_this_dagrun

        load_ap_invoice_csv >> computerease_ap_invoices >> has_invoices_to_sync

        has_invoices_to_sync >> rail.Label('No') >> log_to_sumo
        has_invoices_to_sync >> rail.Label(
            'Yes') >> batch_task >> fetch_procore_projects >> trigger_ap_invoice_sync >> wait_for_ap_invoice_sync >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        batch_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
