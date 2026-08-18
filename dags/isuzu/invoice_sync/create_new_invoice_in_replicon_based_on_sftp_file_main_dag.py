from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from isuzu.invoice_sync.utils import python_callable
null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_create_new_invoice_in_replicon_master_{config.instance}',
        description=f'New invoice files placed on SFTP server will create invoice in Replicon {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='archive_input_file',
            no_task='delete_this_dagrun'
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_base }}.csv"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='load_csv_create_list_from_csv_4_4_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_csv_create_list_from_csv_4_4_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        load_csv_create_list_from_csv_4_4_4 = rail.PythonOperator(
            task_id='load_csv_create_list_from_csv_4_4_4',
            python_callable=lambda: python_callable.get_csv_data_headers_mapped(
                rail.result('download_input_file'), ['Vendor_Invoice_Number',
                                                     'Invoice_Date',
                                                     'Vendor_Name',
                                                     'PO_Number',
                                                     'Request_Custom_15_Posting_Date',
                                                     'Request_ID',
                                                     'Line_Item_Custom_10_Client',
                                                     'Line_Item_Custom_11_Project',
                                                     'Line_Item_Description',
                                                     'Line_Item_Custom_07_Profit_Center',
                                                     'Line_Item_Expense_Type_Name',
                                                     'Line_Item_Quantity',
                                                     'Line_Item_Unit_Price',
                                                     'Invoice_Amount'])
        )
        compose_csv_with_headers = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_headers',
            source="{{ result('load_csv_create_list_from_csv_4_4_4') | to_json }}",
            header=['Vendor_Invoice_Number',
                    'Invoice_Date',
                    'Vendor_Name',
                    'PO_Number',
                    'Request_Custom_15_Posting_Date',
                    'Request_ID',
                    'Line_Item_Custom_10_Client',
                    'Line_Item_Custom_11_Project',
                    'Line_Item_Description',
                    'Line_Item_Custom_07_Profit_Center',
                    'Line_Item_Expense_Type_Name',
                    'Line_Item_Quantity',
                    'Line_Item_Unit_Price',
                    'Invoice_Amount'],
            row=[
                "{{ item['Vendor_Invoice_Number'] }}",
                "{{ item['Invoice_Date'] }}",
                "{{ item['Vendor_Name'] }}",
                "{{ item['PO_Number'] }}",
                "{{ item['Request_Custom_15_Posting_Date'] }}",
                "{{ item['Request_ID'] }}",
                "{{ item['Line_Item_Custom_10_Client'] }}",
                "{{ item['Line_Item_Custom_11_Project'] }}",
                "{{ item['Line_Item_Description'] }}",
                "{{ item['Line_Item_Custom_07_Profit_Center'] }}",
                "{{ item['Line_Item_Expense_Type_Name'] }}",
                "{{ item['Line_Item_Quantity'] }}",
                "{{ item['Line_Item_Unit_Price'] }}",
                "{{ item['Invoice_Amount'] }}"
            ]
        )

        create_collection_create_list_from_csv_4_4_4 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4_4_4',
            source="{{ result('compose_csv_with_headers') }}",
            name="invoicedata",
            columns={
                'Vendor_Invoice_Number': 'Vendor_Invoice_Number',
                'Invoice_Date': 'Invoice_Date',
                'Vendor_Name': 'Vendor_Name',
                'PO_Number': 'PO_Number',
                'Request_Custom_15_Posting_Date': 'Request_Custom_15_Posting_Date',
                'Request_ID': 'Request_ID',
                'Line_Item_Custom_10_Client': 'Line_Item_Custom_10_Client',
                'Line_Item_Custom_11_Project': 'Line_Item_Custom_11_Project',
                'Line_Item_Description': 'Line_Item_Description',
                'Line_Item_Custom_07_Profit_Center': 'Line_Item_Custom_07_Profit_Center',
                'Line_Item_Expense_Type_Name': 'Line_Item_Expense_Type_Name',
                'Line_Item_Quantity': 'Line_Item_Quantity',
                'Line_Item_Unit_Price': 'Line_Item_Unit_Price',
                'Invoice_Amount': 'Invoice_Amount'
            }


        )

        query_list_uniq_requestid_5_uniq_requestid_5_uniq_requestid_5 = rail.QueryCollectionOperator(
            task_id='query_list_uniq_requestid_5_uniq_requestid_5_uniq_requestid_5',
            query="""select distinct  invoicedata.Request_ID from  invoicedata""",
        )
        get_requestid_list = rail.PythonOperator(
            task_id='get_requestid_list',
            python_callable=lambda: [i.get('Request_ID') for i in rail.load_all_records(
                rail.result("query_list_uniq_requestid_5_uniq_requestid_5_uniq_requestid_5"))],
        )

        get_report1_details = rail.RepliconReportDetailsOperator(
            task_id='get_report1_details',
            report_name=config.report1_name
        )
        invoice_report1_generation = rail.run_report2(
            group_id='invoice_report1_generation',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report1_details', {}).get('uri'),
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_10_10_10 = rail.LoadCSVFileOperator(
            task_id="parse_csv_10_10_10",
            document="{{ result('invoice_report1_generation.get_report_result').reportGenerationResults[0].payload }}",
        )
        already_processed_requestid = rail.PythonOperator(
            task_id='already_processed_requestid',
            python_callable=python_callable.get_processed_requestid_list
        )

        already_processed_records = rail.PythonOperator(
            task_id='already_processed_records',
            python_callable=python_callable.get_already_present_records
        )
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        if_already_processed_records = rail.IfOperator(
            task_id='if_already_processed_records',
            test='''{{ result('already_processed_records') | length > 0 }}''',
            yes_task="foreach_query_all_records_list_where_requestid_isalready_processed",
            no_task="log_filename_11",
        )
        foreach_query_all_records_list_where_requestid_isalready_processed = rail.ForEachOperator(
            task_id='foreach_query_all_records_list_where_requestid_isalready_processed',
            items="{{ result('already_processed_records') | to_json }}",
            start_task='write_logs_for_records_already_processed',
            end_task='foreach_query_all_records_list_where_requestid_isalready_processed_end'
        )

        write_logs_for_records_already_processed = rail.WriteLogOperator(
            task_id='write_logs_for_records_already_processed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "request_id": "{{ result('foreach_query_all_records_list_where_requestid_isalready_processed').get('Internal Notes') }}",
                "action": "NA",
                "status": "Ignored",
                "invoice_number": "{{ result('foreach_query_all_records_list_where_requestid_isalready_processed').get('Invoice #') }}",
                "child_job_id": "{{ dag_run_ecid() }} | Already Present"
            }
        )

        foreach_query_all_records_list_where_requestid_isalready_processed_end = rail.EmptyOperator(
            task_id='foreach_query_all_records_list_where_requestid_isalready_processed_end',
        )
        log_filename_11 = rail.PythonOperator(
            task_id='log_filename_11',
            python_callable=lambda dag_run:  f"Invoice_{get_dagrun_ecid(dag_run)}_invoicereportdata.csv"
        )

        query_input_data = rail.QueryCollectionOperator(
            task_id='query_input_data',
            query="""select * from  invoicedata order by Request_ID""",
        )
        get_queried_inputdata = rail.PythonOperator(
            task_id='get_queried_inputdata',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_input_data")),
        )

        trigger_dag_run_create_invoices_in_replicon_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_create_invoices_in_replicon_child_dag',
            retries=0,
            items=lambda: [i for i in rail.result('get_queried_inputdata')if i.get(
                'Request_ID') not in rail.result('already_processed_requestid')],
            trigger_dag_id=f'{config.company_key}_invoice_sync_create_invoices_in_replicon_based_on_sftp_file_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout),
            conf={
                'Vendor_Invoice_Number': "{{item.Vendor_Invoice_Number}}",
                'Invoice_Date': "{{item.Invoice_Date}}",
                'Vendor_Name': "{{item.Vendor_Name}}",
                'PO_Number': "{{item.PO_Number}}",
                'Request_Custom_15_Posting_Date': "{{item.Request_Custom_15_Posting_Date}}",
                'Request_ID': "{{item.Request_ID}}",
                'Line_Item_Custom_10_Client': "{{item.Line_Item_Custom_10_Client}}",
                'Line_Item_Custom_11_Project': "{{item.Line_Item_Custom_11_Project}}",
                'Line_Item_Description': "{{item.Line_Item_Description}}",
                'Line_Item_Custom_07_Profit_Center': "{{item.Line_Item_Custom_07_Profit_Center}}",
                'Line_Item_Expense_Type_Name': "{{item.Line_Item_Expense_Type_Name}}",
                'Line_Item_Quantity': "{{item.Line_Item_Quantity}}",
                'Line_Item_Unit_Price': "{{item.Line_Item_Unit_Price}}",
                'Invoice_Amount': "{{item.Invoice_Amount}}",
                "reportdata": "{{ result('parse_csv_10_10_10') }}",
                "logid": "{{ result('create_log') }}"
            }
        )

        wait_for_completion_trigger_dag_run_create_invoices_in_replicon_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_create_invoices_in_replicon_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout),
            dag_runs='{{ result("trigger_dag_run_create_invoices_in_replicon_child_dag") }}',
        )

        trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27',
            retries=0,
            trigger_dag_id=f'{config.company_key}_get_the_total_invoiced_based_on_projects_and_update_the_custom_field_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout),
            conf={
                "input": "{{ dag_run_ecid() }}"
            }
        )

        wait_for_completion_trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27',
            execution_timeout=timedelta(days=config.execution_timeout),
            dag_runs='{{ result("trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27") }}'
        )

        log_logfilename_31 = rail.PythonOperator(
            task_id='log_logfilename_31',
            python_callable=lambda:  f"{rail.get_company_key()}_createinvoice_{datetime.now().strftime('%Y%m%dT%H%M%S')}.csv"
        )

        invoice_import_logs_entries = rail.PythonOperator(
            task_id='invoice_import_logs_entries',
            python_callable=python_callable.get_logs_data
        )
        if_invoice_import_logs_entries_entries_greater_than_0 = rail.IfOperator(
            task_id='if_invoice_import_logs_entries_entries_greater_than_0',
            test='''{{ result('invoice_import_logs_entries') | length > 0 }}''',
            yes_task="create_csv_lines_5_5_5",
            no_task="finish",
        )

        create_csv_lines_5_5_5 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_5_5_5',
            source=lambda: rail.result('invoice_import_logs_entries'),
            header=['Request ID',
                    'Invoice No',
                    'Status',
                    'Details',
                    'Job Details'],
            row=lambda item: [
                item['request_id'],
                item['invoice_number'],
                item['status'],
                item['child_job_id'].split('|')[1],
                item['child_job_id'].split('|')[0],
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_5_5_5')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        log_checkiflogfilehaserrors_4 = rail.PythonOperator(
            task_id='log_checkiflogfilehaserrors_4',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('invoice_import_logs_entries'), "status", "Error", "status")
        )
        log_subject_line = rail.PythonOperator(
            task_id='log_subject_line',
            python_callable=lambda:  "completed with errors" if rail.result(
                'log_checkiflogfilehaserrors_4') else "has been completed"
        )

        log_body = rail.PythonOperator(
            task_id='log_body',
            python_callable=lambda:  f'''<p><em><strong><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></strong></em></p>
                <p>Hello ,</p>
                <p>The Process to create invoices in Replicon is completed with errors based on Concur input filename - {rail.result('new_file_sensor').split('/')[-1]}. Please find the below link to the logs for reference. <br /> <br /><a href="{rail.result('generate_download_link')}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
                <br />
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''' if rail.result(
                'log_checkiflogfilehaserrors_4') else f'''<p><em><strong><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></strong></em></p>
                <p>Hello ,</p>
                <p>The Process to create invoices in Replicon has been processed based on&nbsp;concur input filename - {rail.result('new_file_sensor').split('/')[-1]}. Please find the below link to the logs for reference. <br /> <br /><a href="{rail.result('generate_download_link')}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''
        )

        send_mail_with_link = rail.EmailOperator(
            task_id='send_mail_with_link',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | Concur to Replicon invoice import {{ result('log_subject_line') }} - {{ current_time('%m-%d-%Y') }}''',
            html_content="{{ result('log_body') }}",
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject='''{{ get_company_key() }}| Concur to Replicon invoice import - Failed to process file''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br/> <br/>Hello Team, <br/> <br/> This is bring immediate attention that the invoice creation has encountered a failure, specifically due to issues processing the input file.</p>
                        <ul> 
                        <li>Input File path: {{ result('new_file_sensor')}}</li>
                        <li>Input file name: {{ result('new_file_sensor') | file_name }}</li>
                        </ul><p> <br/>For any queries, please contact our support team at https://support.deltek.com <br/> <br/>Regards, <br/>Deltek Inc. </p>''',
            params=None,
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )
        new_file_sensor >> download_input_file
        download_input_file >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label(
            "Yes") >> archive_input_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        archive_input_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> load_csv_create_list_from_csv_4_4_4
        load_csv_create_list_from_csv_4_4_4 >> compose_csv_with_headers >> create_collection_create_list_from_csv_4_4_4 >> query_list_uniq_requestid_5_uniq_requestid_5_uniq_requestid_5 >> get_requestid_list >> get_report1_details
        get_report1_details >> invoice_report1_generation >> parse_csv_10_10_10 >> already_processed_requestid >> already_processed_records >> create_log >> if_already_processed_records
        if_already_processed_records >> rail.Label(
            'Yes') >> foreach_query_all_records_list_where_requestid_isalready_processed >> write_logs_for_records_already_processed >> foreach_query_all_records_list_where_requestid_isalready_processed_end >> log_filename_11
        if_already_processed_records >> rail.Label(
            'No') >> log_filename_11
        foreach_query_all_records_list_where_requestid_isalready_processed >> foreach_query_all_records_list_where_requestid_isalready_processed_end
        log_filename_11 >> query_input_data >> get_queried_inputdata >> trigger_dag_run_create_invoices_in_replicon_child_dag >> wait_for_completion_trigger_dag_run_create_invoices_in_replicon_child_dag >> trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27
        trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27 >> wait_for_completion_trigger_dag_run_isuzu_invoice_sync_get_the_total_invoiced_data_based_on_projects_and_update_the_required_custom_fieldasync_27 >> log_logfilename_31 >> invoice_import_logs_entries >> if_invoice_import_logs_entries_entries_greater_than_0
        if_invoice_import_logs_entries_entries_greater_than_0 >> rail.Label(
            'Yes') >> create_csv_lines_5_5_5 >> generate_download_link >> log_checkiflogfilehaserrors_4 >> log_subject_line >> log_body >> send_mail_with_link >> finish
        if_invoice_import_logs_entries_entries_greater_than_0 >> rail.Label(
            'No') >> finish
        finish >> log_to_sumo >> send_task_failure_email >> final_status

    return dag


rail.for_each_instance(create_dag)
