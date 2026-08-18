from datetime import timedelta, date
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'bccsstechnologyservices_update_invoice_in_replicon_master_{config.instance}',
        description=f'Bccsstechnologyservices_update_invoice_in_replicon {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
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
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        is_file_endswith_csv = rail.IfOperator(
            task_id='is_file_endswith_csv',
            test="{{ result('new_file_sensor') | file_name | ends_with('csv')}}",
            yes_task="download_file",
            no_task="finish",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor')}}"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            headers=["Old Invoice #", "New Invoice Number", "Date of Issue", "Description",
                     "Internal Notes", "PO Number"],
            delimiter=',',
            document="{{ result('download_file') }}",
        )

        def today_date():
            today_date = date.today()
            current_date = today_date.strftime("%m/%d/%Y")
            return current_date

        log_todaydate = rail.PythonOperator(
            task_id='log_todaydate',
            python_callable=today_date
        )

        bccstechnology_update_invoice_lookup_table = rail.CreateLogOperator(
            task_id='bccstechnology_update_invoice_lookup_table'
        )

        if_parse_csv_greater_than = rail.IfOperator(
            task_id='if_parse_csv_greater_than',
            test=lambda: bool(rail.load_all_records(rail.result('parse_csv'))),
            yes_task="process_child",
            no_task="send_no_data_mail",
        )

        process_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child',
            retries=0,
            items="{{result('parse_csv')}}",
            trigger_dag_id=f'bccsstechnologyservices_update_invoice_in_replicon_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "lookuptable": rail.result('bccstechnology_update_invoice_lookup_table'),
                "oldinvoicenumber": item['Old Invoice #'].strip() if item['Old Invoice #'] else None,
                "newinvoicenumber": item['New Invoice Number'].strip() if item['New Invoice Number'] else None,
                "dateofissue": item['Date of Issue'].strip() if item['Date of Issue'] else None,
                "description": item['Description'].replace('/\r\n/', '""').replace(
                    '/\n/', '""') if item['Description'] else None,
                "notesforcustomer": item['Internal Notes'].replace('/\r\n/', '""').replace(
                    '/\n/', '""') if item['Internal Notes'] else None,
                "ponumber": item['PO Number'].strip() if item['PO Number'] else None,
                "job_id": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_child") }}'
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log="{{result('bccstechnology_update_invoice_lookup_table')}}",
            severity='Error'
        )

        create_csv = rail.WriteCSVFileOperator(
            task_id='create_csv',
            source=lambda: rail.result(
                'bccstechnology_update_invoice_lookup_table'),
            header=[
                'Old Invoice Number',
                'New Invoice Number',
                'Status',
                'Reason',
                'Job ID'],
            row=lambda item: [
                item['properties']['old_invoice_number'],
                item['properties']['new_invoice_number'],
                item['properties']['status'],
                item['properties']['reason'].split("|")[0],
                item['properties']['jobid'] + "|" +
                item['properties']['reason'].split("|")[-1],
            ]
        )

        download_file_from_sftp = rail.SFTPDownloadFileOperator(
            task_id='download_file_from_sftp',
            remote_filepath=config.archive_filepath +
            "{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}"
        )

        parse_csv_data = rail.LoadCSVFileOperator(
            task_id='parse_csv_data',
            headers=["toaddress"],
            delimiter=',',
            document="{{ result('download_file_from_sftp') }}",
        )

        def get_address():
            result = rail.read_artifact(rail.result('parse_csv_data'))
            return result

        load_address = rail.PythonOperator(
            task_id='load_address',
            python_callable=get_address
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv')}}",
            output_file_name='invoiceupdate{{current_time("%m_%d_%Y")}}' +
            '.csv',
            expires_in_seconds=7*24*60*60,
        )

        if_get_logged_errors_has_data = rail.IfOperator(
            task_id='if_get_logged_errors_has_data',
            test="{{result('get_logged_errors' ,'length') > 0 }}",
            yes_task='send_import_completed_with_error',
            no_task='send_import_complete_mail'
        )

        send_import_complete_mail = rail.EmailOperator(
            task_id='send_import_complete_mail',
            to=config.tenant_email + ',' + "{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }}: Replicon Invoice Update completed based on - {{result("new_file_sensor") | file_name}}',
            html_content="templates/emails/import_complete_mail.html"
        )

        send_import_completed_with_error = rail.EmailOperator(
            task_id='send_import_completed_with_error',
            to=config.tenant_email + ',' + "{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }}: Replicon Invoice Update completed with error based on - {{result("new_file_sensor") | file_name}}',
            html_content="templates/emails/import_complete_with_errors_mail.html",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor')}}",
            new_filename=config.new_filepath +
            "{{ result('new_file_sensor') | file_name }}"
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email + ',' + "{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='BCCSSTechnologyServices: Replicon Invoice Update completed based on - {{result("new_file_sensor") | file_name}}',
            html_content="templates/emails/no_data_mail.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        new_file_sensor >> is_file_endswith_csv >> rail.Label(
            'Yes') >> download_file >> parse_csv >> log_todaydate >> bccstechnology_update_invoice_lookup_table
        bccstechnology_update_invoice_lookup_table >> if_parse_csv_greater_than >> rail.Label(
            'Yes') >> process_child >> wait_for_process_child >> get_logged_errors >> create_csv
        create_csv >> download_file_from_sftp >> parse_csv_data
        parse_csv_data >> load_address >> generate_download_link >> if_get_logged_errors_has_data >> rail.Label(
            'Yes') >> send_import_completed_with_error >> archive_file >> finish
        if_get_logged_errors_has_data >> rail.Label(
            'No') >> send_import_complete_mail >> archive_file >> finish
        if_parse_csv_greater_than >> rail.Label(
            'No') >> send_no_data_mail >> archive_file >> finish
        is_file_endswith_csv >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_dag)
