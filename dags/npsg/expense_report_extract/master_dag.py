from datetime import timedelta
from pendulum import datetime
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'npsg_expense_report_extract_master_{config.instance}',
        description=f'NPSG_expense_report_extract {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2023, 5, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        generate_report = rail.run_report2(
            group_id='generate_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            }
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='{{result("generate_report.get_report_result", "has_data") | is_truthy}}',
            yes_task="if_payload_has_no_columns",
            no_task="stop_job"
        )

        stop_job = rail.EmptyOperator(
            task_id='stop_job'
        )

        if_payload_has_no_columns = rail.IfOperator(
            task_id='if_payload_has_no_columns',
            # pylint: disable=consider-using-f-string,line-too-long
            test="{{result('generate_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s')| is_falsy}}" % config.expected_report_columns,
            yes_task="send_no_data_mail",
            no_task="parse_csv",
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} | Workday Expense Report extract completed successfully | {{ current_time_in_specified_tz("America/New_York") }} ',
            html_content="templates/emails/no_data_mail.html",
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('generate_report.get_report_result').reportGenerationResults[0].payload}}",
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('parse_csv')}}",
            remote_filepath=config.log_filepath + 'NPSG_workday_expense_report' +
            '{{current_time("%d%m%Y%H%M%S")}}' + '.csv'
        )

        def get_expense_sheet():
            records = rail.load_all_records(rail.result('parse_csv'))
            tracking_numbers = [ record.get('Tracking Number') for record in records]
            extracted_data = {"expenselist": list(map(lambda tracking_number: {"trackingnumber": tracking_number,
            "expenseuri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:expense-sheet:{tracking_number}"},tracking_numbers))}
            return extracted_data

        get_unique_expense_sheets = rail.PythonOperator(
            task_id='get_unique_expense_sheets',
            python_callable=get_expense_sheet
        )

        def get_expense_uri():
            results = rail.result('get_unique_expense_sheets')['expenselist']
            expense_uri = []
            expense_uri = [data['expenseuri']
                           for data in results if data['expenseuri'] not in expense_uri]
            return list(set(expense_uri))

        log_get_expenseuri = rail.PythonOperator(
            task_id='log_get_expenseuri',
            python_callable=get_expense_uri
        )


        # not passing config as by default it will go as items
        # processing in batch of 50
        # child will fail if there are any errors noticed after the execution is done
        process_expense_sheet_reimbursement = rail.trigger_parallel_dagrun(
            task_id="process_expense_sheet_reimbursement",
            batch_size=50,
            items=lambda: rail.result('log_get_expenseuri'),
            execution_timeout=timedelta(days=14),
            parallel_count=20,
            trigger_dag_id=f'npsg_expense_report_extract_process_expense_sheet_reimbursement_child_{config.instance}',
        )

        create_csv = rail.WriteCSVFileOperator(
            task_id='create_csv',
            source="{{ result('get_unique_expense_sheets').expenselist | to_json}}",
            header=['Tracking Number',
                    'Marked Reimbursed'],
            row=[
                "{{item.trackingnumber}}",
                "Yes"
            ],
        )
        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            content="{{ result('create_csv') }}",
            remote_filepath=config.export_filepath + '_NPSG_workday_expense_report' +
            '{{current_time("%d%m%Y%H%M%S")}}' + '.csv'
        )
        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} | Workday Expense Report extract completed successfully | {{ current_time_in_specified_tz("America/New_York") }} ',
            html_content="templates/emails/success_mail.html",
        )

        get_report_details >> generate_report >> if_payload_has_data
        if_payload_has_data >> rail.Label(
            'Yes') >> if_payload_has_no_columns
        if_payload_has_data >> rail.Label(
            'No') >> stop_job >> if_payload_has_no_columns
        if_payload_has_no_columns >> rail.Label(
            'Yes') >> send_no_data_mail
        if_payload_has_no_columns >> rail.Label(
            'No') >> parse_csv >> upload_log_to_sftp >> get_unique_expense_sheets
        get_unique_expense_sheets >> log_get_expenseuri
        log_get_expenseuri >> process_expense_sheet_reimbursement\
            >> create_csv >> upload_file_to_sftp >> send_export_complete_email

        return dag


rail.for_each_instance(create_dag)
