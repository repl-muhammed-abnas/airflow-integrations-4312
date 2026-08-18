from pendulum import datetime as dt
import rail
from geografia.journal_sync_replicon_to_xero.utils import python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Geografia Journal Sync from Replicon to Xero Master Dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs_master,
        start_date=dt(2025, 1, 1, tz=config.time_zone)
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,           
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details')['uri'] }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )
 
        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='parse_csv',
 
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}',
            headers=[
                'ProjectName',
                'ProjectCode',
                'ActualCost'
            ]
        )
        if_csv_has_data = rail.IfOperator(
            task_id="if_csv_has_data",
            test=lambda: bool(rail.result('parse_csv')),
            yes_task='loop_through_csv'
        )

        loop_through_csv = rail.PythonOperator(
            task_id='loop_through_csv',
            python_callable=python_callable.for_each_item_from_report,
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source='{{ result("loop_through_csv") }}',
            header=[
                'ProjectName', 
                'ProjectCode', 
                'ActualCost'
            ],
            row=[
                "{{ item['ProjectName'] }}",
                "{{ item['ProjectCode'] }}",
                "{{ item['ActualCost'] }}"
            ]
        )

        get_journal_payload = rail.PythonOperator(
            task_id='get_journal_payload',
            python_callable=python_callable.format_payload
        )

        insert_journal_entries = rail.XeroAPIOperator( 
            task_id='insert_journal_entries',
            endpoint='/api.xro/2.0/ManualJournals',
            request_method='POST',
            request_body=lambda: rail.result('get_journal_payload'),
            xero_conn_id=config.xero_conn_id
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("compose_csv") }}',
            output_file_name='Projectcostdata_{{dag_run_ecid()}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content='{{ result("compose_csv") }}',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.archive_filepath + "/Projectcostdata_{{dag_run_ecid()}}.csv"
        )

        send_email_on_completion = rail.EmailOperator(
            task_id='send_email_on_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Journal Sync Completed Successfully.',
            html_content="templates/email_for_journal_sync_to_xero_success.html"
        )

        get_report_details >> run_report_group_entry >> run_report_group_exit
        run_report_group_exit >> report_has_data >> rail.Label("Yes") >> parse_csv >> if_csv_has_data
        if_csv_has_data >> rail.Label("Yes") >> loop_through_csv >> compose_csv >> get_journal_payload >> insert_journal_entries >> generate_download_link >> upload_to_sftp
        upload_to_sftp  >> send_email_on_completion
    return dag
rail.for_each_instance(create_dag)
