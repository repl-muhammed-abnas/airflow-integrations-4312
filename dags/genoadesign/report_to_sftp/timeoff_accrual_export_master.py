from datetime import timedelta
from pendulum import datetime
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"genoa_design_timeoff_accrual_report_export_master_dag_{config.instance}",
        description=f"Genoa Design Timeoff Accrual Report Export Master Dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timeoff_accrual_report,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_csv',
            no_task= 'finish'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.timeoff_accrual_export_path]
        )

        has_files = rail.IfOperator(
            task_id='has_files',
            test=lambda: bool(rail.result('list_sftp_files').get(
                config.timeoff_accrual_export_path)),
            yes_task='sftp_files',
            no_task='upload_csv_to_sftp'
        )

        sftp_files = rail.CreateCollectionOperator(
            task_id='sftp_files',
            source=lambda: rail.result('list_sftp_files')[
                config.timeoff_accrual_export_path]
        )

        archive_files = rail.TriggerDagRunForEachItemOperator(
            task_id='archive_files',
            retries=0,
            items=lambda: list(
                map(lambda x: {'sftp_file_name': x['name']}, rail.load_all_records(rail.result('sftp_files')))),
            trigger_dag_id=f'genoa_design_move_file_archive_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "export_path": config.timeoff_accrual_export_path,
                "item": item
            }
        )

        wait_for_archive_files = rail.WaitForDagRunsSensor(
            task_id='wait_for_archive_files',
            dag_runs='{{ result("archive_files") }}',
            execution_timeout=timedelta(days=14),
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content='{{ result("load_csv") }}',
            remote_filepath=config.timeoff_accrual_export_path +
                "/Report 2 – Time-off Accrual_{{current_time_in_specified_tz('Canada/Newfoundland', '%Y%m%dT%H%M%S')}}.csv",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_my_report_entry
        run_my_report_exit >> report_has_data

        report_has_data >> rail.Label("Yes") >> load_csv >> list_sftp_files >> has_files
        has_files >> rail.Label("Yes") >> sftp_files >> archive_files >> wait_for_archive_files >> upload_csv_to_sftp
        has_files >> rail.Label("No") >> upload_csv_to_sftp
        report_has_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
