from datetime import timedelta
from pendulum import datetime
from strayeruniversity.fmla_extract.utils import python_callable
import rail

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"strayer_university_fmla_extract_master_dag_{config.instance}",
        description=f"Strayer University FMLA Extract Master Dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=python_callable.logging_details,
            op_args=[config]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
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

        report_data_collection = rail.CreateCollectionOperator(
            task_id='report_data_collection',
            source='{{ result("load_csv") }}'
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id='write_csv_file',
            source='{{ result("report_data_collection") }}',
            header=None,
            row=lambda item: [
                item['Time_Off_Booking_URI'],
                item['Employee_ID'],
                item['Time_Off_Date_Modified'],
                item['FMLA'] if item['FMLA'] else null,
                item['Time_Off_Hrs'],
                null,
                null
            ],
            lineterminator='\n'
        )

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.export_file_path]
        )

        has_files = rail.IfOperator(
            task_id='has_files',
            test=lambda: bool(rail.result('list_sftp_files').get(
                config.export_file_path)),
            yes_task='archive_files',
            no_task='upload_csv_to_sftp'
        )

        archive_files = rail.TriggerDagRunForEachItemOperator(
            task_id='archive_files',
            retries=0,
            items=lambda: list(
                map(lambda x: {'sftp_file_name': x['name'], 'modified_time': x['modify']},
                    filter(lambda data: data["name"].endswith(config.filename + ".csv"),
                        rail.load_all_records(rail.result('list_sftp_files')[config.export_file_path])))),
            trigger_dag_id=f'strayeruniversity_move_file_archive_child_{config.instance}',
            execution_timeout=timedelta(days=14)
        )

        wait_for_archive_files = rail.WaitForDagRunsSensor(
            task_id='wait_for_archive_files',
            dag_runs='{{ result("archive_files") }}',
            execution_timeout=timedelta(days=14),
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content='{{ result("write_csv_file") }}',
            remote_filepath=config.export_file_path + "/" + config.filename + ".csv"
        )

        upload_csv_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_secondary_sftp',
            content='{{ result("write_csv_file") }}',
            sftp_conn_id=config.secondary_sftp_conn_id,
            remote_filepath=config.back_up_export_file_path + "/" + config.filename
                + '_{{ dag_run_ecid() | replace(":", "-") }}.csv'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_logging_details >> get_report_details >> run_my_report_entry
        run_my_report_exit >> report_has_data

        report_has_data >> rail.Label("Yes") >> load_csv >> report_data_collection >> write_csv_file >> list_sftp_files >> has_files
        has_files >> rail.Label("Yes") >> archive_files >> wait_for_archive_files >> \
            upload_csv_to_sftp >> upload_csv_to_secondary_sftp
        has_files >> rail.Label("No") >> upload_csv_to_sftp >> upload_csv_to_secondary_sftp
        report_has_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
