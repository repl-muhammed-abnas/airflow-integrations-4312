from datetime import timedelta
from pendulum import datetime, now
from pwcglobal.distance_data_extract.daily_export_v2 import request_payload
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f"Daily Distance Extract for Netherlands",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.europe_timezone),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda: now(
                config.europe_timezone).strftime("%Y%m%d%H%M%S")
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: "Daily_Distance_Extract_" +
            rail.result("process_start_time") + "_NLD.csv"
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            message="{{result('process_start_time')}} - Process started",
            properties={
                "log": "{{result('process_start_time')}} - Process started"
            }
        )

        logging_the_country = rail.WriteLogOperator(
            task_id="logging_the_country",
            message="{{result('process_start_time')}}- INFO admin Exporting data for Territory : Netherlands",
            properties={
                "log": "{{result('process_start_time')}}- INFO admin Exporting data for Territory : Netherlands"
            }
        )

        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            report_name=config.report_name,
        )

        load_report = rail.run_report(
            group_id='load_report',
            report_params=request_payload.get_run_report_payload,
            target='artifact',
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test="{{result('load_report.get_report_result','has_data')}}",
            yes_task='report_has_expected_columns',
            no_task='finish_export'
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            # pylint: disable=line-too-long
            test="{{ (result('load_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | starts_with('%s') }}" % config.column_order,
            no_task='fail_invalid_report_colums',
            yes_task='report_payload_to_csv',
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document="{{(result('load_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}"
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id="report_data_collection",
            name="report_data",
            source='{{result("report_payload_to_csv")}}'
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Base report column does not match"
        )

        final_data = rail.QueryCollectionOperator(
            task_id="final_data",
            query='''SELECT
                    TransactionDate,
                    TimeEntryID,
                    PartyID,
                    ResourceGrade,
                    LegalEntityPartyID,
                    WorkdayID,
                    TimesheetStartDate,
                    TimesheetEndDate,
                    REPLACE(Mileage, ',', '') AS Mileage,
                    ChargeCode,
                    WorkItemType,
                    Comments,
                    REPLACE(Distance, ',', '') AS Distance,
                    REPLACE(DistanceCarotherfuel, ',', '') AS DistanceCarotherfuel,
                    REPLACE(DistancePublictransport, ',', '') AS DistancePublictransport,
                    REPLACE(DistanceCar100_electric, ',', '') AS DistanceCar100_electric,
                    REPLACE(DistanceCarpetrol, ',', '') AS DistanceCarpetrol,
                    REPLACE(DistanceCar_plugin_hybrid, ',', '') AS DistanceCar_plugin_hybrid,
                    REPLACE(DistanceCardiesel, ',', '') AS DistanceCardiesel,
                    REPLACE(Distance_e__Bikeorwalking, ',', '') AS Distance_e__Bikeorwalking,
                    REPLACE(DistanceMotorbikepetrol, ',', '') AS DistanceMotorbikepetrol,
                    REPLACE(DistanceMotorbikeelectric, ',', '') AS DistanceMotorbikeelectric,
                    REPLACE(Distancescooterpetrol, ',', '') AS Distancescooterpetrol,
                    REPLACE(Distancescooterelectric, ',', '') AS Distancescooterelectric,
                    REPLACE(TotalDistance, ',', '') AS TotalDistance
                FROM report_data
                WHERE
                    NULLIF(TransactionDate, '') IS NOT NULL AND
                    NULLIF(TRIM(TotalDistance), '') IS NOT NULL AND
                    ABS(CAST(REPLACE(TRIM(TotalDistance), ',', '') AS DECIMAL(10,2))) > 0.000 '''
        )

        final_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_data_to_csv",
            source="{{ result('final_data') }}",
            header=[
                'TransactionDate', 'TimeEntryID', 'PartyID', 'ResourceGrade', 'LegalEntityPartyID', 'WorkdayID', 'TimesheetStartDate',
                'TimesheetEndDate', 'Mileage', 'ChargeCode', 'WorkItemType', 'Comments', 'Distance',
                'DistanceCarotherfuel', 'DistancePublictransport', 'DistanceCar100%electric',
                'DistanceCarpetrol', 'DistanceCar(plugin)hybrid', 'DistanceCardiesel', 'Distance(e-)Bikeorwalking', 'DistanceMotorbikepetrol',
                'DistanceMotorbikeelectric', 'Distancescooterpetrol', 'Distancescooterelectric', 'TotalDistance'
            ],
            thread_pool_size=config.thread_pool_size_write_csv,
            row=[
                '{{item.TransactionDate}}',
                '{{item.TimeEntryID}}',
                '{{item.PartyID}}',
                '{{item.ResourceGrade}}',
                '{{item.LegalEntityPartyID}}',
                '{{item.WorkdayID}}',
                '{{item.TimesheetStartDate}}',
                '{{item.TimesheetEndDate}}',
                '{{item.Mileage}}',
                '{{item.ChargeCode}}',
                '{{item.WorkItemType}}',
                '{{item.Comments}}',
                '{{item.Distance}}',
                '{{item.DistanceCarotherfuel}}',
                '{{item.DistancePublictransport}}',
                '{{item.DistanceCar100_electric}}',
                '{{item.DistanceCarpetrol}}',
                '{{item.DistanceCar_plugin_hybrid}}',
                '{{item.DistanceCardiesel}}',
                '{{item.Distance_e__Bikeorwalking}}',
                '{{item.DistanceMotorbikepetrol}}',
                '{{item.DistanceMotorbikeelectric}}',
                '{{item.Distancescooterpetrol}}',
                '{{item.Distancescooterelectric}}',
                '{{item.TotalDistance}}'
            ],
            execution_timeout=timedelta(
                hours=config.execution_timeout_write_csv)
        )

        logging_record_count = rail.WriteLogOperator(
            task_id="logging_record_count",
            message="{{result('process_start_time')}} INFO admin No of records exported = {{result('final_data','length')}}",
            properties={
                "log": "{{result('process_start_time')}} INFO admin No of records exported = {{result('final_data', 'length')}}"
            }
        )

        logging_the_file_creation = rail.WriteLogOperator(
            task_id="logging_the_file_creation",
            message="{{result('process_start_time')}} INFO admin Export File_" +
            '{{result("get_file_name")}}' + "  created",
            properties={
                "log": "{{result('process_start_time')}} INFO admin Export File_" + '{{result("get_file_name")}}' + "  created"
            }
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content='{{result("final_data_to_csv")}}',
            remote_filepath=config.output_file_path +
            '{{result("get_file_name")}}'
        )

        logging_the_file_upload = rail.WriteLogOperator(
            task_id="logging_the_file_upload",
            message="{{result('process_start_time')}} INFO admin Export File_{{result('get_file_name')}} uploaded",
            properties={
                "log": "{{result('process_start_time')}} INFO admin Export File_{{result('get_file_name')}} uploaded"
            }
        )

        final_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="final_logs_to_csv",
            source=lambda: rail.result('logging_job_start_time'),
            header=['Log file'],
            row=[
                '{{item.properties.log}}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_to_sftp",
            content="{{result('final_logs_to_csv')}}",
            remote_filepath=config.log_file_path +
            "Log_" + "{{result('get_file_name')}}"
        )

        is_upload_file_to_different_path = rail.IfOperator(
            task_id="is_upload_file_to_different_path",
            test=config.is_upload_file_to_different_path_required,
            yes_task="upload_file_to_different_path",
            no_task="send_export_complete_email"
        )

        upload_file_to_different_path = rail.SFTPUploadFileOperator(
            task_id="upload_file_to_different_path",
            content='{{result("final_data_to_csv")}}',
            remote_filepath=config.alternate_file_path +
            '{{result("get_file_name")}}'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Daily Distance Extract for Netherlands -{{result('process_start_time')}}",
            html_content="export_complete_mail.html",
            params={
                'output_file_path': config.output_file_path,
                'log_file_path': config.log_file_path
            }
        )

        process_start_time >> get_file_name >> logging_job_start_time >> logging_the_country >> get_specific_report_details
        get_specific_report_details >> load_report >> has_data
        has_data >> rail.Label("No") >> finish_export
        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label(
            "Yes") >> report_payload_to_csv >> report_data_collection >> final_data
        final_data >> [logging_record_count,
                       final_data_to_csv] >> logging_the_file_creation
        logging_the_file_creation >> [
            upload_export_data_to_sftp, logging_the_file_upload] >> final_logs_to_csv
        final_logs_to_csv >> upload_log_to_sftp >> is_upload_file_to_different_path

        is_upload_file_to_different_path >> rail.Label(
            "Yes") >> upload_file_to_different_path >> send_export_complete_email

        is_upload_file_to_different_path >> rail.Label(
            "No") >> send_export_complete_email

        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_colums

    return dag


rail.for_each_instance(create_main_dag)
