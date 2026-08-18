from datetime import timedelta
from capgemini.shift_schedule_extract_to_TETA.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null=None

def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.export_child_dag_id,
        description=f'Shift Schedule Extract to TETA - Capgemini Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_shift_assignment_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_shift_assignment_report_details',
            end_task='finish_extract',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_shift_assignment_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_shift_assignment_report_details',
            report_name=config.shift_assignment_report
        )

        run_shift_assignment_report = rail.run_report2(
            group_id='run_shift_assignment_report',
            report_params=request_payload.get_shift_assignment_report_batch_payload,
            target='artifact'
        )

        is_shift_assignment_report_failed = rail.IfOperator(
            task_id='is_shift_assignment_report_failed',
            test="{{ (result('run_shift_assignment_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy}}",
            yes_task='fail_shift_assignment_report_generation',
            no_task='shift_assignment_report_has_data'
        )

        fail_shift_assignment_report_generation = rail.FailOperator(
            task_id='fail_shift_assignment_report_generation',
            message="{{ (result('run_shift_assignment_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
        )

        shift_assignment_report_has_data = rail.IfOperator(
            task_id='shift_assignment_report_has_data',
            test="{{result('run_shift_assignment_report.get_report_result','has_data')}}",
            yes_task='is_shift_assignment_report_has_expected_columns',
            no_task='write_shift_blankdata_to_csv'
        )

        is_shift_assignment_report_has_expected_columns = rail.IfOperator(
            task_id='is_shift_assignment_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_shift_assignment_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.shift_assignment_report_columns,
            yes_task='load_shift_assignment_report_data',
            no_task='fail_shift_assignment_has_no_expected_columns',
        )

        fail_shift_assignment_has_no_expected_columns = rail.FailOperator(
            task_id='fail_shift_assignment_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_shift_assignment_report_data = rail.LoadCSVFileOperator(
            task_id='load_shift_assignment_report_data',
            document="{{ (result('run_shift_assignment_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=';'
        )

        create_shift_assignment_collection = rail.CreateCollectionOperator(
            task_id='create_shift_assignment_collection',
            source='{{ result("load_shift_assignment_report_data") }}',
            columns={
                "Local ID": "local_id",
                "Card Number": "card_number",
                "Shift Name": "shift_name",
                "Shift Date": "shift_date",
                "Shift Start Time": "shift_start_time",
                "Shift End Time": "shift_end_time",
                "Number of Hours": "no_of_hours"
            },
            name="shift_assignment_report_data"
        )

        query_valid_local_id_users = rail.QueryCollectionOperator(
            task_id='query_valid_local_id_users',
            query="SELECT * FROM shift_assignment_report_data WHERE NULLIF(local_id, '') IS NOT NULL",
            name='valid_shift_assignment_report_data'
        )

        if_valid_users_data = rail.IfOperator(
            task_id='if_valid_users_data',
            test='{{ result("query_valid_local_id_users", "length") > 0 }}',
            yes_task='generate_calendar_dates',
            no_task='write_shift_blankdata_to_csv'
        )

        generate_calendar_dates = rail.CreateCollectionOperator(
            task_id='generate_calendar_dates',
            source=custom_methods.get_generate_calendar_dates,
            name="calendar_dates"
        )

        get_required_holiday_calendar_uri = rail.RepliconServiceOperator(
            task_id='get_required_holiday_calendar_uri',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            data_handler=custom_methods.get_holiday_uri
        )

        get_public_holidays_in_date_range = rail.RepliconServiceOperator(
            task_id='get_public_holidays_in_date_range',
            endpoint='/services/HolidayCalendarService2.svc/GetHolidaysInDateRange',
            data=request_payload.public_holidays_in_daterange_payload,
            data_handler=custom_methods.get_holidays_list
        )

        query_load_users_shifts = rail.QueryCollectionOperator(
            task_id='query_load_users_shifts',
            query="""SELECT sd.local_id, "0" AS card_number, t.shift_name, cd.date AS shift_date,
                    cd.week_day, cd.week_number, t.shift_start_time, t.shift_end_time, t.no_of_hours
                FROM
                    (SELECT DISTINCT local_id FROM valid_shift_assignment_report_data) sd
                CROSS JOIN
                    calendar_dates cd
                LEFT JOIN
                    valid_shift_assignment_report_data t ON sd.local_id = t.local_id
                    AND cd.date = t.shift_date
                ORDER BY
                    sd.local_id, strftime("%d/%m/%Y", cd.date)""",
            name='users_all_shifts_data'
        )

        add_type_of_shift_column = rail.QueryCollectionOperator(
            task_id='add_type_of_shift_column',
            query="""SELECT uasd.local_id, uasd.card_number, uasd.shift_date,
                COALESCE(uasd.shift_start_time, '00:00:00') AS shift_start_time,
                COALESCE(uasd.shift_end_time, '00:00:00') AS shift_end_time,
                COALESCE(uasd.no_of_hours, '00.00') AS no_of_hours,
                CASE
	                WHEN uasd.shift_date IN ({{ result("get_public_holidays_in_date_range") }}) AND
                        (NULLIF(uasd.shift_name, '') IS NULL OR uasd.shift_name = 'PL_Public Holiday') THEN 'S'
                    WHEN uasd.week_day = 'Sunday'
                        THEN CASE
                            WHEN (uasd.shift_name = 'PL_Sunday' OR NULLIF(uasd.shift_name, '') IS NULL) THEN 'N'
                            WHEN uasd.shift_name = 'PL_Saturday' THEN 'W'
                            WHEN uasd.shift_name = 'PL_Scheduled Day off' THEN 'C'
                            WHEN uasd.shift_name = 'PL_Public Holiday' THEN 'WS'
                            ELSE ""
                        END
                    WHEN uasd.week_day = 'Saturday'
                        THEN CASE
                            WHEN (uasd.shift_name = 'PL_Saturday' OR NULLIF(uasd.shift_name, '') IS NULL) THEN 'W'
                            WHEN uasd.shift_name = 'PL_Sunday' THEN 'WN'
                            WHEN uasd.shift_name = 'PL_Scheduled Day off' THEN 'C'
                            WHEN uasd.shift_name = 'PL_Public Holiday' THEN 'WS'
                            ELSE ""
                        END
                    WHEN uasd.shift_name = 'PL_Saturday' THEN 'W'
                    WHEN uasd.shift_name = 'PL_Sunday' THEN 'WN'
                    WHEN uasd.shift_name = 'PL_Scheduled Day off' THEN 'C'
                    WHEN uasd.shift_name = 'PL_Public Holiday' THEN 'WS'
                    ELSE ""
                END type_of_shift
            FROM users_all_shifts_data uasd ORDER BY uasd.local_id, strftime("%d/%m/%Y", uasd.shift_date)"""
        )

        has_shift_data = rail.IfOperator(
            task_id='has_shift_data',
            test='{{ result("add_type_of_shift_column", "length") > 0 }}',
            yes_task='write_shift_data_to_csv',
            no_task='write_shift_blankdata_to_csv'
        )

        write_shift_blankdata_to_csv = rail.WriteCSVFileOperator(
            task_id='write_shift_blankdata_to_csv',
            source=[],
            header=config.export_columns,
            row=[],
            delimiter=';',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        encrypt_shift_blankexport_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_shift_blankexport_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_shift_blankdata_to_csv') }}"
        )

        upload_shift_blankexport_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_shift_blankexport_to_sftp',
            content='{{ result("encrypt_shift_blankexport_data_csv") }}',
            remote_filepath=config.input_filepath +
            '/{{ dag_run.conf.export_filename }}.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon shift schedule extract to TETA for {{ dag_run.conf.export_month }}'
                + ' - No records to export - {{ current_time_in_specified_tz("'+ config.time_zone +'") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.time_zone
            }
        )

        write_shift_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_shift_data_to_csv',
            source='{{ result("add_type_of_shift_column") }}',
            header=config.export_columns,
            row=custom_methods.get_shift_data_csv_rows,
            delimiter=';',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=10
        )

        upload_shift_export_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_shift_export_to_s3',
            source='{{ result("write_shift_data_to_csv") }}',
            key_name=config.s3_upload_filepath +
            '/{{ dag_run.conf.export_filename }}',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_shift_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_shift_export_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_shift_data_to_csv') }}"
        )

        upload_shift_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_shift_export_to_sftp',
            content='{{ result("encrypt_shift_export_data_csv") }}',
            remote_filepath=config.input_filepath +
            '/{{ dag_run.conf.export_filename }}.pgp'
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon shift schedule extract to TETA for {{ dag_run.conf.export_month }} is completed'
                + ' - {{ current_time_in_specified_tz("'+ config.time_zone +'") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'time_zone': config.time_zone
            }
        )

        finish_extract = rail.EmptyOperator(
            task_id='finish_extract'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish_extract
        can_run_batch_task >> rail.Label('No') >> get_shift_assignment_report_details

        get_shift_assignment_report_details >> run_shift_assignment_report >> is_shift_assignment_report_failed

        is_shift_assignment_report_failed >> rail.Label("Yes") >> fail_shift_assignment_report_generation >> finish_extract
        is_shift_assignment_report_failed >> rail.Label("No") >> shift_assignment_report_has_data

        shift_assignment_report_has_data >> rail.Label("Yes") >> is_shift_assignment_report_has_expected_columns
        shift_assignment_report_has_data >> rail.Label("No") >> write_shift_blankdata_to_csv

        is_shift_assignment_report_has_expected_columns >> rail.Label("Yes") >> load_shift_assignment_report_data \
            >> create_shift_assignment_collection >> query_valid_local_id_users >> if_valid_users_data

        if_valid_users_data >> rail.Label("Yes") >> generate_calendar_dates \
            >> get_required_holiday_calendar_uri >> get_public_holidays_in_date_range \
                >> query_load_users_shifts >> add_type_of_shift_column >> has_shift_data

        if_valid_users_data >> rail.Label("No") >> write_shift_blankdata_to_csv

        has_shift_data >> rail.Label("Yes") >> write_shift_data_to_csv >> upload_shift_export_to_s3 >> encrypt_shift_export_data_csv \
            >> upload_shift_export_to_sftp >> send_valid_export_complete_email >> finish_extract
        has_shift_data >> rail.Label("No") >> write_shift_blankdata_to_csv >> encrypt_shift_blankexport_data_csv \
            >> upload_shift_blankexport_to_sftp >> send_empty_export_email >> finish_extract

        is_shift_assignment_report_has_expected_columns >> rail.Label("No") >> fail_shift_assignment_has_no_expected_columns >> finish_extract

    return dag

rail.for_each_instance(create_child_dag)
