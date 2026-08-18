from datetime import timedelta
from capgemini.france_place_of_work_export_to_sopra_v1.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null=None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.export_child_dag_id,
        description=f'Capgemini France Place of Work Export to SOPRA Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_export_runs,
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
            no_task='get_time_entry_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_time_entry_report_details',
            end_task='dagrun_log_to_sumo',
        )

        get_time_entry_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_time_entry_report_details',
            report_name=config.report_name
        )

        get_time_entry_data = rail.run_report2(
            group_id='get_time_entry_data',
            report_params=request_payload.get_time_entry_report_parameters,
            target='artifact'
        )

        is_time_entry_report_failed = rail.IfOperator(
            task_id='is_time_entry_report_failed',
            test="{{ (result('get_time_entry_data.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_time_entry_report_generation',
            no_task='time_entry_report_has_data'
        )

        fail_time_entry_report_generation = rail.FailOperator(
            task_id='fail_time_entry_report_generation',
            message="{{ (result('get_time_entry_data.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        time_entry_report_has_data = rail.IfOperator(
            task_id='time_entry_report_has_data',
            test="{{result('get_time_entry_data.get_report_result','has_data')}}",
            yes_task='is_time_entry_report_has_expected_columns',
            no_task='create_place_of_work_blank_data_xml'
        )

        is_time_entry_report_has_expected_columns = rail.IfOperator(
            task_id='is_time_entry_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('get_time_entry_data.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='process_time_entry_report_data',
            no_task='fail_time_entry_no_expected_columns',
        )

        fail_time_entry_no_expected_columns = rail.FailOperator(
            task_id='fail_time_entry_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        process_time_entry_report_data = rail.EmptyOperator(
            task_id='process_time_entry_report_data'
        )

        load_time_entry_csv = rail.LoadCSVFileOperator(
            task_id='load_time_entry_csv',
            document="{{ (result('get_time_entry_data.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=';'
        )

        create_place_of_work_data_collection = rail.CreateCollectionOperator(
            task_id='create_place_of_work_data_collection',
            source='{{ result("load_time_entry_csv") }}',
            columns={
                "Employee ID": "employee_id",
                "Entry Date": "entry_date",
                "Place of Work (FRA)": "place_of_work_fra",
                "Hours": "hours"
            },
            name='place_of_work_data'
        )

        get_scheduled_hours_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_scheduled_hours_report_details',
            report_name=config.scheduled_hours_report_name
        )

        get_scheduled_hours_data = rail.run_report2(
            group_id='get_scheduled_hours_data',
            report_params=request_payload.get_scheduled_hours_report_parameters,
            target='artifact'
        )

        is_scheduled_hours_report_failed = rail.IfOperator(
            task_id='is_scheduled_hours_report_failed',
            test="{{ (result('get_scheduled_hours_data.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_scheduled_hours_report_generation',
            no_task='scheduled_hours_report_has_data'
        )

        fail_scheduled_hours_report_generation = rail.FailOperator(
            task_id='fail_scheduled_hours_report_generation',
            message="{{ (result('get_scheduled_hours_data.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        scheduled_hours_report_has_data = rail.IfOperator(
            task_id='scheduled_hours_report_has_data',
            test="{{result('get_scheduled_hours_data.get_report_result','has_data')}}",
            yes_task='is_scheduled_hours_report_has_expected_columns',
            no_task='fail_scheduled_hours_no_data'
        )

        fail_scheduled_hours_no_data = rail.FailOperator(
            task_id='fail_scheduled_hours_no_data',
            message='''Time Entry Report has data but Scheduled hours report has no data to proceed. Please check and action.'''
        )

        is_scheduled_hours_report_has_expected_columns = rail.IfOperator(
            task_id='is_scheduled_hours_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('get_scheduled_hours_data.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_scheduled_hours_columns,
            yes_task='process_scheduled_hours_report_data',
            no_task='fail_scheduled_hours_no_expected_columns',
        )

        fail_scheduled_hours_no_expected_columns = rail.FailOperator(
            task_id='fail_scheduled_hours_no_expected_columns',
            message='''Scheduled hours report column order doesn't match'''
        )

        process_scheduled_hours_report_data = rail.EmptyOperator(
            task_id='process_scheduled_hours_report_data'
        )

        load_scheduled_hours_csv = rail.LoadCSVFileOperator(
            task_id='load_scheduled_hours_csv',
            document="{{ (result('get_scheduled_hours_data.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=';'
        )

        create_scheduled_hours_data_collection = rail.CreateCollectionOperator(
            task_id='create_scheduled_hours_data_collection',
            source='{{ result("load_scheduled_hours_csv") }}',
            columns={
                "Employee ID": "employee_id",
                "Date": "entry_date",
                "Scheduled Work Hours": "scheduled_hours"
            },
            name='scheduled_hours_data'
        )

        query_valid_users = rail.QueryCollectionOperator(
            task_id='query_valid_users',
            query="SELECT * FROM place_of_work_data WHERE NULLIF(employee_id, '') IS NOT NULL",
            name='valid_place_of_work_data'
        )

        query_join_place_of_work_and_scheduled_hours_data = rail.QueryCollectionOperator(
            task_id='query_join_place_of_work_and_scheduled_hours_data',
            query="""SELECT 
                p.employee_id,
                p.entry_date,
                p.place_of_work_fra,
                p.hours,
                t.scheduled_hours
            FROM valid_place_of_work_data p
            LEFT JOIN scheduled_hours_data t ON p.employee_id = t.employee_id AND p.entry_date = t.entry_date
            WHERE t.scheduled_hours IS NOT NULL AND CAST(t.scheduled_hours AS FLOAT) > 0""",
            name='place_of_work_and_scheduled_hours_data'
        )

        # This query follows the same pattern as the original DAG but calculates allowances instead of days
        # Step 1 (DailyAllowances): For each employee/date
        #   - Sums ALL hours (Home Medical + Flex Abroad + Home) for the day
        #   - Assigns place_of_work based on priority: Home Medical > Flex Abroad > Home
        #   - Calculates daily allowance based on total hours: 1.0 if >= scheduled, 0.5 if >= 50% scheduled, else 0.0
        # Step 2 (Main query): Groups by employee and place_of_work
        #   - Sums all daily allowances for each employee/place_of_work combination
        #   - Both Home Medical and Flex Abroad map to 'Home Medical' for IVY pay code
        # Result: Similar to original DAG output but with fractional allowances instead of day counts

        query_place_of_work_count_data = rail.QueryCollectionOperator(
            task_id='query_place_of_work_count_data',
            query="""SELECT
                employee_id,
                place_of_work_fra,
                SUM(daily_allowance) AS bucket
            FROM (
                SELECT
                    employee_id,
                    entry_date,
                    CASE
                        WHEN MAX(CASE WHEN place_of_work_fra = 'Home Medical' THEN 1 ELSE 0 END) = 1 THEN 'Home Medical'
                        WHEN MAX(CASE WHEN place_of_work_fra = 'Flex Abroad' THEN 1 ELSE 0 END) = 1 THEN 'Home Medical'
                        WHEN MAX(CASE WHEN place_of_work_fra = 'Home' THEN 1 ELSE 0 END) = 1 THEN 'Home'
                    END AS place_of_work_fra,
                    CASE
                        WHEN SUM(CAST(hours AS FLOAT)) >= MAX(CAST(scheduled_hours AS FLOAT)) THEN 1.0
                        WHEN SUM(CAST(hours AS FLOAT)) >= MAX(CAST(scheduled_hours AS FLOAT)) * 0.5 THEN 0.5
                        ELSE 0.0
                    END AS daily_allowance
                FROM place_of_work_and_scheduled_hours_data
                GROUP BY employee_id, entry_date
            ) AS DailyAllowances
            WHERE place_of_work_fra IS NOT NULL
            GROUP BY employee_id, place_of_work_fra
            ORDER BY employee_id,
                CASE 
                    WHEN place_of_work_fra = 'Home Medical' THEN 1
                    WHEN place_of_work_fra = 'Home' THEN 2
                END
            """
        )

        is_place_of_work_data_exists = rail.IfOperator(
            task_id='is_place_of_work_data_exists',
            test='{{ result("query_place_of_work_count_data", "length") > 0 }}',
            yes_task='write_place_of_work_data_csv',
            no_task='create_place_of_work_blank_data_xml'
        )

        write_place_of_work_data_csv = rail.WriteCSVFileOperator(
            task_id='write_place_of_work_data_csv',
            source="{{ result('query_place_of_work_count_data') }}",
            header=config.export_headers,
            row=custom_methods.get_place_of_work_csv_data,
            execution_timeout=timedelta(minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.write_csv_thread_pool_size
        )

        create_place_of_work_data_xml = rail.RenderTemplateOperator(
            task_id='create_place_of_work_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_place_of_work.xml',
            dataset='{{ result("write_place_of_work_data_csv") }}'
        )

        upload_place_of_work_extract_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_place_of_work_extract_to_s3',
            source="{{ result('create_place_of_work_data_xml') }}",
            key_name=config.s3_upload_filepath + '/{{ dag_run.conf.export_filename }}',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_place_of_work_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_place_of_work_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_place_of_work_data_xml') }}"
        )

        upload_place_of_work_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_place_of_work_extract_to_sftp",
            content='{{ result("encrypt_place_of_work_extract_data_xml") }}',
            remote_filepath=config.input_filepath + '/{{ dag_run.conf.export_filename }}.pgp'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon place of work data extract to SOPRA for France for {{ dag_run.conf.export_month }} month'
                + ' is completed - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'time_zone': config.time_zone
            }
        )

        create_place_of_work_blank_data_xml = rail.RenderTemplateOperator(
            task_id='create_place_of_work_blank_data_xml',
            target='artifact',
            template_file='xml_schema/sopra_place_of_work.xml',
            dataset=custom_methods.get_place_of_work_csv_no_data,
        )

        encrypt_blank_place_of_work_extract_data_xml = rail.PGPEncryptionOperator(
            task_id='encrypt_blank_place_of_work_extract_data_xml',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('create_place_of_work_blank_data_xml') }}"
        )

        upload_blank_place_of_work_extract_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_blank_place_of_work_extract_to_sftp",
            content='{{ result("create_place_of_work_blank_data_xml") }}', # TO Revert for UAT deployment
            remote_filepath=config.input_filepath + '/{{ dag_run.conf.export_filename }}.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id='send_empty_export_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon place of work data extract to SOPRA for France for {{ dag_run.conf.export_month }} month'
                + ' - No records to export - {{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'time_zone': config.time_zone
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_place_of_work_extract'
        )

        fail_place_of_work_extract = rail.FailOperator(
            task_id='fail_place_of_work_extract',
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> get_time_entry_report_details

        get_time_entry_report_details >> get_time_entry_data >> is_time_entry_report_failed

        is_time_entry_report_failed >> rail.Label("Yes") >> fail_time_entry_report_generation >> dagrun_log_to_sumo
        is_time_entry_report_failed >> rail.Label("No") >> time_entry_report_has_data

        time_entry_report_has_data >> rail.Label("Yes") >> is_time_entry_report_has_expected_columns
        time_entry_report_has_data >> rail.Label("No") >> create_place_of_work_blank_data_xml

        is_time_entry_report_has_expected_columns >> rail.Label("Yes") >> process_time_entry_report_data >> load_time_entry_csv
        is_time_entry_report_has_expected_columns >> rail.Label("No") >> fail_time_entry_no_expected_columns >> dagrun_log_to_sumo

        load_time_entry_csv >> create_place_of_work_data_collection >> query_valid_users >> get_scheduled_hours_report_details

        get_scheduled_hours_report_details >> get_scheduled_hours_data >> is_scheduled_hours_report_failed
        
        is_scheduled_hours_report_failed >> rail.Label("Yes") >> fail_scheduled_hours_report_generation >> dagrun_log_to_sumo
        is_scheduled_hours_report_failed >> rail.Label("No") >> scheduled_hours_report_has_data
        
        scheduled_hours_report_has_data >> rail.Label("Yes") >> is_scheduled_hours_report_has_expected_columns
        scheduled_hours_report_has_data >> rail.Label("No") >> fail_scheduled_hours_no_data >> dagrun_log_to_sumo
        
        is_scheduled_hours_report_has_expected_columns >> rail.Label("Yes") >> process_scheduled_hours_report_data >> load_scheduled_hours_csv
        is_scheduled_hours_report_has_expected_columns >> rail.Label("No") >> fail_scheduled_hours_no_expected_columns >> dagrun_log_to_sumo
        
        load_scheduled_hours_csv >> create_scheduled_hours_data_collection >> query_join_place_of_work_and_scheduled_hours_data
        
        query_join_place_of_work_and_scheduled_hours_data >> query_place_of_work_count_data \
            >> is_place_of_work_data_exists
        is_place_of_work_data_exists >> rail.Label("Yes") >> write_place_of_work_data_csv >> create_place_of_work_data_xml
        is_place_of_work_data_exists >> rail.Label("No") >> create_place_of_work_blank_data_xml

        create_place_of_work_data_xml >> upload_place_of_work_extract_to_s3 >> encrypt_place_of_work_extract_data_xml \
            >> upload_place_of_work_extract_to_sftp >> send_export_complete_email >> dagrun_log_to_sumo
        create_place_of_work_blank_data_xml >> encrypt_blank_place_of_work_extract_data_xml \
            >> upload_blank_place_of_work_extract_to_sftp >> send_empty_export_email >> dagrun_log_to_sumo

        is_time_entry_report_has_expected_columns >> rail.Label("No") >> fail_time_entry_no_expected_columns >> dagrun_log_to_sumo

        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_place_of_work_extract

    return dag

rail.for_each_instance(create_child_dag)
