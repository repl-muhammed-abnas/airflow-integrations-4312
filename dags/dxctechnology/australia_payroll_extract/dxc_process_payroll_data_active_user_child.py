# pylint: disable=too-many-statements
from datetime import datetime as dt
import rail
from dxctechnology.australia_payroll_extract.utils import request_payload
from dxctechnology.australia_payroll_extract.utils import response_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_{config.instance}',
        description=f'DXC_Australia_PayrollData_Export_Active_User_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_all_timeOffTypes = rail.RepliconServiceOperator(
            task_id="get_all_timeOffTypes",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            response_filter= response_filter.get_time_off_type_uris_for_active_user
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda dag_run: dag_run.conf['file_name']
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        process_start_time_ymd_format = rail.PythonOperator(
            task_id="process_start_time_ymd_format",
            python_callable=lambda:  dt.utcnow().strftime("%Y%m%d")
        )

        process_start_time_hms_format = rail.PythonOperator(
            task_id="process_start_time_hms_format",
            python_callable=lambda:  dt.utcnow().strftime("%H%M%S")
        )

        get_active_user_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_active_user_balance_report_details',
            report_name=config.active_user_report_name
        )

        load_active_user_balance_report = rail.run_report(
            group_id='load_active_user_balance_report',
            report_params=request_payload.get_run_active_user_balance_report_payload
        )

        active_user_balance_report_has_data = rail.IfOperator(
            task_id="active_user_balance_report_has_data",
            test='{{ result("load_active_user_balance_report.get_report_result", "has_data") }}',
            yes_task='active_user_balance_report_has_expected_columns',
            no_task='send_email_for_no_active_user_balance_data'
        )

        send_email_for_no_active_user_balance_data = rail.EmailOperator(
            task_id='send_email_for_no_active_user_balance_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for AUS Active User Balance is skipped for Australia location  on - {{ current_time_in_specified_tz() }}',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/empty_active_user.html",
        )

        # pylint: disable=line-too-long
        active_user_balance_expected_report_columns = "User Name,Time Off Type,Time Off Balance,Employee ID,International Assignee,International assignee start date,IA PERNER ID,Actual Employee ID,User End Date"
        active_user_balance_report_has_expected_columns = rail.IfOperator(
            task_id="active_user_balance_report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('load_active_user_balance_report.get_report_result').reportGenerationResults[0].payload |\
                 starts_with('%s') }}" % active_user_balance_expected_report_columns,
            no_task='fail_invalid_active_user_report_colums',
            yes_task='active_user_balance_report_payload_to_csv',
        )

        fail_invalid_active_user_report_colums = rail.FailOperator(
            task_id="fail_invalid_active_user_report_colums",
            message="Base report column does not match"
        )

        active_user_balance_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="active_user_balance_report_payload_to_csv",
            document='{{result("load_active_user_balance_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        active_user_balance_report_data_collection = rail.CreateCollectionOperator(
            task_id="active_user_balance_report_data_collection",
            name='active_userbalance',
            source='{{result("active_user_balance_report_payload_to_csv")}}'
        )

        query_invalid_active_user_balance_data = rail.QueryCollectionOperator(
            task_id="query_invalid_active_user_balance_data",
            query='''SELECT * FROM active_userbalance WHERE NULLIF(Employee_ID, '') IS NULL OR Employee_ID == '' ''',
        )

        has_invalid_data = rail.IfOperator(
            task_id='has_invalid_data',
            test='{{ result("query_invalid_active_user_balance_data", "length") > 0 }}',
            yes_task="logging_no_of_invalid_records",
        )

        logging_no_of_invalid_records = rail.WriteLogOperator(
            task_id="logging_no_of_invalid_records",
            log="{{ result('create_log') }}",
            message=lambda: "The number of users skipped - {{result('query_invalid_active_user_balance_data','length')}}",
            properties={
                "log": "The number of users skipped -{{result('query_invalid_active_user_balance_data','length')}}"
            }
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id="query_list_in_final_payroll_collection",
            query="SELECT * FROM active_userbalance WHERE NULLIF(Employee_ID, '') IS NOT NULL",
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("query_list_in_final_payroll_collection", "length") > 0 }}',
            yes_task="final_active_user_balance_data_to_csv",
            no_task="finish_export_no_valid_data"
        )

        finish_export_no_valid_data = rail.EmptyOperator(
            task_id="finish_export_no_valid_data",
        )

        no_of_records_size_including_header_footer=rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result('query_list_in_final_payroll_collection','length')) + 2
        )

        final_active_user_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_active_user_balance_data_to_csv",
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY","CLIID","INTCA","ORDNO","IOPER","INFTY","SUBTY","BEGDA",
            "ENDDA","OBJPS","SPRPS","SEQNR","EXTRA","BEGUZ","ENDUZ","KTART","ANZHL","DESTA","DEEND",
            "VTKEN","KVERB", "TDLANGU", "TDSUBLA", "TDTYPE", "QUONR"],
            row=request_payload.get_active_user_balance_us_data_row
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/active_user_export_data.txt',
            dataset="{{ result('final_active_user_balance_data_to_csv') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_export_data_to_sftp",
            content='{{result("pgp_encyrpt_item_file")}}',
            remote_filepath=config.output_filepath +
            '{{ dag_run.conf.file_name}}.SAP.pgp'
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content='{{result("create_document")}}',
            remote_filepath=config.secondary_output_filepath +
            '{{ dag_run.conf.file_name}}.SAP'
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        is_upload_data_to_sftp_failed = rail.IfOperator(
            task_id='is_upload_data_to_sftp_failed',
            test=request_payload.is_upload_data_to_sftp_failed,
            yes_task="send_email_for_sftp_failure",
            no_task="fail_export"
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | AUS-2006 Active User Balance data export automation - SFTP failure for {{ dag_run.conf.location }} location  on - current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ('{{ dag_run.conf.file_name }}.SAP.pgp', '{{result("pgp_encyrpt_item_file")}}')]
        )

        logging_no_of_valid_records = rail.WriteLogOperator(
            task_id="logging_no_of_valid_records",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
            '{{ dag_run.conf.file_name}}' + " created",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
                    '{{ dag_run.conf.file_name}}' + ".txt"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_log') }}",
            message="{{result('process_end_time')}} - Process ended",
            properties={
                "log": "{{result('process_end_time')}} - Process ended"
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        send_email_for_export_completion = rail.EmailOperator(
            task_id='send_email_for_export_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for AUS-2006 completed  on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath

            },
            html_content="templates/email/export_success.html"
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath +
            "log_"+'{{ dag_run.conf.file_name}}' + ".txt"
        )

        is_upload_log_to_sftp_failed = rail.IfOperator(
            task_id='is_upload_log_to_sftp_failed',
            test=request_payload.is_upload_log_to_sftp_failed,
            yes_task="send_email_for_log_upload_failure",
            no_task="fail_export_before_log"
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            to=config.alert_email,
            subject='{{ get_company_key() }} | Replicon payroll export for AUS-2006 - SFTP failure for {{ dag_run.conf.location }} location {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_"+'{{ dag_run.conf.file_name }}', '{{result("log_file_data_to_csv")}}')]
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="active_user file export has failed"
        )

        fail_export_before_log = rail.FailOperator(
            task_id="fail_export_before_log",
            message="active_user file export has failed"
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        # pylint: disable=line-too-long
        create_log >> get_all_timeOffTypes >> get_all_enabled_divisions >> get_file_name >> process_start_time >> process_start_time_ymd_format >> \
            process_start_time_hms_format >> get_active_user_balance_report_details >> load_active_user_balance_report
        load_active_user_balance_report >> active_user_balance_report_has_data >> rail.Label("Yes"
                                                                                             ) >> active_user_balance_report_has_expected_columns
        active_user_balance_report_has_data >> rail.Label(
            "No") >> send_email_for_no_active_user_balance_data
        active_user_balance_report_has_expected_columns >> rail.Label(
            "Yes") >> active_user_balance_report_payload_to_csv >> active_user_balance_report_data_collection
        active_user_balance_report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_active_user_report_colums
        active_user_balance_report_data_collection >> query_invalid_active_user_balance_data >> has_invalid_data
        has_invalid_data >> rail.Label("Yes") >> logging_no_of_invalid_records
        active_user_balance_report_data_collection >> query_list_in_final_payroll_collection
        query_list_in_final_payroll_collection >> has_valid_data >> rail.Label("Yes"
                ) >> final_active_user_balance_data_to_csv >>no_of_records_size_including_header_footer>> create_document\
                    >>pgp_encyrpt_item_file >>upload_export_data_to_sftp >> upload_encrypted_export_data_to_sftp
        has_valid_data >> rail.Label("No") >> finish_export_no_valid_data
        upload_encrypted_export_data_to_sftp >> rail.Label(
            "on_success") >>upload_export_data_to_secondary_sftp>> logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_data_to_sftp_failed >> rail.Label("Yes"
                                                                                                                           ) >> send_email_for_sftp_failure
        is_upload_data_to_sftp_failed >> rail.Label("No") >> fail_export
        logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv >> send_email_for_export_completion >> upload_log_data_to_sftp
        upload_log_data_to_sftp >> rail.Label("on_success") >> finish_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_log_to_sftp_failed >> rail.Label("Yes"
                                                                                                                       ) >> send_email_for_log_upload_failure
        is_upload_log_to_sftp_failed >> rail.Label(
            "No") >> fail_export_before_log

    return dag


rail.for_each_instance(create_child_dag)
