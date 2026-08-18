# pylint: disable=too-many-statements
from datetime import datetime as dt
import rail
from dxctechnology.lcsc_les_us_termination_balance_v3 import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_lcsc_les_terminationbalance_child_v3_{config.instance}',
        description=f'DXC_LCSC_terminationbalance_child - V3.0 {config.instance}',
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
        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
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
        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{result('process_start_time')}} - Process started",
            properties={
                "log": "{{result('process_start_time')}} - Process started"
            }
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name='{{dag_run.conf.user_report_name}}',
        )
        load_user_report = rail.run_report(
            group_id='load_user_report',
            report_params=request_payload.get_run_us_user_report_payload
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{"No Data" not in result("load_user_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task='report_has_expected_columns',
            no_task='send_email_for_no_users_data'
        )
        send_email_for_no_users_data = rail.EmailOperator(
            task_id='send_email_for_no_users_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for LCSC Termination file is skipped for {{ dag_run.conf.location }} location  on - {{ current_time_in_specified_tz() }}',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="email_no_users_file_format.html",
        )
        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )
        user_report_expected_report_columns = "User Name,Location (Current),UserUri,User End Date"
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('load_user_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % user_report_expected_report_columns,
            no_task='fail_invalid_user_report_colums',
            yes_task='users_report_payload_to_csv',
        )
        fail_invalid_user_report_colums = rail.FailOperator(
            task_id="fail_invalid_user_report_colums",
            message="Base report column does not match"
        )

        users_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="users_report_payload_to_csv",
            document='{{result("load_user_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        formated_users_data_to_csv = rail.WriteCSVFileOperator(
            task_id="formated_users_data_to_csv",
            source="{{ result('users_report_payload_to_csv') }}",
            header=["username", "location",
                    "useruri", "userenddate"],
            row=request_payload.get_formated_user_row
        )
        users_report_data_collection = rail.CreateCollectionOperator(
            task_id="users_report_data_collection",
            name='getalluserdata',
            source='{{result("formated_users_data_to_csv")}}'
        )

        query_disabled_users_data = rail.QueryCollectionOperator(
            task_id="query_disabled_users_data",
            # pylint: disable=line-too-long
            query=f"SELECT * FROM getalluserdata WHERE DATE(userenddate) > DATE('{request_payload.get_start_date_begin_of_week()}') AND  DATE(userenddate) < DATE('{request_payload.get_end_date_begin_of_week()}')"
        )

        has_any_users_data = rail.IfOperator(
            task_id='has_any_users_data',
            test='{{ result("query_disabled_users_data", "length") > 0 }}',
            yes_task="final_users_data_to_csv",
            no_task="finish_export_no_user"
        )

        finish_export_no_user = rail.EmptyOperator(
            task_id="finish_export_no_user",
        )

        final_users_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_users_data_to_csv",
            source="{{ result('query_disabled_users_data') }}",
            header=["username", "location", "useruri", "id"],
            row=request_payload.get_final_users_data_row
        )

        users_final_data_collection = rail.CreateCollectionOperator(
            task_id="users_final_data_collection",
            source='{{result("final_users_data_to_csv")}}'
        )

        query_all_users_data = rail.QueryCollectionOperator(
            task_id="query_all_users_data",
            query="SELECT * FROM users_final_data_collection",
        )

        has_any_users_final_data = rail.IfOperator(
            task_id='has_any_users_final_data',
            test='{{ result("query_all_users_data", "length") > 0 }}',
            yes_task="get_termination_balance_report_details",
        )

        get_termination_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_termination_balance_report_details',
            report_name='{{dag_run.conf.termination_balance_report_name}}',
        )

        load_termination_balance_report = rail.run_report(
            group_id='load_termination_balance_report',
            report_params=request_payload.get_run_termination_balance_report_payload
        )

        termination_balance_report_has_data = rail.IfOperator(
            task_id="termination_balance_report_has_data",
            test='{{"No Data" not in \
                result("load_termination_balance_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task='termination_balance_report_has_expected_columns',
            no_task='send_email_for_no_termination_balance_data'
        )

        send_email_for_no_termination_balance_data = rail.EmailOperator(
            task_id='send_email_for_no_termination_balance_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for LCSC Termination file is skipped for Canada location  on - {{ current_time_in_specified_tz() }}',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="email_no_termination_balance_file_format.html",
        )
        # pylint: disable=line-too-long
        termination_balance_expected_report_columns = "User Name,Time Off Type,Time Off Balance,TimeOffTypeUri,Employee ID,User End Date,Actual Employee ID"
        termination_balance_report_has_expected_columns = rail.IfOperator(
            task_id="termination_balance_report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('load_termination_balance_report.get_report_result').reportGenerationResults[0].payload |\
                 starts_with('%s') }}" % termination_balance_expected_report_columns,
            no_task='fail_invalid_termination_report_colums',
            yes_task='termination_balance_report_payload_to_csv',
        )
        fail_invalid_termination_report_colums = rail.FailOperator(
            task_id="fail_invalid_termination_report_colums",
            message="Base report column does not match"
        )
        termination_balance_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="termination_balance_report_payload_to_csv",
            document='{{result("load_termination_balance_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        termination_balance_report_data_collection = rail.CreateCollectionOperator(
            task_id="termination_balance_report_data_collection",
            name='terminationbalance',
            source='{{result("termination_balance_report_payload_to_csv")}}'
        )

        query_invalid_termination_balance_data = rail.QueryCollectionOperator(
            task_id="query_invalid_termination_balance_data",
            query="SELECT * FROM terminationbalance WHERE Employee_ID IS NULL OR Employee_ID = ''",
        )

        has_invalid_data = rail.IfOperator(
            task_id='has_invalid_data',
            test='{{ result("query_invalid_termination_balance_data", "length") > 0 }}',
            yes_task="logging_no_of_invalid_records",
        )
        logging_no_of_invalid_records = rail.WriteLogOperator(
            task_id="logging_no_of_invalid_records",
            log="{{ result('create_log') }}",
            message=lambda: "The number of users skipped - {{result('query_invalid_termination_balance_data','length')}}",
            properties={
                "log": "The number of users skipped -{{result('query_invalid_termination_balance_data','length')}}"
            }
        )
        query_valid_termination_balance_data = rail.QueryCollectionOperator(
            task_id="query_valid_termination_balance_data",
            query="SELECT * FROM terminationbalance WHERE NULLIF(Employee_ID, '') IS NOT NULL",
        )
        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("query_valid_termination_balance_data", "length") > 0 }}',
            yes_task="final_termination_balance_data_to_csv",
            no_task="finish_export_no_valid_data"
        )
        finish_export_no_valid_data = rail.EmptyOperator(
            task_id="finish_export_no_valid_data",
        )
        no_of_records_size_including_header_footer=rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result('query_valid_termination_balance_data','length')) + 2
        )
        final_termination_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_termination_balance_data_to_csv",
            source="{{ result('query_valid_termination_balance_data') }}",
            header=["RECTY","CLIID","INTCA","ORDNO","IOPER","INFTY","paycodecode","BEGDA",
            "ENDDA","OBJPS","SPRPS","SEQNR","EXTRA","paycodecode2","STDAZ","BEGUZ","ENDUZ","BETRG","WAERS",
            "PayCodeHours","ZEINH"],
            row=request_payload.get_termination_balance_us_data_row
        )
        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='usles_uscsc_export_data.txt',
            dataset="{{ result('final_termination_balance_data_to_csv') }}",
        )
        is_encryption_required = rail.IfOperator(
            task_id='is_encryption_required',
            test='{{ dag_run.conf.encyrpt_file | is_truthy}}',
            yes_task=["pgp_encyrpt_item_file","can_upload_to_tertiary_sftp"],
            no_task="upload_export_data_to_sftp"
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
            '{{ dag_run.conf.file_name}}.pgp'
        )
        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content='{{result("create_document")}}',
            remote_filepath=config.output_filepath +
            '{{ dag_run.conf.file_name}}'
        )

        can_upload_to_tertiary_sftp = rail.IfOperator(
            task_id = 'can_upload_to_tertiary_sftp',
            test= config.can_upload_to_tertiary_sftp,
            yes_task='pgp_encyrpt_for_tertiary_sftp',
            no_task='finish'
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        # this encryption is for uploading this encrypted file to Replicon SFTP(Tertiary SFTP)
        pgp_encyrpt_for_tertiary_sftp = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_for_tertiary_sftp",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.tertiary_pgp_conn_id
        )

        upload_encrypted_file_tertiary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_file_tertiary_sftp",
            sftp_conn_id=config.tertiary_sftp_conn_id,
            content="{{ result('pgp_encyrpt_for_tertiary_sftp') }}",
            remote_filepath=config.tertiary_encrypted_filepath +
            "{{ dag_run.conf.file_name}}.pgp"
        )

        fail_tertiary_sftp_upload_error = rail.FailOperator(
            task_id='fail_tertiary_sftp_upload_error',
            trigger_rule='one_failed',
            message=config.error_template
        )

        upload_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content='{{result("create_document")}}',
            remote_filepath=config.secondary_output_filepath +
            '{{ dag_run.conf.file_name}}'
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
            subject='{{ get_company_key() }} | Termination balance data export automation - SFTP failure for {{ dag_run.conf.location }} location  on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="email_for_sftp_failure.html",
            files=[
                ('{{ dag_run.conf.file_name}}', '{{result("final_termination_balance_data_to_csv")}}')]
        )
        logging_no_of_valid_records = rail.WriteLogOperator(
            task_id="logging_no_of_valid_records",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_valid_termination_balance_data','length')}}",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_valid_termination_balance_data','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
            '{{ dag_run.conf.file_name}}' + " created",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
                    '{{ dag_run.conf.file_name}}' + " created"
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

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for LCSC Termination file completed  on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath

            },
            html_content="email_for_export_success.html"
        )
        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath +
            "log_"+'{{ dag_run.conf.file_name}}'
        )

        can_upload_logs_to_tertiary_sftp = rail.IfOperator(
            task_id = 'can_upload_logs_to_tertiary_sftp',
            test= config.can_upload_to_tertiary_sftp and '{{ dag_run.conf.encyrpt_file | is_truthy}}',
            yes_task='upload_log_data_to_tertiary_sftp',
            no_task='finish_log'
        )

        finish_log = rail.EmptyOperator(
            task_id = "finish_log"
        )

        upload_log_data_to_tertiary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_tertiary_sftp",
            sftp_conn_id=config.tertiary_sftp_conn_id,
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.tertiary_log_filepath + "log_" +
            "{{ dag_run.conf.file_name }}" + ".txt"
        )

        fail_tertiary_sftp_log_upload_error = rail.FailOperator(
            task_id='fail_tertiary_sftp_log_upload_error',
            trigger_rule='one_failed',
            message=config.error_template
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
            subject='{{ get_company_key() }} | Replicon payroll export for LCSC Termination file - SFTP failure for {{ dag_run.conf.location }} location {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="email_for_log_upload_failure.html",
            files=[
                 ("log_"+'{{ dag_run.conf.file_name| file_base }}.txt', '{{result("log_file_data_to_csv")}}')]
        )
        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="termination file export has failed"
        )
        fail_export_before_log = rail.FailOperator(
            task_id="fail_export_before_log",
            message="termination file export has failed"
        )
        # pylint: disable=line-too-long
        create_log>>get_all_enabled_divisions>>process_start_time >>process_start_time_ymd_format >> process_start_time_hms_format>> \
         logging_job_start_time >> get_user_report_details >> load_user_report >> has_data >> rail.Label("Yes"
                                                                                            ) >> report_has_expected_columns
        has_data >> rail.Label("No") >> send_email_for_no_users_data
        report_has_expected_columns >> rail.Label(
            "Yes") >> users_report_payload_to_csv >> formated_users_data_to_csv >> users_report_data_collection
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_user_report_colums
        users_report_data_collection >> query_disabled_users_data >> has_any_users_data >> rail.Label("Yes"
                                                                                                      ) >> final_users_data_to_csv
        has_any_users_data >> rail.Label("No") >> finish_export_no_user
        final_users_data_to_csv >> users_final_data_collection >> query_all_users_data >> has_any_users_final_data
        has_any_users_final_data >> rail.Label(
            "Yes") >> get_termination_balance_report_details >> load_termination_balance_report
        load_termination_balance_report >> termination_balance_report_has_data >> rail.Label("Yes"
                                                                                             ) >> termination_balance_report_has_expected_columns
        termination_balance_report_has_data >> rail.Label(
            "No") >> send_email_for_no_termination_balance_data
        termination_balance_report_has_expected_columns >> rail.Label(
            "Yes") >> termination_balance_report_payload_to_csv >> termination_balance_report_data_collection
        termination_balance_report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_termination_report_colums
        termination_balance_report_data_collection >> query_invalid_termination_balance_data >> has_invalid_data
        has_invalid_data >> rail.Label("Yes") >> logging_no_of_invalid_records
        termination_balance_report_data_collection >> query_valid_termination_balance_data
        query_valid_termination_balance_data >> has_valid_data >> rail.Label("Yes"
                ) >> final_termination_balance_data_to_csv >>no_of_records_size_including_header_footer>> create_document

        create_document >>is_encryption_required>>rail.Label("Yes") >> [pgp_encyrpt_item_file, can_upload_to_tertiary_sftp]
        can_upload_to_tertiary_sftp >> rail.Label('Yes') >> pgp_encyrpt_for_tertiary_sftp >> upload_encrypted_file_tertiary_sftp
        can_upload_to_tertiary_sftp >> rail.Label('No') >> finish
        upload_encrypted_file_tertiary_sftp >> rail.Label("on_error") >>  fail_tertiary_sftp_upload_error

        pgp_encyrpt_item_file >>upload_encrypted_export_data_to_sftp
        is_encryption_required>>rail.Label("No") >>upload_export_data_to_sftp
        has_valid_data >> rail.Label("No") >> finish_export_no_valid_data
        upload_encrypted_export_data_to_sftp >> rail.Label(
            "on_success") >>upload_export_data_to_secondary_sftp>> logging_no_of_valid_records
        upload_export_data_to_sftp >> rail.Label(
            "on_success") >> upload_export_data_to_secondary_sftp>>logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_error
        upload_export_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_data_to_sftp_failed >> rail.Label("Yes"
                                                                                                                           ) >> send_email_for_sftp_failure
        is_upload_data_to_sftp_failed >> rail.Label("No") >> fail_export
        logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv >> send_email_for_export_copmpletion

        send_email_for_export_copmpletion >> [upload_log_data_to_sftp, can_upload_logs_to_tertiary_sftp]
        can_upload_logs_to_tertiary_sftp >> rail.Label('Yes') >> upload_log_data_to_tertiary_sftp
        can_upload_logs_to_tertiary_sftp >> rail.Label('No') >> finish_log
        upload_log_data_to_tertiary_sftp >> rail.Label('on_error') >> fail_tertiary_sftp_log_upload_error
        upload_log_data_to_tertiary_sftp >> rail.Label('on_success') >> finish_log

        upload_log_data_to_sftp >> rail.Label("on_success") >> finish_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_log_to_sftp_failed >> rail.Label("Yes"
                                                                                                                       ) >> send_email_for_log_upload_failure
        is_upload_log_to_sftp_failed >> rail.Label(
            "No") >> fail_export_before_log
    return dag


rail.for_each_instance(create_child_dag)
