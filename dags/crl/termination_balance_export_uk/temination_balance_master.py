from datetime import datetime, timedelta
import pendulum
import rail
from crl.termination_balance_export_uk.utils import request_payload
from crl.termination_balance_export_uk.utils import python_callable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"CRL UK - termination_balance_Master - {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_conf"
        )

        run_dag_on_payrollcalendar = rail.IfOperator(
            task_id="run_dag_on_payrollcalendar",
            test= lambda dag_run: not bool(dag_run.conf),
            yes_task='can_process_run',
            no_task='process_start_time'
        )
        
        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            current_hour = int(pendulum.now(config.time_zone).strftime("%H"))
            print(f"Current Date: {current_date}, Current Hour: {current_hour}")
            matched_payroll_period = rail.find_first_by_attr_and_get_attr(
                config.UK_PAYROLL_CALENDER_MAPPER_TO_USE, "payroll_processing_date", current_date)
            return bool(
                matched_payroll_period and
                matched_payroll_period.get("processing_time") == current_hour
            )

        can_process_run = rail.IfOperator(
            task_id = "can_process_run",
            test= can_process_run_test,
            yes_task="process_start_time",
            no_task="finish_export_no_scheduled_run"
        )

        finish_export_no_scheduled_run = rail.EmptyOperator(
            task_id='finish_export_no_scheduled_run'
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{ result('process_start_time').start_time }} - Process started",
            properties={
                "log": "{{ result('process_start_time').start_time }} - Process started"
            }
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: "P" + config.adp_gv_system + config.gv_system_number + "476" + "_" +
            pendulum.now(config.time_zone).strftime(
                "%Y%m%d%H%M%S") + "_" + "GBTIME_HRMD03_MUT8G2I"
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report_name,
        )

        load_user_report = rail.run_report(
            group_id='load_user_report',
            report_params=request_payload.get_run_user_report_payload
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ result("load_user_report.get_report_result", "has_data") }}',
            yes_task='report_has_expected_columns',
            no_task='send_email_for_no_users_data'
        )

        send_email_for_no_users_data = rail.EmailOperator(
            task_id='send_email_for_no_users_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification - Export skipped |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location + ' - Completed no records processed',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/empty_users_data.html",
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        user_report_expected_report_columns = "User Name,Location (Current),UserUri,User Start Date,User End Date,Term Exported"
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test="{{ result('load_user_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % user_report_expected_report_columns,
            no_task='fail_invalid_user_report_columns',
            yes_task='users_report_payload_to_csv',
        )

        fail_invalid_user_report_columns = rail.FailOperator(
            task_id="fail_invalid_user_report_columns",
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
                    "useruri", "userstartdate", "userenddate", "exported"],
            row=request_payload.get_formated_user_row
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id="users_report_data_collection",
            name='getalluserdata',
            source='{{result("formated_users_data_to_csv")}}'
        )

        query_disabled_users_data = rail.QueryCollectionOperator(
            task_id="query_disabled_users_data",
            query=f"SELECT * FROM getalluserdata WHERE DATE(userenddate) > DATE('{request_payload.get_start_date_begin_of_week()}') AND  DATE(userenddate) < DATE('{request_payload.get_end_date_begin_of_week()}') AND exported != 'Yes' "
        )

        has_any_users_data = rail.IfOperator(
            task_id='has_any_users_data',
            test='{{ result("query_disabled_users_data", "length") > 0 }}',
            yes_task="final_users_data_to_csv",
            no_task="send_email_for_no_disabled_users_data"
        )

        send_email_for_no_disabled_users_data = rail.EmailOperator(
            task_id='send_email_for_no_disabled_users_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification - Export skipped |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location + ' - Completed no records processed',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/empty_users_data.html",
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

        get_termination_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_termination_balance_report_details',
            report_name=config.termination_balance_report_name,
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
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification - Export skipped |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location + ' - Completed no records processed',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/no_termination_balance.html",
        )


        termination_balance_expected_report_columns = "User Name,Time Off Type,Time Off Balance,Employee ID,User Start Date,User End Date,useruri,Employee Status"
        termination_balance_report_has_expected_columns = rail.IfOperator(
            task_id="termination_balance_report_has_expected_columns",
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
            query='''SELECT * FROM terminationbalance WHERE NULLIF(Employee_ID, '') IS NULL OR Employee_ID == '' ''',
        )

        logging_no_of_invalid_records = rail.WriteLogOperator(
            task_id="logging_no_of_invalid_records",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin No of invalid users (Without Employee Id) skipped = {{result('query_invalid_termination_balance_data','length')}}",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin No of invalid users (Without Employee Id) skipped = {{result('query_invalid_termination_balance_data','length')}}"
            }
        )

        query_final_termination_collection = rail.QueryCollectionOperator(
            task_id="query_final_termination_collection",
            query="SELECT Time_Off_Type ," +
            "SUM(CASE WHEN Employee_Status NOT IN ('Suspended','Unpaid Leave') " +
            "THEN Time_off_Balance " +
            "ELSE 0 " +
            "END) AS Time_off_Balance,Employee_ID ,User_Start_Date ,User_End_Date,useruri ,Employee_Status " +
            "FROM terminationbalance " +
            "WHERE NULLIF(Employee_ID, '') IS NOT NULL " +
            "AND Time_Off_Type IN "+request_payload.format_sql_in_list(config.termination_timeoff_types) +
            " GROUP BY useruri " +
            "HAVING SUM(CASE WHEN Employee_Status NOT IN ('Suspended','Unpaid Leave') " +
            "THEN Time_off_Balance " +
            "ELSE 0 " +
            "END) > 0"
        )

        query_valid_termination_balance_data = rail.QueryCollectionOperator(
            task_id="query_valid_termination_balance_data",
            query="SELECT * FROM query_final_termination_collection",
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("query_valid_termination_balance_data", "length") > 0 }}',
            yes_task="final_termination_balance_data_to_csv",
            no_task="send_email_for_no_valid_termination_balance_data"
        )

        send_email_for_no_valid_termination_balance_data = rail.EmailOperator(
            task_id='send_email_for_no_valid_termination_balance_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification - Export skipped |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location + ' - Completed no records processed',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/no_valid_termination_balance.html",
        )

        no_of_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result(
                'query_valid_termination_balance_data', 'length')) + 2
        )

        final_termination_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_termination_balance_data_to_csv",
            source="{{ result('query_valid_termination_balance_data') }}",
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ", "ENDUZ", "BETRG",
                    "WAERS", "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ", "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR",
                    "TRFST", "PRAKN", "PRAKZ", "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA", "TDTYPE"],
            row=request_payload.get_termination_balance_uk_data_row,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_exported_custom_field = rail.RepliconServiceOperator(
            task_id="get_exported_custom_field",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'Term Exported', 'uri')
        )

        process_child_udf_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child_udf_update',
            retries=0,
            items="{{ result('query_final_termination_collection') }}",
            trigger_dag_id=config.child_dag_id_udf_update,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'exported_udf_uri': rail.result("get_exported_custom_field"),
                'user_uri': item['useruri']
            }
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/termination_balance_uk.txt',
            dataset="{{ result('final_termination_balance_data_to_csv') }}",
        )

        pgp_encrypt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            sign=True
        )

        upload_encrypted_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_export_data_to_sftp",
            content='{{ result("pgp_encrypt_item_file") }}',
            remote_filepath=config.output_filepath +
            '{{ result("get_file_name")}}.SAP.pgp'
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        upload_encrypted_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content='{{ result("pgp_encrypt_item_file") }}',
            remote_filepath=config.secondary_encrypted_output_filepath + "{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content='{{result("create_document")}}',
            remote_filepath=config.secondary_output_filepath +
            '{{ result("get_file_name")}}.SAP'
        )

        if_error_in_upload_to_sftp = rail.IfOperator(
            task_id="if_error_in_upload_to_sftp",
            test= request_payload.is_upload_data_to_sftp_failed,
            yes_task='send_email_for_sftp_failure',
            no_task='fail_export'
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification - SFTP failure |' + \
            ' {{ result("process_start_time").start_time }} | for '+ config.location + ' - Completed with errors',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ('{{ result("get_file_name") }}.SAP.pgp', '{{result("pgp_encrypt_item_file")}}')
            ]
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
            '{{ result("get_file_name")}}' + " created",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
                    '{{ result("get_file_name")}}' + ".txt"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_log') }}",
            message="{{ result('process_end_time').start_time }} - Process ended",
            properties={
                "log": "{{ result('process_end_time').start_time }} - Process ended"
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
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification |' + \
            ' {{ result("process_start_time").start_time }} | for '+ config.location + ' - Completed successfully',
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
            "log_{{ result('get_file_name')}}_{{ result('process_end_time').ymd_format }}{{ result('process_end_time').hms_format }}"+ ".txt"
        )

        if_error_in_log_upload_to_sftp = rail.IfOperator(
            task_id="if_error_in_log_upload_to_sftp",
            test= request_payload.is_upload_log_to_sftp_failed,
            yes_task='send_email_for_log_upload_failure',
            no_task='fail_export_before_logs'
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            to=config.alert_email,
            subject='{{ get_company_key() }} | Replicon ADP Termination Balance Export Notification - SFTP Log Upload Failure |' + \
            ' {{ result("process_start_time").start_time }} | for '+ config.location + ' - Completed with errors',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_{{ result('get_file_name')}}_{{ result('process_end_time').ymd_format }}{{ result('process_end_time').hms_format }}.txt", '{{result("log_file_data_to_csv")}}')]
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="termination file export has failed"
        )

        fail_export_before_logs = rail.FailOperator(
            task_id="fail_export_before_logs",
            message="termination file export has failed"
        )

 
        run_dag_on_payrollcalendar >> rail.Label("No") >> process_start_time
        run_dag_on_payrollcalendar >> rail.Label("Yes") >> can_process_run >> rail.Label('Yes') >> process_start_time
        can_process_run >> rail.Label('No') >> finish_export_no_scheduled_run
        
        process_start_time >> create_log >> get_file_name >> logging_job_start_time \
        >> get_user_report_details >> load_user_report >> has_data >> rail.Label("Yes") >> report_has_expected_columns
        has_data >> rail.Label("No") >> send_email_for_no_users_data >> finish_export_no_user
        report_has_expected_columns >> rail.Label(
            "Yes") >> users_report_payload_to_csv >> formated_users_data_to_csv >> users_report_data_collection
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_user_report_columns
        users_report_data_collection >> query_disabled_users_data >> has_any_users_data >> rail.Label("Yes"
                                                                                                      ) >> final_users_data_to_csv
        has_any_users_data >> rail.Label("No") >> send_email_for_no_disabled_users_data >> finish_export_no_user
        final_users_data_to_csv >> users_final_data_collection >> query_all_users_data >> get_termination_balance_report_details >> load_termination_balance_report
        load_termination_balance_report >> termination_balance_report_has_data >> rail.Label("Yes"
                                                                                             ) >> termination_balance_report_has_expected_columns
        termination_balance_report_has_data >> rail.Label(
            "No") >> send_email_for_no_termination_balance_data >> finish_export
        termination_balance_report_has_expected_columns >> rail.Label(
            "Yes") >> termination_balance_report_payload_to_csv >> termination_balance_report_data_collection
        termination_balance_report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_termination_report_colums
        termination_balance_report_data_collection >> query_invalid_termination_balance_data >> logging_no_of_invalid_records
        
        termination_balance_report_data_collection >> query_final_termination_collection >> query_valid_termination_balance_data
        query_valid_termination_balance_data >> has_valid_data >> rail.Label("Yes") >> final_termination_balance_data_to_csv >> get_exported_custom_field >> process_child_udf_update \
            >> no_of_records_size_including_header_footer >> create_document >> pgp_encrypt_item_file >> \
            upload_encrypted_export_data_to_secondary_sftp >> upload_encrypted_export_data_to_sftp
        
        has_valid_data >> rail.Label("No") >> send_email_for_no_valid_termination_balance_data >> finish_export
        
        upload_encrypted_export_data_to_sftp >> rail.Label("on_success") >> upload_export_data_to_secondary_sftp >> logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_error >> if_error_in_upload_to_sftp >> rail.Label("Yes") >> send_email_for_sftp_failure 

        if_error_in_upload_to_sftp >> rail.Label("No") >> fail_export 

        logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv >> upload_log_data_to_sftp
        
        upload_log_data_to_sftp >> rail.Label("on_success") >> send_email_for_export_completion >> finish_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_error >> if_error_in_log_upload_to_sftp >> rail.Label("Yes") >> send_email_for_log_upload_failure

        if_error_in_log_upload_to_sftp >> rail.Label("No") >> fail_export_before_logs

    return dag

rail.for_each_instance(create_main_dag)
