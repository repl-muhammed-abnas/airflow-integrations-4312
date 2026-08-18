# pylint: disable=too-many-statements
from datetime import datetime as dt, timedelta
import rail
from crl.termination_balance_export_v2.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'crl_terminationbalance_child_{config.instance}v2',
        description=f'CRL terminationbalance_child {config.instance}',
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
            test='{{ result("load_user_report.get_report_result", "has_data") }}',
            yes_task='report_has_expected_columns',
            no_task='send_email_for_no_users_data'
        )

        send_email_for_no_users_data = rail.EmailOperator(
            task_id='send_email_for_no_users_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Termination file is skipped for {{ dag_run.conf.location }} location  on - {{ current_time_in_specified_tz() }}',
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
            # pylint: disable=consider-using-f-string
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
                    "useruri", "userstartdate", "userenddate", "exported","timeofftemplate"],
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
            query=f"SELECT * FROM getalluserdata WHERE DATE(userenddate) > DATE('{request_payload.get_start_date_begin_of_week()}') AND  DATE(userenddate) < DATE('{request_payload.get_end_date_begin_of_week()}') AND exported != 'Yes' "
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
            subject='{{ get_company_key() }} | Replicon payroll export for Termination file is skipped for Canada location  on - {{ current_time_in_specified_tz() }}',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/no_termination_balance.html",
        )

        # pylint: disable=line-too-long
        termination_balance_expected_report_columns = "User Name,Time Off Type,Time Off Balance,Employee ID,User Start Date,User End Date,useruri,Employee Status"
        termination_balance_report_has_expected_columns = rail.IfOperator(
            task_id="termination_balance_report_has_expected_columns",
            # pylint: disable=consider-using-f-string
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

        has_invalid_data = rail.IfOperator(
            task_id='has_invalid_data',
            test='{{ result("query_invalid_termination_balance_data", "length") > 0 }}',
            yes_task="logging_no_of_invalid_records",
        )

        logging_no_of_invalid_records = rail.WriteLogOperator(
            task_id="logging_no_of_invalid_records",
            log="{{ result('create_log') }}",
            message="The number of users skipped - {{result('query_invalid_termination_balance_data','length')}}",
            properties={
                "log": "The number of users skipped -{{result('query_invalid_termination_balance_data','length')}}"
            }
        )

        query_final_payroll_collection = rail.QueryCollectionOperator(
            task_id="query_final_payroll_collection",
            query="SELECT User_Name ,Time_Off_Type ," +
            "SUM(CASE WHEN Employee_Status NOT IN ('Suspended','Unpaid Leave') " +
            "THEN Time_off_Balance " +
            "ELSE 0 " +
            "END) AS Time_off_Balance,Employee_ID ,User_Start_Date ,User_End_Date,useruri ,Employee_Status,User_Status  " +
            "FROM terminationbalance " +
            "WHERE NULLIF(Employee_ID, '') IS NOT NULL " +
            "AND Time_Off_Type IN "+str(config.vacation_timeoff) +
            "GROUP BY useruri " +
            "HAVING SUM(CASE WHEN Employee_Status NOT IN ('Suspended','Unpaid Leave') " +
            "THEN Time_off_Balance " +
            "ELSE 0 " +
            "END) > 0" +
            " UNION " +
            "SELECT " +
            "User_Name , " +
            "Time_Off_Type , " +
            "SUM(CASE WHEN Employee_Status NOT IN " +
            "('Suspended', 'Unpaid Leave') THEN Time_off_Balance ELSE 0 END) AS Time_off_Balance, " +
            "Employee_ID , " +
            "User_Start_Date , " +
            "User_End_Date, " +
            "useruri , " +
            "Employee_Status, " +
            "User_Status " +
            "FROM " +
            "terminationbalance " +
            "WHERE " +
            "NULLIF(Employee_ID, " +
            "'') IS NOT NULL " +
            "AND ( Time_Off_Type = '[CAN] Vacation /Vacances' " +
          		"OR Time_Off_Type = '[CAN] Exception vacances/Exception Vacation') " +
            "GROUP BY " +
            "useruri " +
            "HAVING " +
            "SUM(CASE WHEN Employee_Status NOT IN ('Suspended', 'Unpaid Leave') " +
            "THEN Time_off_Balance ELSE 0 END) > 0 " +
            " UNION " +
            "SELECT " +
            "User_Name , " +
            "Time_Off_Type , " +
            "SUM(CASE WHEN Employee_Status NOT IN " +
            "('Suspended', 'Unpaid Leave') THEN Time_off_Balance ELSE 0 END) AS Time_off_Balance, " +
            "Employee_ID , " +
            "User_Start_Date , " +
            "User_End_Date, " +
            "useruri , " +
            "Employee_Status, " +
            "User_Status " +
            "FROM " +
            "terminationbalance " +
            "WHERE " +
            "NULLIF(Employee_ID, " +
            "'') IS NOT NULL " +
            "AND ( Time_Off_Type = '[CAN] Heures supplémentaires cumulées/Time off in Lieu SC' " +
          		"OR Time_Off_Type = '[CAN] Heures supplémentaires cumulées/Time off in Lieu') " +
            "GROUP BY " +
            "useruri " +
            "HAVING " +
            "SUM(CASE WHEN Employee_Status NOT IN ('Suspended', 'Unpaid Leave') " +
            "THEN Time_off_Balance ELSE 0 END) > 0"
        )

        query_valid_termination_balance_data = rail.QueryCollectionOperator(
            task_id="query_valid_termination_balance_data",
            query="SELECT * FROM query_final_payroll_collection",
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
            row=request_payload.get_termination_balance_us_data_row,
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
            items="{{ result('query_final_payroll_collection') }}",
            trigger_dag_id=f'crl_ermination_balance_udf_update_child_{config.instance}v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'exported_udf_uri': rail.result("get_exported_custom_field"),
                'user_uri': item['useruri']
            }
        )

        # Get all previously exported users who have time-off templates
        # Only include users where Term Exported = Yes AND Time Off Template is assigned
        # Also, Include users where Term Exported = No AND Time Off Template is assigned
        get_all_user_data_eligible_for_timeoff_template_removal = rail.QueryCollectionOperator(
            task_id="get_all_user_data_eligible_for_timeoff_template_removal",
            query=f"""SELECT * FROM getalluserdata 
                        WHERE COALESCE(timeofftemplate, '') != ''
                        AND (
                            exported = 'Yes' 
                            OR (
                                DATE(userenddate) > DATE('{request_payload.get_start_date_begin_of_week()}')
                                AND DATE(userenddate) < DATE('{request_payload.get_end_date_begin_of_week()}')
                                AND COALESCE(exported, 'No') != 'Yes'
                            )
                        )"""
        )
        
        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: {
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
            }
        )

        # Trigger timeoff template removal for each user
        process_timeoff_template_removal = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timeoff_template_removal',
            retries=0,
            items="{{ result('get_all_user_data_eligible_for_timeoff_template_removal')}}",
            trigger_dag_id=f'crl_terminationbalance_remove_timeoff_template_child_{config.instance}v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'user_uri': item['useruri'],
                'user_name': item['username'],
                'timeoff_policy_set_uri': rail.result('get_all_policy_sets')['timeoff'],
            }   
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/termination_balance_aus.txt',
            dataset="{{ result('final_termination_balance_data_to_csv') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            sign=True
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
            remote_filepath=config.secondary_encrypted_output_filepath +
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
            subject='{{ get_company_key() }} | Termination balance data export automation - SFTP failure for {{ dag_run.conf.location }} location  on - current_time_in_specified_tz() }}',
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

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for Termination file completed  on - {{ current_time_in_specified_tz() }}',
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
            subject='{{ get_company_key() }} | Replicon payroll export for Termination file - SFTP failure for {{ dag_run.conf.location }} location {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_"+'{{ dag_run.conf.file_name }}', '{{result("log_file_data_to_csv")}}')]
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
        create_log >> get_all_enabled_divisions >> get_file_name >> process_start_time >> process_start_time_ymd_format >> process_start_time_hms_format >> \
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
        termination_balance_report_data_collection >> query_final_payroll_collection >> query_valid_termination_balance_data
        query_valid_termination_balance_data >> has_valid_data >> rail.Label("Yes"
            ) >> final_termination_balance_data_to_csv >> get_exported_custom_field >> process_child_udf_update
        process_child_udf_update >> get_all_user_data_eligible_for_timeoff_template_removal

        get_all_user_data_eligible_for_timeoff_template_removal >> get_all_policy_sets >> process_timeoff_template_removal >> no_of_records_size_including_header_footer >> create_document >>\
            pgp_encyrpt_item_file >> upload_export_data_to_sftp >> upload_encrypted_export_data_to_sftp
        has_valid_data >> rail.Label("No") >> finish_export_no_valid_data
        upload_encrypted_export_data_to_sftp >> rail.Label(
            "on_success") >> upload_export_data_to_secondary_sftp >> logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_data_to_sftp_failed >> rail.Label("Yes"
                                                                                                                                     ) >> send_email_for_sftp_failure
        is_upload_data_to_sftp_failed >> rail.Label("No") >> fail_export
        logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv >> send_email_for_export_copmpletion >> upload_log_data_to_sftp
        upload_log_data_to_sftp >> rail.Label("on_success") >> finish_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_log_to_sftp_failed >> rail.Label("Yes"
                                                                                                                       ) >> send_email_for_log_upload_failure
        is_upload_log_to_sftp_failed >> rail.Label(
            "No") >> fail_export_before_log

    return dag


rail.for_each_instance(create_child_dag)
