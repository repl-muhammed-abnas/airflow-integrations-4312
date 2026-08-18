# pylint: disable=too-many-statements
from datetime import datetime as dt, timedelta
import itertools
import rail
from dxctechnology.australia_payroll_extract_v1.utils import request_payload
from dxctechnology.australia_payroll_extract_v1.utils import response_filter
from dxctechnology.australia_payroll_extract_v1.utils import python_callable_method

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_payrolldata_export_user_schedule_child_v1_{config.instance}',
        description=f'DXC_Australia_PayrollData_Export_User_Schedule_Child V1 {config.instance}',
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

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleListService1.svc/GetData",
            data= {
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:office-schedule-list-column:name",
                        "urn:replicon:office-schedule-list-column:description"
                    ],
                    "sort": [],
                    "filterExpression": None
                },
            response_filter= response_filter.get_office_schedules
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

        get_user_schedule_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_schedule_balance_report_details',
            report_name=config.user_schedule_report_name
        )

        load_user_schedule_balance_report = rail.run_report(
            group_id='load_user_schedule_balance_report',
            report_params=request_payload.get_run_user_schedule_balance_report_payload
        )

        user_schedule_balance_report_has_data = rail.IfOperator(
            task_id="user_schedule_balance_report_has_data",
            test='{{ result("load_user_schedule_balance_report.get_report_result", "has_data") }}',
            yes_task='user_schedule_balance_report_has_expected_columns',
            no_task='send_email_for_no_user_schedule_balance_data'
        )

        send_email_for_no_user_schedule_balance_data = rail.EmailOperator(
            task_id='send_email_for_no_user_schedule_balance_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for AUS User Schedule Balance is skipped for Australia location  on - {{ current_time_in_specified_tz() }}',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week()
            },
            html_content="templates/email/empty_user_schedule_data.html",
        )

        # pylint: disable=line-too-long
        user_schedule_balance_expected_report_columns = "User Name,Employee ID,Actual Employee ID,Entry Date,Shift Description,Schedule Name"
        user_schedule_balance_report_has_expected_columns = rail.IfOperator(
            task_id="user_schedule_balance_report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('load_user_schedule_balance_report.get_report_result').reportGenerationResults[0].payload |\
                 starts_with('%s') }}" % user_schedule_balance_expected_report_columns,
            no_task='fail_invalid_user_schedule_report_colums',
            yes_task='user_schedule_balance_report_payload_to_csv',
        )

        fail_invalid_user_schedule_report_colums = rail.FailOperator(
            task_id="fail_invalid_user_schedule_report_colums",
            message="Base report column does not match"
        )

        user_schedule_balance_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="user_schedule_balance_report_payload_to_csv",
            document='{{result("load_user_schedule_balance_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        user_schedule_balance_report_data_collection = rail.CreateCollectionOperator(
            task_id="user_schedule_balance_report_data_collection",
            source='{{result("user_schedule_balance_report_payload_to_csv")}}',
            columns={
                'User Name': 'User_Name',
                'Employee ID': 'Employee_Id',
                'Actual Employee ID': 'Actual_Employee_ID',
                'Entry Date': 'Entry_Date',
                'Shift Description': 'Shift_Description',
                'Schedule Name': 'Schedule_Name',
            }
        )

        create_csv_lines_for_raw_data = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_raw_data',
            source="{{ result('user_schedule_balance_report_data_collection') }}",
            header=[
                'User_Name',
                'Employee_Id',
                'Actual_Employee_ID',
                'Entry_Date',
                'Shift_Description',
                'Schedule_Name',
                'Office_Schedule'],
            row=lambda item: [
                item['User_Name'],
                item['Employee_Id'],
                item['Actual_Employee_ID'],
                dt.strptime(item['Entry_Date'], '%d %B %Y').strftime("%Y-%m-%d") if item['Entry_Date'] else None,
                item['Shift_Description'],
                rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),'displaytext',
                                    item['Schedule_Name'],'description'),
                item['Schedule_Name']
            ],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        final_report_data_collection = rail.CreateCollectionOperator(
            task_id="final_report_data_collection",
            name='active_userbalance',
            source='{{result("create_csv_lines_for_raw_data")}}',
            columns={
                'User_Name': 'User_Name',
                'Employee_Id': 'Employee_Id',
                'Actual_Employee_ID': 'Actual_Employee_ID',
                'Entry_Date': 'Entry_Date',
                'Shift_Description': 'Shift_Description',
                'Schedule_Name': 'Schedule_Name',
                'Office_Schedule': 'Office_Schedule'
            }
        )

        query_invalid_user_schedule_balance_data = rail.QueryCollectionOperator(
            task_id="query_invalid_user_schedule_balance_data",
            query='''SELECT * FROM active_userbalance WHERE NULLIF(Employee_Id, '') IS NULL OR Employee_Id == '' OR (Office_Schedule == 'Shift Schedule' AND (NULLIF(Shift_Description, '') IS NULL OR Shift_Description == '' ))''',
        )

        has_invalid_data = rail.IfOperator(
            task_id='has_invalid_data',
            test='{{ result("query_invalid_user_schedule_balance_data", "length") > 0 }}',
            yes_task="logging_number_of_users_skipped",
        )

        logging_number_of_users_skipped = rail.WriteLogOperator(
            task_id="logging_number_of_users_skipped",
            log="{{ result('create_log') }}",
            message=lambda: "The number of users skipped - {{result('query_invalid_user_schedule_balance_data','length')}}",
            properties={
                "log": "The number of users skipped -{{result('query_invalid_user_schedule_balance_data','length')}}"
            }
        )

        query_employee_data_collection = rail.QueryCollectionOperator(
            task_id="query_employee_data_collection",
            query="SELECT DISTINCT Employee_Id,Shift_Description,Office_Schedule FROM active_userbalance WHERE NULLIF(Employee_Id, '') IS NOT NULL AND ((Office_Schedule == 'Shift Schedule' AND NULLIF(Shift_Description, '') IS NOT NULL) OR Office_Schedule != 'Shift Schedule')",
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("query_employee_data_collection", "length") > 0 }}',
            yes_task="process_child_dag_runs",
            no_task="finish_export_no_valid_data"
        )

        process_child_dag_runs = rail.EmptyOperator(
            task_id='process_child_dag_runs'
        )

        process_each_user_based_empid= rail.trigger_parallel_dagrun(
            task_id= 'process_each_user_based_empid',
            parallel_count=config.parallel_trigger_dagrun_count,
            items="{{ result('query_employee_data_collection') }}",
            trigger_dag_id=f'dxctechnology_australia_payroll_export_process_each_user_child_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'employee_id': "{{ item.Employee_Id}}",
                'shift_description': "{{ item.Shift_Description}}",
                'schedule_name': "{{ item.Office_Schedule}}"
            }
        )

        get_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_user_based_empid_{x+1}'), range(config.parallel_trigger_dagrun_count))))),
            show_return_value_in_logs= False
        )

        gather_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_data',
            dag_runs="{{ result('get_process_users_dag_ids') }}",
            dagrun_task_id='get_required_data_for_empid',
            flatten=True,
        )

        gather_child_data_collection = rail.CreateCollectionOperator(
            task_id="gather_child_data_collection",
            source="{{result('gather_child_data') | to_json }}",
            name= 'rawdata'
        )

        query_gather_child_data_collection = rail.QueryCollectionOperator(
            task_id = "query_gather_child_data_collection",
            query="""SELECT * from rawdata"""
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id="list_reference_file",
            paths=[config.reference_file_path],
            sftp_conn_id= config.secondary_sftp_conn_id
        )

        has_any_reference_files = rail.IfOperator(
            task_id="has_any_reference_files",
            test=lambda: python_callable_method.has_any_file(
                result_task_id="list_reference_file", input_file_path=config.reference_file_path),
            yes_task="get_reference_file_name",
            no_task="get_collection_to_user"
        )

        get_reference_file_name= rail.PythonOperator(
            task_id= 'get_reference_file_name',
            python_callable=lambda: rail.result("list_reference_file")[config.reference_file_path][0]['name'] if rail.result(
                "list_reference_file") else None
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath= config.reference_file_path +
            "{{ result('get_reference_file_name') }}",
            sftp_conn_id= config.secondary_sftp_conn_id
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename=config.reference_file_path +
            '{{ result("get_reference_file_name") }}',
            new_filename=config.reference_file_archive_path +
            '{{ result("get_reference_file_name") }}',
            sftp_conn_id= config.secondary_sftp_conn_id
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_file')}}",
            headers=["employee_id", "description", "start_date", "end_date", 'md5']
        )

        reference_file_data_collection = rail.CreateCollectionOperator(
            task_id="reference_file_data_collection",
            source="{{result('parse_reference_file')}}",
        )

        query_final_payroll_collection = rail.QueryCollectionOperator(
            task_id = "query_final_payroll_collection",
            query="""SELECT * FROM rawdata where md5 not in (SELECT DISTINCT md5 from reference_file_data_collection)""",
            name= "referencefinaldata"
        )

        get_collection_to_user = rail.PythonOperator(
            task_id = "get_collection_to_user",
            python_callable= lambda: 'referencefinaldata' if python_callable_method.has_any_file(
                result_task_id="list_reference_file", input_file_path=config.reference_file_path) else 'rawdata'
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id = "query_list_in_final_payroll_collection",
            query="SELECT * from {{result('get_collection_to_user')}}",
        )

        get_reference_file_data= rail.PythonOperator(
            task_id = "get_reference_file_data",
            python_callable= lambda: rail.load_all_records(rail.result("query_list_in_final_payroll_collection"))
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('get_reference_file_data'),
            header=["employee_id", "description", "start_date", "end_date", 'md5'],
            row=[
                '{{item.Employee_Id}}',
                '{{item.Description}}',
                '{{item.Start_date}}',
                '{{item.End_date}}',
                '{{item.md5}}'
            ]
        )

        upload_new_reference_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file_to_sftp',
            content="{{ result('create_reference_file') }}",
            remote_filepath=config.reference_file_path +
            '/reference_file_{{ dag_run_ecid() | replace(":", "-")}}.csv',
            sftp_conn_id= config.secondary_sftp_conn_id
        )

        finish_export_no_valid_data = rail.EmptyOperator(
            task_id="finish_export_no_valid_data",
        )

        no_of_records_size_including_header_footer=rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result('query_list_in_final_payroll_collection','length')) + 2
        )

        final_user_schedule_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_user_schedule_balance_data_to_csv",
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY","CLIID","INTCA","ORDNO","IOPER","INFTY","SUBTY","BEGDA",
            "ENDDA","OBJPS","SPRPS","SEQNR","EXTRA","SCHKZ","ZTERF","EMPCT","ARBST","WKWDY","TEILK",
            "DYSCH","MINTA", "MAXTA", "MINWO", "MAXWO", "MINMO", "MAXMO","MINJA", "MAXJA", "KZTIM", "WWEEK", "MOSTD", "WOSTD", "JRSTD"],
            row=request_payload.get_user_schedule_balance_us_data_row
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/user_schedule_export_data.txt',
            dataset="{{ result('final_user_schedule_balance_data_to_csv') }}",
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
            subject='{{ get_company_key() }} | Replicon payroll export for AUS User Schedule Balance - SFTP failure for {{ dag_run.conf.location }} location  on - current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ('{{ dag_run.conf.file_name}}.SAP.pgp', '{{result("pgp_encyrpt_item_file")}}')]
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
            subject='{{ get_company_key() }} | Replicon payroll export for AUS User Schedule Balance completed  on - {{ current_time_in_specified_tz() }}',
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
            subject='{{ get_company_key() }} | Replicon payroll export for AUS User Schedule Balance - SFTP failure for {{ dag_run.conf.location }} location {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_"+'{{ dag_run.conf.file_name}}', '{{result("log_file_data_to_csv")}}')]
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
        create_log >> get_all_enabled_divisions >> get_all_office_schedules >> get_file_name >> process_start_time >> process_start_time_ymd_format >> \
            process_start_time_hms_format >> get_user_schedule_balance_report_details >> load_user_schedule_balance_report
        load_user_schedule_balance_report >> user_schedule_balance_report_has_data >> rail.Label("Yes"
                                                                                             ) >> user_schedule_balance_report_has_expected_columns
        user_schedule_balance_report_has_data >> rail.Label(
            "No") >> send_email_for_no_user_schedule_balance_data
        user_schedule_balance_report_has_expected_columns >> rail.Label(
            "Yes") >> user_schedule_balance_report_payload_to_csv >> user_schedule_balance_report_data_collection
        user_schedule_balance_report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_user_schedule_report_colums
        user_schedule_balance_report_data_collection >> query_invalid_user_schedule_balance_data >> has_invalid_data
        has_invalid_data >> rail.Label("Yes") >> logging_number_of_users_skipped
        user_schedule_balance_report_data_collection >> create_csv_lines_for_raw_data >> final_report_data_collection >> query_employee_data_collection
        query_employee_data_collection >> has_valid_data >> rail.Label("Yes"
                ) >> process_child_dag_runs >> process_each_user_based_empid >> get_process_users_dag_ids >> gather_child_data >> \
                    gather_child_data_collection >> query_gather_child_data_collection >> list_reference_file >> has_any_reference_files
        has_any_reference_files >> rail.Label(
            "Yes") >> get_reference_file_name >> download_file >> archive_file >> parse_reference_file >> reference_file_data_collection >> \
                query_final_payroll_collection >> get_collection_to_user >> query_list_in_final_payroll_collection >> get_reference_file_data >> create_reference_file >> upload_new_reference_file_to_sftp >> final_user_schedule_balance_data_to_csv
        has_any_reference_files >> rail.Label(
            "No") >> get_collection_to_user >> query_list_in_final_payroll_collection
        final_user_schedule_balance_data_to_csv >>no_of_records_size_including_header_footer>> create_document>>\
            pgp_encyrpt_item_file >>upload_export_data_to_sftp >> upload_encrypted_export_data_to_sftp
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
