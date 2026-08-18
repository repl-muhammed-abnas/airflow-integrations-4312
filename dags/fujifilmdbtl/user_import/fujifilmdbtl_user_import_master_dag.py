from airflow.models import Variable
from datetime import timedelta, datetime
import hashlib
import rail

null = None


def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdbtl_user_import_master_{config.instance}',
        description=f'FUJIFILMBDTL User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=timedelta(days=config.master_dag_interval), Set accordingly when deploying to PROD
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        file_name_incorrect = rail.IfOperator(
            task_id='file_name_incorrect',
            test='{{result("new_file_sensor") | file_name != "User_Import_Replicon_wShifts.csv"}}',
            yes_task="rename_archive_input_file",
            no_task="log_formatted_job_start_time",
        )

        rename_archive_input_file = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file',
            new_filename=config.archive_filepath +
            "/{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}",
            existing_filename="{{ result('new_file_sensor')}}",
        )

        log_formatted_job_start_time = rail.PythonOperator(
            task_id='log_formatted_job_start_time',
            python_callable=lambda: datetime.now().strftime("%d%m%YT%H%M%S")
        )

        if_name_not_ends_with_csv = rail.IfOperator(
            task_id='if_name_not_ends_with_csv',
            test='{{ result("new_file_sensor") | lower | file_ext | lower != "csv" }}',
            yes_task="send_mail_for_incorrect_file_format",
            no_task="fdt_user_import_logs",
        )

        send_mail_for_incorrect_file_format = rail.EmailOperator(
            task_id='send_mail_for_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User import has been skipped - {{ current_time_in_specified_tz("' + config.time_zone + '") }} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import is skipped, since the file - '{{ result('new_file_sensor') | file_name }}' is not in .csv file format. Please correct the file name and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        rename_archive_input_file_skipped = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file_skipped',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '/Skipped_{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        fdt_user_import_logs = rail.CreateLogOperator(
            task_id='fdt_user_import_logs',
            existing_log_mode='truncate',
            tenant_wide_name='fdt_user_import_logs'
        )

        fdt_supervisor_assignment_lookup_table = rail.CreateLogOperator(
            task_id='fdt_supervisor_assignment_lookup_table',
            existing_log_mode='truncate',
            tenant_wide_name='fdt_supervisor_assignment_lookup_table'
        )

        download_input_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        parse_input_csv_file = rail.LoadCSVFileOperator(
            task_id="parse_input_csv_file",
            document="{{ result('download_input_csv_file') }}",
            headers=[
                "emplid", "file", "paygroup", "lastname", "firstname", "email", "annualsalary", "eestatus", "servicedate",
                "rehiredate", "eetype", "deptid", "department", "autolinkratetype", "regulartemporary", "fullparttime",
                "managerid", "reporttoname", "company", "hourlyratejobdata", "jobtitle", "shiftassignment", "hourlyrate2"
            ],
            has_no_header=False
        )

        if_input_csv_file_records_less_than_1 = rail.IfOperator(
            task_id='if_input_csv_file_records_less_than_1',
            test='''{{ result('parse_input_csv_file') | load_all_records() | length <1 }}''',
            yes_task="send_mail_for_no_records",
            no_task="create_csv_lines_from_input_file",
        )

        send_mail_for_no_records = rail.EmailOperator(
            task_id='send_mail_for_no_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User import has been skipped - {{ current_time_in_specified_tz("' + config.time_zone + '") }} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import is skipped, since the file - '{{ result('new_file_sensor') | file_name }}' doesn't contain any row(data). Please correct the feed file and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        rename_archive_input_file_2 = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file_2',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '/{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        create_csv_lines_from_input_file = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_from_input_file',
            source="{{ result('parse_input_csv_file')}}",
            header=[
                'emplid', 'file', 'paygroup', 'lastname', 'firstname', 'email', 'annualsalary', 'eestatus', 'servicedate', 'rehiredate',
                'eetype', 'deptid', 'department', 'autolinkratetype', 'regulartemporary', 'fullparttime', 'managerid', 'reporttoname',
                'company', 'hourlyratejobdata', 'jobtitle', 'shiftassignment', 'hourlyrate2', 'encoded'
            ],
            row=lambda item: [
                item['emplid'].strip() if item['emplid'] else null,
                item['file'].strip() if item['file'] else null,
                item['paygroup'].strip() if item['paygroup'] else null,
                item['lastname'].strip() if item['lastname'] else null,
                item['firstname'].strip() if item['firstname'] else null,
                item['email'].strip() if item['email'] else null,
                item['annualsalary'].strip() if item['annualsalary'] else null,
                item['eestatus'].strip() if item['eestatus'] else null,
                item['servicedate'].strip() if item['servicedate'] else null,
                item['rehiredate'].strip() if item['rehiredate'] else null,
                item['eetype'].strip() if item['eetype'] else null,
                item['deptid'].strip() if item['deptid'] else null,
                item['department'].strip() if item['department'] else null,
                item['autolinkratetype'].strip(
                ) if item['autolinkratetype'] else null,
                item['regulartemporary'].strip(
                ) if item['regulartemporary'] else null,
                item['fullparttime'].strip() if item['fullparttime'] else null,
                item['managerid'].strip() if item['managerid'] else null,
                item['reporttoname'].strip() if item['reporttoname'] else null,
                item['company'].strip() if item['company'] else null,
                item['hourlyratejobdata'].strip(
                ) if item['hourlyratejobdata'] else null,
                item['jobtitle'].strip() if item['jobtitle'] else null,
                item['shiftassignment'].strip(
                ) if item['shiftassignment'] else null,
                item['hourlyrate2'].strip() if item['hourlyrate2'] else null,
                hashlib.md5(
                    (str(item['emplid']) + ","
                     + str(item['file']) + ","
                     + str(item['paygroup']) + ","
                     + str(item['lastname']) + ","
                     + str(item['firstname']) + ","
                     + str(item['email']) + ","
                     + str(item['annualsalary']) + ","
                     + str(item['eestatus']) + ","
                     + str(item['servicedate']) + ","
                     + str(item['rehiredate']) + ","
                     + str(item['eetype']) + ","
                     + str(item['deptid']) + ","
                     + str(item['department']) + ","
                     + str(item['autolinkratetype']) + ","
                     + str(item['regulartemporary']) + ","
                     + str(item['fullparttime']) + ","
                     + str(item['managerid']) + ","
                     + str(item['reporttoname']) + ","
                     + str(item['company']) + ","
                     + str(item['hourlyratejobdata']) + ","
                     + str(item['jobtitle']) + ","
                     + str(item['shiftassignment']) + ","
                     + str(item['hourlyrate2']) + ","
                     ).encode('utf-8')).hexdigest()
            ]
        )

        load_input_csv_file = rail.LoadCSVFileOperator(
            task_id='load_input_csv_file',
            document="{{result('create_csv_lines_from_input_file')}}"
        )

        create_input_file_list_from_input_csv_file = rail.CreateCollectionOperator(
            task_id='create_input_file_list_from_input_csv_file',
            source="{{ result('load_input_csv_file') }}",
            name="inputfile"
        )

        query_get_data_from_collection = rail.QueryCollectionOperator(
            task_id='query_get_data_from_collection',
            query="""SELECT * FROM  inputfile""",
        )

        query_get_user_with_blank_file_or_incorrect_file = rail.QueryCollectionOperator(
            task_id='query_get_user_with_blank_file_or_incorrect_file',
            query="""SELECT * FROM  inputfile WHERE (NULLIF(file,'') IS NULL)"""
        )

        if_query_list_get_user_with_blank_file_or_incorrect_file_greater_than_0 = rail.IfOperator(
            task_id='if_query_list_get_user_with_blank_file_or_incorrect_file_greater_than_0',
            test='''{{ result('query_get_user_with_blank_file_or_incorrect_file', 'length') > 0 }}''',
            yes_task="fdt_user_import_logs_add_entries",
            no_task="query_get_user_without_blank_or_incorrect_file"
        )

        fdt_user_import_logs_add_entries = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entries',
            log="{{ result('fdt_user_import_logs') }}",
            items=lambda: rail.result(
                'query_get_user_with_blank_file_or_incorrect_file'),
            message="No/incorrect File Number received.",
            severity="Info",
            properties={
                "username": "{{ item.firstname }} {{ item.lastname }}",
                "loginname": "{{ item.email }}",
                "employeeid": "{{ item.emplid }}",
                "importaction": "Validation",
                "status": "Skipped",
                "details": "No File Number received."
            }
        )

        query_get_user_without_blank_or_incorrect_file = rail.QueryCollectionOperator(
            task_id='query_get_user_without_blank_or_incorrect_file',
            query="""SELECT * FROM  inputfile WHERE (NULLIF(file,'') IS NOT NULL)""",
        )

        create_validated_input_list = rail.CreateCollectionOperator(
            task_id='create_validated_input_list',
            source="{{ result('query_get_user_without_blank_or_incorrect_file') }}",
            name="validatedinputlist",
        )

        new_file_sensor_adfile = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_adfile',
            path=config.ad_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        adfile_check = rail.IfOperator(
            task_id='adfile_check',
            test='{{get_task_state("new_file_sensor") == "success"}}',
            yes_task="download_adfile_from_sftp",
            no_task="send_no_adfile_mail",
        )

        send_no_adfile_mail = rail.EmailOperator(
            task_id='send_no_adfile_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User import has been skipped - {{ current_time_in_specified_tz("' + config.time_zone + '") }} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import for the file name "{{ result('new_file_sensor') | file_name }}" is skipped, since there is no 'Active Directory' file in the required folder. Please add the required 'Active Directory' file and place a new input file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        rename_archive_input_file_3 = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file_3',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '/{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        download_adfile_from_sftp = rail.SFTPDownloadFileOperator(
            task_id='download_adfile_from_sftp',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor_adfile') }}"
        )

        load_adfile_csv = rail.LoadCSVFileOperator(
            task_id="load_adfile_csv",
            document="{{result('download_adfile_from_sftp')}}",
            headers=[
                'name',
                'loginname',
                'filenumber'
            ],
            encoding="utf-8"
        )

        create_collection_from_adfile_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_adfile_csv',
            source="{{ result('load_adfile_csv') }}",
            name="adfilelist"
        )

        query_list_check_if_any_user_is_not_available_in_adfile = rail.QueryCollectionOperator(
            task_id='query_list_check_if_any_user_is_not_available_in_adfile',
            query="""SELECT emplid, file, firstname, lastname FROM validatedinputlist WHERE file NOT IN (SELECT DISTINCT filenumber FROM adfilelist WHERE (NULLIF(filenumber,'') IS NOT NULL)) """,
        )

        if_query_list_check_if_any_user_is_not_available_in_adfile_rows_greater_than_0 = rail.IfOperator(
            task_id='if_query_list_check_if_any_user_is_not_available_in_adfile_rows_greater_than_0',
            test='''{{ result('query_list_check_if_any_user_is_not_available_in_adfile', 'length') > 0 }}''',
            yes_task="create_csv_for_user_missing_logs",
            no_task="query_create_list_adfile_data",
        )

        create_csv_for_user_missing_logs = rail.WriteCSVFileOperator(
            task_id='create_csv_for_user_missing_logs',
            source="{{ result('query_list_check_if_any_user_is_not_available_in_adfile') }}",
            header=['Employee ID', 'filenumber', 'username'],
            row=[
                "{{ item.emplid }}",
                "{{ item.file }}",
                "{{ item.firstname }}{{ item.lastname }}"
            ],
        )

        upload_user_missing_log_file = rail.SFTPUploadFileOperator(
            task_id='upload_user_missing_log_file',
            content='''{{ result('create_csv_for_user_missing_logs') }}''',
            remote_filepath=config.log_filepath +
            '/log_{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        send_mail_notification_for_user_missing_in_adfile = rail.EmailOperator(
            task_id='send_mail_notification_for_user_missing_in_adfile',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User import has been skipped - {{ current_time_in_specified_tz("' + config.time_zone + '") }} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong></p><p>Hello, <br /> <br /> The User Import for the file name "{{ result('new_file_sensor') | file_name }}" is  skipped since few users are not available in the 'Active Directory' file. Please find the user list at below SFTP location.</p><ul><li>Path: /UserSync/logs</li><li>Name: log_{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}</li></ul><p>Please update the required 'Active Directory' file and place a new input file for processing.<br /><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''',
            params=None,
        )

        rename_archive_input_file_4 = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file_4',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '/{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        query_create_list_adfile_data = rail.QueryCollectionOperator(
            task_id='query_create_list_adfile_data',
            query="""SELECT name, loginname, filenumber FROM adfilelist WHERE (NULLIF(filenumber,'') IS NOT NULL)""",
            name='adfiledata'
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.report_name
        )

        run_report_to_extract_user_list_from_replicon = rail.run_report2(
            group_id="generate_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": '{{ result("get_report_details").uri }}',
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
        )

        if_generate_report_has_error = rail.IfOperator(
            task_id='if_generate_report_has_error',
            test="{{ (result('generate_report.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task="can_fail_dag",
            no_task="load_user_list_csv_from_replicon",
        )

        load_user_list_csv_from_replicon = rail.LoadCSVFileOperator(
            task_id='load_user_list_csv_from_replicon',
            document="{{ (result('generate_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
            headers=[
                'username',
                'employeeid',
                'enabled',
                'useruri',
                'filenumber',
                'startdate'
            ],
            has_no_header=False,
        )

        create_user_list_from_replicon = rail.CreateCollectionOperator(
            task_id='create_user_list_from_replicon',
            source="{{ result('load_user_list_csv_from_replicon') }}",
            name="userlistfromreplicon",
        )

        query_list_users_from_replicon_with_file_and_correct_file = rail.QueryCollectionOperator(
            task_id='query_list_users_from_replicon_with_file_and_correct_file',
            query="""SELECT * FROM  userlistfromreplicon WHERE (NULLIF(filenumber,'') IS NOT NULL)""",
        )

        create_csv_lines_for_validated_user_list_from_replicon = rail.LoadCSVFileOperator(
            task_id="create_csv_lines_for_validated_user_list_from_replicon",
            document="{{ result('query_list_users_from_replicon_with_file_and_correct_file') }}",
        )

        create_validated_user_list_from_replicon = rail.CreateCollectionOperator(
            task_id='create_validated_user_list_from_replicon',
            source="{{ result('query_list_users_from_replicon_with_file_and_correct_file') }}",
            name="validateduserlistfromreplicon",
        )

        query_list_enabled_users_from_replicon_with_file = rail.QueryCollectionOperator(
            task_id='query_list_enabled_users_from_replicon_with_file',
            query="""SELECT * FROM  userlistfromreplicon WHERE enabled = 'Enabled' AND (NULLIF(filenumber,'') IS NOT NULL)""",
            name="enableduserlistfromreplicon"
        )

        query_list_enabled_users_from_replicon_without_file = rail.QueryCollectionOperator(
            task_id='query_list_enabled_users_from_replicon_without_file',
            query="""SELECT * FROM  userlistfromreplicon WHERE enabled = 'Enabled' AND (NULLIF(filenumber,'') IS NULL)""",
        )

        query_list_validated_users_to_be_disabled = rail.QueryCollectionOperator(
            task_id='query_list_validated_users_to_be_disabled',
            query="""SELECT * FROM  enableduserlistfromreplicon WHERE filenumber NOT IN (SELECT file FROM validatedinputlist)""",
        )

        ##
        # if_users_to_disable_less_than_or_equal_to_disablethreshold = rail.IfOperator(
        #     task_id='if_users_to_disable_less_than_or_equal_to_disablethreshold',
        #     test=lambda: rail.result(
        #         'query_list_validated_users_to_be_disabled', 'length') <= config.disable_threshold,
        #     yes_task="trigger_dag_run_fujifilmdbtl_child_workflow_to_disable_user",
        #     no_task="rename_archive_input_file_skipped_2",
        # )

        # rename_archive_input_file_skipped_2 = rail.SFTPMoveFileOperator(
        #     task_id='rename_archive_input_file_skipped_2',
        #     existing_filename='''{{ result('new_file_sensor') }}''',
        #     new_filename=config.archive_filepath +
        #     '/Skipped_{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        # )

        # send_mail_for_number_of_users_to_be_disabled_more_than_the_threshold = rail.EmailOperator(
        #     task_id='send_mail_for_number_of_users_to_be_disabled_more_than_the_threshold',
        #     to=config.tenant_email,
        #     bcc=config.internal_logs_email,
        #     subject='{{ get_company_key() }} | User import has been skipped - {{ current_time_in_specified_tz("' + config.time_zone + '") }} ',
        #     html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import for the file name "{{ result('new_file_sensor') | file_name }}" is skipped, since the number of disabled users is more than "{{ params.threshold_count }}" in the file. Please correct the feed file and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
        #     params={
        #         'threshold_count' : config.disable_threshold
        #         },
        # )

        # trigger_dag_run_fujifilmdbtl_child_workflow_to_disable_user = rail.TriggerDagRunForEachItemOperator(
        #     task_id='trigger_dag_run_fujifilmdbtl_child_workflow_to_disable_user',
        #     retries=0,
        #     items=lambda: rail.result(
        #         'query_list_validated_users_to_be_disabled'),
        #     trigger_dag_id=f'fujifilmdbtl_child_disable_user_{config.instance}',
        #     execution_timeout=timedelta(days=config.execution_timeout_days),
        #     accumulate_result=True,
        #     conf=lambda item: {
        #         "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
        #         "userloginname": item['username'],
        #         "useruri": item['useruri'],
        #         "startdate": datetime.strptime(item['startdate'], "%B %d, %Y").strftime("%d/%m/%Y"),
        #         "username": item['username'],
        #         "emplid": item['employeeid'],
        #         "userimportlogtable": rail.result('fdt_user_import_logs'),
        #     }
        # )

        # wait_for_child_disable_user = rail.WaitForDagRunsSensor(
        #     task_id='wait_for_child_disable_user',
        #     execution_timeout=timedelta(days=config.execution_timeout_days),
        #     dag_runs='{{ result("trigger_dag_run_fujifilmdbtl_child_workflow_to_disable_user") }}'
        # )

        validated_new_users_to_process_with_ee_status_not_as_a = rail.QueryCollectionOperator(
            task_id='validated_new_users_to_process_with_ee_status_not_as_a',
            query="""SELECT * FROM  validatedinputlist WHERE file NOT IN (SELECT DISTINCT filenumber FROM  validateduserlistfromreplicon) AND eestatus!='A' """,
        )

        if_validated_new_users_to_process_with_ee_status_not_as_a_greater_than_0 = rail.IfOperator(
            task_id='if_validated_new_users_to_process_with_ee_status_not_as_a_greater_than_0',
            test='''{{ result('validated_new_users_to_process_with_ee_status_not_as_a', 'length') > 0 }}''',
            yes_task="fdt_user_import_logs_add_entries_2",
            no_task="validated_new_users_to_process_with_ee_status_as_a",
        )

        fdt_user_import_logs_add_entries_2 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entries_2',
            log="{{ result('fdt_user_import_logs') }}",
            items=lambda: rail.result(
                'validated_new_users_to_process_with_ee_status_not_as_a'),
            message="na",
            severity="na",
            properties=lambda item: {
                "parentjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "username": item['firstname']+item['lastname'],
                "loginname": rail.find_first_by_attr_and_get_attr(rail.result('query_create_list_adfile_data'), 'filenumber', item['file'], 'loginname', ""),
                "emplid": item['emplid'],
                "action": "Add",
                "status": "Skipped",
                "details": "Ee Status in feed file not equals 'A' for new user",
            }
        )

        validated_new_users_to_process_with_ee_status_as_a = rail.QueryCollectionOperator(
            task_id='validated_new_users_to_process_with_ee_status_as_a',
            query="""SELECT * FROM  validatedinputlist WHERE file NOT IN (SELECT DISTINCT filenumber FROM  validateduserlistfromreplicon) AND eestatus='A' """,
            name='final_list_for_add_user'
        )

        join_collections = rail.QueryCollectionOperator(
            task_id='join_collections',
            query='''SELECT final_list_for_add_user.*, adfiledata.loginname FROM final_list_for_add_user JOIN adfiledata ON final_list_for_add_user.file = adfiledata.filenumber'''
        )

        trigger_child_workflow_to_add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_workflow_to_add_user',
            retries=0,
            items=lambda: rail.result('join_collections'),
            trigger_dag_id=f'fujifilmdbtl_child_add_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "emplid": item['emplid'] if item['emplid'] else " ",
                "loginname": (item['loginname']).lower() if item['loginname'] else "",
                "file": item['file'].replace(",", "") if item['file'] else " ",
                "paygroup": item['paygroup'] if item['paygroup'] else " ",
                "lastname": item['lastname'],
                "firstname": item['firstname'],
                "email": item['email'],
                "annualsalary": item['annualsalary'].replace(",", "") if item['annualsalary'] else 0,
                "eestatus": item['eestatus'],
                "servicedate": item['servicedate'],
                "rehiredate": item['rehiredate'],
                "eetype": item['eetype'].lower() if item['paygroup'] else "",
                "deptid": item['deptid'],
                "department": item['department'],
                "autolinkratetype": item['autolinkratetype'].replace(",", "") if item['autolinkratetype'] else 0,
                "regulartemporary": item['regulartemporary'].lower() if item['regulartemporary'] else "",
                "fullparttime": item['fullparttime'].lower() if item['fullparttime'] else "",
                "managerid": item['managerid'].replace(",", "") if item['managerid'] else 0,
                "reporttoname": item['reporttoname'],
                "company": item['company'].lower() if item['company'] else "",
                "hourlyratejobdata": item['hourlyratejobdata'].replace(",", "") if item['hourlyratejobdata'] else "",
                "jobtitle": item['jobtitle'] if item['jobtitle'] else " ",
                "assignedshift": item['shiftassignment'] if item['shiftassignment'] else "1",
                "hourlyrate2": item['hourlyrate2'].replace(",", "") if item['hourlyrate2'] else "",
                "userimportlogtable": rail.result('fdt_user_import_logs'),
                "supervisorassignmentlookuptable": rail.result('fdt_supervisor_assignment_lookup_table'),
                "parentjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_child_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_workflow_to_add_user") }}'
        )

        create_list_update_users_to_process = rail.QueryCollectionOperator(
            task_id='create_list_update_users_to_process',
            query="""SELECT * FROM  validatedinputlist WHERE file IN (SELECT DISTINCT filenumber FROM validateduserlistfromreplicon)""",
            name="updateuserslist",
        )

        if_updateuserslist_row_count_greater_than_1 = rail.IfOperator(
            task_id='if_updateuserslist_row_count_greater_than_1',
            test='''{{ result('create_list_update_users_to_process', 'length') > 0 }}''',
            yes_task="new_file_sensor_referencefile",
            no_task="fujifilmdbtl_supervisor_assignment_logs_search_entries",
        )

        new_file_sensor_referencefile = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_referencefile',
            path=config.reference_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        if_usereferencefile_contains_yes = rail.IfOperator(
            task_id='if_usereferencefile_contains_yes',
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='false').lower() == 'true',
            yes_task="download_reference_file",
            no_task="if_usereferencefile_contains_no",
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor_referencefile') }}"
        )

        load_reference_file_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_file_csv",
            document="{{result('download_reference_file')}}",
        )

        create_userreferencedata_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_userreferencedata_list_from_csv',
            source="{{ result('load_reference_file_csv') }}",
            name="userreferencedata",
            columns={
                'Emplid': 'emplid',
                'File': 'file',
                'Paygroup': 'paygroup',
                'Lastname': 'lastname',
                'Firstname': 'firstname',
                'Email': 'email',
                'Annualsalary': 'annualsalary',
                'Eestatus': 'eestatus',
                'Servicedate': 'servicedate',
                'Rehiredate': 'rehiredate',
                'Eetype': 'eetype',
                'Deptid': 'deptid',
                'Department': 'department',
                'Autolinkratetype': 'autolinkratetype',
                'Regulartemporary': 'regulartemporary',
                'Fullparttime': 'fullparttime',
                'Managerid': 'managerid',
                'Reporttoname': 'reporttoname',
                'Company': 'company',
                'Hourlyratejobdata': 'hourlyratejobdata',
                'Jobtitle': 'jobtitle',
                'Shiftassignment': 'shiftassignment',
                'Hourlyrate': 'hourlyrate',
                'Encoded': 'encoded'
            }
        )

        query_list_userreferencedata_for_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_list_userreferencedata_for_unchanged_records',
            query="""SELECT * FROM  updateuserslist WHERE encoded IN (SELECT DISTINCT encoded FROM userreferencedata)""",
        )

        if_unchanged_records_greater_than_0 = rail.IfOperator(
            task_id='if_unchanged_records_greater_than_0',
            test='''{{ result('query_list_userreferencedata_for_unchanged_records', length) > 0 }}''',
            yes_task="fdt_user_import_logs_add_entries_3",
            no_task="query_list_userreferencedata_for_changed_records",
        )

        fdt_user_import_logs_add_entries_3 = rail.WriteLogOperator(
            task_id='fdt_user_import_logs_add_entries_3',
            log="{{ result('fdt_user_import_logs') }}",
            items=lambda: rail.result(
                'query_list_userreferencedata_for_unchanged_records'),
            message="na",
            severity="na",
            properties={
                "parentjobid": "{{ dag_run_ecid() }}",
                "username": "{{ item.firstname }} {{ item.lastname }}",
                "loginname": "{{ item.email }}",
                "emplid": "{{ item.emplid }}",
                "action": "Update",
                "status": "Skipped",
                "details": "No change in the user record",
            }
        )

        query_list_userreferencedata_for_changed_records = rail.QueryCollectionOperator(
            task_id='query_list_userreferencedata_for_changed_records',
            query="""SELECT * FROM  updateuserslist WHERE encoded NOT IN (SELECT DISTINCT encoded FROM userreferencedata)""",
            name='changed_records'
        )

        query_final_list_for_update_user_with_reference_file = rail.QueryCollectionOperator(
            task_id='query_final_list_for_update_user_with_reference_file',
            query="""SELECT changed_records.* , userlistfromreplicon.useruri , adfiledata.loginname \
                FROM changed_records \
                JOIN userlistfromreplicon ON changed_records.file = userlistfromreplicon.filenumber \
                JOIN adfiledata ON changed_records.file = adfiledata.filenumber"""
        )

        trigger_child_workflow_to_update_user_using_reference_file = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_workflow_to_update_user_using_reference_file',
            retries=0,
            items=lambda: rail.result(
                'query_final_list_for_update_user_with_reference_file'),
            trigger_dag_id=f'fujifilmdbtl_child_update_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "emplid": item['emplid'] if item['emplid'] else "",
                "loginname": (item['loginname']).lower() if item['loginname'] else "",
                "file": item['file'].replace(",", "") if item['file'] else "",
                "paygroup": item['paygroup'] if item['paygroup'] else "",
                "lastname": item['lastname'] if item['lastname'] else "",
                "firstname": item['firstname'] if item['firstname'] else "",
                "email": item['email'] if item['email'] else "",
                "annualsalary": item['annualsalary'].replace(",", "") if item['annualsalary'] else "",
                "eestatus": item['eestatus'] if item['eestatus'] else "",
                "servicedate": item['servicedate'] if item['servicedate'] else "",
                "rehiredate": item['rehiredate'] if item['rehiredate'] else "",
                "eetype": item['eetype'].lower() if item['paygroup'] else "",
                "deptid": item['deptid'] if item['deptid'] else "",
                "department": item['department'] if item['department'] else "",
                "autolinkratetype": item['autolinkratetype'].replace(",", "") if item['autolinkratetype'] else "",
                "regulartemporary": item['regulartemporary'].lower() if item['regulartemporary'] else "",
                "fullparttime": item['fullparttime'].lower() if item['fullparttime'] else "",
                "managerid": item['managerid'].replace(",", "") if item['managerid'] else "",
                "reporttoname": item['reporttoname'],
                "company": item['company'] if item['company'] else "",
                "hourlyratejobdata": item['hourlyratejobdata'].replace(",", "") if item['hourlyratejobdata'] else "",
                "jobtitle": item['jobtitle'],
                "useruri": item['useruri'] if item['useruri'] else "",
                "assignedshift": item['shiftassignment'] if item['shiftassignment'] else "1",
                "hourlyrate2": item['hourlyrate2'].replace(",", "") if item['hourlyrate2'] else "",
                "userimportlogtable": rail.result('fdt_user_import_logs'),
                "supervisorassignmentlookuptable": rail.result('fdt_supervisor_assignment_lookup_table'),
                "parentjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_child_update_user_using_reference_file = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_update_user_using_reference_file',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_workflow_to_update_user_using_reference_file") }}'
        )

        if_usereferencefile_contains_no = rail.IfOperator(
            task_id='if_usereferencefile_contains_no',
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='false').lower() == 'false',
            yes_task="query_final_list_for_update_user_without_reference_file",
            no_task="fujifilmdbtl_supervisor_assignment_logs_search_entries",
        )

        query_final_list_for_update_user_without_reference_file = rail.QueryCollectionOperator(
            task_id='query_final_list_for_update_user_without_reference_file',
            query="""SELECT updateuserslist.* , userlistfromreplicon.useruri , adfiledata.loginname
                FROM updateuserslist
                JOIN userlistfromreplicon ON updateuserslist.file = userlistfromreplicon.filenumber
                JOIN adfiledata ON updateuserslist.file = adfiledata.filenumber"""
        )

        trigger_child_workflow_to_update_user_not_using_reference_file = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_workflow_to_update_user_not_using_reference_file',
            retries=0,
            items=lambda: rail.result(
                'query_final_list_for_update_user_without_reference_file'),
            trigger_dag_id=f'fujifilmdbtl_child_update_user_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "emplid": item['emplid'] if item['emplid'] else "",
                "loginname":  (item['loginname']).lower() if item['loginname'] else "",
                "file": item['file'].replace(",", "") if item['file'] else "",
                "paygroup": item['paygroup'] if item['paygroup'] else "",
                "lastname": item['lastname'] if item['lastname'] else "",
                "firstname": item['firstname'] if item['firstname'] else "",
                "email": item['email'] if item['email'] else "",
                "annualsalary": item['annualsalary'].replace(",", "") if item['annualsalary'] else "",
                "eestatus": item['eestatus'] if item['eestatus'] else "",
                "servicedate": item['servicedate'] if item['servicedate'] else "",
                "rehiredate": item['rehiredate'] if item['rehiredate'] else "",
                "eetype": item['eetype'].lower() if item['paygroup'] else "",
                "deptid": item['deptid'],
                "department": item['department'] if item['department'] else "",
                "autolinkratetype": item['autolinkratetype'].replace(",", "") if item['autolinkratetype'] else "",
                "regulartemporary": item['regulartemporary'].lower() if item['regulartemporary'] else "",
                "fullparttime": item['fullparttime'].lower() if item['fullparttime'] else "",
                "managerid": item['managerid'].replace(",", "") if item['managerid'] else "",
                "reporttoname": item['reporttoname'],
                "company": item['company'] if item['company'] else "",
                "hourlyratejobdata": item['hourlyratejobdata'].replace(",", "") if item['hourlyratejobdata'] else "",
                "jobtitle": item['jobtitle'],
                "useruri":  item['useruri'] if item['useruri'] else "",
                "assignedshift": item['shiftassignment'] if item['shiftassignment'] else "1",
                "hourlyrate2": item['hourlyrate2'].replace(",", "") if item['hourlyrate2'] else "",
                "userimportlogtable": rail.result('fdt_user_import_logs'),
                "supervisorassignmentlookuptable": rail.result('fdt_supervisor_assignment_lookup_table'),
                "parentjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_child_update_user_not_using_reference_file = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_update_user_not_using_reference_file',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_workflow_to_update_user_not_using_reference_file") }}'
        )

        fujifilmdbtl_supervisor_assignment_logs_search_entries = rail.FilterLogEntriesOperator(
            task_id='fujifilmdbtl_supervisor_assignment_logs_search_entries',
            log="{{result('fdt_supervisor_assignment_lookup_table')}}",
            properties={
                "parentjobid": "{{dag_run_ecid()}}"
            },
            remove_filtered_entries=True
        )

        if_supervisor_assignment_logs_present = rail.IfOperator(
            task_id='if_supervisor_assignment_logs_present',
            test='''{{ result('fujifilmdbtl_supervisor_assignment_logs_search_entries','length') > 0 | is_truthy }}''',
            yes_task="trigger_child_supervisor_assignment",
            no_task="rename_archive_input_file_5",
        )

        trigger_child_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_supervisor_assignment',
            retries=0,
            items=lambda:  rail.result(
                'fujifilmdbtl_supervisor_assignment_logs_search_entries'),
            trigger_dag_id=f'fujifilmdbtl_child_supervisor_assignment_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['properties']['userloginname'],
                "username": item['properties']['user_name'],
                "supervisorloginname": item['properties']['supervisorloginname'],
                "parentjobid": item['properties']['parentjobid'],
                "childjobid": item['properties']['childjobid'],
                "useruri": item['properties']['user_uri'],
                "action": item['properties']['action'],
                "employeeid": item['properties']['emplid'],
                "supervisorid": item['properties']['supervisor_id'],
                "userimportlogtable": rail.result('fdt_user_import_logs'),
                "supervisorassignmentlookuptable": rail.result('fdt_supervisor_assignment_lookup_table')
            }
        )

        waitfor_child_assign_supervisor = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_assign_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_supervisor_assignment") }}'
        )

        rename_archive_input_file_5 = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file_5',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '/{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        dir_get_the_reference_file_details = rail.SFTPAnyFileSensor(
            task_id='dir_get_the_reference_file_details',
            sftp_conn_id=config.sftp_conn_id,
            path=config.reference_filepath
        )

        rename_archive_reference_input_file = rail.SFTPMoveFileOperator(
            task_id='rename_archive_reference_input_file',
            new_filename=config.archive_filepath +
            "/Old_{{ result('log_formatted_job_start_time') }}_{{ result('dir_get_the_reference_file_details') | file_name }}",
            existing_filename=config.reference_filepath +
            "/{{ result('dir_get_the_reference_file_details') | file_name }}"
        )

        upload_reference_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_reference_file_to_sftp',
            content='''{{ result('create_csv_lines_from_input_file') }}''',
            remote_filepath=config.reference_filepath +
            '/reference_{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        fujifilmdbtl_user_import_logs_filter_entries = rail.FilterLogEntriesOperator(
            task_id='fujifilmdbtl_user_import_logs_filter_entries',
            log="{{result('fdt_user_import_logs')}}",
            properties={
                "parentjobid": "{{dag_run_ecid()}}"
            }
        )

        create_csv_lines_for_logs = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_logs',
            source=lambda: rail.result(
                'fujifilmdbtl_user_import_logs_filter_entries'),
            header=[
                'username',
                'loginname',
                'employeeid',
                'importaction',
                'status',
                'details',
                'jobid'
            ],
            row=[
                "{{ item.properties.username }}",
                "{{ item.properties.loginname }}",
                "{{ item.properties.emplid }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ],
        )

        upload_log_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_file_to_sftp',
            sftp_conn_id=config.sftp_conn_id,
            content='''{{ result('create_csv_lines_for_logs') }}''',
            remote_filepath=config.log_filepath +
            '/log_{{ result("log_formatted_job_start_time") }}_{{ result("new_file_sensor") | file_name }}'
        )

        if_upload_to_sftp_failed = rail.IfOperator(
            task_id='if_upload_to_sftp_failed',
            trigger_rule='one_failed',
            test='{{ get_task_state("upload_log_file_to_sftp") == "failed" }}',
            yes_task='send_mail_logs_sftp_upload_failed',
        )

        send_mail_logs_sftp_upload_failed = rail.EmailOperator(
            task_id='send_mail_logs_sftp_upload_failed',
            to=config.alert_email,
            subject='{{ get_company_key() }} | User Import - Uploading Logs to SFTP failed - {{ current_time_in_specified_tz("' + config.time_zone + '") }} ',
            html_content='''<p>Hi Team,<br /> <br /> The user import for {{ get_company_key() }} instance,
                has been completed for file "{{ result('new_file_sensor') | file_name }}", 
                however, the log upload to sftp has failed. Attached is the log file for reference.</p>
                <p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.
                <br /> <br /> Regards,<br /> Replicon Integrations Team</p>''',
            files=[
                ("log_{{ result('log_formatted_job_start_time') }}_{{ result('new_file_sensor') | file_name }}.csv",
                 '{{result("create_csv_lines_for_logs")}}')
            ]
        )

        check_error_in_final_user_import_logs = rail.FilterLogEntriesOperator(
            task_id='check_error_in_final_user_import_logs',
            log="{{result('fdt_user_import_logs')}}",
            properties={
                "parentjobid": "{{dag_run_ecid()}}",
                "status": "Error"
            }
        )

        check_exception_in_final_user_import_logs = rail.FilterLogEntriesOperator(
            task_id='check_exception_in_final_user_import_logs',
            log="{{result('fdt_user_import_logs')}}",
            properties={
                "parentjobid": "{{dag_run_ecid()}}",
                "status": "Exception"
            }
        )

        check_skipped_in_final_user_import_logs = rail.FilterLogEntriesOperator(
            task_id='check_skipped_in_final_user_import_logs',
            log="{{result('fdt_user_import_logs')}}",
            properties={
                "parentjobid": "{{dag_run_ecid()}}",
                "status": "Skipped"
            }
        )

        len_error_exception_skipped_records = rail.PythonOperator(
            task_id='len_error_exception_skipped_records',
            python_callable=lambda: int(rail.result('check_error_in_final_user_import_logs', 'length') + rail.result(
                'check_error_in_final_user_import_logs', 'length') + rail.result('check_error_in_final_user_import_logs', 'length'))
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('check_error_in_final_user_import_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User Sync Complete, Please Review Logs for " }} \
                {%- if result("len_error_exception_skipped_records") > 0 -%} \
                    skipped/exceptions/error records (if any) \
                {%- else -%} \
                    details  \
                {%- endif -%} \
                {{ " | " + current_time_in_specified_tz("' + config.time_zone + '") }}',
            params={
                'log_filepath': config.log_filepath,
            },
            html_content="templates/emails/import_complete_mail.html",
        )

        can_fail_dag = rail.IfOperator(
            task_id='can_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        new_file_sensor >> file_name_incorrect >> rail.Label('No') >> log_formatted_job_start_time \
            >> if_name_not_ends_with_csv >> rail.Label('No') >> fdt_user_import_logs >> fdt_supervisor_assignment_lookup_table  \
            >> download_input_csv_file >> parse_input_csv_file >> if_input_csv_file_records_less_than_1 >> rail.Label('No') >> create_csv_lines_from_input_file \
            >> load_input_csv_file >> create_input_file_list_from_input_csv_file >> query_get_data_from_collection >> query_get_user_with_blank_file_or_incorrect_file \
            >> if_query_list_get_user_with_blank_file_or_incorrect_file_greater_than_0 >> rail.Label('No') \
            >> query_get_user_without_blank_or_incorrect_file >> create_validated_input_list >> new_file_sensor_adfile >> adfile_check >> rail.Label('Yes') >> download_adfile_from_sftp \
            >> load_adfile_csv >> create_collection_from_adfile_csv >> query_list_check_if_any_user_is_not_available_in_adfile \
            >> if_query_list_check_if_any_user_is_not_available_in_adfile_rows_greater_than_0 >> rail.Label('No') >> query_create_list_adfile_data >> get_report_details \
            >> run_report_to_extract_user_list_from_replicon >> if_generate_report_has_error >> rail.Label('No') >> load_user_list_csv_from_replicon >> create_user_list_from_replicon \
            >> query_list_users_from_replicon_with_file_and_correct_file >> create_csv_lines_for_validated_user_list_from_replicon \
            >> create_validated_user_list_from_replicon >> query_list_enabled_users_from_replicon_with_file >> query_list_enabled_users_from_replicon_without_file \
            >> query_list_validated_users_to_be_disabled

        ##
        # query_list_validated_users_to_be_disabled >> if_users_to_disable_less_than_or_equal_to_disablethreshold
        # if_users_to_disable_less_than_or_equal_to_disablethreshold >> rail.Label('Yes') >> trigger_dag_run_fujifilmdbtl_child_workflow_to_disable_user \
        # >> wait_for_child_disable_user >> validated_new_users_to_process_with_ee_status_not_as_a

        ## Below line need to be removed
        query_list_validated_users_to_be_disabled >> validated_new_users_to_process_with_ee_status_not_as_a
        
        validated_new_users_to_process_with_ee_status_not_as_a >> if_validated_new_users_to_process_with_ee_status_not_as_a_greater_than_0 >> rail.Label('No') >> validated_new_users_to_process_with_ee_status_as_a >> join_collections \
            >> trigger_child_workflow_to_add_user >> wait_for_child_add_user >> create_list_update_users_to_process >> if_updateuserslist_row_count_greater_than_1 >> rail.Label('Yes') \
            >> new_file_sensor_referencefile >> if_usereferencefile_contains_yes >> rail.Label('Yes') >> download_reference_file >> load_reference_file_csv >> create_userreferencedata_list_from_csv \
            >> query_list_userreferencedata_for_unchanged_records >> if_unchanged_records_greater_than_0 >> rail.Label('No') >> query_list_userreferencedata_for_changed_records >> query_final_list_for_update_user_with_reference_file >> trigger_child_workflow_to_update_user_using_reference_file \
            >> wait_for_child_update_user_using_reference_file >> if_usereferencefile_contains_no >> rail.Label('Yes') >> query_final_list_for_update_user_without_reference_file >> trigger_child_workflow_to_update_user_not_using_reference_file \
            >> wait_for_child_update_user_not_using_reference_file >> fujifilmdbtl_supervisor_assignment_logs_search_entries >> if_supervisor_assignment_logs_present >> rail.Label('Yes') >> trigger_child_supervisor_assignment >> waitfor_child_assign_supervisor \
            >> rename_archive_input_file_5 >> dir_get_the_reference_file_details >> rename_archive_reference_input_file >> upload_reference_file_to_sftp \
            >> fujifilmdbtl_user_import_logs_filter_entries >> create_csv_lines_for_logs >> upload_log_file_to_sftp

        upload_log_file_to_sftp >> check_error_in_final_user_import_logs >> check_exception_in_final_user_import_logs \
            >> check_skipped_in_final_user_import_logs >> len_error_exception_skipped_records >> send_import_complete_email >> can_fail_dag
        upload_log_file_to_sftp >> if_upload_to_sftp_failed

        file_name_incorrect >> rail.Label(
            'Yes') >> rename_archive_input_file >> can_fail_dag
        if_name_not_ends_with_csv >> rail.Label(
            'Yes') >> send_mail_for_incorrect_file_format >> rename_archive_input_file_skipped >> can_fail_dag
        if_input_csv_file_records_less_than_1 >> rail.Label(
            'Yes') >> send_mail_for_no_records >> rename_archive_input_file_2 >> can_fail_dag
        if_query_list_get_user_with_blank_file_or_incorrect_file_greater_than_0 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entries >> query_get_user_without_blank_or_incorrect_file
        adfile_check >> rail.Label(
            'No') >> send_no_adfile_mail >> rename_archive_input_file_3 >> can_fail_dag
        if_query_list_check_if_any_user_is_not_available_in_adfile_rows_greater_than_0 >> rail.Label(
            'Yes') >> create_csv_for_user_missing_logs >> upload_user_missing_log_file >> send_mail_notification_for_user_missing_in_adfile >> rename_archive_input_file_4 >> can_fail_dag
        if_generate_report_has_error >> rail.Label('Yes') >> can_fail_dag

        ##
        # if_users_to_disable_less_than_or_equal_to_disablethreshold >> rail.Label(
        #     'No') >> rename_archive_input_file_skipped_2 >> send_mail_for_number_of_users_to_be_disabled_more_than_the_threshold >> can_fail_dag

        if_validated_new_users_to_process_with_ee_status_not_as_a_greater_than_0 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entries_2 >> validated_new_users_to_process_with_ee_status_as_a
        if_updateuserslist_row_count_greater_than_1 >> rail.Label(
            'No') >> fujifilmdbtl_supervisor_assignment_logs_search_entries
        if_usereferencefile_contains_yes >> rail.Label(
            'No') >> if_usereferencefile_contains_no
        if_unchanged_records_greater_than_0 >> rail.Label(
            'Yes') >> fdt_user_import_logs_add_entries_3 >> query_list_userreferencedata_for_changed_records
        if_usereferencefile_contains_no >> rail.Label(
            'No') >> fujifilmdbtl_supervisor_assignment_logs_search_entries
        if_supervisor_assignment_logs_present >> rail.Label(
            'No') >> rename_archive_input_file_5

        if_upload_to_sftp_failed >> rail.Label(
            'Yes') >> send_mail_logs_sftp_upload_failed >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dag

    return dag


rail.for_each_instance(create_main_dag)
