from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from allenphilip.project_rate_update.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'allenphilip_project_rate_update_master_{config.instance}',
        description=f'Allenphilp_project_rate_update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        if_name_downcase_ends_with_csv = rail.IfOperator(
            task_id='if_name_downcase_ends_with_csv',
            test="{{result('new_file_sensor') | file_name | lower | ends_with('.csv') }}",
            yes_task="log_current_date",
            no_task="finish",
        )

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable=python_callable.get_current_date
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor')}}",
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            headers=["LoginID"],
            delimiter=',',
            document="{{ result('download_file') }}",
        )

        if_csv_has_data_present = rail.IfOperator(
            task_id='if_csv_has_data_present',
            test="{{result('parse_csv') | load_all_records | length > 0 }}",
            yes_task="get_report_details",
            no_task="if_loginname_not_present",
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_data_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report_user_data',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_report_user_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_all_users_list = rail.CreateCollectionOperator(
            task_id='create_all_users_list',
            source="{{ result('load_report_data')}}",
            name="allusers",
            columns={
                'Login Name': 'loginname',
                'User Name': 'username',
                'User Default Billing Rate': 'defaultbillingrate',
                'UserUri': 'useruri',
                'UserHourlyBilling__amount': 'hourlybillingamount',
            }
        )

        load_csv_input_list = rail.LoadCSVFileOperator(
            task_id="load_csv_input_list",
            document="{{result('download_file')}}",
        )

        create_input_file_list = rail.CreateCollectionOperator(
            task_id='create_input_file_list',
            source="{{ result('load_csv_input_list')}}",
            name="inputfile",
            columns={
                'LoginID': 'loginname',
            }
        )

        query_list_eligible_users = rail.QueryCollectionOperator(
            task_id='query_list_eligible_users',
            query="""SELECT allusers.loginname, allusers.username, allusers.defaultbillingrate, allusers.useruri,
            allusers.hourlybillingamount, inputfile.* FROM allusers INNER JOIN inputfile ON
            allusers.loginname=inputfile.loginname WHERE NOT allusers.hourlybillingamount='0.00'""",
        )

        if_eligible_users_present = rail.IfOperator(
            task_id='if_eligible_users_present',
            test="{{result('query_list_eligible_users') | length > 0 }}",
            yes_task="query_list_eligibleusers_to_csv",
            no_task="query_list_ineligible_users",
        )

        query_list_eligibleusers_to_csv = rail.QueryCollectionOperator(
            task_id='query_list_eligibleusers_to_csv',
            query="""SELECT allusers.loginname, allusers.username, allusers.defaultbillingrate,
            allusers.useruri, allusers.hourlybillingamount, inputfile.* FROM Allusers INNER JOIN inputfile ON
            allusers.loginname=inputfile.loginname WHERE NOT allusers.hourlybillingamount='0.00'""",
        )

        get_project_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_project_report_details',
            report_name=config.project_data_report_name,

        )

        project_report_details = rail.RepliconServiceOperator(
            task_id='project_report_details',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{result('get_project_report_details').uri}}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        load_project_report_details = rail.LoadCSVFileOperator(
            task_id='load_project_report_details',
            document="{{result('project_report_details').payload}}",
        )

        create_projects_list = rail.CreateCollectionOperator(
            task_id='create_projects_list',
            source="{{ result('load_project_report_details')}}",
            name="projects",
            columns={
                "Project Name": "projectname",
                "Billing Type": "billingtype",
                "Billing Rate Time Period": "billingtimeperiod",
                "Time & Expense Entry Type": "timeandexpenseentrytype",
                "Project Status": "projectstatus",
                "ProjectUri": "projecturi"
            }
        )

        query_eligible_projects = rail.QueryCollectionOperator(
            task_id='query_eligible_projects',
            query="""SELECT * FROM projects WHERE billingtimeperiod='Hourly' AND projectstatus='In Progress'""",
        )

        allen_philip_lookup_table = rail.CreateLogOperator(
            task_id='allen_philip_lookup_table'
        )

        allen_log_lookuptable = rail.CreateLogOperator(
            task_id='allen_log_lookuptable'
        )

        process_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child',
            retries=0,
            items="{{result('query_eligible_projects')}}",
            trigger_dag_id=f'allenphilip_project_rate_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "lookup_table": rail.result('allen_philip_lookup_table'),
                "projectname": item['projectname'],
                "projecturi": item['projecturi'],
                "billingtype": item['billingtype'],
                "billingtimeperiod": item['billingtimeperiod'],
                "timeandexpenseentrytype": item['timeandexpenseentrytype'],
                "projectstatus": item['projectstatus'],
                "userdata": rail.load_all_records(rail.result('query_list_eligibleusers_to_csv')),
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "user_logtable": rail.result('allen_log_lookuptable'),
            }
        )

        if_need_to_wait = rail.IfOperator(
            task_id='if_need_to_wait',
            test="{{result('process_child') | is_truthy}}",
            yes_task="wait_for_process_child",
            no_task="stop_job_if_no_child_present",
        )

        stop_job_if_no_child_present = rail.EmptyOperator(
            task_id='stop_job_if_no_child_present'
        )

        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_child") }}'
        )

        query_list_ineligible_users = rail.QueryCollectionOperator(
            task_id='query_list_ineligible_users',
            query="""SELECT allusers.loginname, allusers.username, allusers.defaultbillingrate,
            allusers.useruri, allusers.hourlybillingamount, inputfile.* FROM allusers INNER JOIN inputfile ON
            allusers.loginname=inputfile.loginname WHERE allusers.hourlybillingamount='0.00'""",

        )

        add_entry_for_ineligible_users = rail.WriteLogOperator(
            task_id='add_entry_for_ineligible_users',
            log="{{result('allen_philip_lookup_table')}}",
            items="{{result('query_list_ineligible_users')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "jobid": "{{dag_run_ecid()}}",
                "projectname": "",
                "loginname": item["loginname"],
                "defaultbillingrate": item["defaultbillingrate"],
                "status": "Ignored",
                "details": "Default Billing Rate in User Profile is set to " + item['defaultbillingrate'],
                "childjobid": "",
            }
        )

        query_list_nonexistent_users = rail.QueryCollectionOperator(
            task_id='query_list_nonexistent_users',
            query="""SELECT * FROM  inputfile WHERE  inputfile.loginname NOT IN (SELECT  allusers.loginname FROM  allusers)""",
        )

        add_entry_for_nonexistent_users = rail.WriteLogOperator(
            task_id='add_entry_for_nonexistent_users',
            log="{{result('allen_philip_lookup_table')}}",
            items="{{result('query_list_nonexistent_users')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "jobid": "{{dag_run_ecid()}}",
                "projectname": "",
                "loginname": item["loginname"],
                "defaultbillingrate": "",
                "status": "Ignored",
                "details": "User not found or in 'Disabled' status",
                "childjobid": "",
            }
        )

        search_entries_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_lookup_table',
            log="{{result('allen_philip_lookup_table')}}",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
            }
        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test="{{result('search_entries_in_lookup_table') | length > 0 | is_truthy }}",
            yes_task='get_logged_errors',
            no_task='if_entry_not_present'
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log="{{result('allen_philip_lookup_table')}}",
            severity='Error'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source=lambda: rail.result('search_entries_in_lookup_table'),
            delimiter=',',
            header=['jobid',
                    'Login Name',
                    'Project Name',
                    'Default Billing Rate',
                    'Status',
                    'Details'],
            row=lambda item: [
                item['properties']['jobid'] + "|" +
                item['properties']['childjobid'],
                item['properties']['loginname'],
                item['properties']['projectname'],
                item['properties']['defaultbillingrate'],
                item['properties']['status'],
                item['properties']['details'],
            ]
        )

        log_date = rail.PythonOperator(
            task_id='log_date',
            python_callable=lambda: datetime.now().strftime("%Y-%m-%dT%H:%M%S.%f")
        )

        upload_reference_s3_file = rail.S3UploadFileOperator(
            task_id='upload_reference_s3_file',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('create_csv_lines') }}",
            bucket_name=lambda: Variable.get(config.bucket_name),
            key_name=config.log_file_path +
            '{{dag_run_ecid()}}_{{result("new_file_sensor") | file_name}}_{{result("log_date")}}'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines')}}",
            output_file_name='{{dag_run_ecid()}}_{{result("new_file_sensor") | file_name}}',
            expires_in_seconds=7*24*60*60,
        )

        download_file_from_sftp = rail.SFTPDownloadFileOperator(
            task_id='download_file_from_sftp',
            remote_filepath=config.address_filepath +
            "{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}"
        )

        parse_toaddress = rail.LoadCSVFileOperator(
            task_id='parse_toaddress',
            document="{{ result('download_file_from_sftp') }}",
        )

        def get_address():
            result = rail.read_artifact(rail.result('parse_toaddress'))
            return result

        load_address = rail.PythonOperator(
            task_id='load_address',
            python_callable=get_address
        )

        if_get_logged_errors_has_data = rail.IfOperator(
            task_id='if_get_logged_errors_has_data',
            test="{{result('get_logged_errors' ,'length') > 0 }}",
            yes_task='send_import_complete_mail_with_error',
            no_task='send_import_complete_email'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to="{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Project Billing Rate update - Completed Successfully {{result("log_current_date")}} ',
            html_content="/templates/email/update_complete_mail.html",
            params={
                'filepath': config.log_file_path
            }
        )

        send_import_complete_mail_with_error = rail.EmailOperator(
            task_id='send_import_complete_mail_with_error',
            to="{{result('load_address')}}",
            bcc=config.alert_email,
            subject='{{get_company_key()}} | Project Billing Rate update - Completed with Errors {{result("log_current_date")}} ',
            html_content="/templates/email/update_complete_mail.html",
            params={
                'filepath': config.log_file_path
            }
        )

        if_entry_not_present = rail.IfOperator(
            task_id='if_entry_not_present',
            test="{{result('search_entries_in_lookup_table', 'length') == 0 }}",
            yes_task='send_no_item_processed_mail',
            no_task='if_loginname_not_present'

        )

        send_no_item_processed_mail = rail.EmailOperator(
            task_id='send_no_item_processed_mail',
            to="{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} | Project Billing Rate update - No items processed - Completed Successfully {{result("log_current_date")}} ',
            html_content="templates/email/no_data_process_mail.html",
        )

        if_loginname_not_present = rail.IfOperator(
            task_id='if_loginname_not_present',
            test="{{result('parse_csv') | load_all_records | length == 0 }}",
            yes_task='send_no_data_mail',
            no_task='archieve_input_file'
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to="{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} | Project Billing Rate update -  No data in file {{result("log_current_date")}}',
            html_content="templates/email/no_data_mail.html",
        )

        archieve_input_file = rail.SFTPMoveFileOperator(
            task_id='archieve_input_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archieve_filepath +
            "{{dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor')| file_name}}",
        )
        finish = rail.EmptyOperator(
            task_id='finish'
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        new_file_sensor >> if_name_downcase_ends_with_csv
        if_name_downcase_ends_with_csv >> rail.Label(
            'Yes') >> log_current_date >> download_file >> parse_csv
        parse_csv >> if_csv_has_data_present >> rail.Label(
            'Yes') >> get_report_details >> run_my_report_entry
        run_my_report_exit >> load_report_data >> create_all_users_list
        create_all_users_list >> load_csv_input_list >> create_input_file_list
        create_input_file_list >> query_list_eligible_users
        query_list_eligible_users >> if_eligible_users_present
        if_eligible_users_present >> rail.Label(
            'Yes') >> query_list_eligibleusers_to_csv >> get_project_report_details
        get_project_report_details >> project_report_details >> load_project_report_details
        load_project_report_details >> create_projects_list
        create_projects_list >> query_eligible_projects >> allen_philip_lookup_table >> allen_log_lookuptable
        allen_log_lookuptable >> process_child >> if_need_to_wait
        if_need_to_wait >> rail.Label('Yes') >> wait_for_process_child
        if_need_to_wait >> rail.Label('No') >> stop_job_if_no_child_present >> finish
        wait_for_process_child >> query_list_ineligible_users
        query_list_ineligible_users >> add_entry_for_ineligible_users
        add_entry_for_ineligible_users >> query_list_nonexistent_users >> add_entry_for_nonexistent_users
        add_entry_for_nonexistent_users >> search_entries_in_lookup_table >> if_entry_present
        if_entry_present >> rail.Label(
            'Yes') >> get_logged_errors >> create_csv_lines >> log_date >> upload_reference_s3_file >> generate_download_link
        generate_download_link >> download_file_from_sftp >> parse_toaddress
        parse_toaddress >> load_address >> if_get_logged_errors_has_data >> rail.Label(
            'Yes') >> send_import_complete_mail_with_error >> if_entry_not_present
        if_get_logged_errors_has_data >> rail.Label(
            'No') >> send_import_complete_email >> if_entry_not_present
        if_entry_present >> rail.Label(
            'No') >> if_entry_not_present
        if_entry_not_present >> rail.Label(
            'Yes') >> send_no_item_processed_mail >> if_loginname_not_present
        if_entry_not_present >> rail.Label(
            'No') >> if_loginname_not_present
        if_loginname_not_present >> rail.Label(
            'Yes') >> send_no_data_mail >> archieve_input_file
        if_loginname_not_present >> rail.Label(
            'No') >> archieve_input_file >> finish
        if_eligible_users_present >> rail.Label(
            'No') >> query_list_ineligible_users
        if_csv_has_data_present >> rail.Label(
            'No') >> if_loginname_not_present
        if_name_downcase_ends_with_csv >> rail.Label(
            'No') >> finish

        return dag


rail.for_each_instance(create_dag)
