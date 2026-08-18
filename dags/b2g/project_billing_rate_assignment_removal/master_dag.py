from datetime import timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'b2g_project_billing_rate_assignment_removal_master_{config.instance}',
        description=f'B2g_project_billing_rate_assignment_removal_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='download_file_from_sftp'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='download_file_from_sftp',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        download_file_from_sftp = rail.SFTPDownloadFileOperator(
            task_id='download_file_from_sftp',
            remote_filepath="{{result('new_file_sensor')}}"
        )

        download_file_from_address = rail.SFTPDownloadFileOperator(
            task_id='download_file_from_address',
            remote_filepath=config.new_filepath + '{{ result("new_file_sensor") | file_name | replace(".csv", ".txt")}}'
        )

        parse_toaddress = rail.LoadCSVFileOperator(
            task_id='parse_toaddress',
            headers=["toaddress"],
            delimiter=',',
            document="{{ result('download_file_from_address') }}",
        )

        def get_address():
            result = rail.read_artifact(rail.result('parse_toaddress'))
            return result

        load_address = rail.PythonOperator(
            task_id='load_address',
            python_callable=get_address
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file_from_sftp') }}",
            delimiter=','
        )

        create_billingrate_list = rail.CreateCollectionOperator(
            task_id='create_billingrate_list',
            source="{{ result('parse_csv') }}",
            name="billingrateinput",
            columns={
                'Login Name': 'loginname',
                'Project Name': 'projectname',
                'Billing Rate': 'billingrate',
                'Action': 'action',
            }
        )

        if_list_has_data = rail.IfOperator(
            task_id='if_list_has_data',
            test="{{result('create_billingrate_list' ,'length') < 1 }}",
            yes_task='send_import_skipped_mail',
            no_task='project_billing_rate_logtable'
        )

        send_import_skipped_mail = rail.EmailOperator(
            task_id='send_import_skipped_mail',
            to="{{result('load_address')}}",
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Billing Rate  import skipped - {{ current_time() }}',
            html_content="templates/emails/import_skipped_mail.html"
        )

        project_billing_rate_logtable = rail.CreateLogOperator(
            task_id='project_billing_rate_logtable'
        )

        b2g_joint_venture_billing_assignment_logs = rail.CreateLogOperator(
            task_id='b2g_joint_venture_billing_assignment_logs'
        )

        query_missing_mandatory_values = rail.QueryCollectionOperator(
            task_id='query_missing_mandatory_values',
            query="""SELECT * FROM billingrateinput WHERE NULLIF(loginname, '') IS NULL  OR NULLIF(projectname, '') IS NULL OR NULLIF(billingrate, '') IS NULL OR NULLIF(action, '') IS NULL""",
        )

        add_ignored_entries = rail.WriteLogOperator(
            task_id='add_ignored_entries',
            log="{{result('project_billing_rate_logtable')}}",
            items="{{result('query_missing_mandatory_values')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                'loginname': item['loginname'],
                'projectname': item['projectname'],
                'billingrate': item['billingrate'],
                'action': item['action'],
                'status': 'Ignored',
                'details': 'One or more mandatory field is missing',
                'jobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        query_records_mandatory_values = rail.QueryCollectionOperator(
            task_id='query_records_mandatory_values',
            query="""SELECT * FROM billingrateinput WHERE NULLIF(loginname, '') IS NOT NULL AND NULLIF(projectname, '') IS NOT NULL AND NULLIF(billingrate, '') IS NOT NULL AND NULLIF(action, '') IS NOT NULL """,
        )

        if_query_has_data_present = rail.IfOperator(
            task_id='if_query_has_data_present',
            test="{{result('query_records_mandatory_values','length') > 0}}",
            yes_task='get_report_details',
            no_task='archive_file'
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
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

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='{{result("run_report.get_report_result", "has_data") | is_truthy}}',
            yes_task="if_payload_has_no_columns",
            no_task="stop_job"
        )

        if_payload_has_no_columns = rail.IfOperator(
            task_id='if_payload_has_no_columns',
            test=lambda: not (('Login Name,User URI') in rail.result(
                'run_report.get_report_result')['reportGenerationResults'][0]['payload']),
            yes_task="stop_job_with_error",
            no_task="parse_csv_data",
        )

        parse_csv_data = rail.LoadCSVFileOperator(
            task_id='parse_csv_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload}}",
        )

        get_report_details_data = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_data',
            report_name=config.project_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report_data',
            report_params={
                "reportParameters": [
                    {
                     "reportUri": "{{ result('get_report_details_data').uri }}",
                     "filterValues": [],
                     "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_project_data = rail.LoadCSVFileOperator(
            task_id='parse_csv_project_data',
            document="{{ result('run_report_data.get_report_result').reportGenerationResults[0].payload}}",
        )

        load_parse_csv_project_data = rail.PythonOperator(
            task_id='load_parse_csv_project_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_project_data'))
        )

        load_parse_csv_data = rail.PythonOperator(
            task_id='load_parse_csv_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_data'))
        )

        def get_projecturi(item):
            record = rail.result('load_parse_csv_project_data')
            records_list = rail.find_first_by_attr_and_get_attr(
                record, 'Project Name', item['projectname'], 'project uri', null)
            return records_list

        def get_useruri(item):
            records = rail.result('load_parse_csv_data')
            user_list = rail.find_first_by_attr_and_get_attr(
                records, 'Login Name', item['loginname'], 'User URI', null)
            return user_list

        create_csv_for_records = rail.WriteCSVFileOperator(
            task_id='create_csv_for_records',
            source="{{ result('query_records_mandatory_values') }}",
            delimiter=',',
            header=['Login Name', 'Project Name',
                    'Billing Rate', 'Action', 'projecturi', 'useruri'],
            row=lambda item: [
                item['loginname'],
                item['projectname'],
                item['billingrate'],
                item['action'],
                get_projecturi(item),
                get_useruri(item)
            ]
        )

        create_merged_input_for_records = rail.CreateCollectionOperator(
            task_id='create_merged_input_for_records',
            source="{{ result('create_csv_for_records') }}",
            name="merged_input",
            columns={
                'Login Name': 'login_name',
                'Project Name': 'project_name',
                'Billing Rate': 'billing_rate',
                'Action': 'action',
                'projecturi': 'project_uri',
                'useruri': 'user_uri'
            }
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query="""SELECT * FROM merged_input WHERE  NULLIF(user_uri, '') IS NULL OR NULLIF(project_uri, '') IS NULL""",
        )

        add_ignored_entries_for_invalid_records = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_invalid_records',
            log="{{result('project_billing_rate_logtable')}}",
            items="{{result('query_invalid_records')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                'loginname': item['login_name'],
                'projectname': item['project_name'],
                'billingrate': item['billing_rate'],
                'action': item['action'],
                'status': 'Ignored',
                'details': 'User/Project is not available in Replicon',
                'jobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM merged_input WHERE NULLIF(project_uri, '') IS NOT NULL AND NULLIF(user_uri, '') IS NOT NULL """,
        )

        def get_loginname():
            records = rail.load_all_records(rail.result('query_valid_records'))
            if records and records[0]['login_name']:
                return False
            return True

        if_query_valid_records_has_no_data = rail.IfOperator(
            task_id='if_query_valid_records_has_no_data',
            test=get_loginname,
            yes_task="create_csv_for_valid_records",
            no_task="get_enabled_company_billingrates",
        )

        create_csv_for_valid_records = rail.WriteCSVFileOperator(
            task_id='create_csv_for_valid_records',
            source="{{ result('project_billing_rate_logtable') }}",
            delimiter=',',
            header=['loginname', 'projectname',
                    'billingrate', 'action', 'status', 'details', 'jobid'],
            row=lambda item: [
                item['properties']['loginname'],
                item['properties']['projectname'],
                item['properties']['billingrate'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['jobid'],
            ]
        )

        get_enabled_company_billingrates = rail.RepliconServiceOperator(
            task_id='get_enabled_company_billingrates',
            endpoint='/services/BillingrateService1.svc/GetEnabledCompanyBillingRates',
            data=None
        )

        process_project_billing_rate_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_project_billing_rate_child',
            retries=0,
            items='{{ result("query_valid_records") }}',
            trigger_dag_id=f'b2g_assign_remove_billing_rate_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "billing_rate_items": item,
                "lookup_table": rail.result('b2g_joint_venture_billing_assignment_logs'),
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "billingrateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_company_billingrates'), 'name', item['billing_rate'], 'uri', null) if rail.result('get_enabled_company_billingrates') else null
            }
        )

        wait_for_process_project_billing_rate_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_project_billing_rate_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_project_billing_rate_child") }}'
        )

        search_entries_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_lookup_table',
            log="{{result('b2g_joint_venture_billing_assignment_logs')}}",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
            }
        )

        add_entries_from_logtable = rail.WriteLogOperator(
            task_id='add_exception_entries_for_useruri',
            log="{{result('project_billing_rate_logtable')}}",
            items="{{result('search_entries_in_lookup_table')}}",
            message="na",
            severity="",
            properties=lambda item: {
                'loginname': item['properties']['loginname'],
                'projectname': item['properties']['projectname'],
                'billingrate': item['properties']['billingrate'],
                'action': item['properties']['action'],
                'status': item['properties']['status'],
                'details': item['properties']['details'],
                'jobid': item['properties']['jobid'] + "|" + item['properties']['childjobid']
            }
        )

        create_csv_for_all_records = rail.WriteCSVFileOperator(
            task_id='create_csv_for_all_records',
            source="{{result('project_billing_rate_logtable') }}",
            delimiter=',',
            header=['loginname', 'projectname',
                    'billingrate', 'action', 'status', 'details', 'jobid'],
            row=lambda item: [
                item['properties']['loginname'],
                item['properties']['projectname'],
                item['properties']['billingrate'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['jobid'],
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_for_all_records')}}",
            output_file_name='logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}',
            expires_in_seconds=7*24*60*60,
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log="{{result('project_billing_rate_logtable')}}",
            properties={'status': 'error'}
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            log="{{result('project_billing_rate_logtable')}}",
            properties={'status': 'exception'}
        )

        send_update_complete_email = rail.EmailOperator(
            task_id='send_update_complete_email',
            to="{{result('load_address')}}",
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                "+config.internal_logs_email+"\
            {%- else -%}\
                "+config.alert_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() }} | Replicon Billing Rate assignment  - {{" "}} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="/templates/emails/update_complete_mail.html"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{result('new_file_sensor')}}",
            new_filename=config.archive_filepath +
            "{{ result('new_file_sensor') | file_name }}"
        )

        stop_job = rail.FailOperator(
            task_id='stop_job',
            message='No Data in the base report'
        )

        stop_job_with_error = rail.FailOperator(
            task_id='stop_job_with_error',
            message='Base report column order does not match'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> download_file_from_sftp
        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        was_new_file_found >> rail.Label(
            'Yes') >> can_run_batch_task
        download_file_from_sftp >> download_file_from_address >> parse_toaddress >> load_address >> parse_csv
        parse_csv >> create_billingrate_list >> if_list_has_data >> rail.Label(
            'Yes') >> send_import_skipped_mail >> archive_file >> finish
        if_list_has_data >> rail.Label(
            'No') >> project_billing_rate_logtable >> b2g_joint_venture_billing_assignment_logs >> query_missing_mandatory_values
        query_missing_mandatory_values >> add_ignored_entries >> query_records_mandatory_values >> if_query_has_data_present
        if_query_has_data_present >> rail.Label('Yes') >> get_report_details
        get_report_details >> run_report_entry
        run_report_exit >> if_payload_has_data
        if_payload_has_data >> rail.Label('Yes') >> if_payload_has_no_columns
        if_payload_has_no_columns >> rail.Label(
            'Yes') >> stop_job_with_error >> finish
        if_payload_has_no_columns >> rail.Label(
            'No') >> parse_csv_data >> get_report_details_data >> run_report_group_entry
        run_report_group_exit >> parse_csv_project_data
        parse_csv_project_data >> load_parse_csv_project_data >> load_parse_csv_data >> create_csv_for_records >> create_merged_input_for_records
        create_merged_input_for_records >> query_invalid_records >> add_ignored_entries_for_invalid_records >> query_valid_records
        query_valid_records >> if_query_valid_records_has_no_data >> rail.Label(
            'Yes') >> create_csv_for_valid_records >> search_entries_in_lookup_table
        if_query_valid_records_has_no_data >> rail.Label(
            'No') >> get_enabled_company_billingrates >> process_project_billing_rate_child
        process_project_billing_rate_child >> wait_for_process_project_billing_rate_child >> search_entries_in_lookup_table
        search_entries_in_lookup_table >> add_entries_from_logtable >> create_csv_for_all_records >> generate_download_link
        generate_download_link >> get_logged_errors >> get_logged_exceptions >> send_update_complete_email
        send_update_complete_email >> archive_file
        if_payload_has_data >> rail.Label(
            'No') >> stop_job >> finish >> log_to_sumo
        if_query_has_data_present >> rail.Label(
            'No') >> archive_file >> finish >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
