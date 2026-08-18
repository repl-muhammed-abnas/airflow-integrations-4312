from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.python_callable_method import get_all_userlogs, get_varreplicon_feedfile_enabledusers
from terraconconsultants.user_import.utils.request_payload import get_row_data, write_unchanged_logs
from terraconconsultants.user_import.utils.response_filter import get_department_response, page_handler, get_user_response

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


# pylint: disable=too-many-statements
def create_processfile_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_process_each_file_{config.instance}',
        description=f'TerraconConsultants User Import Process each file {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_jobstarttime'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_jobstarttime',
            end_task='should_fail_dag',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_jobstarttime = rail.PythonOperator(
            task_id='get_jobstarttime',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%H%M%S')
        )

        is_csvfile = rail.IfOperator(
            task_id='is_csvfile',
            test="{{ dag_run.conf.filename | lower | ends_with('csv') }}",
            yes_task="download_file",
            no_task="send_bad_file_format_email",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath='{{ dag_run.conf.filepath }}'
        )

        load_csv_contents = rail.LoadCSVFileOperator(
            task_id='load_csv_contents',
            document="{{ result('download_file') }}"
        )

        load_rawdata_collection = rail.CreateCollectionOperator(
            task_id='load_rawdata_collection',
            source="{{ result('load_csv_contents') }}",
            name="rawinput",
            columns={
                'Employee_Number': 'employeenumber',
                'First_Name': 'firstname',
                'Last_Name': 'lastname',
                'Start_Date': 'startdate',
                'End_Date': 'enddate',
                'Rehire_Date': 'rehiredate',
                'Email_Address': 'emailaddress',
                'Principal_Status': 'principalstatus',
                'Department': 'department',
                'Authentication_Type': 'authenticationtype',
                'Spv_employee_number': 'supervisoremployeeid',
                'Timezone_code': 'Timezone_code',
                'Employee_Location_State': 'Employee_Location_State',
                'Employee_Org_Code': 'Employee_Org_Code',
                'Chargeability_%': 'Chargeability',
                'Full_Time_Availability': 'Full_Time_Availability',
                'Job_Title': 'Job_Title',
                'Hourly_Salaried': 'Hourly_Salaried_Code',
                'Assignment_Status': 'Assignment_Status',
                'Assignment_status_effective_date': 'Assignment_status_effective_date',
                'Assignment_Category': 'Assignment_Category',
                'Assignment_category_effective_date': 'Assignment_category_effective_date',
                'Service_Date': 'Service_Date',
                'Govt_Reporting_Entity': 'Govt_Reporting_Entity',
                'Local_Tax_Code': 'Local_Tax_Code',
                'Floating_Holiday': 'Floating_Holiday',
                'PTO_Accrued': 'PTO_Accrued',
                'FTO_Accrued': 'FTO_Accrued',
                'ESLB_Accrued': 'ESLB_Accrued',
                'MSL_Accured': 'MSL_Accured',
                'Floating_Holiday_Balance': 'floatingholidaybalance',
                'Supervior_Needed': 'supervisor_required',
                'Timesheet_Template': 'timesheettemplate',
                'md5_reference': 'md5_reference'
            }
        )

        compose_csvfile = rail.WriteCSVFileOperator(
            task_id='compose_csvfile',
            source="{{ result('load_rawdata_collection') }}",
            row=get_row_data
        )

        create_rawinputdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawinputdata_collection',
            source="{{ result('compose_csvfile') }}",
            name='rawinput'
        )

        get_enabledusers = rail.RepliconServicePageOperator(
            task_id='get_enabledusers',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": True
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_user_response
        )

        get_variancereplicon_vs_feedfile_enabledusers = rail.PythonOperator(
            task_id='get_variancereplicon_vs_feedfile_enabledusers',
            python_callable=get_varreplicon_feedfile_enabledusers
        )

        is_variance_greater_5 = rail.IfOperator(
            task_id='is_variance_greater_5',
            test=lambda: rail.result(
                'get_variancereplicon_vs_feedfile_enabledusers') >= 5,
            yes_task="send_feedfile_warning_email",
            no_task="list_referencefiles",
        )

        send_feedfile_warning_email = rail.EmailOperator(
            task_id='send_feedfile_warning_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Feed file warning - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content='templates/email/feed_file_warning.html'
        )

        remove_filefromprocessing = rail.SFTPDeleteFileOperator(
            task_id='remove_filefromprocessing',
            existing_filename='{{ dag_run.conf.filepath }}'
        )

        list_referencefiles = rail.S3ListKeysOperator(
            task_id='list_referencefiles',
            bucket_name=config.bucket_name,
            prefix=config.reference_key_name,
            aws_conn_id=config.aws_conn_id
        )

        should_use_referencefile = rail.IfOperator(
            task_id='should_use_referencefile',
            test=lambda: len(rail.result('list_referencefiles')) > 0,
            yes_task='trigger_referencefile_download_child',
            no_task='fail_dag'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message='Reference file not available'
        )

        trigger_referencefile_download_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_referencefile_download_child',
            retries=0,
            items=lambda: rail.result('list_referencefiles'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'terraconconsultants_userimport_child_referencefile_{config.instance}',
            conf=lambda item: {
                'keyname': item,
                'action': 'download'
            }
        )

        wait_for_referencefile_download_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_referencefile_download_child',
            dag_runs="{{ result('trigger_referencefile_download_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_userreference_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_userreference_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_referencefile_download_child') }}",
            dagrun_task_id='create_userreference_data',
            flatten=True
        )

        create_userreference_data_collection = rail.CreateCollectionOperator(
            task_id='create_userreference_data_collection',
            name='referencefile',
            source=lambda: rail.result('gather_userreference_data')
        )

        query_changeditems = rail.QueryCollectionOperator(
            task_id='query_changeditems',
            query="""SELECT * FROM rawinput WHERE
                    md5_reference NOT IN (SELECT DISTINCT md5_reference FROM referencefile)""",
            name='changeditems'
        )

        query_unchangeditems = rail.QueryCollectionOperator(
            task_id='query_unchangeditems',
            query="""SELECT * FROM rawinput WHERE
                    md5_reference IN (SELECT DISTINCT md5_reference FROM referencefile)""",
            name='unchangeditems'
        )

        get_userreference_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userreference_report_details',
            report_name=config.master_user_reference_report
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='userreference_report_generation',
            report_params={
                'reportParameters': [
                    {
                        'reportUri': "{{ result('get_userreference_report_details').uri }}",
                        'filterValues': [],
                        'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
                    }
                ]
            },
            wait_timeout=120
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test=lambda: bool(rail.result('userreference_report_generation.get_report_result')[
                'reportGenerationResults'][0]['error']) or not rail.result('userreference_report_generation.get_report_result', 'has_data'),
            yes_task='fail_userreference_report_generation',
            no_task='load_report_data'
        )

        fail_userreference_report_generation = rail.FailOperator(
            task_id='fail_userreference_report_generation',
            message='Report Failed'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('userreference_report_generation.get_report_result').reportGenerationResults[0].payload }}"
        )

        parse_report_data = rail.PythonOperator(
            task_id='parse_report_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('load_report_data'))
        )

        get_all_departmentgroups = rail.RepliconServicePageOperator(
            task_id='get_all_departmentgroups',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:code"
                ]
            },
            page_handler=page_handler,
            all_result_data_handler=get_department_response
        )

        get_required_permissionsets = rail.RepliconServiceOperator(
            task_id='get_required_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'supervisor_permissionuri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Supervisor', 'uri', ''),
                'project_manager_permissionuri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Manager', 'uri', ''),
                'project_resource_reports_permissionuri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Resource with Reports', 'uri', ''),
            }
        )

        is_querychanged_items = rail.IfOperator(
            task_id='is_querychanged_items',
            test="{{ result('query_changeditems', 'length') > 0 }}",
            yes_task="query_unique_jobtitles",
            no_task="is_queryunchanged_items",
        )

        query_unique_jobtitles = rail.QueryCollectionOperator(
            task_id='query_unique_jobtitles',
            query="""SELECT DISTINCT Job_Title FROM changeditems""",
            name='uniquejobtitles'
        )

        trigger_jobtitle_customfield_check = rail.TriggerDagRunOperator(
            task_id='trigger_jobtitle_customfield_check',
            retries=0,
            trigger_dag_id=f'terraconconsultants_userimport_child_jobtitle_customfield_check_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        is_queryunchanged_items = rail.IfOperator(
            task_id='is_queryunchanged_items',
            test="{{ result('query_unchangeditems', 'length') > 0 }}",
            yes_task="create_unchanged_records_log",
            no_task="create_supervisorlog",
        )

        create_unchanged_records_log = rail.CreateLogOperator(
            task_id='create_unchanged_records_log'
        )

        write_unchanged_records = rail.WriteLogOperator(
            task_id='write_unchanged_records',
            log="{{ result('create_unchanged_records_log') }}",
            items="{{ result('query_unchangeditems') }}",
            severity='Skipped',
            message='No change found from previous input file',
            properties=write_unchanged_logs
        )

        create_supervisorlog = rail.CreateLogOperator(
            task_id='create_supervisorlog'
        )

        get_userimport_reference_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userimport_reference_report_details',
            report_name=config.user_import_reference_report
        )

        get_report_filteruri_userimport_reference = rail.PythonOperator(
            task_id='get_report_filteruri_userimport_reference',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_userimport_reference_report_details')[
                    'filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', '')
        )

        trigger_user_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_child_dag',
            retries=0,
            items="{{ result('query_changeditems') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'terraconconsultants_userimport_child_processuser_{config.instance}',
            conf=lambda item: {
                **{k.lower(): v for k, v in item.items()},
                'useruri': rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'parse_report_data'), 'Login Name', item['employeenumber'],
                    'useruri', ''),
                'supervisor_log': rail.result('create_supervisorlog'),
                'departmentgroupuri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_departmentgroups'), 'code', item['Employee_Org_Code'], 'uri', ''),
                'reporturi': rail.result('get_userimport_reference_report_details')['uri'],
                'reportfilteruri': rail.result('get_report_filteruri_userimport_reference'),
                **dict(rail.result('get_required_permissionsets').items())
            }
        )

        wait_for_user_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_child_dag',
            dag_runs="{{ result('trigger_user_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs="{{ result('trigger_user_child_dag') }}",
            dagrun_task_id='create_userlog',
            flatten=True
        )

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('create_supervisorlog') }}",
            severity='Pending',
            remove_filtered_entries=True
        )

        is_supervisorcheck_queued_logs = rail.IfOperator(
            task_id='is_supervisorcheck_queued_logs',
            test="{{ result('get_supervisorcheck_queued_logs', 'length') > 0 }}",
            yes_task='process_supervisor_child_dag',
            no_task='create_repliconactiveusers'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'terraconconsultants_userimport_child_supervisor_assignment_{config.instance}',
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_log': rail.result('create_supervisorlog'),
                'supervisor_permissionuri': rail.result(
                    'get_required_permissionsets')['supervisor_permissionuri']
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        create_repliconactiveusers = rail.CreateCollectionOperator(
            task_id='create_repliconactiveusers',
            source=lambda: rail.result('get_enabledusers'),
            name="repliconactiveusers",
        )

        query_repliconusers_not_in_feedfile = rail.QueryCollectionOperator(
            task_id='query_repliconusers_not_in_feedfile',
            query="""SELECT * FROM repliconactiveusers WHERE
                    loginname NOT IN (SELECT DISTINCT employeenumber FROM rawinput)""",
        )

        is_users_greater_than_zero = rail.IfOperator(
            task_id='is_users_greater_than_zero',
            test="{{ result('query_repliconusers_not_in_feedfile', 'length') > 0 }}",
            yes_task="trigger_user_child_dag2",
            no_task="process_logs",
        )

        trigger_user_child_dag2 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_child_dag2',
            retries=0,
            items="{{ result('query_repliconusers_not_in_feedfile') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'terraconconsultants_userimport_child_processuser_{config.instance}',
            conf=lambda item: {
                **{k.lower(): v for k, v in item.items()},
                'actiontype': 'disable'
            }
        )

        wait_for_user_child_dag2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_child_dag2',
            dag_runs="{{ result('trigger_user_child_dag2') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_child_logs_disabled = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs_disabled',
            dag_runs="{{ result('trigger_user_child_dag2') }}",
            dagrun_task_id='create_userlog',
            flatten=True
        )

        process_logs = rail.TriggerDagRunOperator(
            task_id='process_logs',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'terraconconsultants_userimport_child_log_{config.instance}',
            conf=lambda dag_run: {
                'user_logs': get_all_userlogs(),
                'filename': dag_run.conf['filename']
            }
        )

        trigger_referencefile_archive_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_referencefile_archive_child',
            retries=0,
            items=lambda: rail.result('list_referencefiles'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'terraconconsultants_userimport_child_referencefile_{config.instance}',
            conf=lambda item: {
                'keyname': item,
                'action': 'archive',
                'archive_keyname': f"{config.archive_key_name}/{item.split('/')[-1]}"
            }
        )

        wait_for_referencefile_archive_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_referencefile_archive_child',
            dag_runs="{{ result('trigger_referencefile_archive_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        upload_reference_file = rail.S3UploadFileOperator(
            task_id='upload_reference_file',
            bucket_name=config.bucket_name,
            source="{{ result('compose_csvfile') }}",
            key_name=config.reference_key_name +
            "/newreference_{{ current_time('%H%M%S') }}_{{ dag_run.conf.filename }}",
            aws_conn_id=config.aws_conn_id
        )

        remove_filefromprocessing2 = rail.SFTPDeleteFileOperator(
            task_id='remove_filefromprocessing2',
            existing_filename='{{ dag_run.conf.filepath }}'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Incorrect file format received - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/email_bad_format_email2.html"
        )

        remove_filefromprocessing3 = rail.SFTPDeleteFileOperator(
            task_id='remove_filefromprocessing3',
            existing_filename='{{ dag_run.conf.filepath }}'
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='upload_reference_file2',
            no_task='dagrun_log_to_sumo'
        )

        upload_reference_file2 = rail.S3UploadFileOperator(
            task_id='upload_reference_file2',
            bucket_name=config.bucket_name,
            source="{{ result('compose_csvfile') }}",
            key_name=config.reference_key_name +
            "/newreference_{{ current_time('%H%M%S') }}_{{ dag_run.conf.filename }}",
            aws_conn_id=config.aws_conn_id
        )

        remove_filefromprocessing4 = rail.SFTPDeleteFileOperator(
            task_id='remove_filefromprocessing4',
            existing_filename='{{ dag_run.conf.filepath }}'
        )

        fail_dag2 = rail.FailOperator(
            task_id='fail_dag2',
            message="{{ get_error_message() }}"
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_fail_dag
        can_run_batch_task >> rail.Label(
            'No') >> get_jobstarttime >> is_csvfile
        is_csvfile >> rail.Label(
            'Yes') >> download_file >> load_csv_contents >> load_rawdata_collection >> \
            compose_csvfile >> create_rawinputdata_collection >> \
            get_enabledusers >> get_variancereplicon_vs_feedfile_enabledusers >> \
            is_variance_greater_5

        is_variance_greater_5 >> rail.Label(
            'Yes') >> send_feedfile_warning_email >> remove_filefromprocessing >> should_fail_dag
        is_variance_greater_5 >> rail.Label(
            'No') >> list_referencefiles >> should_use_referencefile
        should_use_referencefile >> rail.Label(
            'No') >> fail_dag
        should_use_referencefile >> rail.Label(
            'Yes') >> trigger_referencefile_download_child >> wait_for_referencefile_download_child >> \
            gather_userreference_data >> create_userreference_data_collection >> \
            query_changeditems >> query_unchangeditems >> get_userreference_report_details >> \
            run_report_group_entry
        run_report_group_exit >> is_report_failed
        is_report_failed >> rail.Label(
            'Yes') >> fail_userreference_report_generation >> should_fail_dag

        is_report_failed >> rail.Label(
            'No') >> load_report_data >> parse_report_data >> get_all_departmentgroups >> \
            get_required_permissionsets >> is_querychanged_items

        is_querychanged_items >> rail.Label(
            'Yes') >> query_unique_jobtitles >> trigger_jobtitle_customfield_check >> is_queryunchanged_items
        is_querychanged_items >> rail.Label(
            'No') >> is_queryunchanged_items
        is_queryunchanged_items >> rail.Label(
            'Yes') >> create_unchanged_records_log >> write_unchanged_records >> create_supervisorlog
        is_queryunchanged_items >> rail.Label(
            'No') >> create_supervisorlog
        create_supervisorlog >> get_userimport_reference_report_details >> \
            get_report_filteruri_userimport_reference >> trigger_user_child_dag >> \
            wait_for_user_child_dag >> gather_child_logs >> \
            get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs
        is_supervisorcheck_queued_logs >> rail.Label(
            'Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> create_repliconactiveusers
        is_supervisorcheck_queued_logs >> rail.Label(
            'No') >> create_repliconactiveusers
        create_repliconactiveusers >> query_repliconusers_not_in_feedfile >> is_users_greater_than_zero
        is_users_greater_than_zero >> rail.Label(
            'Yes') >> trigger_user_child_dag2 >> wait_for_user_child_dag2 >> gather_child_logs_disabled >> \
            process_logs
        is_users_greater_than_zero >> rail.Label(
            'No') >> process_logs
        process_logs >> trigger_referencefile_archive_child >> wait_for_referencefile_archive_child >> \
            upload_reference_file >> remove_filefromprocessing2 >> should_fail_dag
        is_csvfile >> rail.Label(
            'No') >> send_bad_file_format_email >> remove_filefromprocessing3 >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> upload_reference_file2 >> remove_filefromprocessing4 >> fail_dag2

        should_fail_dag >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_processfile_dag)
