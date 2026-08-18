from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_tasks_file_dag_id,
        description=f'Audaxgroup process tasks file {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)



        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='audax_project_task_file_processing_lookup_table'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='audax_project_task_file_processing_lookup_table',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        audax_project_task_file_processing_lookup_table = rail.CreateLogOperator(
            task_id="audax_project_task_file_processing_lookup_table"
        )

        audax_project_task_import_logs = rail.CreateLogOperator(
            task_id="audax_project_task_import_logs"
        )

        log_timenow_3=rail.PythonOperator(
            task_id='log_timenow_3',
            python_callable= lambda: datetime.now().strftime('%m-%d-%Y %H:%M')
        )

        is_csv_4 = rail.IfOperator(
            task_id='is_csv_4',
            test="{{ dag_run.conf.fullpath | file_ext | lower == 'csv' }}",
            yes_task='audax_project_task_file_processing_add_entry_7',
            no_task='send_mail_5'
        )

        send_mail_5=rail.EmailOperator(
            task_id='send_mail_5',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=config.company_key + ''' | Task Import - Invalid file format {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Task Import job has not been processed because of invalid file format for the file - '{{ dag_run.conf.filename }}'.<br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
                        params=None,
                    )

        audax_project_task_file_processing_add_entry_7=rail.WriteLogOperator(
            task_id='audax_project_task_file_processing_add_entry_7',
            log="{{ dag_run.conf.audax_project_task_file_processing_lookup_table }}",
            message="na",
            severity="Info",
            properties={
                "recipeid": "{{ dag_run.conf.parent_ecid }}",
                "filename": "{{ dag_run.conf.filename }}"
            }
        )

        audax_users_and_departments_lookup_table = rail.CreateLogOperator(
            task_id="audax_users_and_departments_lookup_table"
        )

        download_9 = rail.SFTPDownloadFileOperator(
            task_id='download_9',
            remote_filepath="{{ dag_run.conf.fullpath }}",
        )

        parse_csv_10 = rail.LoadCSVFileOperator(
            task_id='parse_csv_10',
            document="{{ result('download_9') }}",
            delimiter=',',

        )

        if_csv_has_data_present_11 = rail.IfOperator(
            task_id='if_csv_has_data_present_11',
            test="{{result('parse_csv_10') | load_all_records | length > 0 }}",
            yes_task="create_collection_list_from_input_csv",
            no_task="send_mail_12",
        )
        create_collection_list_from_input_csv = rail.CreateCollectionOperator(
            task_id='create_collection_list_from_input_csv',
            source="{{ result('parse_csv_10') }}",
            name="input",
            columns = {
                "Project Name":"projectname",
                "Task Name Level1":"tasklevel1",
                "Task Name Level2":"tasklevel2",
                "Task Name Level3":"tasklevel3",
                "Task Code":"taskcode",
                "TimeEntry Start Date":"timeentrystartdate",
                "TimeEntry End Date":"timeentryenddate",
                "Assigned Resource - Users":"assignedusers",
                "Assigned Resource - Departments":"assigneddepartments",
                "Is Time Entry Allowed":"istimeentryallowed",
                "Task Description":"taskdescription",
                "TaskInfo1":"taskinfo1",
                "TaskInfo2":"taskinfo2",
                "TaskInfo3":"taskinfo3",
                "Desired Sort Order":"sortorder"
            }
        )

        send_mail_12=rail.EmailOperator(
            task_id='send_mail_12',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Task Import - No data in file {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Task Import job was not completed because there was no data in the file - '{{ dag_run.conf.filename }}'.<br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        audax_project_task_file_processing_truncate_13=rail.EmptyOperator(
            task_id='audax_project_task_file_processing_truncate_13',
        )

        get_all_reports_16=rail.RepliconServiceOperator(
            task_id='get_all_reports_16',
            endpoint="/services/reportService1.svc/GetAllReports",
            data=None
        )

        log_department_report_uri_17=rail.PythonOperator(
            task_id='log_department_report_uri_17',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_reports_16'),'displayText', "Department Report for Replicon Integration",'uri', '')
        )

        department_report_18=rail.RepliconServiceOperator(
            task_id='department_report_18',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('log_department_report_uri_17') }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
        )

        if_first_payload_not_contains_nodata_19 = rail.IfOperator(
            task_id='if_first_payload_not_contains_nodata_19',
            test='''{{ result('department_report_18').payload | is_truthy and not result('department_report_18').payload | matches('No Data') and result('department_report_18').error | is_falsy }}''',
            yes_task="parse_csv_departmentreport_22",
            no_task="finish",
        )

        parse_csv_departmentreport_22=rail.LoadCSVFileOperator(
            task_id='parse_csv_departmentreport_22',
            document="{{ result('department_report_18').payload }}",
        )

        compose_csv_23 = rail.WriteCSVFileOperator(
            task_id="compose_csv_23",
            source=lambda: rail.result('parse_csv_departmentreport_22'),
            header=[
                "Department Name",
                "Department Code",
                "DepartmentUri",
                "Parent Department Name",
                "Department Full Name",
            ],
            row=lambda item:[
                item['Department Name'],
                item['Department Code'],
                item['DepartmentUri'],
                item['Parent Department Name'],
                '/'.join(item['Department Full Name'].split(" / ")) if (item['Department Full Name'] and '/' in item['Department Full Name']) else ''
            ],

        )

        parse_csv_formatted_departmentreport_24 = rail.LoadCSVFileOperator(
            task_id='parse_csv_formatted_departmentreport_24',
            document="{{ result('compose_csv_23') }}",
        )

        audaxgroup_users_and_departments_add_batch_of_entries_25= rail.WriteLogOperator(
            task_id='audaxgroup_users_and_departments_add_batch_of_entries_25',
            log="{{result('audax_users_and_departments_lookup_table')}}",
            items="{{result('parse_csv_formatted_departmentreport_24')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "name": item["Department Full Name"],
                "uri": item["DepartmentUri"],
            }
        )

        get_report_details_30 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_30',
            report_name='User Report for Replicon Integration',
        )

        generate_reports_batch_30 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_30',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data={"reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details_30').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )
        execute_generate_reports_batch_30 = rail.batch_execution(
            group_id='execute_generate_reports_batch_30',
            creation_task_id='generate_reports_batch_30',
        )

        get_report_batch_results_32 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_32',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{ result('generate_reports_batch_30') }}"},
        )

        if_first_payload_not_contains_nodata_33=rail.IfOperator(
            task_id='if_first_payload_starts_with_nodata_33',
            test='''{{ result('get_report_batch_results_32').reportGenerationResults | is_truthy and not result('get_report_batch_results_32').reportGenerationResults[0].payload |  matches('No Data') }}''',
            yes_task="parse_csv_36",
            no_task="finish",
        )

        parse_csv_36=rail.LoadCSVFileOperator(
            task_id='parse_csv_36',
            document="{{ result('get_report_batch_results_32').reportGenerationResults[0].payload }}"
        )

        audaxgroup_users_and_departments_add_batch_of_entries_37=rail.WriteLogOperator(
            task_id='audaxgroup_users_and_departments_add_batch_of_entries_37',
            log="{{result('audax_users_and_departments_lookup_table')}}",
            items="{{result('parse_csv_36')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "name": item["Login Name"],
                "uri": item["UserUri"],
            }
        )

        get_report_details_42 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_42',
            report_name='Project Task Report for Replicon Integration',
        )

        generate_reports_batch_42 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_42',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data={"reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details_42').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_generate_reports_batch_42 = rail.batch_execution(
            group_id='execute_execute_generate_reports_batch_42',
            creation_task_id='generate_reports_batch_42',
        )

        get_report_batch_results_44 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_44',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{ result('generate_reports_batch_42') }}"},
        )

        if_first_payload_not_contains_nodata_45 = rail.IfOperator(
            task_id='if_first_payload_not_contains_nodata_45',
            test='''{{ result('get_report_batch_results_44').reportGenerationResults | is_truthy and not result('get_report_batch_results_44').reportGenerationResults[0].payload |  matches('No Data') }}''',
            yes_task="load_csv_create_list_from_csv_48",
            no_task="finish",
        )

        load_csv_create_list_from_csv_48=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_48",
            document="{{result('get_report_batch_results_44').reportGenerationResults[0].payload }}}",
        )

        create_collection_create_list_from_csv_48 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_48',
            source = "{{ result('load_csv_create_list_from_csv_48') }}",
            name = "allprojects",
            columns = {
                'Project Name':'existingprojectname',
                'ProjectUri':'projecturi',
                'Project Status':'currentstatus'
            }
        )

        query_list_nonexistent_projects_49=rail.QueryCollectionOperator(
            task_id='query_list_nonexistent_projects_49',
            query="""SELECT * FROM  input WHERE input.projectname NOT IN (SELECT DISTINCT  allprojects.existingprojectname FROM  allprojects) AND NULLIF(input.projectname,'') IS NOT NULL""",
        )

        audaxgroup_project_task_import_logs_add_batch_of_entries_50=rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_batch_of_entries_50',
            log="{{result('audax_project_task_import_logs')}}",
            items="{{result('query_list_nonexistent_projects_49')}}",
            message="na",
            severity="Exception",
            properties=lambda item: {
                "jobid": "{{dag_run_ecid()}}",
                "projectname": item["projectname"],
                "taskname": (item.get('tasklevel1') or '') + '|' + (item.get('tasklevel2') or '') + '|' + (item.get('tasklevel3') or ''),
                "taskcode": item['taskcode'],
                "status": "Exception",
                "details": "Project not present in Replicon - "+ item['projectname'],
                "childjobid": "",
            }
        )

        query_list_existing_projects_53=rail.QueryCollectionOperator(
            task_id='query_list_existing_projects_53',
            query="""SELECT  allprojects.projecturi, input.* FROM  allprojects INNER JOIN  input ON  allprojects.existingprojectname=input.projectname""",
        )

        if_query_list_existing_projects_53_rows_greater_than_0_54=rail.IfOperator(
            task_id='if_query_list_existing_projects_53_rows_greater_than_0_54',
            test='''{{ result('query_list_existing_projects_53', 'length') > 0 }}''',
            yes_task="create_list_55",
            no_task="rename_94",
        )

        create_list_55 = rail.CreateCollectionOperator(
            task_id='create_list_55',
            source = "{{ result('query_list_existing_projects_53') }}",
            name = "Existingprojects",
        )


        query_list_distinct_projects_56=rail.QueryCollectionOperator(
            task_id='query_list_distinct_projects_56',
            query="""SELECT DISTINCT  Existingprojects.projectname as existingproject, Existingprojects.projecturi as projecturi FROM  Existingprojects WHERE   NULLIF(Existingprojects.projectname,'') IS NOT NULL""",
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='dagrunlist',
            value=[]
        )

        foreach_query_list_distinct_projects_56_57=rail.ForEachOperator(
            task_id='foreach_query_list_distinct_projects_56_57',
            items="{{ result('query_list_distinct_projects_56') }}",
            start_task = 'query_list_dataperproject_58',
            end_task = 'foreach_query_list_distinct_projects_56_57_end'
        )

        query_list_dataperproject_58=rail.QueryCollectionOperator(
            task_id='query_list_dataperproject_58',
            query="""SELECT * FROM  input WHERE  input.projectname="{{ result('foreach_query_list_distinct_projects_56_57').existingproject }}" """,
        )

        if_query_list_dataperproject_58_rows_present_59=rail.IfOperator(
            task_id='if_query_list_dataperproject_58_rows_present_59',
            test='''{{ result('query_list_dataperproject_58') | is_truthy }}''',
            yes_task="trigger_dag_run_live_audaxgroup_process_tasks_per_projectasync_60",
            no_task="foreach_query_list_distinct_projects_56_57_end",
        )


        trigger_dag_run_live_audaxgroup_process_tasks_per_projectasync_60=rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_audaxgroup_process_tasks_per_projectasync_60',
            retries=0,
            trigger_dag_id=config.process_tasks_per_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "projectname": "{{ result('foreach_query_list_distinct_projects_56_57').existingproject }}",
                "projecturi": "{{ result('foreach_query_list_distinct_projects_56_57').projecturi }}",
                "tenanturl": "{{get_tenant_slug()}}",
                "projecttaskdata": "{{ result('query_list_dataperproject_58') }}",
                "parent_ecid": "{{dag_run_ecid()}}",
                "audax_project_task_import_logs":"{{result('audax_project_task_import_logs')}}",
                "audax_users_and_departments_lookup_table":"{{result('audax_users_and_departments_lookup_table')}}"
            }
        )

        insert_to_list = rail.SetVariableOperator(
            task_id='insert_to_list',
            append=True,
            name='{{ result("declare_list").name }}',
            value='{{result("trigger_dag_run_live_audaxgroup_process_tasks_per_projectasync_60")}}'
        )

        foreach_query_list_distinct_projects_56_57_end=rail.EmptyOperator(
            task_id='foreach_query_list_distinct_projects_56_57_end',
        )

        wait_for_completion_dag_add_update_tasksasync = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_add_update_tasksasync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ dag_run_var(result('declare_list').name) | to_json }}"
        )

        query_list_invalid_projectsblankprojectnames_61=rail.QueryCollectionOperator(
            task_id='query_list_invalid_projectsblankprojectnames_61',
            query="""SELECT * FROM input WHERE  NULLIF(input.projectname,'') IS NULL""",
        )

        audaxgroup_project_task_import_logs_add_batch_of_entries_62=rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_batch_of_entries_62',
            log="{{result('audax_project_task_import_logs')}}",
            items="{{result('query_list_invalid_projectsblankprojectnames_61')}}",
            message="na",
            severity="Exception",
            properties=lambda item: {
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "projectname": item["projectname"],
                "taskname": (item.get('tasklevel1') or '') + '|' + (item.get('tasklevel2') or '') + '|' + (item.get('tasklevel3') or ''),
                "taskcode": item['taskcode'],
                "status": "Exception",
                "details": "Project name not provided",
                "childjobid": "",
            }
        )

        log_waittime_65=rail.PythonOperator(
            task_id='log_waittime_65',
            python_callable= lambda:  int((rail.result('query_list_distinct_projects_56', 'length')* 3) + 1)
        )

        audaxgroup_project_task_import_logs_search_entries_67=rail.FilterLogEntriesOperator(
            task_id='audaxgroup_project_task_import_logs_search_entries_67',
            log="{{result('audax_project_task_import_logs')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_first_id_present_68=rail.IfOperator(
            task_id='if_first_id_present_68',
            test="{{result('audaxgroup_project_task_import_logs_search_entries_67') | length > 0 | is_truthy }}",
            yes_task="create_csv_lines_69",
            no_task="rename_94",
        )

        create_csv_lines_69=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_69',
            source= "{{result('audaxgroup_project_task_import_logs_search_entries_67')}}",
            header=[
                    'Projectname',
                    'Tasklevel1',
                    'Tasklevel2',
                    'Tasklevel3',
                    'Taskcode',
                    'Status',
                    'Details',
                    'JobID'
                    ],
            row= [
                "{{ item.properties.projectname }}",
                "{{ (item.properties.taskname | default('||', true)).split('|')[0] }}",
                "{{ (item.properties.taskname | default('||', true)).split('|')[1] }}",
                "{{ (item.properties.taskname | default('||', true)).split('|')[2] }}",
                "{{ item.properties.taskcode | default('', true) }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid | default('', true) }}|{{ item.properties.childjobid | default('', true) }}"
            ],
        )

        upload_71=rail.SFTPUploadFileOperator(
            task_id='upload_71',
            content='''{{ result('create_csv_lines_69') }}''',
            remote_filepath= config.log_filepath+'''/Task_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename }}''',
        )

        log_checkforerrors_76 = rail.PythonOperator(
            task_id='log_checkforerrors_76',
            python_callable=lambda: next(
                (item['properties']['status']
                 for item in rail.load_all_records(rail.result('audaxgroup_project_task_import_logs_search_entries_67'))
                 if item.get('properties', {}).get('status') == 'Error'),
                None)
        )

        if_log_checkforerrors_76_blank_77=rail.IfOperator(
            task_id='if_log_checkforerrors_76_blank_77',
            test='''{{ result('log_checkforerrors_76') | is_falsy }}''',
            yes_task="log_checkfor_exceptions_78",
            no_task="send_mail_completedwitherrors_84",
        )


        log_checkfor_exceptions_78=rail.PythonOperator(
            task_id='log_checkfor_exceptions_78',
            python_callable=lambda: next(
                (item['properties']['status']
                 for item in rail.load_all_records(rail.result('audaxgroup_project_task_import_logs_search_entries_67'))
                 if item.get('properties', {}).get('status') == 'Exception'),
                None)
        )


        if_log_checkfor_exceptions_78_blank_79=rail.IfOperator(
            task_id='if_log_checkfor_exceptions_78_blank_79',
            test='''{{ result('log_checkfor_exceptions_78') | is_falsy }}''',
            yes_task="send_mail_completedsuccessfully_80",
            no_task="send_mail_completedwith_exceptions_82",
        )


        send_mail_completedsuccessfully_80=rail.EmailOperator(
            task_id='send_mail_completedsuccessfully_80',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Task Import - Completed Successfully {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Task Import job is completed successfully based on the file - '{{ dag_run.conf.filename }}'. Please find the Task import logs placed in the below SFTP location for reference. <br />
            <br />
            Logs: {{params.log_filepath}}/Task_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename}} 
            <br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
            ''',
            params={'log_filepath': config.log_filepath},
        )


        send_mail_completedwith_exceptions_82=rail.EmailOperator(
            task_id='send_mail_completedwith_exceptions_82',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Task Import - Completed with Exceptions {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
                <br />Hello, <br />
                <br />
                The Task Import job is completed with exceptions based on the file - '{{ dag_run.conf.filename }}'. Please find the Task import logs placed in the below SFTP location for reference. <br />
                <br />
                Logs: {{params.log_filepath}}/Task_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename}}  <br />
                <br />
                <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                ''',
            params={'log_filepath': config.log_filepath},
        )


        send_mail_completedwitherrors_84=rail.EmailOperator(
            task_id='send_mail_completedwitherrors_84',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Task Import - Completed with Errors {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Task Import job is completed with Errors based on the file - '{{ dag_run.conf.filename }}'. Please find the Task import logs placed in the below SFTP location for reference. <br />
            <br />
            Logs: {{params.log_filepath}}/Task_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename}} <br />
            <br />
            <br />
            For any queries, please contact our support team at https://support.deltek.com <br />
            <br />
            Regards, <br />
            Deltek Inc. ''',
            params={'log_filepath': config.log_filepath},
        )

        rename_94=rail.SFTPMoveFileOperator(
            task_id='rename_94',
            new_filename=config.archive_filepath+'''/{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename }}''',
            existing_filename= '''{{ dag_run.conf.fullpath }}''',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.audax_project_task_file_processing_lookup_table}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "childjobid": "{{ dag_run_ecid() }}"
            },
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> audax_project_task_file_processing_lookup_table
        send_mail_5 >> finish
        audax_project_task_file_processing_lookup_table >> log_timenow_3 >> is_csv_4
        is_csv_4 >> rail.Label('Yes') >> audax_project_task_file_processing_add_entry_7 >> audax_users_and_departments_lookup_table >> audax_project_task_import_logs >> download_9 \
        >> parse_csv_10 >> if_csv_has_data_present_11
        if_csv_has_data_present_11 >> rail.Label('Yes') >> create_collection_list_from_input_csv >> get_all_reports_16 >> log_department_report_uri_17 >> department_report_18  \
        >> if_first_payload_not_contains_nodata_19
        if_first_payload_not_contains_nodata_19 >> rail.Label('Yes') >> parse_csv_departmentreport_22 >> compose_csv_23 >> parse_csv_formatted_departmentreport_24 \
        >> audaxgroup_users_and_departments_add_batch_of_entries_25 >> get_report_details_30 >> generate_reports_batch_30 >> execute_generate_reports_batch_30[
            0] >> execute_generate_reports_batch_30[1] >> get_report_batch_results_32 >> if_first_payload_not_contains_nodata_33
        if_first_payload_not_contains_nodata_33 >> rail.Label('Yes') >> parse_csv_36 >> audaxgroup_users_and_departments_add_batch_of_entries_37 \
        >> get_report_details_42 >> generate_reports_batch_42 >> execute_generate_reports_batch_42[
            0] >> execute_generate_reports_batch_42[1] >> get_report_batch_results_44 >> if_first_payload_not_contains_nodata_45
        if_first_payload_not_contains_nodata_45 >> rail.Label('Yes') >> load_csv_create_list_from_csv_48 >> create_collection_create_list_from_csv_48 \
        >> query_list_nonexistent_projects_49 >> audaxgroup_project_task_import_logs_add_batch_of_entries_50 >> query_list_existing_projects_53 \
        >> if_query_list_existing_projects_53_rows_greater_than_0_54
        if_query_list_existing_projects_53_rows_greater_than_0_54 >> rail.Label('Yes') >> create_list_55 >> query_list_distinct_projects_56 >> declare_list >> foreach_query_list_distinct_projects_56_57 \
        >> query_list_dataperproject_58 >> if_query_list_dataperproject_58_rows_present_59
        if_query_list_dataperproject_58_rows_present_59 >> rail.Label('Yes') >> trigger_dag_run_live_audaxgroup_process_tasks_per_projectasync_60 >> insert_to_list >> foreach_query_list_distinct_projects_56_57_end
        foreach_query_list_distinct_projects_56_57 >> foreach_query_list_distinct_projects_56_57_end >> wait_for_completion_dag_add_update_tasksasync >> query_list_invalid_projectsblankprojectnames_61 \
        >> audaxgroup_project_task_import_logs_add_batch_of_entries_62 \
        >> log_waittime_65 >> audaxgroup_project_task_import_logs_search_entries_67 >> if_first_id_present_68
        if_query_list_dataperproject_58_rows_present_59 >> rail.Label('No') >> foreach_query_list_distinct_projects_56_57_end
        if_first_id_present_68 >> rail.Label('Yes') >> create_csv_lines_69 >> upload_71 >> log_checkforerrors_76 >> if_log_checkforerrors_76_blank_77
        if_log_checkforerrors_76_blank_77 >> rail.Label('Yes') >> log_checkfor_exceptions_78 >> if_log_checkfor_exceptions_78_blank_79
        if_log_checkfor_exceptions_78_blank_79 >> rail.Label('Yes') >> send_mail_completedsuccessfully_80 >> rename_94
        if_log_checkfor_exceptions_78_blank_79 >> rail.Label('No') >> send_mail_completedwith_exceptions_82 >> rename_94
        if_log_checkforerrors_76_blank_77 >> rail.Label('No') >> send_mail_completedwitherrors_84 >> rename_94
        if_first_id_present_68 >> rail.Label('No') >> rename_94
        if_query_list_existing_projects_53_rows_greater_than_0_54 >> rail.Label('No') >> rename_94 >>finish
        if_first_payload_not_contains_nodata_45 >> rail.Label('No') >> finish
        if_first_payload_not_contains_nodata_33 >> rail.Label('No') >> finish
        if_first_payload_not_contains_nodata_19 >> rail.Label('No') >> finish
        if_csv_has_data_present_11 >> rail.Label('No') >> send_mail_12 >> audax_project_task_file_processing_truncate_13 >> finish
        is_csv_4 >> rail.Label('No') >> send_mail_5 >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
