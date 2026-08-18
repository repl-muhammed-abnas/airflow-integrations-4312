
from datetime import timedelta, datetime
from rail.lib.ecid import get_dagrun_ecid
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'Mccarthy - Creating the project in Replicon Master {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
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

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='move_file_to_processing',
            no_task='delete_this_dagrun',
        )

        move_file_to_processing=rail.SFTPMoveFileOperator(
            task_id='move_file_to_processing',
            new_filename=config.processing_filepath + "{{ result('new_file_sensor') | file_name }}",
            existing_filename= "{{ result('new_file_sensor') }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        if_filename_ends_with_csv=rail.IfOperator(
            task_id='if_filename_ends_with_csv',
            test="{{result('new_file_sensor') | ends_with('csv') }}",
            yes_task="load_csv",
            no_task="send_mail_incorrect_format",
        )

        send_mail_incorrect_format=rail.EmailOperator(
            task_id='send_mail_incorrect_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon project import - Incorrect File Format - {{ current_time() }} ''',
            html_content= "templates/incorrect_format_mail.html",
        )

        archive_file_from_processing=rail.SFTPMoveFileOperator(
            task_id='archive_file_from_processing',
            new_filename=config.archive_filepath + "{{ dag_run_ecid() }}-{{ result('new_file_sensor') | file_name }}",
            existing_filename= config.processing_filepath + "{{ result('new_file_sensor') | file_name }}",
        )

        load_csv=rail.LoadCSVFileOperator(
            task_id="load_csv",
            document="{{result('download_file')}}",
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source = "{{ result('load_csv') }}",
            name = "inputfile",
            columns = {
                'Region Name':'RegionName', 
                'Project Name':'ProjectName', 
                'Project Code':'ProjectCode', 
                'Project Description':'ProjectDescription', 
                'Project Start Date':'ProjectStartDate', 
                'Project End Date':'ProjectEndDate', 
                'Task Name':'TaskName', 
                'Task Code':'TaskCode', 
                'Task Start Date':'TaskStartDate', 
                'Task End Date':'TaskEndDate'
            }
        )

        if_no_data_in_file=rail.IfOperator(
            task_id='if_no_data_in_file',
            test='''{{ result('create_inputfile_collection','length') < 1 }}''',
            yes_task="send_mail_no_data_in_file",
            no_task="get_all_project_report_details",
        )

        send_mail_no_data_in_file=rail.EmailOperator(
            task_id='send_mail_no_data_in_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}}' + '|' + ' Replicon project import - No data in the input file - ' + "{{ current_time()}}",
            html_content='templates/no_data_mail.html',
        )

        get_all_project_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_all_project_report_details',
            report_name=config.all_project_report
        )

        run_all_project_report = rail.run_report2(
            group_id='run_all_project_report',
            report_params= lambda: {
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_all_project_report_details')['uri'],
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv=rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('run_all_project_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_collection_from_report = rail.CreateCollectionOperator(
            task_id='create_collection_from_report',
            source = "{{ result('parse_csv') }}",
            name = "projectstatus",
            columns = {
                'Project Name':'projectname', 
                'Project URI':'projecturi', 
                'Project Status':'Projectstatus'
            }
        )

        query_new_projects=rail.QueryCollectionOperator(
            task_id='query_new_projects',
            query="""Select  * from  inputfile where
                    inputfile.ProjectName NOT IN (Select  projectstatus.projectname from  projectstatus group by projectstatus.projectname)""",
        )

        query_existing_distinct_projects=rail.QueryCollectionOperator(
            task_id='query_existing_distinct_projects',
            query="""Select Distinct inputfile.RegionName, inputfile.ProjectName, inputfile.ProjectCode, inputfile.ProjectDescription, inputfile.ProjectStartDate,
                    inputfile.ProjectEndDate, projectstatus.projecturi, projectstatus.Projectstatus from  projectstatus,
                    inputfile where  inputfile.ProjectName =  projectstatus.projectname""",
        )

        get_project_import_logs_lookup_table = rail.CreateLogOperator(
            task_id='get_project_import_logs_lookup_table',
        )

        query_distinct_new_projects = rail.QueryCollectionOperator(
            task_id = 'query_distinct_new_projects',
            query="SELECT DISTINCT query_new_projects.ProjectName FROM query_new_projects WHERE NULLIF(ProjectName, '') IS NOT NULL"
        )

        query_projects_with_name_blank = rail.QueryCollectionOperator(
            task_id = 'query_projects_with_name_blank',
            query="SELECT * FROM query_new_projects WHERE NULLIF(ProjectName, '') IS NULL"
        )

        load_new_projects_to_create = rail.PythonOperator(
            task_id = 'load_new_projects_to_create',
            python_callable = lambda: rail.load_all_records(rail.result('query_new_projects'))
        )

        if_projects_with_blank_name_present = rail.IfOperator(
            task_id = 'if_projects_with_blank_name_present',
            test="{{result('query_projects_with_name_blank','length') > 0 }}",
            yes_task='process_projects_with_blank_name',
            no_task='trigger_child_to_create_new_projects'
        )

        process_projects_with_blank_name = rail.TriggerDagRunForEachItemOperator(
            task_id="process_projects_with_blank_name",
            trigger_dag_id=config.process_new_projects_dag,
            items="{{ result('query_projects_with_name_blank') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "regionname": item['RegionName'],
                "projectname": item['ProjectName'],
                "projectcode": item['ProjectCode'],
                "projectdescription": item['ProjectDescription'],
                "projectstartdate": item['ProjectStartDate'],
                "projectenddate": item['ProjectEndDate'],
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "lookuptable": rail.result('get_project_import_logs_lookup_table'),
                "inputfilename": rail.result('new_file_sensor').split('/')[-1].split('.')[0]
            },
        )

        wait_for_processing_child = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_processing_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('process_projects_with_blank_name')}}"
        )

        def get_payload_create_child(item):
            newproject = {}
            for project in rail.result('load_new_projects_to_create'):
                if project['ProjectName'] == item['ProjectName']:
                    newproject = project
                    break
            return {
                "regionname": newproject['RegionName'],
                "projectname": newproject['ProjectName'],
                "projectcode": newproject['ProjectCode'],
                "projectdescription": newproject['ProjectDescription'],
                "projectstartdate": newproject['ProjectStartDate'],
                "projectenddate": newproject['ProjectEndDate'],
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "lookuptable": rail.result('get_project_import_logs_lookup_table'),
                "inputfilename": rail.result('new_file_sensor').split('/')[-1].split('.')[0]
            }

        trigger_child_to_create_new_projects = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_child_to_create_new_projects",
            trigger_dag_id=config.process_new_projects_dag,
            items="{{ result('query_distinct_new_projects') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_payload_create_child
        )

        if_child_to_create_triggered= rail.IfOperator(
            task_id = 'if_child_to_create_triggered',
            test = lambda: rail.result('trigger_child_to_create_new_projects'),
            yes_task='wait_for_dag_create_project',
            no_task='trigger_child_to_update_project'
        )

        wait_for_dag_create_project=rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_create_project',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_child_to_create_new_projects')}}"
        )

        def get_payload_update_child(item):
            return  {
                'Regionname': item['RegionName'],
                'Projectname': item['ProjectName'],
                'Projectcode': item['ProjectCode'],
                'Projectdescription': item['ProjectDescription'],
                'Projectstartdate': item['ProjectStartDate'],
                'Projectenddate': item['ProjectEndDate'],
                'JobID': get_dagrun_ecid(rail.get_current_context()['dag_run']),
                'ProjectURI': item['projecturi'],
                'Projectstatus': item['Projectstatus'],
                'lookuptable': rail.result('get_project_import_logs_lookup_table'),
                'inputfilename': rail.result('new_file_sensor').split('/')[-1].split('.')[0]
            }

        trigger_child_to_update_project=rail.TriggerDagRunForEachItemOperator(
            task_id = 'trigger_child_to_update_project',
            retries = 0,
            items= "{{result('query_existing_distinct_projects')}}",
            trigger_dag_id=config.update_projects_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_payload_update_child
        )

        if_child_to_update_triggered= rail.IfOperator(
            task_id = 'if_child_to_update_triggered',
            test = lambda: rail.result('trigger_child_to_update_project'),
            yes_task='wait_for_dag_update_project',
            no_task='search_all_entries'
        )

        wait_for_dag_update_project=rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_update_project',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_child_to_update_project')}}"
        )

        search_all_entries=rail.FilterLogEntriesOperator(
            task_id='search_all_entries',
            log= "{{ result('get_project_import_logs_lookup_table') }}",
            properties={
                'jobid': "{{ dag_run_ecid() }}"
            }
        )

        search_success_entries=rail.FilterLogEntriesOperator(
            task_id='search_success_entries',
            log= "{{ result('get_project_import_logs_lookup_table') }}",
            properties={
                'jobid': "{{ dag_run_ecid() }}",
                'status': 'Success'
            }
        )

        def get_count():
            return rail.result('search_all_entries','length') - rail.result('search_success_entries','length')

        count_failed_entries=rail.PythonOperator(
            task_id='count_failed_entries',
            python_callable= get_count
        )

        archive_file_after_processing=rail.SFTPMoveFileOperator(
            task_id='archive_file_after_processing',
            new_filename=config.archive_filepath + "{{ dag_run_ecid() }}-{{ result('new_file_sensor') | file_name }}",
            existing_filename=config.processing_filepath + "{{ result('new_file_sensor') | file_name }}",
        )

        compose_csv_logs=rail.WriteCSVFileOperator(
            task_id='compose_csv_logs',
            source="{{ result('get_project_import_logs_lookup_table')}}",
            header=['Project Name',
                'Project Code',
                'Task Name',
                'Task Code',
                'Job ID',
                'Child Job ID',
                'Status'],
            row= lambda item: [
                    item['properties']['projectname'],
                    item['properties']['projectcode'],
                    item['properties']['taskname'],
                    item['properties']['taskcode'],
                    item['ecid'],
                    # item['properties']['jobid'],
                    item['properties']['childjobid'],
                    item['properties']['status']
            ],
        )

        get_log_file_name=rail.PythonOperator(
            task_id='get_log_file_name',
            python_callable= lambda: config.upload_filepath +
                                get_dagrun_ecid(rail.get_current_context()['dag_run']) +
                                "_Projectimport_" + datetime.now().strftime('%m%d%YT%H%M%S') +
                                "_" + rail.result('new_file_sensor').split('/')[-1]
        )

        upload_log_file=rail.SFTPUploadFileOperator(
            task_id='upload_log_file',
            content='''{{ result('compose_csv_logs') }}''',
            remote_filepath="{{result('get_log_file_name')}}"
        )

        if_no_failed_entries=rail.IfOperator(
            task_id='if_no_failed_entries',
            test='''{{ result('count_failed_entries') == 0 }}''',
            yes_task="send_success_mail",
            no_task="send_completed_with_errors_mail",
        )

        send_success_mail=rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject= "{{get_company_key()}}" + "| Project Import for Mccarthy completed successfully at " + "{{ current_time()}}",
            html_content='templates/success_mail.html',
        )

        send_completed_with_errors_mail=rail.EmailOperator(
            task_id='send_completed_with_errors_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject="{{get_company_key()}}" + "| Project Import for Mccarthy completed with errors at " + "{{ current_time()}}",
            html_content='templates/completed_with_errors_mail.html',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule='all_done',
            test="{{ result('new_file_sensor') | is_truthy}}",
            yes_task="log_to_sumo"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "file_name": "{{ result('new_file_sensor') | file_name }}",
                "archived_file_name":  "{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": "{{result('get_log_file_name')}}"
            }
        )

        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> move_file_to_processing
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> if_filename_ends_with_csv
        if_filename_ends_with_csv >> rail.Label('Yes')  >> send_mail_incorrect_format >> archive_file_from_processing >> finish
        if_filename_ends_with_csv >> rail.Label('No') >> load_csv >> create_inputfile_collection >> if_no_data_in_file
        if_no_data_in_file >> rail.Label("Yes") >> send_mail_no_data_in_file >> finish
        if_no_data_in_file >> rail.Label("No") >> get_all_project_report_details
        get_all_project_report_details >> run_all_project_report
        run_all_project_report >> parse_csv >> create_collection_from_report >> query_new_projects >> query_existing_distinct_projects
        query_existing_distinct_projects >> get_project_import_logs_lookup_table >> query_distinct_new_projects >> query_projects_with_name_blank
        query_projects_with_name_blank >> load_new_projects_to_create >> if_projects_with_blank_name_present >> rail.Label(
            'Yes') >> process_projects_with_blank_name >> wait_for_processing_child >> trigger_child_to_create_new_projects
        trigger_child_to_create_new_projects >> if_child_to_create_triggered >> rail.Label(
            'Yes') >> wait_for_dag_create_project >> trigger_child_to_update_project
        if_child_to_create_triggered >> rail.Label(
            'No') >> trigger_child_to_update_project >> if_child_to_update_triggered >> rail.Label('Yes') >> wait_for_dag_update_project
        if_child_to_update_triggered >> rail.Label('No') >> search_all_entries
        wait_for_dag_update_project >> search_all_entries
        search_all_entries >> search_success_entries >> count_failed_entries >> archive_file_after_processing >> compose_csv_logs
        compose_csv_logs >> get_log_file_name >> upload_log_file >> if_no_failed_entries
        if_no_failed_entries >> rail.Label('Yes')  >> send_success_mail >> finish
        if_no_failed_entries >> rail.Label('No') >> send_completed_with_errors_mail >> finish >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo
        if_projects_with_blank_name_present >> rail.Label('No') >> trigger_child_to_create_new_projects
    return dag

rail.for_each_instance(create_dag)
