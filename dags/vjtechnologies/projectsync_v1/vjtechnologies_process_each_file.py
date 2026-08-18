from datetime import timedelta
import hashlib
from vjtechnologies.projectsync_v1.mappers.vjtechnologies_projectstatus_mapper import projectstatus_mapper
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_each_file_dagid,
        description=f'VJTechnologies_{config.entity_name}_Client_Project_Import_Process_each_file_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_filesize_greater_than_zero'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_filesize_greater_than_zero',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )


        if_filesize_greater_than_zero=rail.IfOperator(
            task_id='if_filesize_greater_than_zero',
            test=lambda dag_run: int(dag_run.conf['filesize']) > 0,
            yes_task="if_filename_ends_with_csv",
            no_task="log_to_sumo",
        )

        if_filename_ends_with_csv=rail.IfOperator(
            task_id='if_filename_ends_with_csv',
            test='''{{ dag_run.conf.filename | ends_with('csv') }}''',
            yes_task="download_file",
            no_task="send_mail_incorrect_fileformat",
        )

        download_file=rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ dag_run.conf.filename }}"
        )

        archive_input_file=rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            new_filename=config.archive_filepath + "{{dag_run.conf.jobstarttime}}_input_{{dag_run.conf.filename | file_name}}",
            existing_filename= "{{ dag_run.conf.filename }}",
        )

        parse_csv_input_file=rail.LoadCSVFileOperator(
            task_id='parse_csv_input_file',
            encoding='cp1252',
            document="{{result('download_file')}}",
        )

        if_file_has_no_data=rail.IfOperator(
            task_id='if_file_has_no_data',
            test=lambda: len(rail.load_all_records(rail.result('parse_csv_input_file'))) < 1,
            yes_task="send_mail_no_records_found",
            no_task="compose_csv_with_md5",
        )

        send_mail_no_records_found=rail.EmailOperator(
            task_id='send_mail_no_records_found',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Client project import - no records in file - {{ dag_run.conf.jobstarttime }} ''',
            html_content= '''templates/no_records_in_file_mail.html''',
        )

        compose_csv_with_md5=rail.WriteCSVFileOperator(
            task_id='compose_csv_with_md5',
            source="{{ result('parse_csv_input_file') }}",
            header=['VJ Entity Name',
                    'Project Name',
                    'Project Code',
                    'Project Status',
                    'Projected start',
                    'Projected end',
                    'Client ID',
                    'Client Name',
                    'Project Controller',
                    'Task Name',
                    'Task Code',
                    'Project Category',
                    'Task Start Date',
                    'Task End Date',
                    'Estimated Effort Hours',
                    'User ID',
                    'encoded'],
            row=lambda item: [
                item['VJ Entity Name'].strip() if item['VJ Entity Name'] else '',
                item['Project Name'].strip() if item['Project Name'] else '',
                item['Project Code'].strip() if item['Project Code'] else '',
                item['Project Status'].strip() if item['Project Status'] else '',
                item['Projected start'],
                item['Projected end'],
                item['Client ID'].strip() if item['Client ID'] else '',
                item['Client Name'].strip() if item['Client Name'] else '',
                item['Project Controller'].strip() if item['Project Controller'] else '',
                item['Task Name'].strip() if item['Task Name'] else '',
                item['Task Code'].strip() if item['Task Code'] else '',
                item['Project Category'].strip() if item['Project Category'] else '',
                item['Task Start Date'],
                item['Task End Date'],
                item['Estimated Effort Hours'].strip() if item['Estimated Effort Hours'] else '',
                item['User ID'],
                hashlib.md5((str(
                    str(item['VJ Entity Name']) + ',' + str(item['Project Name']) + ',' + str(item['Project Code']) + ',' + str(item['Project Status']) + ',' +
                    str(item['Projected start']) + ',' + str(item['Projected end']) + ',' + str(item['Client ID']) + ',' + str(item['Client Name']) + ',' +
                    str(item['Project Controller']) + ',' + str(item['Task Name']) + ',' + str(item['Task Code']) + ',' + str(item['Project Category']) + ',' +
                    str(item['Task Start Date']) + ',' + str(item['Task End Date']) + ',' + str(item['Estimated Effort Hours']) + ',' + str(item['User ID']))
                ).encode('utf-8')).hexdigest()

            ],
        )

        create_collection_encodeddata = rail.CreateCollectionOperator(
            task_id='create_collection_encodeddata',
            source = "{{ result('compose_csv_with_md5') }}",
            name = "encodeddata",
            columns = {
                'VJ Entity Name':'vjentityname',
                'Project Name':'projectname',
                'Project Code':'projectcode',
                'Project Status':'projectstatus',
                'Projected start':'projectstart',
                'Projected end':'projectend',
                'Client ID':'clientid',
                'Client Name':'clientname',
                'Project Controller':'projectcontroller',
                'Task Name':'taskname',
                'Task Code':'taskcode',
                'Project Category':'projectcategory',
                'Task Start Date':'taskstartdate',
                'Task End Date':'taskenddate',
                'Estimated Effort Hours':'estimatedefforthours',
                'User ID':'userid',
                'encoded':'encoded'
            }
        )

        list_files_from_reference_filepath=rail.SFTPListFilesOperator(
            task_id='list_files_from_reference_filepath',
            paths=[config.reference_filepath],
        )

        log_reference_filename = rail.PythonOperator(
            task_id = 'log_reference_filename',
            python_callable=lambda: config.reference_filepath + rail.result('list_files_from_reference_filepath')[config.reference_filepath][0]['name']
        )

        download_reference_file=rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('log_reference_filename') }}"
        )

        load_csv_reference_file=rail.LoadCSVFileOperator(
            task_id="load_csv_reference_file",
            document="{{result('download_reference_file') }}",
        )

        create_collection_referencedata = rail.CreateCollectionOperator(
            task_id='create_collection_referencedata',
            source = "{{ result('load_csv_reference_file') }}",
            name = "referencedata",
            columns = {
                'VJ Entity Name':'vjentityname',
                'Project Name':'projectname',
                'Project Code':'projectcode',
                'Project Status':'projectstatus',
                'Projected start':'projectstart',
                'Projected end':'projectend',
                'Client ID':'clientid',
                'Client Name':'clientname',
                'Project Controller':'projectcontroller',
                'Task Name':'taskname',
                'Task Code':'taskcode',
                'Project Category':'projectcategory',
                'Task Start Date':'taskstartdate',
                'Task End Date':'taskenddate',
                'Estimated Effort Hours':'estimatedefforthours',
                'User ID':'userid',
                'encoded':'encoded'
            }
        )

        create_projectsync_logs_lookuptable = rail.CreateLogOperator(
            task_id = 'create_projectsync_logs_lookuptable'
        )

        query_unchanged_records=rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  encodeddata WHERE  encodeddata.encoded IN (SELECT DISTINCT  referencedata.encoded FROM  referencedata)""",
        )

        if_unchanged_records_present=rail.IfOperator(
            task_id='if_unchanged_records_present',
            test='''{{ result('query_unchanged_records','length') > 0 }}''',
            yes_task="add_log_no_change_in_client_or_project",
            no_task="query_new_or_updated_records",
        )

        add_log_no_change_in_client_or_project=rail.WriteLogOperator(
            task_id='add_log_no_change_in_client_or_project',
            log="{{ result('create_projectsync_logs_lookuptable') }}",
            items = "{{result('query_unchanged_records')}}",
            message="na",
            severity="Skipped",
            properties={
                "client": "{{item.clientname}}",
                "type": 'project',
                "project": "{{item.projectname}}",
                "code": "{{item.projectcode}}",
                "task": "{{item.taskname}}",
                "status": "Skipped",
                "reason": "No change in client/project record",
                "jobid": "{{dag_run.conf.masterdagid}}",
                "childjobid":''
            }
        )

        query_new_or_updated_records=rail.QueryCollectionOperator(
            task_id='query_new_or_updated_records',
            name='validatedinput',
            query="""SELECT * FROM  encodeddata WHERE  encodeddata.encoded NOT IN (SELECT DISTINCT  referencedata.encoded FROM  referencedata)""",
        )

        if_inputfile_has_data=rail.IfOperator(
            task_id='if_inputfile_has_data',
            test='''{{ result('create_collection_encodeddata','length') > 0 }}''',
            yes_task="query_client_records",
            no_task="archive_reference_file",
        )

        query_client_records=rail.QueryCollectionOperator(
            task_id='query_client_records',
            query="""SELECT DISTINCT  validatedinput.clientname, validatedinput.clientid FROM  validatedinput""",
        )

        trigger_child_to_process_each_client_record=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_to_process_each_client_record',
            retries=0,
            items="{{ result('query_client_records') }}",
            trigger_dag_id=config.process_each_client_record_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "name": item['clientname'],
                "code": item['clientid'],
                "logslookuptable": rail.result('create_projectsync_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run.conf.masterdagid}}")
            }
        )

        if_distinct_client_records_present = rail.IfOperator(
            task_id = 'if_distinct_client_records_present',
            test="{{result('query_client_records','length') > 0}}",
            yes_task='wait_for_child_to_process_each_client_record',
            no_task='query_project_records'
        )

        wait_for_child_to_process_each_client_record = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_process_each_client_record',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_process_each_client_record") }}'
        )

        query_project_records=rail.QueryCollectionOperator(
            task_id='query_project_records',
            name = 'projectrecords',
            query="""SELECT * FROM  validatedinput""",
        )

        query_unique_projects = rail.QueryCollectionOperator(
            task_id = 'query_unique_projects',
            name='uniqueprojects',
            query="""SELECT vjentityname, projectname, projectcode,MIN(projectstatus) as projectstatus, projectstart, projectend, clientid, clientname,
                projectcontroller, taskname, taskcode, projectcategory, taskstartdate, taskenddate, estimatedefforthours, userid,
                encoded FROM projectrecords GROUP BY projectcode, projectname"""
        )

        query_rest_of_projects = rail.QueryCollectionOperator(
            task_id = 'query_rest_of_projects',
            name='restprojects',
            query="""SELECT * FROM projectrecords EXCEPT SELECT * FROM uniqueprojects"""
        )

        if_project_records_present=rail.IfOperator(
            task_id='if_project_records_present',
            test='''{{ result('query_project_records','length') > 0 }}''',
            yes_task="get_projectstatus_mapper_entries",
            no_task="archive_reference_file",
        )

        get_projectstatus_mapper_entries=rail.PythonOperator(
            task_id='get_projectstatus_mapper_entries',
            python_callable= lambda:  list(filter(lambda entry: entry["identifier"] == "yes" ,projectstatus_mapper))
        )

        trigger_child_to_process_each_unique_project_record=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_to_process_each_unique_project_record',
            retries=0,
            items="{{ result('query_unique_projects') }}",
            trigger_dag_id=config.process_each_project_record_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "clientname": item['clientname'],
                "projectname": item['projectname'],
                "projectcode": str(item['projectcode']) if item['projectcode'] else '',
                "projectstart": item['projectstart'] if item['projectstart'] else '',
                "projectend": item['projectend'] if item['projectend'] else '',
                "projectmanager": item['projectcontroller'],
                "projectstatus": item['projectstatus'],
                "taskname": item['taskname'],
                "taskcode": item['taskcode'],
                "taskdescription": item['projectcategory'],
                "taskstartdate": item['taskstartdate'] if item['taskstartdate'] else '',
                "taskenddate": item['taskenddate'] if item['taskenddate'] else '',
                "estimatedefforthours": item['estimatedefforthours'],
                "userid": item['userid'],
                "companycode": item['vjentityname'],
                "clientcode": item['clientid'],
                "logslookuptable": rail.result('create_projectsync_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run.conf.masterdagid}}")
            }
        )

        wait_for_child_to_process_each_unique_project_record = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_process_each_unique_project_record',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_process_each_unique_project_record") }}'
        )

        trigger_child_to_process_each_restof_project_record=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_to_process_each_restof_project_record',
            retries=0,
            items="{{ result('query_rest_of_projects') }}",
            trigger_dag_id=config.process_each_project_record_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "clientname": item['clientname'],
                "projectname": item['projectname'],
                "projectcode": str(item['projectcode']) if item['projectcode'] else '',
                "projectstart": item['projectstart'] if item['projectstart'] else '',
                "projectend": item['projectend'] if item['projectend'] else '',
                "projectmanager": item['projectcontroller'],
                "projectstatus": item['projectstatus'],
                "taskname": item['taskname'],
                "taskcode": item['taskcode'],
                "taskdescription": item['projectcategory'],
                "taskstartdate": item['taskstartdate'] if item['taskstartdate'] else '',
                "taskenddate": item['taskenddate'] if item['taskenddate'] else '',
                "estimatedefforthours": item['estimatedefforthours'],
                "userid": item['userid'],
                "companycode": item['vjentityname'],
                "clientcode": item['clientid'],
                "logslookuptable": rail.result('create_projectsync_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run.conf.masterdagid}}")
            }
        )

        wait_for_child_to_process_each_restof_project_record = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_process_each_restof_project_record',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_process_each_restof_project_record") }}'
        )

        search_log_entries_in_lookuptable = rail.FilterLogEntriesOperator(
            task_id = 'search_log_entries_in_lookuptable',
            log="{{result('create_projectsync_logs_lookuptable')}}",
            properties={
                'jobid': '{{dag_run.conf.masterdagid}}',
                'type': 'project'
            }
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id = 'compose_logs_csv',
            source="{{result('search_log_entries_in_lookuptable')}}",
            header=['Client Name',
                    'Project Name',
                    'Code',
                    'TaskName',
                    'Status',
                    'Details',
                    'Job ID'],
            row= [
                    "{{ item.properties.client }}",
                    "{{ item.properties.project }}",
                    "{{ item.properties.code }}",
                    "{{ item.properties.task}}",
                    "{{ item.properties.status }}",
                    "{{ item.properties.reason }}",
                    "{{ item.properties.jobid }}|{{item.properties.childjobid}}"
                ],
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content='''{{ result('compose_logs_csv') }}''',
            remote_filepath=config.log_filepath +  '''log_{{dag_run.conf.jobstarttime}}_{{ dag_run.conf.filename | file_name }}''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name='log_{{dag_run.conf.jobstarttime}}_{{ dag_run.conf.filename | file_name}}',
            expires_in_seconds=7*24*60*60,
        )

        def get_job_status():
            logentries = rail.load_all_records(rail.result('search_log_entries_in_lookuptable'))
            errorcheck = rail.find_first_by_attr_and_get_attr(logentries,'properties.status','Error','properties.status','')
            exceptioncheck = rail.find_first_by_attr_and_get_attr(logentries,'properties.status','Exception','properties.status','')
            return {
                'errorcheckoutput': errorcheck,
                'exceptioncheckoutput': exceptioncheck,
                'subjectoutput': 'completed with errors' if errorcheck else ( 'completed with exceptions' if exceptioncheck else 'completed succesfully'),
                #pylint: disable = line-too-long
                'bodyoutput': '<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>' if errorcheck else '<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'
            }

        get_statuses = rail.PythonOperator(
            task_id = 'get_statuses',
            python_callable=get_job_status
        )

        if_error_or_exception_present=rail.IfOperator(
            task_id='if_error_or_exception_present',
            test='''{{ result('get_statuses').errorcheckoutput | is_truthy  or result('get_statuses').exceptioncheckoutput | is_truthy }}''',
            yes_task="send_completion_mail",
            no_task="archive_reference_file",
        )

        send_completion_mail=rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc="{%- if result('get_statuses')['errorcheckoutput'] -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }}| Client project import {{ result('get_statuses').subjectoutput }} - {{ dag_run.conf.jobstarttime }} ''',
            html_content= '''templates/completion_mail.html''',
        )

        archive_reference_file=rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.archive_filepath + "{{dag_run.conf.masterdagid}}_{{result('log_reference_filename') | file_name}}",
            existing_filename="{{result('log_reference_filename')}}"
        )

        upload_new_reference_file=rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('compose_csv_with_md5') }}''',
            remote_filepath= config.reference_filepath + 'reference_{{dag_run.conf.filename | file_name }}',
        )

        if_new_or_updated_records_not_present=rail.IfOperator(
            task_id='if_new_or_updated_records_not_present',
            test='''{{ result('query_new_or_updated_records','length') < 1 }}''',
            yes_task="send_mail_no_records_to_process",
            no_task="log_to_sumo",
        )

        send_mail_no_records_to_process=rail.EmailOperator(
            task_id='send_mail_no_records_to_process',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Client project import completed successfully - {{ dag_run.conf.jobstarttime }} ''',
            html_content= '''templates/no_records_to_process_mail.html''',
        )

        send_mail_incorrect_fileformat=rail.EmailOperator(
            task_id='send_mail_incorrect_fileformat',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Client project import - File processing is skipped - {{ dag_run.conf.jobstarttime }} ''',
            html_content= '''templates/incorrect_file_format_mail.html''',
        )

        archive_the_input_file=rail.SFTPMoveFileOperator(
            task_id='archive_the_input_file',
            new_filename=config.archive_filepath + "{{dag_run.conf.jobstarttime}}_input_{{dag_run.conf.filename | file_name}}",
            existing_filename= "{{ dag_run.conf.filename }}",
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_filesize_greater_than_zero
        if_filesize_greater_than_zero >> rail.Label('Yes')  >> if_filename_ends_with_csv
        if_filename_ends_with_csv >> rail.Label('Yes') >> download_file >> archive_input_file >> parse_csv_input_file >> if_file_has_no_data
        if_file_has_no_data >> rail.Label('Yes')  >> send_mail_no_records_found >> log_to_sumo
        if_file_has_no_data >> rail.Label(
            'No') >> compose_csv_with_md5 >> create_collection_encodeddata >> list_files_from_reference_filepath >> log_reference_filename
        log_reference_filename >> download_reference_file >> load_csv_reference_file >> create_collection_referencedata
        create_collection_referencedata >> create_projectsync_logs_lookuptable >> query_unchanged_records >> if_unchanged_records_present
        if_unchanged_records_present >> rail.Label('Yes')  >> add_log_no_change_in_client_or_project >> query_new_or_updated_records
        if_unchanged_records_present >> rail.Label('No') >> query_new_or_updated_records >> if_inputfile_has_data
        if_inputfile_has_data >> rail.Label('Yes') >> query_client_records >> trigger_child_to_process_each_client_record >> if_distinct_client_records_present
        if_inputfile_has_data >> rail.Label('No') >> archive_reference_file
        if_distinct_client_records_present >> rail.Label('No') >> query_project_records
        if_distinct_client_records_present >> rail.Label('Yes') >> wait_for_child_to_process_each_client_record >> query_project_records
        query_project_records >> query_unique_projects >> query_rest_of_projects >> if_project_records_present
        if_project_records_present >> rail.Label('Yes') >> get_projectstatus_mapper_entries >> trigger_child_to_process_each_unique_project_record
        trigger_child_to_process_each_unique_project_record >> wait_for_child_to_process_each_unique_project_record
        wait_for_child_to_process_each_unique_project_record >> trigger_child_to_process_each_restof_project_record
        trigger_child_to_process_each_restof_project_record >> wait_for_child_to_process_each_restof_project_record >> search_log_entries_in_lookuptable
        search_log_entries_in_lookuptable >> compose_logs_csv >> upload_logs_to_sftp >> generate_download_link >> get_statuses >> if_error_or_exception_present
        if_error_or_exception_present >> rail.Label('Yes') >> send_completion_mail >> archive_reference_file
        if_error_or_exception_present >> rail.Label('No') >> archive_reference_file
        if_project_records_present >> rail.Label('No') >> archive_reference_file >> upload_new_reference_file >> if_new_or_updated_records_not_present
        if_new_or_updated_records_not_present >> rail.Label('Yes')  >> send_mail_no_records_to_process >> log_to_sumo
        if_new_or_updated_records_not_present >> rail.Label('No') >> log_to_sumo
        if_filename_ends_with_csv >> rail.Label('No') >> send_mail_incorrect_fileformat >> archive_the_input_file >> log_to_sumo
        if_filesize_greater_than_zero >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
