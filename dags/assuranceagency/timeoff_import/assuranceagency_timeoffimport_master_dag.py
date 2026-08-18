
from datetime import timedelta, datetime
import hashlib
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_timeoff_import_master_{config.instance}',
        description=f'Assuranceagency timeoffimport - Master {config.instance}',
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
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath + "{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_file_ends_with_csv'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_file_ends_with_csv',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_file_ends_with_csv=rail.IfOperator(
            task_id='if_file_ends_with_csv',
            test='''{{ result('new_file_sensor') | ends_with('.csv') }}''',
            yes_task="parse_csv_8",
            no_task="send_mail_3",
        )

        send_mail_3=rail.EmailOperator(
            task_id='send_mail_3',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content= '''templates/incorrect_file_format_mail.html''',
        )

        parse_csv_8=rail.LoadCSVFileOperator(
            task_id='parse_csv_8',
            document="{{result('download_file')}}"
        )

        if_file_has_no_data=rail.IfOperator(
            task_id='if_file_has_no_data',
            test=lambda: not bool(rail.load_all_records(rail.result('parse_csv_8'))),
            yes_task="send_mail_10",
            no_task="create_csv_lines_13",
        )

        send_mail_10=rail.EmailOperator(
            task_id='send_mail_10',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content= '''templates/no_data_in_file_mail.html''',
        )

        create_csv_lines_13=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_13',
            source="{{ result('parse_csv_8') }}",
            header=['username',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'startdaytype',
                    'starttime',
                    'startdayhours',
                    'enddate',
                    'status',
                    'bookingcomments',
                    'approvalcomments',
                    'md5',
                    'formattedtimeofftype'],
            row=lambda item: [
                    item['User Name'],
                    item['Employee ID'],
                    item['TimeOff Type'],
                    datetime.strptime(item['Start Date'],'%m/%d/%Y').strftime('%Y-%m-%d') if item['Start Date'] else null,
                    item['Start Day Type'],
                    item['Start Time'],
                    item['Start Day Hours'],
                    datetime.strptime(item['End Date'],'%m/%d/%Y').strftime('%Y-%m-%d') if item['End Date'] else null,
                    item['Status'],
                    item['Booking Comments'],
                    item['Approval Comments'],
                    hashlib.md5((str(str(item['Employee ID']) + '_' + str(item['Start Date']) + '_' + str(item['Start Day Hours']) +
                    '_' + str(item['TimeOff Type']))).encode('utf-8')).hexdigest(),
                    ((item['TimeOff Type'].split(" "))[0] if item['TimeOff Type'].startswith('PTO') else
                    item['TimeOff Type']) if item['TimeOff Type'] else null
                ]
        )

        log_formatteddatetime_14=rail.PythonOperator(
            task_id='log_formatteddatetime_14',
            python_callable= lambda:  datetime.now().strftime("%Y%m%dT%H%M%S")
        )

        create_collection_create_list_from_csv_15 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_15',
            source = "{{ result('create_csv_lines_13') }}",
            name = "input_data",
            columns = {
                'username':'username', 
                'employeeid':'employeeid', 
                'timeofftype':'timeofftype', 
                'startdate':'startdate', 
                'startdaytype':'startdaytype', 
                'starttime':'starttime', 
                'startdayhours':'startdayhours', 
                'enddate':'enddate', 
                'status':'status', 
                'bookingcomments':'bookingcomments', 
                'approvalcomments':'approvalcomments', 
                'md5':'md5', 
                'formattedtimeofftype':'formattedtimeofftype'
            }
        )

        list_files_in_reference_folder=rail.SFTPListFilesOperator(
            task_id='list_files_in_reference_folder',
            paths=[config.reference_filepath],
        )

        get_reference_filename = rail.PythonOperator(
            task_id = 'get_reference_filename',
            python_callable= lambda: config.reference_filepath + (rail.result('list_files_in_reference_folder')[config.reference_filepath])[0]['name']
        )

        download_17=rail.SFTPDownloadFileOperator(
            task_id='download_17',
            remote_filepath="{{result('get_reference_filename')}}"
        )

        load_csv_create_list_from_csv_18=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_18",
            document="{{result('download_17') }}",
        )

        create_collection_create_list_from_csv_18 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_18',
            source = "{{ result('load_csv_create_list_from_csv_18') }}",
            name = "reference_data",
            columns = {
                'username':'username', 
                'employeeid':'employeeid', 
                'timeofftype':'timeofftype', 
                'startdate':'startdate', 
                'startdaytype':'startdaytype', 
                'starttime':'starttime', 
                'startdayhours':'startdayhours', 
                'enddate':'enddate', 
                'status':'status', 
                'bookingcomments':'bookingcomments', 
                'approvalcomments':'approvalcomments', 
                'md5':'md5', 
                'formattedtimeofftype':'formattedtimeofftype'
            }
        )

        query_list_queryforunchangedrecords_19=rail.QueryCollectionOperator(
            task_id='query_list_queryforunchangedrecords_19',
            query="""SELECT * FROM  input_data WHERE  input_data.md5 IN (SELECT  reference_data.md5 FROM  reference_data)""",
        )

        create_logs_lookuptable=rail.CreateLogOperator(
            task_id='create_logs_lookuptable',
        )

        if_query_list_queryforunchangedrecords_19_rows_greater_than_0_21=rail.IfOperator(
            task_id='if_query_list_queryforunchangedrecords_19_rows_greater_than_0_21',
            test='''{{ result('query_list_queryforunchangedrecords_19','length') > 0 }}''',
            yes_task="log_unchanged_records",
            no_task="query_list_queryforchangedrecords_23",
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id = 'log_unchanged_records',
            log="{{result('create_logs_lookuptable')}}",
            items="{{result('query_list_queryforunchangedrecords_19')}}",
            message='na',
            severity='Ignored',
            properties={
                'username': "{{item.username}}",
                'employeeid': "{{item.employeeid}}",
                'timeofftype': "{{item.timeofftype}}",
                'startdate': "{{item.startdate}}",
                'hours': "{{item.startdayhours}}",
                'status': "ignored",
                'details': "No change in the record",
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': ''
            }
        )

        query_list_queryforchangedrecords_23=rail.QueryCollectionOperator(
            task_id='query_list_queryforchangedrecords_23',
            query="""SELECT * FROM  input_data WHERE  input_data.md5 NOT IN (SELECT  reference_data.md5 FROM  reference_data)""",
        )

        if_query_list_queryforchangedrecords_23_rows_greater_than_0_24=rail.IfOperator(
            task_id='if_query_list_queryforchangedrecords_23_rows_greater_than_0_24',
            test='''{{ result('query_list_queryforchangedrecords_23','length') > 0 }}''',
            yes_task="create_timeoffimport_reopenedtimesheets_lookuptable",
            no_task="rename_archivethereferencefile_54",
        )

        create_timeoffimport_reopenedtimesheets_lookuptable=rail.CreateLogOperator(
            task_id='create_timeoffimport_reopenedtimesheets_lookuptable',
        )

        get_enabled_users_list_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_enabled_users_list_report_details',
            report_name=config.enabled_users_list_report
        )

        generate_enabled_users_list_report=rail.run_report2(
            group_id='generate_enabled_users_list_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                    "reportUri": "{{result('get_enabled_users_list_report_details').uri}}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        if_generate_report_27_payload_starts_with_nodata_28=rail.IfOperator(
            task_id='if_generate_report_27_payload_starts_with_nodata_28',
            #pylint: disable = line-too-long
            test="{{(result('generate_enabled_users_list_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | starts_with('No Data')}}",
            yes_task="stop_29",
            no_task="if_generate_report_27_payload_starts_with_usernameemployeeiduseruri_30",
        )

        stop_29=rail.FailOperator(
            task_id='stop_29',
            message='''No Data in the base report'''
        )

        if_generate_report_27_payload_starts_with_usernameemployeeiduseruri_30=rail.IfOperator(
            task_id='if_generate_report_27_payload_starts_with_usernameemployeeiduseruri_30',
            #pylint: disable = line-too-long
            test='''{{ (result('generate_enabled_users_list_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | starts_with('User Name,Employee ID,UserUri') }}''',
            yes_task="parse_csv_32",
            no_task="stop_31",
        )

        stop_31=rail.FailOperator(
            task_id='stop_31',
            message='''Base report column order doesn't match'''
        )

        parse_csv_32=rail.LoadCSVFileOperator(
            task_id='parse_csv_32',
            document="{{(result('generate_enabled_users_list_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}"
        )

        get_enabled_time_off_types_33=rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types_33',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        load_enbabled_users_list = rail.PythonOperator(
            task_id = 'load_enbabled_users_list',
            python_callable=lambda: rail.load_all_records(rail.result('parse_csv_32'))
        )

        trigger_dag_process_bookings_child_35=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_process_bookings_child_35',
            retries=0,
            items="{{ result('query_list_queryforchangedrecords_23') }}",
            trigger_dag_id=f'assuranceagency_timeoffimport_process_bookings_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "username": item['username'],
                "employeeid": item['employeeid'],
                "timeofftype": item['timeofftype'],
                "startdate": item['startdate'],
                "startdaytype": item['startdaytype'],
                "starttime": item['starttime'],
                "startdayhours": item['startdayhours'],
                "enddate": item['enddate'],
                "status": item['status'],
                "bookingcomments": item['bookingcomments'],
                "approvalcomments": item['approvalcomments'],
                "md5": item['md5'],
                "useruri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_enbabled_users_list'),'Employee ID',item['employeeid'],'UserUri','') if item['employeeid'] else null,
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_enabled_time_off_types_33'),'displayText',item['formattedtimeofftype'],'uri','') if item['formattedtimeofftype'] else null,
                "formattedtimeofftype": item['formattedtimeofftype'],
                "logslookuptable": rail.result('create_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}"),
                "reopenedtimesheetslookup": rail.result('create_timeoffimport_reopenedtimesheets_lookuptable')
            }
        )

        wait_for_completion_trigger_dag_process_bookings_child_35 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_process_bookings_child_35',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_process_bookings_child_35") }}'
        )

        trigger_dag_process_reopenedtimesheets_child37=rail.TriggerDagRunOperator(
            task_id='trigger_dag_process_reopenedtimesheets_child37',
            retries=0,
            trigger_dag_id=f'assuranceagency_timeoffimport_process_reopenedtimesheets_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "callerjobid": "{{ dag_run_ecid() }}",
                "reopenedtimesheetslookup": "{{result('create_timeoffimport_reopenedtimesheets_lookuptable')}}"
            }
        )

        wait_for_completion_trigger_dag_run_process_reopenedtimesheets_child37 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_process_reopenedtimesheets_child37',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_process_reopenedtimesheets_child37") }}'
        )

        assuranceagency_timeoffimport_logs_search_entries_38=rail.FilterLogEntriesOperator(
            task_id='assuranceagency_timeoffimport_logs_search_entries_38',
            log="{{result('create_logs_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        create_csv_lines_41=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_41',
            source="{{ result('assuranceagency_timeoffimport_logs_search_entries_38') }}",
            header=['username',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'hours',
                    'status',
                    'details',
                    'jobid'],
            row= [
                "{{ item.properties.username }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.timeofftype }}",
                "{{ item.properties.startdate }}",
                "{{ item.properties.hours }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}_{{ item.properties.childjobid }}"
            ],
        )

        upload_uploadthelogfile_42=rail.SFTPUploadFileOperator(
            task_id='upload_uploadthelogfile_42',
            content='''{{ result('create_csv_lines_41') }}''',
            remote_filepath=config.log_filepath + '''assuranceagency_timeoffimportlogs_{{ result('log_formatteddatetime_14') }}.csv''',
        )

        generate_downloadlink=rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('create_csv_lines_41')}}",
            output_file_name="assuranceagency_timeoffimportlogs_{{ result('log_formatteddatetime_14') }}.csv",
            expires_in_seconds=7*24*60*60,
        )

        log_checkforfailedrecords_44=rail.PythonOperator(
            task_id='log_checkforfailedrecords_44',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.load_all_records(rail.result(
                'assuranceagency_timeoffimport_logs_search_entries_38')),'properties.status','failed','properties.status','')
        )

        if_log_checkforfailedrecords_44_present_45=rail.IfOperator(
            task_id='if_log_checkforfailedrecords_44_present_45',
            test='''{{ result('log_checkforfailedrecords_44') | is_truthy }}''',
            yes_task="send_mail_with_cshare_46",
            no_task="send_mail_with_cshare_48",
        )

        send_mail_with_cshare_46=rail.EmailOperator(
            task_id='send_mail_with_cshare_46',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{get_company_key()}} | Replicon timeoff import completed with failed records -{{ current_time() }} ''',
            html_content= '''templates/success_with_failed_records_mail.html''',
        )

        send_mail_with_cshare_48=rail.EmailOperator(
            task_id='send_mail_with_cshare_48',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Replicon timeoff import completed successfully -{{ current_time() }} ''',
            html_content= '''templates/success_mail.html''',
        )

        rename_archivethereferencefile_50=rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferencefile_50',
            new_filename=config.archive_filepath + '{{result("get_reference_filename") | file_name}}',
            existing_filename='''{{result("get_reference_filename")}}''',
        )

        upload_uploadthereferencefile_51=rail.SFTPUploadFileOperator(
            task_id='upload_uploadthereferencefile_51',
            content='''{{ result('create_csv_lines_13') }}''',
            remote_filepath= config.reference_filepath + '''reference_{{ result('log_formatteddatetime_14') }}.csv''',
        )

        rename_archivethereferencefile_54=rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferencefile_54',
            new_filename=config.archive_filepath + '{{result("get_reference_filename") | file_name}}',
            existing_filename='''{{result("get_reference_filename")}}''',
        )

        upload_uploadthereferencefile_55=rail.SFTPUploadFileOperator(
            task_id='upload_uploadthereferencefile_55',
            content='''{{ result('create_csv_lines_13') }}''',
            remote_filepath= config.reference_filepath + '''reference_{{ result('log_formatteddatetime_14') }}.csv''',
        )

        search_logs_in_lookuptable = rail.FilterLogEntriesOperator(
            task_id = 'search_logs_in_lookuptable',
            log="{{result('create_logs_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_declare_list_20_list_items_greater_than_0_56=rail.IfOperator(
            task_id='if_declare_list_20_list_items_greater_than_0_56',
            test='''{{ result('search_logs_in_lookuptable','length') > 0 }}''',
            yes_task="create_csv_lines_58",
            no_task="log_to_sumo",
        )

        create_csv_lines_58=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_58',
            source="{{ result('search_logs_in_lookuptable')}}",
            header=['username',
                    'employeeid',
                    'timeofftype',
                    'startdate',
                    'hours',
                    'status',
                    'details',
                    'jobid'],
            row= [
                    "{{ item.properties.username }}",
                    "{{ item.properties.employeeid }}",
                    "{{ item.properties.timeofftype }}",
                    "{{ item.properties.startdate }}",
                    "{{ item.properties.hours }}",
                    "{{ item.properties.status }}",
                    "{{ item.properties.details }}",
                    "{{ item.properties.jobid }}_{{ item.properties.childjobid }}"
                ],
        )

        upload_uploadthelogfile_59=rail.SFTPUploadFileOperator(
            task_id='upload_uploadthelogfile_59',
            content='''{{ result('create_csv_lines_58') }}''',
            remote_filepath= config.log_filepath + '''assuranceagency_timeoffimportlogs_{{ result('log_formatteddatetime_14') }}.csv''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_58')}}",
            output_file_name="assuranceagency_timeoffimportlogs_{{ result('log_formatteddatetime_14') }}.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_mail_with_cshare_61=rail.EmailOperator(
            task_id='send_mail_with_cshare_61',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Replicon timeoff import completed successfully -{{ current_time() }} ''',
            html_content= '''templates/success_with_no_delta_records_mail.html''',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_file_ends_with_csv
        if_file_ends_with_csv >> rail.Label('No')  >> send_mail_3 >> log_to_sumo
        if_file_ends_with_csv >> rail.Label('Yes') >> parse_csv_8 >> if_file_has_no_data
        if_file_has_no_data >> rail.Label('Yes')  >> send_mail_10 >> log_to_sumo
        if_file_has_no_data >> rail.Label('No') >> create_csv_lines_13 >> log_formatteddatetime_14 >> create_collection_create_list_from_csv_15
        create_collection_create_list_from_csv_15 >> list_files_in_reference_folder >> get_reference_filename >> download_17
        download_17 >> load_csv_create_list_from_csv_18 >> create_collection_create_list_from_csv_18 >> query_list_queryforunchangedrecords_19
        query_list_queryforunchangedrecords_19 >> create_logs_lookuptable >>  if_query_list_queryforunchangedrecords_19_rows_greater_than_0_21
        if_query_list_queryforunchangedrecords_19_rows_greater_than_0_21 >> rail.Label('Yes') >> log_unchanged_records >> query_list_queryforchangedrecords_23
        if_query_list_queryforunchangedrecords_19_rows_greater_than_0_21 >> rail.Label(
            'No') >> query_list_queryforchangedrecords_23 >> if_query_list_queryforchangedrecords_23_rows_greater_than_0_24
        if_query_list_queryforchangedrecords_23_rows_greater_than_0_24 >> rail.Label(
            'Yes') >> create_timeoffimport_reopenedtimesheets_lookuptable >> get_enabled_users_list_report_details >> generate_enabled_users_list_report
        generate_enabled_users_list_report >> if_generate_report_27_payload_starts_with_nodata_28
        if_generate_report_27_payload_starts_with_nodata_28 >> rail.Label('Yes')  >> stop_29 >> log_to_sumo
        if_generate_report_27_payload_starts_with_nodata_28 >> rail.Label('No') >> if_generate_report_27_payload_starts_with_usernameemployeeiduseruri_30
        if_generate_report_27_payload_starts_with_usernameemployeeiduseruri_30 >> rail.Label('No')  >> stop_31 >> log_to_sumo
        if_generate_report_27_payload_starts_with_usernameemployeeiduseruri_30 >> rail.Label(
            'Yes') >> parse_csv_32 >> get_enabled_time_off_types_33 >> load_enbabled_users_list
        load_enbabled_users_list >> trigger_dag_process_bookings_child_35
        trigger_dag_process_bookings_child_35 >> wait_for_completion_trigger_dag_process_bookings_child_35
        wait_for_completion_trigger_dag_process_bookings_child_35 >> trigger_dag_process_reopenedtimesheets_child37
        trigger_dag_process_reopenedtimesheets_child37 >> wait_for_completion_trigger_dag_run_process_reopenedtimesheets_child37
        wait_for_completion_trigger_dag_run_process_reopenedtimesheets_child37 >> assuranceagency_timeoffimport_logs_search_entries_38 >> create_csv_lines_41
        create_csv_lines_41 >> upload_uploadthelogfile_42 >> generate_downloadlink >> log_checkforfailedrecords_44
        log_checkforfailedrecords_44 >> if_log_checkforfailedrecords_44_present_45
        if_log_checkforfailedrecords_44_present_45 >> rail.Label('Yes')  >> send_mail_with_cshare_46 >> rename_archivethereferencefile_50
        if_log_checkforfailedrecords_44_present_45 >> rail.Label(
            'No') >> send_mail_with_cshare_48 >> rename_archivethereferencefile_50 >> upload_uploadthereferencefile_51 >> log_to_sumo
        if_query_list_queryforchangedrecords_23_rows_greater_than_0_24 >> rail.Label('No') >> rename_archivethereferencefile_54
        rename_archivethereferencefile_54 >> upload_uploadthereferencefile_55 >> search_logs_in_lookuptable >> if_declare_list_20_list_items_greater_than_0_56
        if_declare_list_20_list_items_greater_than_0_56 >> rail.Label(
            'Yes') >> create_csv_lines_58 >> upload_uploadthelogfile_59 >> generate_download_link >> send_mail_with_cshare_61 >> log_to_sumo
        if_declare_list_20_list_items_greater_than_0_56 >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
