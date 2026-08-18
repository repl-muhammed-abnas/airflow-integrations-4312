
from datetime import timedelta
import itertools
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_timeoff_policy_import_kla_time_off_balance_and_accrual_import_master_{config.instance}',
        description=f'KLA_Time Off balance and accrual Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=60),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task='batch_task',
            no_task='if_name_downcase_ends_with_csv_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_name_downcase_ends_with_csv_2',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_name_downcase_ends_with_csv_2 = rail.IfOperator(
            task_id='if_name_downcase_ends_with_csv_2',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task="download_3",
            no_task="send_mail_incorrectfileformat_57",
        )

        download_3 = rail.SFTPDownloadFileOperator(
            task_id='download_3',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.sftp_archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}.csv''',
            existing_filename=config.sftp_input_filepath +
            '''/{{ result("new_file_sensor") | file_name }}''',
        )

        parse_csv_4 = rail.LoadCSVFileOperator(
            task_id='parse_csv_4',
            document="{{ result('download_3') }}",
            headers=['employeeid',
                     'Loginname',
                     'Timeofftypename',
                     'Balance',
                     'Effectivedate',
                     'Accrualrate',
                     ]
        )

        create_csv_lines_withmd5_5 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_withmd5_5',
            source="{{ result('parse_csv_4') }}",
            header=['employeeid',
                    'Loginname',
                    'Timeofftypename',
                    'Balance',
                    'Effectivedate',
                    'Accrualrate',
                    'md5'],
            row=[
                "{{ item.employeeid }}",
                "{{ item.Loginname }}",
                "{{ item.Timeofftypename }}",
                "{{ item.Balance }}",
                "{{ item.Effectivedate }}",
                "{{ item.Accrualrate }}",
                "{{ ( item.employeeid  + item.Loginname + item.Timeofftypename + item.Balance + item.Effectivedate + item.Accrualrate ) | md5 }}",
            ],
        )

        load_csv_create_list_from_csv_6 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_6",
            document="{{result('create_csv_lines_withmd5_5') }}",
        )

        create_collection_create_list_from_csv_6 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_6',
            source="{{ result('load_csv_create_list_from_csv_6') }}",
            name="rawfilewithmd5",
            columns={
                'employeeid': 'employeeid',
                'Loginname': 'loginname',
                'Timeofftypename': 'timeofftypename',
                'Balance': 'balance',
                'Effectivedate': 'effectivedate',
                'Accrualrate': 'accrualrate',
                'md5': 'md5'
            }
        )

        dir_7 = rail.SFTPListFilesOperator(
            task_id='dir_7',
            paths=[config.sftp_ref_filepath],
        )

        if_first_name_present_8 = rail.IfOperator(
            task_id='if_first_name_present_8',
            test='''{{ result('dir_7') | is_truthy }}''',
            yes_task="download_9",
            no_task="stop_11",
        )

        download_9 = rail.SFTPDownloadFileOperator(
            task_id='download_9',
            remote_filepath=config.sftp_ref_file,
        )

        stop_11 = rail.FailOperator(
            task_id='stop_11',
            message='''Reference file not found'''
        )

        load_csv_create_list_from_csv_12 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_12",
            document="{{result('download_9')}}",
        )

        create_collection_create_list_from_csv_12 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_12',
            source="{{ result('load_csv_create_list_from_csv_12') }}",
            name="referencefile",
            columns={
                'employeeid': 'employeeid',
                'loginname': 'loginname',
                'timeofftypename': 'timeofftypename',
                'balance': 'balance',
                'effectivedate': 'effectivedate',
                'accrualrate': 'accrualrate',
                'md5': 'md5'
            }
        )

        query_list_newchangedvalues_13 = rail.QueryCollectionOperator(
            task_id='query_list_newchangedvalues_13',
            query="""SELECT * FROM  rawfilewithmd5 WHERE  rawfilewithmd5.md5 NOT IN (SELECT DISTINCT  referencefile.md5 FROM  referencefile)""",
        )

        query_list_unchangedvalues_14 = rail.QueryCollectionOperator(
            task_id='query_list_unchangedvalues_14',
            query="""SELECT * FROM  rawfilewithmd5 WHERE  rawfilewithmd5.md5 IN (SELECT DISTINCT  referencefile.md5 FROM  referencefile)""",
        )

        declare_list_15 = rail.SetVariableOperator(
            task_id='declare_list_15',
            append=False,
            name='importlogger',
            value=[]
        )

        if_query_list_unchangedvalues_14_rows_greater_than_0_16 = rail.IfOperator(
            task_id='if_query_list_unchangedvalues_14_rows_greater_than_0_16',
            test='''{{ result('query_list_unchangedvalues_14','length') > 0 }}''',
            yes_task="insert_to_list_17",
            no_task="if_query_list_newchangedvalues_13_rows_greater_than_0_23",
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=False,
            name='{{ result("declare_list_15").name }}',
            value=lambda: list(map(lambda item: {
                "empid": item['employeeid'],
                "loginname": item['loginname'],
                "status": "Ignored",
                "details": "No change received compared to last file.",
                "jobid": rail.render_template("{{ dag_run_ecid() }}"),
                "childjob": "NA"
            }, rail.load_all_records(rail.result('query_list_unchangedvalues_14'))))

        )

        invoke_custom_ruby_code_18 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_18',
            python_callable=lambda: rail.result('insert_to_list_17')['value'],
        )

        create_csv_lines_19 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_19',
            source="{{ result('invoke_custom_ruby_code_18') | to_json }}",
            header=['employeeid',
                    'loginname',
                    'Status',
                    'reason',
                    'JobID',
                    'Child Job ID'],
            row=[
                "{{ item.empid }}",
                "{{ item.loginname }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}",
                "{{ item.childjob }}"
            ],
        )

        if_query_list_newchangedvalues_13_rows_greater_than_0_23 = rail.IfOperator(
            task_id='if_query_list_newchangedvalues_13_rows_greater_than_0_23',
            test='''{{ result('query_list_newchangedvalues_13','length') > 0 }}''',
            yes_task="get_allreports_25",
            no_task="send_mail_nodeltarecords_54",
        )

        get_allreports_25 = rail.RepliconServiceOperator(
            task_id='get_allreports_25',
            endpoint="/services/reportService1.svc/GetAllReports",
            data=None
        )

        log_referencereporturi_26 = rail.PythonOperator(
            task_id='log_referencereporturi_26',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_allreports_25'), 'displayText', "***RIT - Time off balance and accrual reference", 'uri')
        )

        if_log_referencereporturi_26_present_27 = rail.IfOperator(
            task_id='if_log_referencereporturi_26_present_27',
            test='''{{ result('log_referencereporturi_26') | is_truthy }}''',
            yes_task="get_reportdetails_28",
            no_task="send_mail_sendtointegrationalertswhenimportreferencereporthasissueinthe_repliconinstance_52",
        )

        get_reportdetails_28 = rail.RepliconServiceOperator(
            task_id='get_reportdetails_28',
            endpoint="/services/reportService1.svc/GetReportDetails2",
            data={
                "reportUri": "{{ result('log_referencereporturi_26') }}"
            }
        )

        trigger_dag_run_live_kla_time_off_balance_and_accrual_import_child_v2_0async_30 = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_live_kla_time_off_balance_and_accrual_import_child_v2_0async_30',
            items="{{ result('query_list_newchangedvalues_13') }}",
            parallel_count=50,
            trigger_dag_id=f'kla_timeoff_policy_import_kla_time_off_balance_and_accrual_import_child_v2_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "timeofftypename": item['timeofftypename'],
                "loginname": item['loginname'],
                "timeoffbalance": item['balance'],
                "effectivedate": item['effectivedate'],
                "accrualrate": item['accrualrate'],
                "reporturi": rail.result('log_referencereporturi_26'),
                "userfilteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_reportdetails_28')['filterConfiguration']['enabledFilters'], 'displayText', "UserFilter", ('uri')),
                "timeofftypefilteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_reportdetails_28')['filterConfiguration']['enabledFilters'], 'displayText', "TimeOffTypeFilter", ('uri')),
                "asofdatefilteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_reportdetails_28')['filterConfiguration']['enabledFilters'], 'displayText', "AsOfDateFilter", ('uri')),
                "employeeid": item['employeeid']
            }
        )

        get_child_dags_task_ids = rail.PythonOperator(
            task_id='get_child_dags_task_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_dag_run_live_kla_time_off_balance_and_accrual_import_child_v2_0async_30_{x+1}'), range(50))))),
            show_return_value_in_logs=False
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("get_child_dags_task_ids") }}',
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs'))))))
        )

        log_checkforerror_35 = rail.PythonOperator(
            task_id='log_checkforerror_35',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        create_csv_lines_34 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_34',
            source="{{ result('format_logs') | to_json }}",
            header=['employeeid',
                    'loginname',
                    'Status',
                    'reason',
                    'JobID',
                    'Child Job ID'],
            row=[
                "{{ item.properties.employeeid }}",
                "{{ item.properties.loginname }}",
                "{{ item.properties.status }}",
                "{{ item.properties.reason }}",
                "{{ dag_run_ecid() }}",
                "{{ item.properties.child_job_id }}",
            ],
        )

        log_log_filenametobeused_36 = rail.PythonOperator(
            task_id='log_log_filenametobeused_36',
            python_callable=lambda:  rail.render_template(
                '''kla_logs_{{ result("new_file_sensor") | file_name }}''')
        )

        rename_archiveoldreferencefile_37 = rail.SFTPMoveFileOperator(
            task_id='rename_archiveoldreferencefile_37',
            new_filename=config.sftp_ref_archive_path +
            '''/oldreference_{{ dag_run_ecid() }}.csv''',
            existing_filename=config.sftp_ref_file,
        )

        upload_uploadreferencefile_38 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadreferencefile_38',
            content='''{{ result('create_csv_lines_withmd5_5') }}''',
            # append = false,
            remote_filepath=config.sftp_ref_file,
        )

        upload_40 = rail.SFTPAppendCSVFileOperator(
            task_id='upload_40',
            content='''{{ result('create_csv_lines_34') }}''',
            # append = true,
            remote_filepath=config.sftp_log_filepath + \
            '''/{{ result('log_log_filenametobeused_36') }}''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_34')}}",
            output_file_name='{{ result("log_log_filenametobeused_36") }}',
            expires_in_seconds=7*24*60*60,
        )

        if_log_checkforerror_35_present_44 = rail.IfOperator(
            task_id='if_log_checkforerror_35_present_44',
            test='''{{ result('log_checkforerror_35') | is_truthy }}''',
            yes_task="send_mail_sendtointegrationalerts_45",
            no_task="send_mail_47",
        )

        send_mail_sendtointegrationalerts_45 = rail.EmailOperator(
            task_id='send_mail_sendtointegrationalerts_45',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }}| Time Off Balance and Accrual update is completed with errors - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br /> Hello, <br /> <br /> The Time Off balance and accrual update job is completed with errors based on the file - {{ result("new_file_sensor") | file_name }}. Please find the below link to the import logs for reference. <br /><br /><a href="{{ result('generate_download_link') }}">Download log file</a></p>
<p><em><span style="font-size: 9pt;">The download link is valid for 30 days.</span></em></p>''',
            params=None,
        )

        send_mail_47 = rail.EmailOperator(
            task_id='send_mail_47',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Time Off Balance and Accrual update is completed successfully - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br /> Hello, <br /> <br /> The Time Off balance and accrual update job is completed successfully based on the file - {{ result("new_file_sensor") | file_name }}. Please find the below link to the import logs for reference. <br /><br /><a href="{{ result('generate_download_link') }}">Download log file</a></p>
<p><em><span style="font-size: 9pt;">The download link is valid for 30 days.</span></em></p>
<p>For any queries, please contact our support team at https://support.deltek.com <br /> <br /> Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_sendtointegrationalertswhenimportreferencereporthasissueinthe_repliconinstance_52 = rail.EmailOperator(
            task_id='send_mail_sendtointegrationalertswhenimportreferencereporthasissueinthe_repliconinstance_52',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | Time Off Balance and Accrual update - Time Off import reference report file not found!! - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br /> Hello Team, <br /> <br /> The Time Off balance and accrual update job is not processed since the base time off import reference report "***RIT - Time off balance and accrual reference" was not found under the user account "rnadmin". Please action this on priority.<br /> <br />Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_nodeltarecords_54 = rail.EmailOperator(
            task_id='send_mail_nodeltarecords_54',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Time Off Balance and Accrual update - no records in file - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br /> Hello, <br /> <br /> The Time Off balance and accrual update job is completed. There were no delta records in the file - {{ result("new_file_sensor") | file_name }} to be processed.<br /> <br /> For any queries, please contact our support team at https://support.deltek.com <br /> <br /> Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_incorrectfileformat_57 = rail.EmailOperator(
            task_id='send_mail_incorrectfileformat_57',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Time Off Balance and Accrual update - Incorrect file format received - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br /> Hello, <br /> <br /> The Time Off balance and accrual update job is completed. The data was not imported due incorrect file format received for the file - {{ result("new_file_sensor") | file_name }} to be processed.<br /> <br /> For any queries, please contact our support team at https://support.deltek.com <br /> <br /> Regards, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        new_file_sensor >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_name_downcase_ends_with_csv_2
        if_name_downcase_ends_with_csv_2
        if_name_downcase_ends_with_csv_2 >> rail.Label(
            'Yes') >> download_3 >> archive_file >> parse_csv_4 >> create_csv_lines_withmd5_5 >> load_csv_create_list_from_csv_6 >> create_collection_create_list_from_csv_6 >> dir_7 >> if_first_name_present_8
        if_first_name_present_8 >> rail.Label(
            'Yes') >> download_9 >> load_csv_create_list_from_csv_12 >> create_collection_create_list_from_csv_12 >> query_list_newchangedvalues_13 >> query_list_unchangedvalues_14 >> declare_list_15 >> if_query_list_unchangedvalues_14_rows_greater_than_0_16
        if_first_name_present_8 >> rail.Label(
            'No') >> stop_11 >> finish
        if_query_list_unchangedvalues_14_rows_greater_than_0_16 >> rail.Label(
            'Yes') >> insert_to_list_17 >> invoke_custom_ruby_code_18 >> create_csv_lines_19 >> if_query_list_newchangedvalues_13_rows_greater_than_0_23
        if_query_list_unchangedvalues_14_rows_greater_than_0_16 >> rail.Label(
            'No') >> if_query_list_newchangedvalues_13_rows_greater_than_0_23
        if_query_list_newchangedvalues_13_rows_greater_than_0_23 >> rail.Label(
            'Yes') >> get_allreports_25 >> log_referencereporturi_26 >> if_log_referencereporturi_26_present_27
        if_log_referencereporturi_26_present_27 >> rail.Label(
            'Yes') >> get_reportdetails_28 >> trigger_dag_run_live_kla_time_off_balance_and_accrual_import_child_v2_0async_30 >> get_child_dags_task_ids >> gather_child_logs >> format_logs >> create_csv_lines_34 >> log_checkforerror_35 >> log_log_filenametobeused_36 >> rename_archiveoldreferencefile_37 >> upload_uploadreferencefile_38 >> upload_40 >> generate_download_link >> if_log_checkforerror_35_present_44
        if_log_checkforerror_35_present_44 >> rail.Label(
            'Yes') >> send_mail_sendtointegrationalerts_45 >> finish
        if_log_checkforerror_35_present_44 >> rail.Label(
            'No') >> send_mail_47 >> finish
        if_log_referencereporturi_26_present_27 >> rail.Label(
            'No') >> send_mail_sendtointegrationalertswhenimportreferencereporthasissueinthe_repliconinstance_52 >> finish
        if_name_downcase_ends_with_csv_2 >> rail.Label(
            'No') >> send_mail_incorrectfileformat_57 >> finish >> log_to_sumo
        if_query_list_newchangedvalues_13_rows_greater_than_0_23 >> rail.Label(
            'No') >> send_mail_nodeltarecords_54 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
