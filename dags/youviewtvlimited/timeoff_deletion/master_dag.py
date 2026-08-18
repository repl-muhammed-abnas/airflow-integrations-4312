
from datetime import timedelta
import itertools
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'youviewtvlimited_timeoff_deletion_master_{config.instance}',
        description=f'Timeoff_Deletion - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
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
            yes_task='download_2',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        download_2 = rail.SFTPDownloadFileOperator(
            task_id='download_2',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        parse_csv_3 = rail.LoadCSVFileOperator(
            task_id='parse_csv_3',
            document="{{ result('download_2') }}",
        )

        create_csv_lines_4 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_4',
            source="{{ result('parse_csv_3') }}",
            header=['Emailid',
                    'employee ID',
                    'absenceid',
                    'startdate',
                    'enddate',
                    'absencetype',
                    'status',
                    'md5'],
            row=[
                "{{ item['User Name'] }}",
                "{{ item['SAGE employee id'] }}",
                "{{ item['Absence #'] }}",
                "{{ item['absence start date'] }}",
                "{{ item['absence end date'] }}",
                "{{ item['absence type'] }}",
                "{{ item['Cancellation'] }}",
                "{{(item['User Name'] | sn + ', ' + item['SAGE employee id']  | sn + ', ' + item['absence start date']  | sn + ', ' + item['absence end date'] | sn + ', ' + item['absence type'] | sn + ', ' + item['Cancellation'] | sn) | md5 }}"
            ],
        )

        load_csv_create_list_from_csv_5 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_5",
            document="{{ result('create_csv_lines_4') }}",
        )

        create_collection_create_list_from_csv_5 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_5',
            source="{{ result('load_csv_create_list_from_csv_5') }}",
            name="inputdata",
            columns={
                'Emailid': 'email',
                'employee ID': 'empid',
                'absenceid': 'absenceid',
                'startdate': 'startdate',
                'enddate': 'enddate',
                'absencetype': 'absencetype',
                'status': 'status',
                'md5': 'md5'
            }
        )

        download_downloadreferencefile_6 = rail.SFTPDownloadFileOperator(
            task_id='download_downloadreferencefile_6',
            remote_filepath=config.sftp_ref_file_path
        )

        load_csv_create_list_from_csv_7 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_7",
            document="{{ result('download_downloadreferencefile_6') }}",
        )

        create_collection_create_list_from_csv_7 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_7',
            source="{{ result('load_csv_create_list_from_csv_7') }}",
            name="referencedata",
            columns={
                'Emailid': 'email',
                'employee ID': 'empid',
                'absenceid': 'absenceid',
                'startdate': 'startdate',
                'enddate': 'enddate',
                'absencetype': 'absencetype',
                'status': 'status',
                'md5': 'md5'
            }
        )

        query_list_deltarecords_8 = rail.QueryCollectionOperator(
            task_id='query_list_deltarecords_8',
            query="""SELECT * FROM  inputdata WHERE  inputdata.md5 NOT IN (SELECT DISTINCT  referencedata.md5 FROM  referencedata)""",
        )

        if_query_list_deltarecords_8_rows_greater_than_0_9 = rail.IfOperator(
            task_id='if_query_list_deltarecords_8_rows_greater_than_0_9',
            test='''{{ result('query_list_deltarecords_8','length')> 0 }}''',
            yes_task="get_all_custom_fields_12",
            no_task="upload_uploadnewreference_51",
        )

        get_all_custom_fields_12 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_12',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:time-off"
            }
        )

        declar_dag_run_list = rail.SetVariableOperator(
            task_id='declar_dag_run_list',
            name='dag_runs',
            value=[]
        )

        foreach_query_list_deltarecords_8_16 = rail.ForEachOperator(
            task_id='foreach_query_list_deltarecords_8_16',
            items="{{ result('query_list_deltarecords_8') }}",
            start_task='get_validation_message',
            end_task='foreach_query_list_deltarecords_8_16_end'
        )

        get_validation_message = rail.PythonOperator(
            task_id='get_validation_message',
            python_callable=lambda: "".join(filter(lambda x: x, [
                'Absence# not provided;' if not rail.result('foreach_query_list_deltarecords_8_16')[
                    'absenceid'] else '',
                'Employee login name(email) is not present;' if not rail.result('foreach_query_list_deltarecords_8_16')[
                    'email'] else '',
                'Booking start date is not present;' if not rail.result('foreach_query_list_deltarecords_8_16')[
                    'startdate'] else '',
                'Booking end date is not present;' if not rail.result('foreach_query_list_deltarecords_8_16')[
                    'enddate'] else '',
            ]))
        )

        if_split_smart_join_present_18 = rail.IfOperator(
            task_id='if_split_smart_join_present_18',
            test="{{ result('get_validation_message') | is_truthy }}",
            yes_task="youviewtvlimited_timeoff_deletion_logs_add_entry_19",
            no_task="search_users_22",
        )

        youviewtvlimited_timeoff_deletion_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='youviewtvlimited_timeoff_deletion_logs_add_entry_19',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "employeeid": "{{ result('foreach_query_list_deltarecords_8_16').email }}",
                "absence#": "{{ result('foreach_query_list_deltarecords_8_16').absenceid }}",
                "startdate": "{{ result('foreach_query_list_deltarecords_8_16').startdate }}",
                "enddate": "{{ result('foreach_query_list_deltarecords_8_16').enddate }}",
                "status": "Skipped",
                "details": "{{ result('get_validation_message') }}"
            }
        )

        search_users_22 = rail.RepliconServiceOperator(
            task_id='search_users_22',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{ result('foreach_query_list_deltarecords_8_16').email }}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda data: data[0] if data else null
        )

        if_smart_join_presencenil_present_26 = rail.IfOperator(
            task_id='if_smart_join_presencenil_present_26',
            test='''{{ result('search_users_22') | is_truthy }}''',
            yes_task="if_smart_join_presencenil_is_true_28",
            no_task="youviewtvlimited_timeoff_deletion_logs_add_entry_33",
        )

        if_smart_join_presencenil_is_true_28 = rail.IfOperator(
            task_id='if_smart_join_presencenil_is_true_28',
            test='''{{ result('search_users_22').userDetails.isEnabled | is_truthy }}''',
            yes_task="trigger_dag_run_live_time_off_deletion_childasync_29",
            no_task="youviewtvlimited_timeoff_deletion_logs_add_entry_31",
        )

        trigger_dag_run_live_time_off_deletion_childasync_29 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_time_off_deletion_childasync_29',
            retries=0,
            items=[1],
            trigger_dag_id=f'youviewtvlimited_timeoff_deletion_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "Username": "{{ result('foreach_query_list_deltarecords_8_16').email }}",
                "Startdate": "{{ result('foreach_query_list_deltarecords_8_16').startdate }}",
                "Enddate": "{{ result('foreach_query_list_deltarecords_8_16').enddate }}",
                "userURI": "{{ result('search_users_22').userDetails.uri }}",
                "AbsenceID": "{{ result('foreach_query_list_deltarecords_8_16').absenceid }}",
                "Status": "{{ result('foreach_query_list_deltarecords_8_16').status }}",
                "absenceoefuri": "{{ result('get_all_custom_fields_12') | find_first_by_attr_and_get_attr('displayText','Absence# (Should not be modified)','uri') }}",
                "startdate": "{{ result('foreach_query_list_deltarecords_8_16').startdate }}",
                "enddate":  "{{ result('foreach_query_list_deltarecords_8_16').enddate }}",
            }
        )

        append_dag_run_list = rail.SetVariableOperator(
            task_id='append_dag_run_list',
            name='dag_runs',
            append=True,
            value="{{ result('trigger_dag_run_live_time_off_deletion_childasync_29')[0] }}"
        )

        youviewtvlimited_timeoff_deletion_logs_add_entry_31 = rail.WriteLogOperator(
            task_id='youviewtvlimited_timeoff_deletion_logs_add_entry_31',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "employeeid": "{{ result('foreach_query_list_deltarecords_8_16').email }}",
                "absence#": "{{ result('foreach_query_list_deltarecords_8_16').absenceid }}",
                "startdate": "{{ result('foreach_query_list_deltarecords_8_16').startdate }}",
                "enddate": "{{ result('foreach_query_list_deltarecords_8_16').enddate }}",
                "status": "Skipped",
                "details": "User disabled in Replicon"
            }
        )

        youviewtvlimited_timeoff_deletion_logs_add_entry_33 = rail.WriteLogOperator(
            task_id='youviewtvlimited_timeoff_deletion_logs_add_entry_33',
            log="{{ result('create_log') }}",
            message="na",
            severity="Skipped",
            properties={
                "employeeid": "{{ result('foreach_query_list_deltarecords_8_16').email }}",
                "absence#": "{{ result('foreach_query_list_deltarecords_8_16').absenceid }}",
                "startdate": "{{ result('foreach_query_list_deltarecords_8_16').startdate }}",
                "enddate": "{{ result('foreach_query_list_deltarecords_8_16').enddate }}",
                "status": "Skipped",
                "details": "User not found in Replicon"
            }
        )

        foreach_query_list_deltarecords_8_16_end = rail.EmptyOperator(
            task_id='foreach_query_list_deltarecords_8_16_end',
        )

        dag_runs_expr = '{{ result("append_dag_run_list").value | to_json if result("append_dag_run_list") | is_truthy and result("append_dag_run_list").value | is_truthy else [] }}'
        wait_for_completion_trigger_dag_run_live_time_off_deletion_childasync_29 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_time_off_deletion_childasync_29',
            execution_timeout=timedelta(days=14),
            dag_runs=dag_runs_expr,
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs=dag_runs_expr,
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs')+[rail.result('create_log')])))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        get_logged_exceptions = rail.PythonOperator(
            task_id='get_logged_exceptions',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Exception')
        )

        get_logged_success = rail.PythonOperator(
            task_id='get_logged_success',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Success')
        )

        create_csv_lines_36 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_36',
            source="{{ result('format_logs') | to_json }}",
            header=['Employee ID',
                    'Absence#',
                    'Start Date',
                    'End Date',
                    'Status',
                    'Details',
                    'JobId'],
            row=[
                "{{ item.properties.employeeid }}",
                "{{ item.properties['absence#'] }}",
                "{{ item.properties.startdate }}",
                "{{ item.properties.enddate }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ],
        )

        get_signed_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id='get_signed_download_url',
            artifact_name='''{{ result('create_csv_lines_36') }}''',
            output_file_name="Log_{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        if_pluckentry_wherecol7error_present_44 = rail.IfOperator(
            task_id='if_pluckentry_wherecol7error_present_44',
            test="{{ result('get_logged_errors') | is_truthy }}",
            yes_task="send_mail_with_cshare_completedwitherrors_45",
            no_task="if_pluckentry_wherecol7exception_present_47",
        )

        send_mail_with_cshare_completedwitherrors_45 = rail.EmailOperator(
            task_id='send_mail_with_cshare_completedwitherrors_45',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{get_company_key()}} | Timeoff Deletion import Completed with failures {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> Please find the below link to download the timeoff deletion logs for reference. <br /> <br /><a href="{{ result('get_signed_download_url') }}">Download log file</a> </p>
            <p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        if_pluckentry_wherecol7exception_present_47 = rail.IfOperator(
            task_id='if_pluckentry_wherecol7exception_present_47',
            test="{{ result('get_logged_exceptions') | is_truthy }}",
            yes_task="send_mail_with_cshare_completedwithexception_48",
            no_task="send_mail_with_cshare_completedsuccessfully_50",
        )

        send_mail_with_cshare_completedwithexception_48 = rail.EmailOperator(
            task_id='send_mail_with_cshare_completedwithexception_48',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{get_company_key()}}| Timeoff Deletion import Completed with Exception {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> Please find the below link to download the timeoff deletion logs for reference. <br /> <br /><a href="{{ result('get_signed_download_url') }}">Download log file</a> </p>
<p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_with_cshare_completedsuccessfully_50 = rail.EmailOperator(
            task_id='send_mail_with_cshare_completedsuccessfully_50',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Timeoff Deletion import Completed Successfully {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> Please find the below link to download the timeoff deletion logs for reference. <br /> <br /><a href="{{ result('get_signed_download_url') }}">Download log file</a> </p>
<p><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        upload_uploadnewreference_51 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreference_51',
            content='''{{ result('create_csv_lines_4') }}''',
            # append = false,
            remote_filepath=config.sftp_ref_file_path,
        )

        rename_archivingtheinputfile_52 = rail.SFTPMoveFileOperator(
            task_id='rename_archivingtheinputfile_52',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.sftp_archive_file_path +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> download_2
        download_2 >> create_log >> parse_csv_3 >> create_csv_lines_4 >> load_csv_create_list_from_csv_5 >> create_collection_create_list_from_csv_5 >> download_downloadreferencefile_6 >> load_csv_create_list_from_csv_7 >> create_collection_create_list_from_csv_7 >> query_list_deltarecords_8 >> if_query_list_deltarecords_8_rows_greater_than_0_9
        if_query_list_deltarecords_8_rows_greater_than_0_9 >> rail.Label(
            'Yes') >> get_all_custom_fields_12 >> declar_dag_run_list >> foreach_query_list_deltarecords_8_16 >> get_validation_message >> if_split_smart_join_present_18
        if_split_smart_join_present_18 >> rail.Label(
            'Yes') >> youviewtvlimited_timeoff_deletion_logs_add_entry_19 >> foreach_query_list_deltarecords_8_16_end
        if_split_smart_join_present_18 >> rail.Label(
            'No') >> search_users_22 >> if_smart_join_presencenil_present_26
        if_smart_join_presencenil_present_26 >> rail.Label(
            'Yes') >> if_smart_join_presencenil_is_true_28
        if_smart_join_presencenil_present_26 >> rail.Label(
            'No') >> youviewtvlimited_timeoff_deletion_logs_add_entry_33 >> foreach_query_list_deltarecords_8_16_end
        if_smart_join_presencenil_is_true_28 >> rail.Label(
            'Yes') >> trigger_dag_run_live_time_off_deletion_childasync_29 >> append_dag_run_list >> foreach_query_list_deltarecords_8_16_end
        if_smart_join_presencenil_is_true_28 >> rail.Label(
            'No') >> youviewtvlimited_timeoff_deletion_logs_add_entry_31 >> foreach_query_list_deltarecords_8_16_end

        foreach_query_list_deltarecords_8_16 >> foreach_query_list_deltarecords_8_16_end >> wait_for_completion_trigger_dag_run_live_time_off_deletion_childasync_29 >> gather_child_logs >> format_logs >> get_logged_errors >> get_logged_exceptions >> get_logged_success >> create_csv_lines_36 >> get_signed_download_url >> if_pluckentry_wherecol7error_present_44
        if_pluckentry_wherecol7error_present_44 >> rail.Label(
            'Yes') >> send_mail_with_cshare_completedwitherrors_45 >> upload_uploadnewreference_51
        if_pluckentry_wherecol7error_present_44 >> rail.Label(
            'No') >> if_pluckentry_wherecol7exception_present_47
        if_pluckentry_wherecol7exception_present_47 >> rail.Label(
            'Yes') >> send_mail_with_cshare_completedwithexception_48 >> upload_uploadnewreference_51
        if_pluckentry_wherecol7exception_present_47 >> rail.Label(
            'No') >> send_mail_with_cshare_completedsuccessfully_50 >> upload_uploadnewreference_51
        if_query_list_deltarecords_8_rows_greater_than_0_9 >> rail.Label(
            'No') >> upload_uploadnewreference_51 >> rename_archivingtheinputfile_52 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
