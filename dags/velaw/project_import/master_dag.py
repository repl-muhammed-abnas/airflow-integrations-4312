
from datetime import timedelta, datetime
from os import path
import csv
import hashlib
from airflow.models import Variable
import rail
from rail.lib.artifact import existing_artifact, is_artifact_name

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_project_import_velaw_projectsync_master_v1_{config.instance}',
        description=f'Velaw_ProjectSync_Master_V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='start_check_file_type',
            no_task='delete_this_dagrun',
        )

        start_check_file_type = rail.EmptyOperator(
            task_id='start_check_file_type'
        )

        if_name_downcase_not_ends_with_txt_3 = rail.IfOperator(
            task_id='if_name_downcase_not_ends_with_txt_3',
            test='{{ result("new_file_sensor") | lower | file_ext | lower != "txt" }}',
            yes_task="send_mail_completed_incorrectfileformat_4",
            no_task="start_check_batch_task",
        )

        send_mail_completed_incorrectfileformat_4 = rail.EmailOperator(
            task_id='send_mail_completed_incorrectfileformat_4',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject=''' {{ get_company_key() }} | Project Import - Incorrect file format received - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Project Import job is completed. The data was not imported due to the incorrect format received for the file: {{ result('new_file_sensor') | file_name }} </p>

<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        rename_5 = rail.SFTPMoveFileOperator(
            task_id='rename_5',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/archived_{{ result('log_dateforfilename_2') }}_{{ result('new_file_sensor') | file_name }}"
        )

        start_check_batch_task = rail.EmptyOperator(
            task_id='start_check_batch_task'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_dateforfilename_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_dateforfilename_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_dateforfilename_2 = rail.PythonOperator(
            task_id='log_dateforfilename_2',
            python_callable=lambda: datetime.now().strftime("%m%d%Y%H%M%S")
        )

        create_projectsync_logs = rail.CreateLogOperator(
            task_id='create_projectsync_logs',
        )

        download_9 = rail.SFTPDownloadFileOperator(
            task_id='download_9',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        def get_csv_data_headers_mapped(document, headers):
            mapped_list = []
            if is_artifact_name(document):

                with existing_artifact(document, mode="r", encoding='utf-8') as artifact:
                    reader = csv.DictReader(
                        artifact.file, delimiter='|', fieldnames=headers)
                    # validate_csv_data(reader)
                    for row in reader:
                        mapped_list.append(row)

            return mapped_list

        parse_csv_10 = rail.PythonOperator(
            task_id='parse_csv_10',
            python_callable=lambda: get_csv_data_headers_mapped(
                rail.result('download_9'), ['projectname',
                                            'projectcode',
                                            'clientname',
                                            'projectdescription',
                                            ])
        )

        def get_csv_rows(item):
            def get_hash_md5():
                return hashlib.md5(
                    (str(item['projectname'].replace("/<>/", "'").replace("/></", '"'))
                     + str(item['projectcode'].replace("/<>/",
                           "'").replace("/></", '"'))
                     + str(item['clientname'].replace("/<>/",
                           "'").replace("/></", '"'))
                     + str(item['projectdescription'].replace("/<>/",
                           "'").replace("/></", '"'))
                     ).encode('utf-8')).hexdigest()

            row_data = [
                item['projectname'].replace("/<>/", "'").replace("/></", '"'),
                item['projectcode'].replace("/<>/", "'").replace("/></", '"'),
                item['clientname'].replace("/<>/", "'").replace("/></", '"'),
                item['projectdescription'].replace(
                    "/<>/", "'").replace("/></", '"'),
                get_hash_md5()
            ]
            return row_data

        create_csv_lines_md5reference_17 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_md5reference_17',
            source="{{ result('parse_csv_10') | to_json }}",
            header=['projectname',
                    'projectcode',
                    'clientname',
                    'projectdescription',
                    'md5_reference'],
            row=get_csv_rows
        )

        load_csv_create_list_from_csv_18 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_18",
            document="{{ result('create_csv_lines_md5reference_17') }}",
        )

        create_collection_create_list_from_csv_18 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_18',
            source="{{ result('load_csv_create_list_from_csv_18') }}",
            name="input",
            columns={
                'projectname': 'projectname',
                'projectcode': 'projectcode',
                'clientname': 'clientname',
                'projectdescription': 'projectdescription',
                'md5_reference': 'md5_reference'
            }
        )

        if_parse_csv_10_lines_less_than_1_11 = rail.IfOperator(
            task_id='if_parse_csv_10_lines_less_than_1_11',
            test='''{{ result('create_collection_create_list_from_csv_18', 'length') < 1 }}''',
            yes_task="send_mail_completed_blankfilereceived_12",
            no_task="dir_15",
        )

        send_mail_completed_blankfilereceived_12 = rail.EmailOperator(
            task_id='send_mail_completed_blankfilereceived_12',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Project Import - Blank File received - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Project Import job is completed based on the file: {{ result('new_file_sensor') | file_name }} and  the file received is blank.</p>

<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        rename_13 = rail.SFTPMoveFileOperator(
            task_id='rename_13',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/archived_{{ result('log_dateforfilename_2') }}_{{ result('new_file_sensor') | file_name }}"
        )

        dir_15 = rail.SFTPListFilesOperator(
            task_id='dir_15',
            paths=[config.reference_filepath]
        )

        def get_reference_file(result_task_id, file_path):
            if not result_task_id or not file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)

            if not data:
                return []

            return list(filter(lambda x: 'newreference_' in x['filename'], list(map(lambda item: {
                'filename': item['name'],
                'size': item['size'],
                'path': file_path + '/' + item['name']
            }, data[file_path])))) if data[file_path] else []
        reference_file = rail.PythonOperator(
            task_id='reference_file',
            python_callable=get_reference_file,
            op_args=['dir_15', config.reference_filepath]
        )

        download_16 = rail.SFTPDownloadFileOperator(
            task_id='download_16',
            remote_filepath=config.reference_filepath +
            '/{{ result("reference_file") | first_or_default | attr_or_default("filename") }}'
        )

        load_csv_create_list_from_csv_19 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_19",
            document="{{ result('download_16') }}",
        )

        create_collection_create_list_from_csv_19 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_19',
            source="{{ result('load_csv_create_list_from_csv_19') }}",
            name="reference",
            columns={
                'projectname': 'projectname',
                'projectcode': 'projectcode',
                'clientname': 'clientname',
                'projectdescription': 'projectdescription',
                'md5_reference': 'md5_reference'
            }
        )

        query_list_changedrecords_20 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecords_20',
            query="""SELECT * FROM input WHERE md5_reference NOT IN (SELECT DISTINCT md5_reference FROM reference)""",
        )

        query_list_unchangedrecords_21 = rail.QueryCollectionOperator(
            task_id='query_list_unchangedrecords_21',
            query="""SELECT * FROM input WHERE md5_reference IN (SELECT DISTINCT md5_reference FROM reference)""",
        )

        query_list_projecttodisable_22 = rail.QueryCollectionOperator(
            task_id='query_list_projecttodisable_22',
            query="""SELECT * FROM reference WHERE projectname NOT IN (SELECT DISTINCT projectname FROM input)""",
        )

        query_list_clientstodisable_23 = rail.QueryCollectionOperator(
            task_id='query_list_clientstodisable_23',
            query="""SELECT clientname FROM reference WHERE clientname NOT IN (SELECT DISTINCT clientname FROM input)""",
        )

        create_project_import_log = rail.CreateLogOperator(
            task_id='create_project_import_log'
        )

        if_query_list_unchangedrecords_21_rows_greater_than_0_28 = rail.IfOperator(
            task_id='if_query_list_unchangedrecords_21_rows_greater_than_0_28',
            test='''{{ result('query_list_unchangedrecords_21', 'length') > 0 }}''',
            yes_task="velawg3_projectsync_logs_unchanged_entry_29",
            no_task="if_query_list_changedrecords_20_rows_less_than_1_32",
        )

        velawg3_projectsync_logs_unchanged_entry_29 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_unchanged_entry_29',
            log="{{ result('create_project_import_log') }}",
            items="{{ result('query_list_unchangedrecords_21') }}",
            message="No changes found",
            severity="Info",
            properties={
                "project_name": "{{ item.projectname }}",
                "client_name": "{{ item.clientname }}",
                "action": "Skipped",
                "details": "No changes found"
            }
        )

        if_query_list_changedrecords_20_rows_less_than_1_32 = rail.IfOperator(
            task_id='if_query_list_changedrecords_20_rows_less_than_1_32',
            test=lambda: rail.result('query_list_changedrecords_20', key='length') < 1 and rail.result(
                'query_list_projecttodisable_22', key='length') < 1,
            yes_task="send_mail_33",
            no_task="before_changedrecords_check",
        )

        send_mail_33 = rail.EmailOperator(
            task_id='send_mail_33',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Project Import - No new records found - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Project Import job is completed based on the file: {{ result('new_file_sensor') | file_name }} and there are no new records to add or update in Replicon.</p>

<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        rename_archivingtheinputfile_34 = rail.SFTPMoveFileOperator(
            task_id='rename_archivingtheinputfile_34',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/NoChange_{{ result('log_dateforfilename_2') }}_{{ result('new_file_sensor') | file_name }}"
        )

        before_changedrecords_check = rail.EmptyOperator(
            task_id='before_changedrecords_check'
        )

        if_query_list_changedrecords_20_rows_greater_than_0_40 = rail.IfOperator(
            task_id='if_query_list_changedrecords_20_rows_greater_than_0_40',
            test='''{{ result('query_list_changedrecords_20', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42",
            no_task="if_query_list_projecttodisable_22_rows_greater_than_0_48",
        )

        trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42',
            retries=0,
            items="{{ result('query_list_changedrecords_20') }}",
            trigger_dag_id=f'velaw_project_import_velaw_projectsync_create_update_chid_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "projectname": item['projectname'],
                "projectcode": item['projectcode'],
                "clientname": item['clientname'],
                "projectdescription": item['projectdescription'],
                "clientcode": item['clientname'].rsplit('-', 1)[-1].strip() if item['clientname'] else null,
                "slug": rail.get_tenant_slug()
            }
        )

        wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42") }}'
        )

        gather_projectsync_create_update_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_projectsync_create_update_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42') }}",
            dagrun_task_id='create_projectsync_create_update_logs',
            flatten=True
        )

        if_query_list_projecttodisable_22_rows_greater_than_0_48 = rail.IfOperator(
            task_id='if_query_list_projecttodisable_22_rows_greater_than_0_48',
            test='''{{ result('query_list_projecttodisable_22', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49",
            no_task="if_query_list_clientstodisable_23_rows_greater_than_0_50",
        )

        trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49',
            retries=0,
            items="{{ result('query_list_projecttodisable_22') }}",
            trigger_dag_id=f'velaw_project_import_velaw_projectsync_disable_chid_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "projectname": item['projectname'],
                "projectcode": item['projectcode'],
                "clientname": item['clientname'],
                "clientcode": item['clientname'].rsplit('-', 1)[-1].strip() if item['clientname'] else null,
                "type": "project"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49") }}'
        )

        gather_projectsync_project_disable_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_projectsync_project_disable_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49') }}",
            dagrun_task_id='create_projectsync_disable_logs',
            flatten=True
        )

        if_query_list_clientstodisable_23_rows_greater_than_0_50 = rail.IfOperator(
            task_id='if_query_list_clientstodisable_23_rows_greater_than_0_50',
            test='''{{ result('query_list_clientstodisable_23', 'length') > 0 }}''',
            yes_task="trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51",
            no_task="rename_archivingtheoldreferencefile_62",
        )

        trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51',
            retries=0,
            items="{{ result('query_list_clientstodisable_23') }}",
            trigger_dag_id=f'velaw_project_import_velaw_projectsync_disable_chid_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "projectname": null,
                "projectcode": null,
                "clientname": item['clientname'],
                "clientcode": item['clientname'].rsplit('-', 1)[-1].strip() if item['clientname'] else null,
                "type": "client"
            }
        )

        wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51") }}'
        )

        gather_projectsync_client_disable_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_projectsync_client_disable_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51') }}",
            dagrun_task_id='create_projectsync_disable_logs',
            flatten=True
        )

        rename_archivingtheoldreferencefile_62 = rail.SFTPMoveFileOperator(
            task_id='rename_archivingtheoldreferencefile_62',
            existing_filename=config.reference_filepath +
            '''/{{ result("reference_file") | first_or_default | attr_or_default("filename") }}''',
            new_filename=config.archive_filepath +
            '''/oldreference_{{ result("reference_file") | first_or_default | attr_or_default("filename") }}''',

        )

        upload_uploadingnewreferencefile_63 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadingnewreferencefile_63',
            content='''{{ result('create_csv_lines_md5reference_17') }}''',
            remote_filepath=config.reference_filepath +
            '''/newreference_{{ dag_run_ecid() | replace(":", "-") }}_{{ result('new_file_sensor') | file_name }}''',
        )

        rename_archive_input_file_64 = rail.SFTPMoveFileOperator(
            task_id='rename_archive_input_file_64',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ result('log_dateforfilename_2') }}_{{ result('new_file_sensor') | file_name }}"
        )

        def get_logs():
            logs = []
            if rail.result('create_project_import_log'):
                logs.append(rail.result('create_project_import_log'))
            if rail.result('gather_projectsync_create_update_logs'):
                logs.extend(rail.result(
                    'gather_projectsync_create_update_logs'))
            if rail.result('gather_projectsync_project_disable_logs'):
                logs.extend(rail.result(
                    'gather_projectsync_project_disable_logs'))
            if rail.result('gather_projectsync_client_disable_logs'):
                logs.extend(rail.result(
                    'gather_projectsync_client_disable_logs'))
            return logs
        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            trigger_dag_id=f'velaw_project_import_velaw_projectsync_loggeneration_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda: {
                "filename": path.split(rail.result('new_file_sensor'))[1],
                "logs": get_logs(),
                "time": rail.result('log_dateforfilename_2')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label(
            "Yes") >> start_check_file_type >> if_name_downcase_not_ends_with_txt_3
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        if_name_downcase_not_ends_with_txt_3 >> rail.Label(
            'Yes') >> send_mail_completed_incorrectfileformat_4 >> rename_5 >> finish
        if_name_downcase_not_ends_with_txt_3 >> rail.Label(
            'No') >> start_check_batch_task >> can_run_batch_task

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_dateforfilename_2 >> create_projectsync_logs >> download_9 \
            >> parse_csv_10 >> create_csv_lines_md5reference_17 >> load_csv_create_list_from_csv_18 \
            >> create_collection_create_list_from_csv_18 >> if_parse_csv_10_lines_less_than_1_11

        if_parse_csv_10_lines_less_than_1_11 >> rail.Label(
            'Yes') >> send_mail_completed_blankfilereceived_12 >> rename_13 >> finish
        if_parse_csv_10_lines_less_than_1_11 >> rail.Label('No') >> dir_15 >> reference_file >> download_16 >> load_csv_create_list_from_csv_19 \
            >> create_collection_create_list_from_csv_19 >> query_list_changedrecords_20 >> query_list_unchangedrecords_21 \
            >> query_list_projecttodisable_22 >> query_list_clientstodisable_23 >> create_project_import_log \
            >> if_query_list_unchangedrecords_21_rows_greater_than_0_28
        if_query_list_unchangedrecords_21_rows_greater_than_0_28 >> rail.Label(
            'Yes') >> velawg3_projectsync_logs_unchanged_entry_29 \
            >> if_query_list_changedrecords_20_rows_less_than_1_32
        if_query_list_unchangedrecords_21_rows_greater_than_0_28 >> rail.Label(
            'No') >> if_query_list_changedrecords_20_rows_less_than_1_32
        if_query_list_changedrecords_20_rows_less_than_1_32 >> rail.Label(
            'Yes') >> send_mail_33 >> rename_archivingtheinputfile_34 >> finish
        if_query_list_changedrecords_20_rows_less_than_1_32 >> rail.Label(
            'No') >> before_changedrecords_check >> if_query_list_changedrecords_20_rows_greater_than_0_40
        if_query_list_changedrecords_20_rows_greater_than_0_40 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42 \
            >> wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_create_update_chid_v1async_42 \
            >> gather_projectsync_create_update_logs \
            >> if_query_list_projecttodisable_22_rows_greater_than_0_48
        if_query_list_changedrecords_20_rows_greater_than_0_40 >> rail.Label(
            'No') >> if_query_list_projecttodisable_22_rows_greater_than_0_48
        if_query_list_projecttodisable_22_rows_greater_than_0_48 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49 \
            >> wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_49 \
            >> gather_projectsync_project_disable_logs >> if_query_list_clientstodisable_23_rows_greater_than_0_50
        if_query_list_projecttodisable_22_rows_greater_than_0_48 >> rail.Label(
            'No') >> if_query_list_clientstodisable_23_rows_greater_than_0_50
        if_query_list_clientstodisable_23_rows_greater_than_0_50 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51 \
            >> wait_for_completion_trigger_dag_run_velaw_project_import_velaw_projectsync_disable_chid_v1async_51 \
            >> gather_projectsync_client_disable_logs >> rename_archivingtheoldreferencefile_62
        if_query_list_clientstodisable_23_rows_greater_than_0_50 >> rail.Label(
            'No') >> rename_archivingtheoldreferencefile_62 >> upload_uploadingnewreferencefile_63 >> rename_archive_input_file_64 \
            >> process_log_generation >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
