
from datetime import datetime
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_timesheet_export_sftp_process_file_child_{config.instance}',
        description=f'Datamart Eng Timesheet Processing_V3.0 {config.instance}',
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

        invoke_custom_ruby_code_3 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_3',
            python_callable=lambda: {
                "replicon_date": rail.parse_date(datetime.utcnow().strftime("%Y%m%d"), "%Y%m%d"),
                "date": datetime.utcnow().strftime("%Y%m%d"),
                "today":  datetime.utcnow().strftime("%m/%d/%Y"),
                "existingfilename": "Processing_Replicon_TimesheetEngr_Download_" + (datetime.utcnow().strftime("%Y%m%d")) + ".csv",
                "newfilename": "Replicon_TimesheetEngr_Download_" + (datetime.utcnow().strftime("%Y%m%d")) + ".csv",
            }
        )

        dir_11 = rail.SFTPListFilesOperator(
            task_id='dir_11',
            paths=[config.sftp_processedrecords_directory]
        )

        if_first_name_present_12 = rail.IfOperator(
            task_id='if_first_name_present_12',
            test=lambda: bool(rail.result(
                'dir_11').values()),
            yes_task="foreach_dir_11_13",
            no_task="dir_15",
        )

        foreach_dir_11_13 = rail.ForEachOperator(
            task_id='foreach_dir_11_13',
            items=lambda: list(rail.result(
                'dir_11').values())[0],
            start_task='rename_14',
            end_task='foreach_dir_11_13_end'
        )

        rename_14 = rail.SFTPMoveFileOperator(
            task_id='rename_14',
            existing_filename=config.sftp_processedrecords_directory +
            "/{{ result('foreach_dir_11_13').name}}",
            new_filename=config.sftp_archive_directory +
            "/{{ result('foreach_dir_11_13').name}}",
        )

        foreach_dir_11_13_end = rail.EmptyOperator(
            task_id='foreach_dir_11_13_end',
        )

        dir_15 = rail.SFTPListFilesOperator(
            task_id='dir_15',
            paths=[config.sftp_rejectedrecords_directory]
        )

        if_first_name_present_16 = rail.IfOperator(
            task_id='if_first_name_present_16',
            test=lambda: bool(rail.result(
                'dir_15').values()),
            yes_task="foreach_dir_15_17",
            no_task="dir_20",
        )

        foreach_dir_15_17 = rail.ForEachOperator(
            task_id='foreach_dir_15_17',
            items=lambda: list(rail.result(
                'dir_15').values())[0],
            start_task='if_foreach_dir_15_17_name_not_contains_costcenter_18',
            end_task='foreach_dir_15_17_end'
        )

        if_foreach_dir_15_17_name_not_contains_costcenter_18 = rail.IfOperator(
            task_id='if_foreach_dir_15_17_name_not_contains_costcenter_18',
            test='''{{ not result('foreach_dir_15_17').name | matches('CostCenter') }}''',
            yes_task="rename_19",
            no_task="foreach_dir_15_17_end",
        )

        rename_19 = rail.SFTPMoveFileOperator(
            task_id='rename_19',
            existing_filename=config.sftp_rejectedrecords_directory +
            "/{{ result('foreach_dir_15_17').name}}",
            new_filename=config.sftp_archive_directory +
            "/{{ result('foreach_dir_15_17').name}}",
        )

        foreach_dir_15_17_end = rail.EmptyOperator(
            task_id='foreach_dir_15_17_end',
        )

        dir_20 = rail.SFTPListFilesOperator(
            task_id='dir_20',
            paths=[config.sftp_processing_directory]
        )

        if_first_name_present_21 = rail.IfOperator(
            task_id='if_first_name_present_21',
            test=lambda: bool(rail.result(
                'dir_20').values()),
            yes_task="foreach_dir_20_22",
            no_task="log_to_sumo",
        )

        foreach_dir_20_22 = rail.ForEachOperator(
            task_id='foreach_dir_20_22',
            items=lambda: list(rail.result(
                'dir_20').values())[0],
            start_task='if_foreach_dir_20_22_name_contains_download_23',
            end_task='foreach_dir_20_22_end'
        )

        if_foreach_dir_20_22_name_contains_download_23 = rail.IfOperator(
            task_id='if_foreach_dir_20_22_name_contains_download_23',
            test='''{{ result('foreach_dir_20_22').name | matches('Download') }}''',
            yes_task="rename_24",
            no_task="log_existing_file_name_26",
        )

        rename_24 = rail.SFTPMoveFileOperator(
            task_id='rename_24',
            existing_filename=config.sftp_processing_directory +
            "/{{ result('foreach_dir_20_22').name}}",
            new_filename=config.sftp_processedrecords_directory +
            "/{{ result('invoke_custom_ruby_code_3').newfilename }}",
        )

        send_mail_25 = rail.EmailOperator(
            task_id='send_mail_25',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Datamart timesheet data export for ENG Department completed (processed Records) - {{ current_time()}}''',
            html_content='''<p><strong>This is an automated mail. Please do not reply.</strong></p>
            <p>Hi Team,</p>
            <p>The 'Datamart timesheet  export' for processed records in 'DTNA ENG' department is completed successfully on {{ result('invoke_custom_ruby_code_3').today }}. Please find the file for processed records file with name {{ result('invoke_custom_ruby_code_3').newfilename }} at {{ params.path }} on the SFTP server.</p>
            <p>For any issue, Please contact our support team at https://support.deltek.com</p>
            <p>Regards,<br>Deltek Inc.</br></p> ''',
            params={
                'path': config.sftp_processedrecords_directory
            },
        )

        log_existing_file_name_26 = rail.PythonOperator(
            task_id='log_existing_file_name_26',
            python_callable=lambda:  f"Processing_Replicon_TimesheetEngr_RejectedRecords_{rail.result('invoke_custom_ruby_code_3')['date']}.csv"
        )

        if_foreach_dir_20_22_name_present_27 = rail.IfOperator(
            task_id='if_foreach_dir_20_22_name_present_27',
            test='''{{ result('foreach_dir_20_22').name | is_truthy  and result('foreach_dir_20_22').name | matches('RejectedRecords')  and result('foreach_dir_20_22').size > 284 }}''',
            yes_task="log_new_file_name_28",
            no_task="if_foreach_dir_20_22_name_present_31",
        )

        log_new_file_name_28 = rail.PythonOperator(
            task_id='log_new_file_name_28',
            python_callable=lambda:  f"Replicon_TimesheetEngr_RejectedRecords_{rail.result('invoke_custom_ruby_code_3')['date']}.csv"
        )

        rename_29 = rail.SFTPMoveFileOperator(
            task_id='rename_29',
            existing_filename=config.sftp_processing_directory +
            "/{{ result('log_existing_file_name_26') }}",
            new_filename=config.sftp_rejectedrecords_directory +
            "/{{ result('log_new_file_name_28') }}",
        )

        send_mail_30 = rail.EmailOperator(
            task_id='send_mail_30',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Datamart timesheet data export for ENG Department completed (Rejected Records) - {{ current_time()}} ''',
            html_content='''<p><strong>This is an automated mail. Please do not reply.</strong></p>
            <p>Hi Team,</p>
            <p>The 'Datamart timesheet  export' for rejected records in 'DTNA ENG' department is completed successfully on {{ result('invoke_custom_ruby_code_3').today }}. Please find the file for rejected records file with name {{ result('log_new_file_name_28') }} at {{ params.path }} on the SFTP server.</p>
            <p>For any issue, Please contact our support team at https://support.deltek.com</p>
            <p>Regards,<br>Deltek Inc.</br></p> ''',
            params={
                'path': config.sftp_rejectedrecords_directory
            }
        )

        if_foreach_dir_20_22_name_present_31 = rail.IfOperator(
            task_id='if_foreach_dir_20_22_name_present_31',
            test='''{{ result('foreach_dir_20_22').name | is_truthy  and result('foreach_dir_20_22').name | matches('RejectedRecords')  and result('foreach_dir_20_22').size < 285 }}''',
            yes_task="remove_32",
            no_task="foreach_dir_20_22_end",
        )

        remove_32 = rail.SFTPDeleteFileOperator(
            task_id='remove_32',
            existing_filename=config.sftp_processing_directory +
            "/{{ result('log_existing_file_name_26') }}"
        )

        foreach_dir_20_22_end = rail.EmptyOperator(
            task_id='foreach_dir_20_22_end',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        invoke_custom_ruby_code_3 >> dir_11 >> if_first_name_present_12
        if_first_name_present_12 >> rail.Label(
            'Yes') >> foreach_dir_11_13 >> rename_14 >> foreach_dir_11_13_end >> dir_15
        foreach_dir_11_13 >> foreach_dir_11_13_end >> dir_15
        if_first_name_present_12 >> rail.Label(
            'No') >> dir_15 >> if_first_name_present_16
        if_first_name_present_16 >> rail.Label(
            'Yes') >> foreach_dir_15_17 >> if_foreach_dir_15_17_name_not_contains_costcenter_18
        if_foreach_dir_15_17_name_not_contains_costcenter_18 >> rail.Label(
            'Yes') >> rename_19 >> foreach_dir_15_17_end >> dir_20
        if_foreach_dir_15_17_name_not_contains_costcenter_18 >> rail.Label(
            'No') >> foreach_dir_15_17_end >> dir_20
        foreach_dir_15_17 >> foreach_dir_15_17_end >> dir_20
        if_first_name_present_16 >> rail.Label(
            'No') >> dir_20 >> if_first_name_present_21
        if_first_name_present_21 >> rail.Label(
            'Yes') >> foreach_dir_20_22 >> if_foreach_dir_20_22_name_contains_download_23
        if_foreach_dir_20_22_name_contains_download_23 >> rail.Label(
            'Yes') >> rename_24 >> send_mail_25 >> log_existing_file_name_26
        if_foreach_dir_20_22_name_contains_download_23 >> rail.Label(
            'No') >> log_existing_file_name_26 >> if_foreach_dir_20_22_name_present_27
        if_foreach_dir_20_22_name_present_27 >> rail.Label(
            'Yes') >> log_new_file_name_28 >> rename_29 >> send_mail_30 >> if_foreach_dir_20_22_name_present_31
        if_foreach_dir_20_22_name_present_27 >> rail.Label(
            'No') >> if_foreach_dir_20_22_name_present_31
        if_foreach_dir_20_22_name_present_31 >> rail.Label(
            'Yes') >> remove_32 >> foreach_dir_20_22_end >> log_to_sumo
        if_foreach_dir_20_22_name_present_31 >> rail.Label(
            'No') >> foreach_dir_20_22_end
        foreach_dir_20_22 >> foreach_dir_20_22_end
        if_first_name_present_21 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
