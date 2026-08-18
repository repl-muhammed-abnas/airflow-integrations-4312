
from datetime import timedelta
import pendulum
import rail
from michaelkorstna.shift_import.task.generate_report_batch import report_batch
from michaelkorstna.shift_import.utils import python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mk_shifts_master_recipe_06_15am_{config.instance}',
        description=f'MK_Shifts_Master_recipe 06:15 AM {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2023, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_06_15_am,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        dir_getallinputfiles_2 = rail.SFTPListFilesOperator(
            task_id='dir_getallinputfiles_2',
            paths=[config.input_filepath]
        )

        csv_files_list = rail.PythonOperator(
            task_id='csv_files_list',
            python_callable=python_callable_method.get_csv_files_list,
            op_args=['dir_getallinputfiles_2', config.input_filepath]
        )

        non_csv_files_list = rail.PythonOperator(
            task_id='non_csv_files_list',
            python_callable=python_callable_method.get_non_csv_files_list,
            op_args=['dir_getallinputfiles_2', config.input_filepath]
        )

        if_first_name_blank_3 = rail.IfOperator(
            task_id='if_first_name_blank_3',
            test='''{{ result('csv_files_list') | length == 0 }}''',
            yes_task="finish",
            no_task="create_shift_import_log"
        )

        create_shift_import_log = rail.CreateLogOperator(
            task_id='create_shift_import_log'
        )

        generate_report = rail.EmptyOperator(task_id='generate_report')

        get_report_details, load_report_data, fail_no_report_data, fail_column_order_mismatch = report_batch(
            config)

        log_timetobeused_12 = rail.PythonOperator(
            task_id='log_timetobeused_12',
            python_callable=lambda: pendulum.now(
                config.time_zone).strftime('%d%m%YT%H%M%S')
        )

        upload_13 = rail.SFTPUploadFileOperator(
            task_id='upload_13',
            content='''{{ result('load_report_data') }}''',
            remote_filepath=config.shifts_filepath +
            '''/Userdatareport_{{ result('log_timetobeused_12') }}.csv'''
        )

        trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16',
            retries=0,
            items="{{ result('csv_files_list') | to_json }}",
            trigger_dag_id=f'mk_shifts_master_eachfile_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "filename": "{{ item.filename }}",
                "userdatafilepath": config.shifts_filepath + "/Userdatareport_{{ result('log_timetobeused_12') }}.csv",
                "time": "{{ result('log_timetobeused_12') }}"
            }
        )

        wait_for_completion_trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16") }}'
        )

        foreach_non_csv_file_list = rail.ForEachOperator(
            task_id='foreach_non_csv_file_list',
            items="{{ result('non_csv_files_list') | to_json }}",
            start_task='mkshiftfilelogs_add_entry_18',
            end_task='foreach_non_csv_file_list_end'
        )

        mkshiftfilelogs_add_entry_18 = rail.WriteLogOperator(
            task_id='mkshiftfilelogs_add_entry_18',
            log="{{ result('create_shift_import_log') }}",
            message="na",
            severity="Skipped - Incorrect File Format",
            properties={
                "filename": "{{ result('foreach_non_csv_file_list').filename }}",
                "status": "Skipped - Incorrect File Format"
            }
        )

        rename_19 = rail.SFTPMoveFileOperator(
            task_id='rename_19',
            new_filename=config.archive_filepath +
            '''/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('foreach_non_csv_file_list').index'}}_{{ result('foreach_non_csv_file_list').filename }}''',
            existing_filename=config.input_filepath +
            '''/{{ result('foreach_non_csv_file_list').filename }}'''
        )

        foreach_non_csv_file_list_end = rail.EmptyOperator(
            task_id='foreach_non_csv_file_list_end'
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16") }}',
            dagrun_task_id='create_shift_import_child_for_master_log',
            flatten=True
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ result('create_shift_import_log') | load_all_records | to_json }}"
        )

        format_shift_import_logs = rail.PythonOperator(
            task_id='format_shift_import_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_logs
        )

        get_error_logs = rail.PythonOperator(
            task_id='get_error_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_shift_import_logs')))), 'length')
        )

        create_csv_lines_13 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_13',
            source="{{ result('format_shift_import_logs') | to_json }}",
            header=['File Name',
                    'File Log Path',
                    'Status',
                    'Reason',
                    'Job ID'],
            row=[
                "{{ item.filename }}",
                "{{ item.log_file_path }}",
                "{{ item.status }}",
                "{{ item.reason }}",
                "{{ ecid() }} - {{ item.childjobid }}"
            ]
        )

        upload_logs_upload_14 = rail.SFTPUploadFileOperator(
            task_id='upload_logs_upload_14',
            content='''{{ result('create_csv_lines_13') }}''',
            remote_filepath=config.log_filepath +
            '''/Filelog_{{ result('log_timetobeused_12') }}.csv'''
        )

        send_mail_15 = rail.EmailOperator(
            task_id='send_mail_15',
            to=config.tenant_email,
            bcc="{%- if result('get_error_logs', key='length') == 0  -%}\
                    "+config.internal_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Shift import " }} \
                {%- if result("get_error_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed \
                {%- endif -%} \
                {{ " - " + result("log_timetobeused_12") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /><br />Hello, <br /><br />The shift import job is completed on {{ current_time() }} and the logs are placed in the folder path: /PROD/Logs/Filelog_{{ result('log_timetobeused_12') }}.</p>
<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        dir_getallinputfiles_2 >> csv_files_list >> non_csv_files_list >> if_first_name_blank_3
        if_first_name_blank_3 >> rail.Label('Yes') >> finish
        if_first_name_blank_3 >> rail.Label(
            'No') >> create_shift_import_log >> generate_report >> get_report_details
        load_report_data >> log_timetobeused_12 >> upload_13 >> trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16 \
            >> wait_for_completion_trigger_dag_run_live_mk_shifts_master_eachfile_v1_0async_16 \
            >> foreach_non_csv_file_list >> mkshiftfilelogs_add_entry_18 >> rename_19 >> foreach_non_csv_file_list_end
        foreach_non_csv_file_list >> foreach_non_csv_file_list_end >> gather_logs >> load_master_log >> format_shift_import_logs \
            >> get_error_logs >> create_csv_lines_13 >> upload_logs_upload_14 >> send_mail_15 >> finish
        fail_no_report_data >> finish
        fail_column_order_mismatch >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
