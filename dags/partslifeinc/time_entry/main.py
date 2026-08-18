from datetime import timedelta
from pendulum import datetime as dt
import rail
from partslifeinc.time_entry.utils import python_callable_methods


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Parts Life - Time Entry Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        start_date=dt(2024, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:


        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_csv_content',
            no_task='send_mail_incorrect_file_ext',
        )


        send_mail_incorrect_file_ext = rail.EmailOperator(
            task_id='send_mail_incorrect_file_ext',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{get_company_key()}} "+"| Time Punch Entry Import - skipped {{ result('get_current_time_tz') }} ''',
            html_content="templates/emails/invalid_ext_email.html"
        )

        rename_archive_the_inputfile = rail.SFTPMoveFileOperator(
            task_id='rename_archive_the_inputfile',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_csv_content = rail.SFTPDownloadFileOperator(
            task_id='download_csv_content',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        get_current_time_tz = rail.PythonOperator(
            task_id='get_current_time_tz',
            python_callable=lambda: rail.render_template('{{current_time_in_specified_tz(fmt="%Y-%m-%dT%H:%M:%S", tz="US/Eastern")}}')
        )

        if_new_file_found = rail.IfOperator(
            task_id='if_new_file_found',
            trigger_rule='all_done',
            test='{{ (get_task_state("new_file_sensor") == "success") and (result("new_file_sensor") | file_ext | lower == "csv") }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_csv_content = rail.LoadCSVFileOperator(
            task_id="load_csv_content",
            document="{{ result('download_csv_content') }}",
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_csv_content') }}",
            name="inputfile",
            columns={
                'Month, Day, Year of Ticket Date': 'timesheet_entry_date',
                'Employee Name': 'employeename',
                'Time_Ticket__Operation_Code': 'taskname',
                'Time_Ticket__Work_Center': 'subtaskname',
                'Attendance_Detail__Break_Code': 'break_type',
                'Attendance_Detail__Attendance_Code': 'attendance_code',
                'Time_Ticket__Job_Number': 'projects_number',
                'Order Number': 'project_name',
                'End Item': 'end_item',
                'Hour of Attendance_Detail__Adjusted_Clock_In' : 'punch_in_hr',
                'Minute of Attendance_Detail__Adjusted_Clock_In': 'punch_in_min',
                'Hour of Attendance_Detail__Adjusted_Clock_Out': 'punch_out_hr',
                'Minute of Attendance_Detail__Adjusted_Clock_Out': 'punch_out_min'
            }
        )

        if_collection_has_no_data = rail.IfOperator(
            task_id='if_collection_has_no_data',
            test='''{{ result('create_collection_from_csv', 'length') < 1 }}''',
            yes_task="send_mail_skipped_import",
            no_task="create_time_entry_logs"
        )

        send_mail_skipped_import = rail.EmailOperator(
            task_id='send_mail_skipped_import',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{get_company_key()}} "+"| Time Punch Entry Import - skipped {{ result('get_current_time_tz') }} ''',
            html_content="templates/emails/skipped_email.html"
        )

        create_time_entry_logs = rail.CreateLogOperator(
            task_id = 'create_time_entry_logs'
        )

        query_list_missing_mandatory_values = rail.QueryCollectionOperator(
            task_id='query_list_missing_mandatory_values',
            query="""SELECT * FROM inputfile WHERE NULLIF(timesheet_entry_date,'') IS NULL OR NULLIF(employeename,'') IS NULL
                    OR NULLIF(punch_in_hr,'') IS NULL OR NULLIF(punch_in_min,'') IS NULL OR NULLIF(punch_out_hr,'') IS NULL
                    OR NULLIF(punch_out_min,'') IS NULL"""
        )

        insert_missingmandatoryvalues_to_log = rail.WriteLogOperator(
            task_id='insert_missingmandatoryvalues_to_log',
            log="{{ result('create_time_entry_logs') }}",
            items="{{ result('query_list_missing_mandatory_values') }}",
            message="One or more mandatory field is missing.",
            severity="Info",
            properties=lambda item:{
                "employeename": item["employeename"],
                "timesheet_entry_date": item["timesheet_entry_date"],
                "break_type": item["break_type"],
                "project_name": item["project_name"],
                "punch_in_hr": item["punch_in_hr"],
                "punch_in_min": item["punch_in_min"],
                "punch_out_hr": item["punch_out_hr"],
                "punch_out_min": item["punch_out_min"],
                "status": "Ignored",
                "details": python_callable_methods.get_missing_field_message(item)
            }
        )


        query_list_recordswithmandatoryvalues = rail.QueryCollectionOperator(
            task_id='query_list_recordswithmandatoryvalues',
            query="""SELECT * FROM inputfile WHERE NULLIF(timesheet_entry_date,'') IS NOT NULL AND NULLIF(employeename,'') IS NOT NULL
                    AND NULLIF(punch_in_hr,'') IS NOT NULL AND NULLIF(punch_in_min,'') IS NOT NULL AND NULLIF(punch_out_hr,'') IS NOT NULL
                    AND NULLIF(punch_out_min,'') IS NOT NULL"""
        )

        if_has_valid_records = rail.IfOperator(
            task_id='if_has_valid_records',
            test='''{{ result('query_list_recordswithmandatoryvalues', 'length') > 0 }}''',
            yes_task="group_records_by_user",
            no_task="format_logs",
        )

        group_records_by_user = rail.PythonOperator(
            task_id = 'group_records_by_user',
            python_callable=lambda: python_callable_methods.group_records_user_and_date(
                rail.load_all_records(rail.result('query_list_recordswithmandatoryvalues')))
        )

        trigger_dag_run_process_time_entry_records_async = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_process_time_entry_records_async',
            items="{{ result('group_records_by_user') | to_json }}",
            trigger_dag_id=config.process_time_entry_child_dagid,
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "employee":  item['employee'],
                "timesheet_entry_date": item['timesheet_entry_date'],
                "data": item['data'],
                "create_time_entry_logs": rail.result('create_time_entry_logs')
            }
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_methods.do_format_logs
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('format_logs') | to_json }}",
            header=['employeename',
                    'timesheet_entry_date',
                    'break_type',
                    'project_name',
                    'punch_in_hr',
                    'punch_in_min',
                    'punch_out_hr',
                    'punch_out_min',
                    'status',
                    'details',
                    'jobid'
                    ],
            row=[
                "{{ item.employeename |sn}}",
                "{{ item.timesheet_entry_date |sn}}",
                "{{ item.break_type |sn}}",
                "{{ item.project_name |sn}}",
                "{{ item.punch_in_hr |sn}}",
                "{{ item.punch_in_min |sn}}",
                "{{ item.punch_out_hr |sn}}",
                "{{ item.punch_out_min |sn}}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("create_csv_lines")}}',
            output_file_name='Logs_time_punch_entry_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}',
            expires_in_seconds=config.log_file_download_link_expiry_in_sec
        )

        filter_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_error_logs",
            log='{{result("create_time_entry_logs")}}',
            severity='Error'
        )

        filter_exception_logs = rail.FilterLogEntriesOperator(
            task_id="filter_exception_logs",
            log='{{result("create_time_entry_logs")}}',
            severity='Exception'
        )

        send_time_entry_sync_mail = rail.EmailOperator(
            task_id="send_time_entry_sync_mail",
            to=config.tenant_email,
            bcc="{%- if result('filter_error_logs')| load_all_records() | length > 0  -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_email+"\
                {%- endif -%}",
            subject="{{get_company_key()}} "+"| Time Punch Entry Import - completed " +
            '{% if result("filter_error_logs")| load_all_records() | length > 0%}\
                with errors\
            {% elif result("filter_exception_logs")| load_all_records() | length > 0%}\
                with exceptions\
              {%else%}\
                successfully \
                {%endif%}' +
            '{{ result("get_current_time_tz") }}',
            html_content="templates/emails/import_mail.html"
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )



        new_file_sensor >> is_csv
        is_csv >> rail.Label('Yes') >> download_csv_content >> get_current_time_tz >> if_new_file_found
        if_new_file_found >> rail.Label('Yes') >> archive_file >> load_csv_content \
        >> create_collection_from_csv >> if_collection_has_no_data
        if_collection_has_no_data >> rail.Label('Yes') >> send_mail_skipped_import >> finish
        if_collection_has_no_data >> rail.Label('No') >> create_time_entry_logs >> query_list_missing_mandatory_values \
        >> insert_missingmandatoryvalues_to_log >> query_list_recordswithmandatoryvalues >> if_has_valid_records
        if_has_valid_records >> rail.Label('Yes') >> group_records_by_user \
        >> trigger_dag_run_process_time_entry_records_async >> format_logs >> create_csv_lines \
        >> generate_download_link >> filter_error_logs >> filter_exception_logs >> send_time_entry_sync_mail >> finish
        if_has_valid_records >> rail.Label('No') >> format_logs
        if_new_file_found >> rail.Label('No') >> delete_this_dagrun >> finish
        is_csv >> rail.Label('No') >> send_mail_incorrect_file_ext >> rename_archive_the_inputfile >> finish
        finish >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
