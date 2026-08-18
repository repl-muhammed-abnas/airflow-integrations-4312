from datetime import datetime, timedelta
from pendulum import now
import itertools
import rail
from mercury_systems_inc.time_import.utils import custom_methods
from mercury_systems_inc.time_import.task.run_project_report import run_report_task_group
from mercury_systems_inc.time_import.task.run_user_report import run_report_task_group_for_user
null = None


def create_airflow_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Mercury Systems Time Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2025, 3, 1),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:
        
        # File detection and download tasks
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_file_path,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
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
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.sftp_archive_file_path +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}")

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda:now(config.time_zone)
        )
        # File format validation
        is_file_csv = rail.IfOperator(
            task_id='is_file_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='parse_csv',
            no_task='send_improper_file_format',
        )

        send_improper_file_format = rail.EmailOperator(
            task_id="send_improper_file_format",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Mercury Systems Time Import - Incorrect File Format - {{ current_time_in_specified_tz(fmt="%m%d%YT%H%M%S") }}',
            html_content="templates/emails/improper_file_format.html"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id="parse_csv",
            document="{{ result('download_file') }}",
            encoding="utf-8-sig"
        )

        # Create log operator
        create_main_log = rail.CreateLogOperator(
            task_id="create_main_log"
        )

        get_run_date = rail.PythonOperator(
            task_id="get_run_date",
            python_callable=lambda: now(
                tz=config.time_zone).strftime("%Y-%m-%d")
        )
        # Process imported data
        create_import_data_collection = rail.CreateCollectionOperator(
            task_id="create_import_data_collection",
            source="{{ result('parse_csv') }}",
            columns={
                "EmpNbr": "employee_id",
                "Postdate": "entry_date",
                "laborhours": "hours",
                "Activitylevel1": "project_code",
                "Activitylevel2": "task_code",
                "lastname": "lastname",
                "firstname": "firstname"
            },
            name="import_data"
        )
        # Check if valid data exists
        any_import_data = rail.IfOperator(
            task_id="any_import_data",
            test="{{ result('create_import_data_collection','length') > 0}}",
            yes_task="update_date_with_sqldate",
            no_task="send_no_records_mail"
        )

        send_no_records_mail = rail.EmailOperator(
            task_id="send_no_records_mail",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Mercury Systems Time Import - No Records - {{ current_time_in_specified_tz(fmt="%m%d%YT%H%M%S") }}',
            html_content="templates/emails/no_valid_records.html"
        )

        update_date_with_sqldate = rail.DataAdaptorOperator(
            task_id="update_date_with_sqldate",
            source='{{result("create_import_data_collection")}}',
            data=custom_methods.get_sqldate_for_import_records
        )

        create_import_data_sql_collection = rail.CreateCollectionOperator(
            task_id="create_import_data_sql_collection",
            source='{{result("update_date_with_sqldate")}}',
            name="raw_import_data"
        )

        # Query for invalid records
        query_invalid_data_collection = rail.QueryCollectionOperator(
            task_id="query_invalid_data_collection",
            query="""SELECT * FROM raw_import_data
                    WHERE NULLIF(employee_id, '') IS NULL OR 
                    NULLIF(entry_date, '') IS NULL OR
                    DATE(entry_date)  IS NULL OR
                    (DATE(entry_date)
                    != DATE('{{result("get_run_date")}}')) OR 
                    NULLIF(hours, '') IS NULL OR 
                    NULLIF(project_code, '') IS NULL OR
                    CAST(hours AS NUMERIC) <= 0""",
            name="invalid_import_records"
        )

        # Check if invalid data exists
        if_invalid_data = rail.IfOperator(
            task_id="if_invalid_data",
            test="{{ result('query_invalid_data_collection','length') > 0}}",
            yes_task="write_log_invalid_data",
            no_task="query_valid_data_collection"
        )

        # Log invalid data
        write_log_invalid_data = rail.WriteLogOperator(
            task_id="write_log_invalid_data",
            log="{{result('create_main_log')}}",
            items="{{ result('query_invalid_data_collection') }}",
            message="Mandatory fields are missing",
            properties=lambda item: {
                "employee_id": item.get('employee_id', ''),
                "entry_date": item.get('entry_date', ''),
                "project_code": item.get('project_code', ''),
                "task_code": item.get('task_code', ''),
                "hours": item.get('hours', ''),
                "status": "Exception",
                "action": "Validation",
                "details": "; ".join(custom_methods.validate_csv_record(item))
            }
        )

        # Query for valid records
        query_valid_data_collection = rail.QueryCollectionOperator(
            task_id="query_valid_data_collection",
            query="""SELECT * FROM raw_import_data
                    WHERE NULLIF(employee_id, '') IS NOT NULL AND 
                    NULLIF(entry_date, '') IS NOT NULL AND
                    DATE(entry_date)  IS NOT NULL AND
                    DATE(entry_date) == DATE('{{result("get_run_date")}}') AND
                    NULLIF(hours, '') IS NOT NULL AND 
                    NULLIF(project_code, '') IS NOT NULL AND 
                    NULLIF(task_code, '') IS NOT NULL AND
                    CAST(hours AS NUMERIC) > 0""",
            name="valid_records"
        )

        if_valid_data = rail.IfOperator(
            task_id="if_valid_data",
            test='{{result("query_valid_data_collection", "length") > 0}}',
            yes_task="query_cumulative_hours",
            no_task="format_logs"
        )

        query_cumulative_hours = rail.QueryCollectionOperator(
            task_id="query_cumulative_hours",
            query="""SELECT *, SUM(hours) as total_hours FROM valid_records
                    GROUP BY
                    entry_date,project_code,task_code,employee_id""",
            name="valid_import_records"
        )

        run_report_task_start = rail.EmptyOperator(
            task_id="run_report_task_start")

        run_project_task_report = run_report_task_group(
            config,
            config.project_task_report_name,
            "project_task_data",
            ["project_code", "task_code", "project_uri","task_uri", "department","employee_id"]
        )

        run_user_report_start = rail.EmptyOperator(
            task_id="run_user_report_start")

        run_user_report = run_report_task_group_for_user(
            config,
            config.user_report_name,
            "user_data",
            ["user_uri", "timesheet_template", "employee_id", "user_status","department"]
        )

        query_invalid_users = rail.QueryCollectionOperator(
            task_id="query_invalid_users",
            query="""SELECT * FROM user_data ptd
                    WHERE ptd.employee_id IN (
                    SELECT employee_id
                    FROM user_data
                    WHERE NULLIF(employee_id, '') IS NOT NULL
                    GROUP BY employee_id
                    HAVING COUNT(*) > 1) OR (
                    ptd.user_status != 'Enabled'
                    OR NULLIF(ptd.timesheet_template, '') IS NULL
                    OR ptd.timesheet_template LIKE '%No Distribution%')""",
            name="invalid_users"
        )

        query_invalid_users_from_import = rail.QueryCollectionOperator(
            task_id="query_invalid_users_from_import",
            query="""SELECT vid.*, inv.user_status, inv.timesheet_template, 1 as employee_id_in_repl
                    FROM valid_import_records vid
                    JOIN invalid_users inv ON vid.employee_id = inv.employee_id
                    UNION ALL
                    SELECT vid.*, "" as user_status, "" as timesheet_template, 0 as employee_id_in_repl
                    FROM valid_import_records vid
                    LEFT JOIN user_data ud ON vid.employee_id = ud.employee_id
                    WHERE ud.employee_id IS NULL""",
            name="invalid_import_users"
        )

        write_log_invalid_users_from_import = rail.WriteLogOperator(
            task_id="write_log_invalid_users_from_import",
            log="{{result('create_main_log')}}",
            items="{{ result('query_invalid_users_from_import') }}",
            message="Employee details mismatch",
            properties=lambda item: {
                "employee_id": item.get('employee_id', ''),
                "entry_date": item.get('entry_date', ''),
                "project_code": item.get('project_code', ''),
                "task_code": item.get('task_code', ''),
                "hours": item.get('total_hours', ''),
                "status": "Exception",
                "action": "Validation",
                "details": custom_methods.get_invalid_user_message(item)
            }
        )

        query_select_valid_user_data_from_import = rail.QueryCollectionOperator(
            task_id="query_select_valid_user_data",
            query="""SELECT vid.*,ud.user_uri,ud.timesheet_template,ud.user_status,ud.department
                    FROM valid_import_records vid
                    JOIN user_data ud ON vid.employee_id = ud.employee_id
                    LEFT JOIN invalid_import_users iiu ON ud.employee_id = iiu.employee_id
                    WHERE iiu.employee_id IS NULL""",
            name="valid_user_import_records"
        )

        query_project_task_user_data = rail.QueryCollectionOperator(
            task_id="query_project_task_user_data",
            query="""SELECT ud.*,ptd.project_uri,ptd.task_uri
                    FROM valid_user_import_records ud JOIN project_task_data ptd
                    ON ((ud.department=ptd.department) OR (ud.employee_id=ptd.employee_id))
                    AND ud.project_code = ptd.project_code AND ud.task_code =ptd.task_code""",
            name="final_valid_records"
        )

        # Query for invalid project/task assignments
        query_invalid_project_task_user_data = rail.QueryCollectionOperator(
            task_id="query_invalid_project_task_user_data",
            query="""SELECT *
                FROM valid_user_import_records vuir
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM final_valid_records fvr
                    WHERE fvr.project_code = vuir.project_code
                    AND fvr.task_code = vuir.task_code
                )""",
            name="invalid_project_task_records"
        )

        # Check if invalid project task data exists
        if_invalid_project_task_user_data = rail.IfOperator(
            task_id="if_invalid_project_task_user_data",
            test="{{ result('query_invalid_project_task_user_data','length') > 0}}",
            yes_task="write_log_invalid_project_task_user_data",
            no_task="query_distinct_users"
        )

        # Log invalid project task data
        write_log_invalid_project_task_user_data = rail.WriteLogOperator(
            task_id="write_log_invalid_project_task_user_data",
            log="{{result('create_main_log')}}",
            items="{{ result('query_invalid_project_task_user_data') }}",
            message="User not assigned to Project/Task or Project/Task not active",
            properties=lambda item: {
                "employee_id": item.get('employee_id', ''),
                "entry_date": item.get('entry_date', ''),
                "project_code": item.get('project_code', ''),
                "task_code": item.get('task_code', ''),
                "hours": item.get('total_hours', ''),
                "status": "Exception",
                "action": "Validation",
                "details": "User or department not assigned to Project/Task or Project/Task not active"
            }
        )

        query_distinct_users = rail.QueryCollectionOperator(
            task_id="query_distinct_users",
            query="""SELECT DISTINCT user_uri,entry_date FROM final_valid_records"""
        )

        # Check if valid project task data exists
        if_valid_project_task_user_data = rail.IfOperator(
            task_id="if_valid_project_task_user_data",
            test="{{ result('query_distinct_users','length') > 0}}",
            yes_task="process_time_import_data_start",
            no_task="format_logs"
        )

        # Process time import data
        process_time_import_data_start = rail.EmptyOperator(
            task_id="process_time_import_data_start"
        )

        # Trigger DAG for processing time import data
        process_time_import_data = rail.trigger_parallel_dagrun(
            task_id="process_time_import_data",
            trigger_dag_id=config.process_time_data_dag_id,
            items=lambda: rail.load_all_records(
                rail.result("query_distinct_users")),
            parallel_count=config.parallel_trigger_dagrun_count,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf=lambda item: {
                "user_uri": item["user_uri"],
                "entry_date": item["entry_date"]
            }
        )

        get_time_import_data_dag_ids = rail.PythonOperator(
            task_id='get_time_import_data_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_time_import_data_{x+1}'), range(config.parallel_trigger_dagrun_count))))),
            show_return_value_in_logs=False
        )

        get_time_entry_import_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_time_entry_import_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{result("get_time_import_data_dag_ids")}}',
            dagrun_task_id="create_time_entry_log",
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_methods.format_logs
        )

        # Generate CSV report
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs')["logs"],
            header=[
                'Employee ID',
                'Entry Date',
                'Project Code',
                'Task Code',
                'Hours',
                'Status',
                'Action',
                'Details',
                "JobId"],
            row=[
                '{{item | attr_or_default("employee_id", "")}}',
                '{{item | attr_or_default("entry_date","") }}',
                '{{item | attr_or_default("project_code","") }}',
                '{{item | attr_or_default("task_code","")}}',
                '{{item | attr_or_default("hours","")}}',
                '{{item | attr_or_default("status","")}}',
                '{{item | attr_or_default("action","")}}',
                '{{item | attr_or_default("details","")}}',
                '{{item.ecid}}'],
            footer=['Number of records found:{{ result("create_import_data_collection","length")}}',
                    'Number of records processed:{{ result("format_logs").total_count}}',
                    'Number of success records: {{ result("format_logs").success_count}}',
                    'Number of error records: {{ result("format_logs").error_count }}',
                    'Number of exception records: {{ result("format_logs").exception_count}}',
                    ]
        )

        def get_log_details():
            job_end_time = now(config.time_zone)
            _start = rail.result("log_start_time")
            return {
                "log_file": rail.render_template(
                '{{result("new_file_sensor") | file_name |replace(".csv", "")+ "_"+current_time_in_specified_tz(fmt="%m%d%YT%H%M%S")}}.csv'),
                    "job_start_time": _start.isoformat(),
                    "job_end_time": job_end_time.isoformat(),
                    "job_duration": ((job_end_time - _start).minutes)
            }

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=get_log_details
        )

        # Upload logs to SFTP
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_log_file_path +
            '/{{result("get_log_file_name").log_file}}'
        )

        # Send import completion email
        send_import_completion_mail = rail.EmailOperator(
            task_id='send_import_completion_mail',
            to=config.tenant_email,
            bcc="{%- if result('format_logs').error_count == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }} | Mercury Systems Time Import - {{" "}}
                {%- if result("format_logs").error_count > 0 -%} 
                    completed with errors  
                {%- else -%} 
                    {%- if result("format_logs").exception_count > 0 -%} 
                        completed with exceptions  
                    {%- else -%} 
                        completed successfully  
                    {%- endif -%} 
                {%- endif -%} 
                {{ " " + current_time_in_specified_tz(fmt="%m%d%YT%H%M%S") }}''',
            html_content="templates/emails/import_complete.html",
            params={
                "log_file_path": config.sftp_log_file_path
            }
        )

        # Log to sumo
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        # Check if DAG should fail
        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="one_failed",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dag"
        )

        # Fail DAG if there are errors
        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="Time import completed with errors"
        )

        # Define the workflow
        new_file_sensor >> download_file >> was_new_file_found

        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun

        download_file >> log_start_time>> is_file_csv

        is_file_csv >> rail.Label("No") >> send_improper_file_format
        is_file_csv >> rail.Label(
            "Yes") >> parse_csv >> create_main_log >> get_run_date >> create_import_data_collection

        create_import_data_collection >> \
            any_import_data >> rail.Label(
                "No") >> send_no_records_mail >> log_to_sumo
        any_import_data >> rail.Label("Yes") >> \
        update_date_with_sqldate>> create_import_data_sql_collection>>\
            query_invalid_data_collection >> if_invalid_data
        if_invalid_data >> rail.Label(
            "Yes") >> write_log_invalid_data >> query_valid_data_collection
        if_invalid_data >> rail.Label("No") >> query_valid_data_collection >>\
            if_valid_data >> rail.Label("No") >> format_logs
        if_valid_data >> rail.Label("Yes") >> query_cumulative_hours >>\
        run_report_task_start >>\
        run_project_task_report >>\
        run_user_report_start >> run_user_report >>\
        query_invalid_users >>\
        query_invalid_users_from_import >> write_log_invalid_users_from_import >>\
        query_select_valid_user_data_from_import>>\
        query_project_task_user_data >>\
        query_invalid_project_task_user_data >>\
        if_invalid_project_task_user_data >> rail.Label(
            "Yes") >> write_log_invalid_project_task_user_data >> query_distinct_users 
        if_invalid_project_task_user_data >> rail.Label(
            "No") >> query_distinct_users >> if_valid_project_task_user_data

        if_valid_project_task_user_data >> rail.Label("No") >> format_logs
        if_valid_project_task_user_data >> rail.Label("Yes") >> process_time_import_data_start >> \
            process_time_import_data >> get_time_import_data_dag_ids >> get_time_entry_import_logs >>\
            format_logs >> render_logs_csv >> get_log_file_name >> upload_log_to_sftp >> send_import_completion_mail

        send_import_completion_mail >> log_to_sumo >> can_fail_dag

        can_fail_dag >> rail.Label("Yes") >> fail_dag

    return dag


rail.for_each_instance(create_airflow_main_dag)
