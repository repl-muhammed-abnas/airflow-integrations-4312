from pendulum import now
import itertools
import rail
import os
from datetime import timedelta

from transparentbpo.time_entry_import.utils import custom_methods
from transparentbpo.time_entry_import.task.run_project_report import run_report_task_group

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'TransparentBPO Time Entry Import - Master DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        log_start_time = rail.PythonOperator(
            task_id = "log_start_time",
            python_callable=lambda: {
                "start_time": now(config.timezone).isoformat()
            }
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_csv_content',
            no_task='send_invalid_format_email',
        )

        send_invalid_format_email = rail.EmailOperator(
            task_id='send_invalid_format_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Time Import - Invalid Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/invalid_format_email.html"
        )

        download_csv_content = rail.SFTPDownloadFileOperator(
            task_id='download_csv_content',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=f"{config.archive_filepath}/archive_{{{{ dag_run_ecid() | replace(':', '-') }}}}_{{{{ result('new_file_sensor') | file_name }}}}"
        )

        def check_file_content():
            with rail.existing_artifact(rail.result('download_csv_content')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=check_file_content,
            yes_task='load_csv_data',
            no_task='send_no_data_email'
        )

        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document="{{ result('download_csv_content') }}",
        )

        create_csv_collection = rail.CreateCollectionOperator(
            task_id='create_csv_collection',
            source="{{ result('load_csv_data') }}",
            name="time_import_records",
            columns=config.column_mapping
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_csv_collection', 'length') > 0 }}",
            yes_task='create_records_log',
            no_task='send_no_data_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Time Import - No Data - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_no_data_email.html"
        )

        create_records_log = rail.CreateLogOperator(
            task_id='create_records_log',
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint ="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler = lambda response: list(map(lambda x : {"activity": x["name"]}, response))
        )

        create_activity_collection = rail.CreateCollectionOperator(
            task_id="create_activity_collection",
            source="{{ result('get_all_activities') | to_json }}",
            name="replicon_activities"
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM time_import_records WHERE NULLIF(employee_id, '') IS NOT NULL 
            AND NULLIF(work_date, '') IS NOT NULL AND NULLIF(start_time, '') IS NOT NULL
            AND NULLIF(end_time, '') IS NOT NULL AND NULLIF(timesheet_category, '') IS NOT NULL 
            AND NULLIF(project, '') IS NOT NULL AND NULLIF(task, '') IS NOT NULL 
            AND (NULLIF(activity, '') IS NOT NULL OR timesheet_category IN ('BREAK', 'LUNCH'))""",
        )
        
        validate_entry_date = rail.PythonOperator(
            task_id='validate_entry_date',
            python_callable=custom_methods.validate_entry_date_format
        )
        
        log_invalid_entry_date = rail.WriteLogOperator(
            task_id='log_invalid_entry_date',
            items='{{ result("validate_entry_date").invalid_records | to_json }}',
            log='{{ result("create_records_log") }}',
            severity='Exception',
            message='Date or Time format is invalid',
            properties={
                'employee_id': '{{ item.employee_id }}',
                'work_date': '{{ item.work_date }}',
                'project': '{{ item.project }}',
                'task': '{{ item.task }}',
                'activity': '{{ item.activity }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Date or Time format is invalid'
            },
        )

        valid_records = rail.CreateCollectionOperator(
            task_id='valid_records',
            source='{{ result("validate_entry_date").valid_records | to_json }}',
            name="valid_record_data",
            columns=list(config.column_mapping.values()),
        )

        store_valid_records = rail.QueryCollectionOperator(
            task_id='store_valid_records',
            query="""SELECT * FROM valid_record_data WHERE activity IN (
            SELECT activity FROM replicon_activities) OR activity = 'N/A' OR timesheet_category IN ('BREAK', 'LUNCH')""",
            name="valid_entries",
        )
        
        store_invalid_records = rail.QueryCollectionOperator(
            task_id='store_invalid_records',
            query="""SELECT * FROM time_import_records WHERE NULLIF(employee_id, '') IS NULL 
            OR NULLIF(work_date, '') IS NULL OR NULLIF(start_time, '') IS NULL OR 
            NULLIF(end_time, '') IS NULL OR NULLIF(timesheet_category, '') IS NULL 
            OR NULLIF(project, '') IS NULL OR NULLIF(task, '') IS NULL 
            OR (NULLIF(activity, '') IS NULL AND timesheet_category NOT IN ('BREAK', 'LUNCH')) OR
            (NULLIF(activity,'') IS NOT NULL AND activity NOT IN (SELECT activity FROM replicon_activities) AND activity <> 'N/A'
            AND timesheet_category NOT IN ('BREAK', 'LUNCH'))""",
            name="invalid_entries"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_records_log') }}",
            severity="Exception",
            items="{{ result('store_invalid_records') }}",
            message="Invalid time import record - missing mandatory field(s)",
            properties=lambda item: {
                'employee_id': item.get('employee_id', ''),
                'work_date': item.get('work_date', ''),
                'project': item.get('project', ''),
                'task': item.get('task', ''),
                'activity': item.get('activity', ''),
                'status': "Exception",
                'action': "Validation",
                'details': custom_methods.get_validation_error_message(item)
            }
        )

        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test="{{ result('store_valid_records', 'length') > 0 }}",
            yes_task='run_report_start',
            no_task='trigger_log_generation'
        )

        run_report_start = rail.EmptyOperator(task_id="run_report_start")

        run_project_task_report = run_report_task_group(
            config,
            config.project_task_report_name,
            "project_task_data",
            ["project", "task", "employee_id", "project_uri","task_uri", ]
        )

        query_project_task_user_data = rail.QueryCollectionOperator(
            task_id="query_project_task_user_data",
            query="""SELECT ud.*,ptd.project_uri,ptd.task_uri
                    FROM valid_entries ud JOIN project_task_data ptd
                    ON ud.employee_id=ptd.employee_id
                    AND ud.project = ptd.project AND ud.task = ptd.task""",
            name="final_valid_records"
        )

        query_invalid_project_task_user_data = rail.QueryCollectionOperator(
            task_id="query_invalid_project_task_user_data",
            query="""SELECT *
                FROM valid_entries vuir
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM final_valid_records fvr
                    WHERE fvr.employee_id = vuir.employee_id
                    AND fvr.project = vuir.project
                    AND fvr.task = vuir.task
                )""",
            name="invalid_project_task_records"
        )

        # Check if invalid project task data exists
        if_invalid_project_task_user_data = rail.IfOperator(
            task_id="if_invalid_project_task_user_data",
            test="{{ result('query_invalid_project_task_user_data','length') > 0}}",
            yes_task="write_log_invalid_project_task_user_data",
            no_task="get_unique_employee_ids"
        )

        # Log invalid project task data
        write_log_invalid_project_task_user_data = rail.WriteLogOperator(
            task_id="write_log_invalid_project_task_user_data",
            log="{{ result('create_records_log') }}",
            items="{{ result('query_invalid_project_task_user_data') }}",
            message="User not assigned to Project/Task or Project/Task not active",
            properties=lambda item: {
                'employee_id': item.get('employee_id', ''),
                'work_date': item.get('work_date', ''),
                'project': item.get('project', ''),
                'task': item.get('task', ''),
                'activity': item.get('activity', ''),
                "status": "Exception",
                "action": "Validation",
                "details": "User not assigned to Project/Task or Project/Task not active"
            }
        )

        get_unique_employee_ids = rail.QueryCollectionOperator(
            task_id='get_unique_employee_ids',
            query="SELECT DISTINCT Employee_ID FROM final_valid_records",
            name="unique_employees"
        )
        
        get_all_breaktypes = rail.RepliconServiceOperator(
            task_id="get_all_breaktypes",
            endpoint="/services/BreakTypeService1.svc/GetAllBreakTypes",
        )

        add_row_to_unique_employee_id = rail.QueryCollectionOperator(
            task_id='add_row_to_unique_employee_id',
            query="SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id, * FROM unique_employees"
        )

        def get_process_each_user_batch_dag_id(record_id):
            modulo = int(record_id)%config.PROCESS_USER_BATCH_COUNT
            return f'{config.process_unique_users_child}_batch_{modulo+1}'

        trigger_process_users = rail.trigger_parallel_dagrun(
            task_id='trigger_process_users',
            items=lambda: rail.result('add_row_to_unique_employee_id'),
            parallel_count=config.process_parallel_count,
            trigger_dag_id=lambda item: get_process_each_user_batch_dag_id(item['record_id']),
            conf=lambda item: {
                "employee_id": item['employee_id'],
                "break_types": rail.result("get_all_breaktypes")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_trigger_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_trigger_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_process_users_{x+1}'), range(config.process_parallel_count))))),
            show_return_value_in_logs= False
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("get_trigger_process_users_dag_ids") }}',
            dagrun_task_id='create_process_user_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'timeentrylogs': rail.result("gather_child_logs") if rail.result("gather_child_logs") else None,
                'otherlogs': [log for log in [rail.result("create_records_log"), rail.result("invalid_format_log")] if log],
                'log_filename': rail.render_template('log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}'),
                'start_time': rail.result("log_start_time")["start_time"],
                'input_filename': rail.render_template('{{ result("new_file_sensor") | file_name }}'),
            }
        )

        new_file_sensor >> log_start_time >> is_csv
        is_csv >> rail.Label("Yes") >> download_csv_content
        is_csv >> rail.Label("No") >> send_invalid_format_email
        
        download_csv_content >> has_file_content
        download_csv_content >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("no") >> delete_this_dagrun
        
        has_file_content >> rail.Label("Yes") >> load_csv_data
        has_file_content >> rail.Label("No") >> send_no_data_email
        
        load_csv_data >> create_csv_collection >> has_any_records
        
        has_any_records >> rail.Label("Yes") >> create_records_log
        has_any_records >> rail.Label("No") >> send_no_data_email
        
        create_records_log >> get_all_activities >> create_activity_collection >>\
        query_valid_records >> validate_entry_date >> log_invalid_entry_date>>\
        valid_records >> store_valid_records >>\
        store_invalid_records >> log_invalid_records >> has_valid_records
        
        has_valid_records >> rail.Label("Yes") >> run_report_start >>\
        run_project_task_report >> query_project_task_user_data >>\
        query_invalid_project_task_user_data >> \
        if_invalid_project_task_user_data >> rail.Label("Yes") >>\
        write_log_invalid_project_task_user_data >> get_unique_employee_ids
        if_invalid_project_task_user_data >> rail.Label("No") >>\
        get_unique_employee_ids
        has_valid_records >> rail.Label("No") >> trigger_log_generation
        
        get_unique_employee_ids >> get_all_breaktypes >> add_row_to_unique_employee_id >> trigger_process_users
        trigger_process_users >> get_trigger_process_users_dag_ids >> gather_child_logs >> trigger_log_generation

    return dag
rail.for_each_instance(create_dag)