import itertools
from datetime import timedelta
import rail
from omd.punch_time_import.utils import python_callable


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'OMD Punch Time Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
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
            no_task='send_incorrect_file_format_email',
        )

        send_incorrect_file_format_email = rail.EmailOperator(
            task_id='send_incorrect_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Punch Time Import - skipped {{ result('get_current_time_tz') }} ''',
            html_content="templates/emails/invalid_ext_email.html"
        )

        download_csv_content = rail.SFTPDownloadFileOperator(
            task_id='download_csv_content',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        get_current_time_tz = rail.PythonOperator(
            task_id='get_current_time_tz',
            python_callable=lambda: rail.render_template('{{current_time_in_specified_tz()}}')
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
            name="raw_input_data",
            columns={
                'EmployeeCode': 'employee_id',
                'LogDate': 'entry_date',
                'log Time': 'punch_time',
                'Direction': 'direction',
                'Devicelocation': 'devicelocation',
                'Deviceld': 'deviceid',
                'EmployeeCodeInDevice': 'employeecodeindevice'
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
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Punch Time Import - skipped {{ result('get_current_time_tz') }} ''',
            html_content="templates/emails/skipped_email.html"
        )

        create_time_entry_logs = rail.CreateLogOperator(
            task_id = 'create_time_entry_logs'
        )

        query_invalid_entries = rail.QueryCollectionOperator(
            task_id='query_invalid_entries',
            query="""SELECT * FROM raw_input_data WHERE 
                NULLIF(employee_id,'') IS NULL OR 
                NULLIF(entry_date,'') IS NULL OR 
                NULLIF(punch_time,'') IS NULL
            """,
            name='invalid_entries'
        )

        log_missing_madatory_fields = rail.WriteLogOperator(
            task_id='log_missing_madatory_fields',
            log="{{ result('create_time_entry_logs') }}",
            items="{{ result('query_invalid_entries') }}",
            message="One or more mandatory field is missing.",
            severity="Info",
            properties=lambda item:{
                "EmployeeCode": item["employee_id"],
                "LogDate": item["entry_date"],
                "log_time": item["punch_time"],
                "status": "Skipped",
                "details": python_callable.get_missing_field_message(item)
            }
        )


        query_valid_entries = rail.QueryCollectionOperator(
            task_id='query_valid_entries',
            query="""SELECT * FROM raw_input_data WHERE 
                NULLIF(employee_id,'') IS NOT NULL AND 
                NULLIF(entry_date,'') IS NOT NULL AND 
                NULLIF(punch_time,'') IS NOT NULL
            """,
            name='valid_entries'
        )

        if_has_valid_records = rail.IfOperator(
            task_id='if_has_valid_records',
            test='''{{ result('query_valid_entries', 'length') > 0 }}''',
            yes_task="query_unique_users_from_valid_data",
            no_task="process_log_generation",
        )

        query_unique_users_from_valid_data = rail.QueryCollectionOperator(
            task_id="query_unique_users_from_valid_data",
            query="SELECT DISTINCT employee_id FROM valid_entries"
        )

        empty_process_users = rail.EmptyOperator(
            task_id='empty_process_users'
        )

        trigger_unique_users = rail.trigger_parallel_dagrun(
            task_id='trigger_unique_users',
            items="{{ result('query_unique_users_from_valid_data') }}",
            trigger_dag_id=config.process_unique_users_child,
            parallel_count=config.parallel_count_unique_users,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "employee_id":  item['employee_id']
            }
        )
        
        get_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'trigger_unique_users_{x+1}') if rail.result(
                    f'trigger_unique_users_{x+1}') else []), range(config.parallel_count_unique_users))))),
            show_return_value_in_logs= False
        )

        gather_process_users_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_users_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_process_user_log',
            execution_timeout=timedelta(
                hours=config.execution_timeout_days),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'userlogs': "{{result('gather_process_users_logs')}}",
                'otherlogs': "{{ result('create_time_entry_logs') }}",
                'log_filename': "log_{{ get_company_key() }}_punch_time_import_{{current_time_in_specified_tz(fmt='%Y-%m-%dT%H-%M-%S') | replace(':', '-')}}.csv"
            }
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
        is_csv >> rail.Label('Yes') >> download_csv_content >> get_current_time_tz >> was_new_file_found
        get_current_time_tz >> load_csv_content >> create_collection_from_csv
        was_new_file_found >> rail.Label('Yes') >> archive_file
        create_collection_from_csv >> if_collection_has_no_data
        if_collection_has_no_data >> rail.Label('Yes') >> send_mail_skipped_import >> finish
        if_collection_has_no_data >> rail.Label('No') >> create_time_entry_logs >> query_invalid_entries \
        >> log_missing_madatory_fields >> query_valid_entries >> if_has_valid_records
        if_has_valid_records >> rail.Label('Yes') >> query_unique_users_from_valid_data >> empty_process_users \
        >> trigger_unique_users >> get_process_users_dag_ids >> gather_process_users_logs \
        >> process_log_generation >> finish
        if_has_valid_records >> rail.Label('No') >> process_log_generation
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        is_csv >> rail.Label('No') >> send_incorrect_file_format_email
        finish >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
