"""
TransparentBPO User Import Master DAG
"""
from datetime import datetime, timedelta
from pendulum import now
from airflow.models import Variable
from transparentbpo.user_import.utils import request_payload, custom_methods
import rail

null = None


def create_master_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'TransparentBPO User Import Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=timedelta(minutes=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        log_job_start_timestamps = rail.PythonOperator(
            task_id='log_job_start_timestamps',
            python_callable=lambda: custom_methods.get_job_start_timestamps(
                config)
        )

        log_bamboo_lookback_timestamp = rail.PythonOperator(
            task_id='log_bamboo_lookback_timestamp',
            python_callable=lambda: Variable.get(
                config.bamboo_user_changes_lookback_timestamp, default_var=(now(
                    config.time_zone) - timedelta(minutes=60)).format(config.BAMBOO_DATE_FORMAT))
        )

        get_new_or_updated_employees = rail.BambooHROperator(
            task_id='get_new_or_updated_employees',
            request_method='GET',
            bamboohr_conn_id=config.bamboohr_conn_id,
            company_domain="",
            endpoint="/employees/changed?since={{result('log_bamboo_lookback_timestamp')}}",
            data_handler=lambda response: [v for v in response.get(
                'employees').values()] if response.get('employees') else []
        )

        update_lookback_timestamp = rail.PythonOperator(
            task_id='update_lookback_timestamp',
            python_callable=lambda: Variable.set(
                config.bamboo_user_changes_lookback_timestamp, now(config.time_zone).format(config.BAMBOO_DATE_FORMAT))
        )

        if_changed_employee_records_found = rail.IfOperator(
            task_id='if_changed_employee_records_found',
            test=lambda: rail.result('get_new_or_updated_employees'),
            yes_task='create_user_import_master_log',
            no_task='no_changed_employee_records_found',
        )

        no_changed_employee_records_found = rail.EmptyOperator(
            task_id='no_changed_employee_records_found'
        )

        create_user_import_master_log = rail.CreateLogOperator(
            task_id='create_user_import_master_log'
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id='list_reference_file',
            paths=[config.reference_filepath],
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: rail.result('list_reference_file')[
                config.reference_filepath][0]['name']
            if rail.result('list_reference_file') else None
        )

        if_file_not_present_or_doesnt_end_with_csv = rail.IfOperator(
            task_id='if_file_not_present_or_doesnt_end_with_csv',
            test=lambda: bool(not (rail.result('get_reference_filename')) or (
                rail.result('get_reference_filename').split('.')[-1] != 'csv')),
            yes_task="fail_with_reference_file_missing",
            no_task="download_reference_file",
        )

        fail_with_reference_file_missing = rail.FailOperator(
            task_id='fail_with_reference_file_missing',
            message='''Reference file missing'''
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath +
            "/{{ result('get_reference_filename')}}"
        )

        rename_move_existing_reference_file_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_move_existing_reference_file_to_archive',
            new_filename=config.archive_filepath +
            "/OLD_Reference_{{ result('get_reference_filename') }}.csv",
            existing_filename=config.reference_filepath +
            "/{{ result('get_reference_filename') }}",
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            delimiter=','
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('parse_reference_file') }}",
            name="reference_file_data",
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                "bamboo_hr_id_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Bamboo HR ID', 'uri'),
                "telephony_system_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Telephony System', 'uri'),
                "overtime_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Overtime', 'uri'),
                "telephony_id_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Telephony ID', 'uri'),
                "job_title_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Title', 'uri'),
                "labor_level_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Labor Level', 'uri'),
                "client_name_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Client Name', 'uri'),
                "project_name_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Project Name', 'uri'),
                "ssn_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'SSN', 'uri'),
                "department_cf_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Department', 'uri')
            }
        )

        get_supervisor_permission_set_uri = rail.RepliconServiceOperator(
            task_id='get_supervisor_permission_set_uri',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'name', 'Supervisor', 'uri')
        )

        create_existing_reference_data_log = rail.CreateLogOperator(
            task_id='create_existing_reference_data_log',
        )
        
        log_all_existing_reference_file_records = rail.WriteLogOperator(
            task_id='log_all_existing_reference_file_records',
            log="{{ result('create_existing_reference_data_log') }}",
            items = "{{ result('create_referencefile_collection') }}",
            severity='existing_entry',
            message='na',
            properties={
                'id': "{{item.id}}",
                'md5': "{{item.md5}}",
                'jobdate': "{{item.jobdate}}",
                'ecid': "{{item.ecid}}"
            }
        )

        trigger_process_each_changed_user = rail.trigger_parallel_dagrun(
            task_id='trigger_process_each_changed_user',
            items=lambda: rail.result('get_new_or_updated_employees'),
            parallel_count=config.process_each_changed_user_parallel_count,
            trigger_dag_id=config.process_each_user_dag_id,
            conf=lambda item: request_payload.get_process_each_user_payload(
                item),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: custom_methods.get_process_each_user_payload_dag_ids(
                config.process_each_changed_user_parallel_count),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dag_id,
            conf=lambda: {
                'total_records': len(rail.result('get_new_or_updated_employees')),
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_user_import_master_log'),
                'job_start_time': rail.result('log_job_start_timestamps')['email_timestamp'],
            }
        )

        wait_for_process_log_generation = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_log_generation',
            dag_runs="{{ result('process_log_generation') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        
        gather_projects_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_projects_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_project_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )
        
        
        trigger_process_project_logs_pregeneration_dag = rail.TriggerDagRunOperator(
            task_id='trigger_process_project_logs_pregeneration_dag',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.process_project_logs_pregeneration_dag_id,
            conf=lambda: {
                "project_sync_logs": rail.result("gather_projects_logs")
            }
        )
        
        wait_for_process_project_logs_pregeneration_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_project_logs_pregeneration_dag',
            dag_runs="{{ result('trigger_process_project_logs_pregeneration_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        
        gather_reference_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_reference_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_reference_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )
        
        format_reference_logs = rail.PythonOperator(
            task_id='format_reference_logs',
            python_callable=lambda: custom_methods.format_reference_logs(
                rail.result('gather_reference_logs'), rail.result('create_existing_reference_data_log'))
        )
        
        create_final_reference_file_csv = rail.WriteCSVFileOperator(
            task_id='create_final_reference_file_csv',
            source=lambda: rail.result('format_reference_logs'),
            header=[
                'id',
                'md5',
                'jobdate',
                'ecid'
            ],
            row=[
                '{{ item.id }}',
                '{{ item.md5 }}',
                '{{ item.jobdate }}',
                '{{ item.ecid }}'
            ]
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('create_final_reference_file_csv') }}''',
            remote_filepath=config.reference_filepath +
            "/newreference_{{ result('log_job_start_timestamps').log_timestamp }}.csv",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        log_job_start_timestamps >> log_bamboo_lookback_timestamp >> get_new_or_updated_employees >> update_lookback_timestamp >> if_changed_employee_records_found

        if_changed_employee_records_found >> rail.Label(
            "Yes") >> create_user_import_master_log

        if_changed_employee_records_found >> rail.Label(
            "No") >> no_changed_employee_records_found >> finish

        create_user_import_master_log >> list_reference_file >> get_reference_filename >> if_file_not_present_or_doesnt_end_with_csv

        if_file_not_present_or_doesnt_end_with_csv >> rail.Label(
            'No') >> download_reference_file
        if_file_not_present_or_doesnt_end_with_csv >> rail.Label(
            'Yes') >> fail_with_reference_file_missing >> finish

        download_reference_file >> rename_move_existing_reference_file_to_archive >> parse_reference_file >> create_referencefile_collection \
            >> get_all_custom_fields >> get_supervisor_permission_set_uri >> create_existing_reference_data_log \
                >> log_all_existing_reference_file_records >> trigger_process_each_changed_user

        trigger_process_each_changed_user >> get_process_users_dag_ids >> gather_user_logs >> process_log_generation

        process_log_generation >> wait_for_process_log_generation >> gather_projects_logs >> trigger_process_project_logs_pregeneration_dag \
            >> wait_for_process_project_logs_pregeneration_dag >> gather_reference_logs >> format_reference_logs \
                >> create_final_reference_file_csv >> upload_new_reference_file >> finish

        finish >> can_fail_dag

        can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_master_dag)
