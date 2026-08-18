import hashlib
from datetime import timedelta

import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

from valleychildrens.user_import.utils import request_payload
from valleychildrens.user_import.utils.custom_methods import logging_details
from valleychildrens.user_import.tasks.get_user_prereqs import get_user_prereqs_task_group

_ENCODED_HEADER = [

    'emp_id', 'first_name', 'last_name', 'employee_type', 'company',
    'department', 'cme_entitlement', 'start_date', 'adjusted_start_date',
    'end_date', 'fte_total', 'sup_name', 'sup_id', 'login_name', 'email', 'md5',

]

def _build_encoded_row(item):
    """Map raw CSV columns (uppercase) → renamed lowercase + MD5. Used as the
    `row=` builder for WriteCSVFileOperator."""
    fields = [
        (item.get('EMPLOYEE') or '').strip() if isinstance(item.get('EMPLOYEE'), str) else (item.get('EMPLOYEE') or ''),
        (item.get('FIRST_NAME') or '').strip() if isinstance(item.get('FIRST_NAME'), str) else (item.get('FIRST_NAME') or ''),
        (item.get('LAST_NAME') or '').strip() if isinstance(item.get('LAST_NAME'), str) else (item.get('LAST_NAME') or ''),
        (item.get('EMP_TYPE') or '').strip() if isinstance(item.get('EMP_TYPE'), str) else (item.get('EMP_TYPE') or ''),
        (item.get('COMPANY') or '').strip() if isinstance(item.get('COMPANY'), str) else (item.get('COMPANY') or ''),
        (item.get('DEPARTMENT') or '').strip() if isinstance(item.get('DEPARTMENT'), str) else (item.get('DEPARTMENT') or ''),
        (item.get('CME_ENTITLEMENT') or '').strip() if isinstance(item.get('CME_ENTITLEMENT'), str) else (item.get('CME_ENTITLEMENT') or ''),
        item.get('HIRE_DATE') or '',
        item.get('ADJUSTED_HIRE_DATE') or '',
        item.get('TERM_DATE') or '',
        (item.get('FTE_TOTAL') or '').strip() if isinstance(item.get('FTE_TOTAL'), str) else (item.get('FTE_TOTAL') or ''),
        (item.get('SUP_NAME') or '').strip() if isinstance(item.get('SUP_NAME'), str) else (item.get('SUP_NAME') or ''),
        (item.get('SUP_EMPID') or '').strip() if isinstance(item.get('SUP_EMPID'), str) else (item.get('SUP_EMPID') or ''),
        (item.get('LOGIN_NAME') or '').strip() if isinstance(item.get('LOGIN_NAME'), str) else (item.get('LOGIN_NAME') or ''),
        (item.get('EMAIL') or '').strip() if isinstance(item.get('EMAIL'), str) else (item.get('EMAIL') or ''),
    ]
    md5 = hashlib.md5(''.join(str(v) for v in fields).encode('utf-8')).hexdigest()
    return fields + [md5]

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='ValleyChildrens User Import - Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval='*/15 * * * *',
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_file_directory,
            soft_fail_timeout=timedelta(minutes=10),
        )
        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
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
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
        )
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun',
        )
        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.pacific_timezone],
        )
        load_input_csv = rail.LoadCSVFileOperator(
            task_id='load_input_csv',
            document="{{ result('download_input_file') }}",
            delimiter='|',
        )
        write_csv_with_encoded = rail.WriteCSVFileOperator(
            task_id='write_csv_with_encoded',
            source="{{ result('load_input_csv') }}",
            header=_ENCODED_HEADER,
            row=_build_encoded_row,
        )
        check_reference_exists = rail.SFTPAnyFileSensor(
            task_id='check_reference_exists',
            path=config.reference_directory,
            soft_fail_timeout=timedelta(minutes=2),
        )
        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath,
        )
        load_reference_csv = rail.LoadCSVFileOperator(
            task_id='load_reference_csv',
            document="{{ result('download_reference_file') }}",
            delimiter=',',
            headers=_ENCODED_HEADER,
        )
        create_input_collection = rail.CreateCollectionOperator(
            task_id='create_input_collection',
            source="{{ result('write_csv_with_encoded') }}",
            name='input_data',
        )
        create_reference_collection = rail.CreateCollectionOperator(
            task_id='create_reference_collection',
            source="{{ result('load_reference_csv') }}",
            name='reference_data',
        )
        query_changed_records = rail.QueryCollectionOperator(
            task_id='query_changed_records',
            name='changed_records',
            query="""SELECT * FROM input_data
                     WHERE md5 NOT IN (SELECT md5 FROM reference_data)""",
        )
        query_invalid_changed = rail.QueryCollectionOperator(
            task_id='query_invalid_changed',
            query="""SELECT * FROM changed_records
                     WHERE NULLIF(emp_id, '') IS NULL OR NULLIF(first_name, '') IS NULL
                        OR NULLIF(last_name, '') IS NULL OR NULLIF(employee_type, '') IS NULL
                        OR NULLIF(company, '') IS NULL OR NULLIF(department, '') IS NULL
                        OR NULLIF(start_date, '') IS NULL OR NULLIF(fte_total, '') IS NULL
                        OR NULLIF(login_name, '') IS NULL OR NULLIF(cme_entitlement, '') IS NULL""",
        )
        log_invalid_changed = rail.WriteLogOperator(
            task_id='log_invalid_changed',
            log='{{ result("create_log") }}',
            items="{{ result('query_invalid_changed') }}",
            severity='Exception',
            message=lambda item: request_payload.get_mandatory_fields_exception_message(item),
            properties=lambda item: {
                'employee_id': item.get('emp_id', ''),
                'first_name': item.get('first_name', ''),
                'last_name': item.get('last_name', ''),
                'action': 'Validation',
                'status': 'Exception',
                'details': request_payload.get_mandatory_fields_exception_message(item),
            },
        )
        query_valid_changed = rail.QueryCollectionOperator(
            task_id='query_valid_changed',
            name='valid_changed_records',
            query="""SELECT * FROM changed_records
                     WHERE NULLIF(emp_id, '') IS NOT NULL AND NULLIF(first_name, '') IS NOT NULL
                       AND NULLIF(last_name, '') IS NOT NULL AND NULLIF(employee_type, '') IS NOT NULL
                       AND NULLIF(company, '') IS NOT NULL AND NULLIF(department, '') IS NOT NULL
                       AND NULLIF(start_date, '') IS NOT NULL AND NULLIF(fte_total, '') IS NOT NULL
                       AND NULLIF(login_name, '') IS NOT NULL AND NULLIF(cme_entitlement, '') IS NOT NULL""",
        )
        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('query_valid_changed', 'length') > 0 }}",
            yes_task='create_log',
            no_task='send_blank_payload_email',
        )
        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - blank payload received - {{ current_time_in_specified_tz() }}',
            html_content='templates/emails/blank_payload.html',
        )
        create_log = rail.CreateLogOperator(task_id='create_log')
        create_supervisor_log = rail.CreateLogOperator(task_id='create_supervisor_log')
        get_user_list_report = rail.RepliconReportDetailsOperator(
            task_id='get_user_list_report',
            report_name=config.report_name,
        )
        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                'reportParameters': [
                    {
                        'reportUri': "{{ result('get_user_list_report').uri }}",
                        'filterValues': [],
                        'outputFormatUri': 'urn:replicon:report-output-format-option:csv',
                    }
                ]
            },
        )
        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ result('get_report_details.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='load_existing_users',
        )
        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ result('get_report_details.get_report_result').reportGenerationResults[0].error }}",
        )
        load_existing_users = rail.LoadCSVFileOperator(
            task_id='load_existing_users',
            document="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}",
        )
        dummy_get_user_prereqs, get_prereqs = get_user_prereqs_task_group()
        create_existing_users_collection = rail.CreateCollectionOperator(
            task_id='create_existing_users_collection',
            source="{{ result('load_existing_users') }}",
            name='existing_users',
        )
        query_new_users = rail.QueryCollectionOperator(
            task_id='query_new_users',
            query="""SELECT i.* FROM valid_changed_records i
                     LEFT JOIN existing_users e ON i.emp_id = e.Employee_ID
                     WHERE e.Employee_ID IS NULL""",
        )
        query_update_users = rail.QueryCollectionOperator(
            task_id='query_update_users',
            query="""SELECT i.*, e.UserUri AS user_uri, e.Login_Name AS existing_login_name,
                            e.User_End_Date AS existing_end_date, e.User_Status AS existing_user_status
                     FROM valid_changed_records i
                     INNER JOIN existing_users e ON i.emp_id = e.Employee_ID""",
        )
        process_add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_add_user',
            items="{{ result('query_new_users') }}",
            trigger_dag_id=config.process_add_user_dagid,
            conf=lambda item: request_payload.get_process_add_user_conf(
                item, config,
                rail.result('create_log'), rail.result('create_supervisor_log')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_process_add_user',
            dag_runs="{{ result('process_add_user') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        process_user_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_user_update',
            items="{{ result('query_update_users') }}",
            trigger_dag_id=config.process_user_update_dagid,
            conf=lambda item: request_payload.get_process_user_update_conf(
                item, config,
                rail.result('create_log'), rail.result('create_supervisor_log')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_process_user_update = rail.WaitForDagRunsSensor(
            task_id='wait_process_user_update',
            dag_runs="{{ result('process_user_update') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        get_supervisor_pending_entries = rail.FilterLogEntriesOperator(
            task_id='get_supervisor_pending_entries',
            log="{{ result('create_supervisor_log') }}",
            severity='Pending',
            remove_filtered_entries=True,
        )
        is_supervisor_queued = rail.IfOperator(
            task_id='is_supervisor_queued',
            test="{{ result('get_supervisor_pending_entries', 'length') > 0 }}",
            yes_task='process_supervisor_assignment',
            no_task='dummy_process_log_generation',
        )
        process_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_assignment',
            items="{{ result('get_supervisor_pending_entries') }}",
            trigger_dag_id=config.process_supervisor_assignment_dagid,
            conf=lambda item: request_payload.get_process_supervisor_assignment_conf(
                {**dict(item['properties'].items())}, config,
                rail.result('create_supervisor_log')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )
        wait_process_supervisor_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_process_supervisor_assignment',
            dag_runs="{{ result('process_supervisor_assignment') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation',
        )
        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda: {
                'log_filename': rail.result('get_logging_details')['log_filename'],
                'log_id': rail.result('create_log'),
                'supervisor_log_id': rail.result('create_supervisor_log'),
            },
        )
        is_reference_present_post = rail.IfOperator(
            task_id='is_reference_present_post',
            test='{{ get_task_state("check_reference_exists") == "success" }}',
            yes_task='archive_old_reference',
            no_task='upload_new_reference',
        )
        archive_old_reference = rail.SFTPMoveFileOperator(
            task_id='archive_old_reference',
            existing_filename=config.reference_filepath,
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_Old_" + config.reference_filename,
        )
        upload_new_reference = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference',
            content="{{ result('write_csv_with_encoded') }}",
            remote_filepath=config.reference_filepath,
        )
        can_log_to_sumo = rail.IfOperator(
            task_id='can_log_to_sumo',
            trigger_rule='none_failed_min_one_success',
            test=lambda: request_payload.get_task_state('delete_this_dagrun') != 'success' and
                request_payload.get_task_state('download_input_file') == 'success',
            yes_task='log_to_sumo',
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'file_name': "{{ result('new_file_sensor') }}",
                'archive_file': "{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
                'no_of_input_records': "{{ result('create_input_collection', 'length') }}",
                'no_of_changed_records': "{{ result('query_changed_records', 'length') }}",
                'no_of_new_users': "{{ result('query_new_users', 'length') }}",
                'no_of_update_users': "{{ result('query_update_users', 'length') }}",
            },
        )
        new_file_sensor >> download_input_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_input_file >> get_logging_details >> load_input_csv >> write_csv_with_encoded
        write_csv_with_encoded >> check_reference_exists >> download_reference_file >> load_reference_csv
        write_csv_with_encoded >> create_input_collection
        load_reference_csv >> create_reference_collection
        [create_input_collection, create_reference_collection] >> query_changed_records
        query_changed_records >> [query_invalid_changed, query_valid_changed]
        query_invalid_changed >> log_invalid_changed
        query_valid_changed >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email >> can_log_to_sumo
        has_input_data >> rail.Label('Yes') >> create_log >> create_supervisor_log
        create_supervisor_log >> get_user_list_report >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label('Yes') >> fail_report_generation
        is_report_failed >> rail.Label('No') >> load_existing_users >> dummy_get_user_prereqs
        get_prereqs >> create_existing_users_collection >> query_new_users >> query_update_users
        query_update_users >> process_add_user >> wait_process_add_user
        wait_process_add_user >> process_user_update >> wait_process_user_update
        wait_process_user_update >> get_supervisor_pending_entries >> is_supervisor_queued
        is_supervisor_queued >> rail.Label('Yes') >> process_supervisor_assignment >> wait_process_supervisor_assignment >> dummy_process_log_generation
        is_supervisor_queued >> rail.Label('No') >> dummy_process_log_generation
        dummy_process_log_generation >> log_invalid_changed
        log_invalid_changed >> process_log_generation
        process_log_generation >> is_reference_present_post
        is_reference_present_post >> rail.Label('Yes') >> archive_old_reference >> upload_new_reference
        is_reference_present_post >> rail.Label('No') >> upload_new_reference
        upload_new_reference >> can_log_to_sumo >> log_to_sumo
    return dag

rail.for_each_instance(create_main_dag)

