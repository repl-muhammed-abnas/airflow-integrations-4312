
from datetime import timedelta
import pendulum
import itertools
from airflow.models import Variable
from ge.user_sync_poland.utils import custom_methods, request_payload
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'GE POLAND User Import Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        input_column_map = {
            "EmployeeFirstName": "EmployeeFirstName", "EmployeeLastName": "EmployeeLastName", "EmployeeEmailAddress": "EmployeeEmailAddress",
            "OHRID": "OHRID", "LegalEntityHireDate": "LegalEntityHireDate", "LegacyPayrollID": "LegacyPayrollID", "Job/PositionTitle": "Job_PositionTitle",
            "SupervisorSSOID": "SupervisorSSOID", "SupervisorName": "SupervisorName", "DWSStartDate": "DWSStartDate", "DWSMonday": "DWSMonday",
            "DWSTuesday": "DWSTuesday", "DWSWednesday": "DWSWednesday", "DWSThursday": "DWSThursday", "DWSFriday": "DWSFriday",
            "DWSSaturday": "DWSSaturday", "DWSSunday": "DWSSunday", "TerminationEffectiveDate": "TerminationEffectiveDate", "IndustryFocusGroup": "IndustryFocusGroup",
            "LegalEntity": "LegalEntity", "ContractID": "ContractID", "ContractType": "ContractType", "RadiationFlag": "RadiationFlag",
            "PositionCapacity": "PositionCapacity", "PreviousExperience": "PreviousExperience", "OvertimeEligibility": "OvertimeEligibility",
            "SuspendAssignmentCategory": "SuspendAssignmentCategory", "Payroll": "Payroll", "HealthcareProductLineEIT": "HealthcareProductLineEIT",
            "JobType": "JobType", "CareerBand": "CareerBand", "AdjustedServiceDate": "AdjustedServiceDate", "Work": "Work", "HRMSSOID": "HRMSSOID",
            "HRMName": "HRMName", "SpecialWorkSchedule": "SpecialWorkSchedule", "EducationLevel": "EducationLevel", "WorkLocation": "WorkLocation",
            "AssignmentEffectiveDate": "AssignmentEffectiveDate", "HireEffectiveDate": "HireEffectiveDate", "RevTermEffectiveDate": "RevTermEffectiveDate"
        }

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_file_pgp = rail.IfOperator(
            task_id='is_file_pgp',
            test="{{result('new_file_sensor') | file_name | lower | ends_with('.pgp') }}",
            yes_task="get_time_for_file",
            no_task="send_mail_for_incorrect_file_format",
        )

        send_mail_for_incorrect_file_format = rail.EmailOperator(
            task_id='send_mail_for_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Poland User import - File processing is skipped - {{ current_time_in_specified_tz("US/Pacific", "%d/%m/%YT%H:%M:%S") }} ''',
            html_content="templates/incorrect_file_format.html",
            params=None,
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now(
                config.time_zone).strftime('%Y%m%dT%H%M%S')
        )

        log_integration_run_date = rail.PythonOperator(
            task_id='log_integration_run_date',
            python_callable=lambda: pendulum.now(
                config.time_zone).strftime(config.DATE_DEFAULT_FORMAT)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
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
            new_filename=config.sftp_archive_filepath +
            '''/Processed_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename="{{ result('new_file_sensor') }}",
        )

        decrypt_input_file = rail.PGPDecryptionOperator(
            task_id='decrypt_input_file',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('download_file') }}",
        )

        log_file_name_to_use = rail.PythonOperator(
            task_id='log_file_name_to_use',
            python_callable=lambda:  rail.render_template(
                '''{{ result('new_file_sensor') | file_name | replace('.pgp', '') }}''')
        )

        archive_decrypted_file_internal_sftp = rail.SFTPUploadFileOperator(
            task_id='archive_decrypted_file_internal_sftp',
            content="{{ result('decrypt_input_file') }}",
            sftp_conn_id=config.sftp_ge_internal,
            remote_filepath=config.sftp_archive_filepath +
            '''{{ result('new_file_sensor') | file_name | replace('.pgp', '') }}''',
        )

        parse_csv_input = rail.LoadCSVFileOperator(
            task_id="parse_csv_input",
            document="{{ result('decrypt_input_file') }}",
            delimiter='|'
        )

        load_csv = rail.WriteCSVFileOperator(
            task_id='load_csv',
            source="{{ result('parse_csv_input') }}",
            header=input_column_map.keys(),
            row=request_payload.get_formated_user_row,
            delimiter='|'
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_csv') }}",
            name="inputfilerawdata",
            columns=input_column_map
        )

        if_parse_csv_lines_less_than_1 = rail.IfOperator(
            task_id='if_parse_csv_lines_less_than_1',
            test='{{ result("create_collection_from_csv", "length") == 0 }}',
            yes_task="send_mail_send_email_for_blank_file_no_records",
            no_task="trigger_dag_run_schedule_add",
        )

        send_mail_send_email_for_blank_file_no_records = rail.EmailOperator(
            task_id='send_mail_send_email_for_blank_file_no_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Poland User import - File processing is skipped - {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }}''',
            html_content='''templates/blank_file.html'''
        )

        trigger_dag_run_schedule_add = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_schedule_add',
            retries=0,
            trigger_dag_id=config.child_schedule_add_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf={
                "inputdata": "{{ result('load_csv') }}"
            }
        )

        wait_for_completion_trigger_dag_run_schedule_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_schedule_add',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_schedule_add") }}'
        )

        trigger_dag_run_suspend_assignment_category_custom_field = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_suspend_assignment_category_custom_field',
            retries=0,
            trigger_dag_id=config.child_suspend_assignment_category_custom_field_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf={
                "inputdata": "{{ result('load_csv') }}"
            }
        )

        wait_for_completion_trigger_dag_run_suspend_assignment_category_custom_field = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_suspend_assignment_category_custom_field',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_suspend_assignment_category_custom_field") }}'
        )

        trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master',
            retries=0,
            trigger_dag_id=config.child_legacy_payroll_id_servicecenter_add_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf={
                "inputdata": "{{ result('load_csv') }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master',
            execution_timeout=timedelta(config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master") }}'
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id='get_all_service_centers',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:effectively-enabled",
                    "urn:replicon:service-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=custom_methods.get_all_service_centers_list
        )

        get_enabled_departments = rail.RepliconServiceOperator(
            task_id='get_enabled_departments',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments"
        )

        poland_master_mapper_search_to_find_department_name = rail.PythonOperator(
            task_id='poland_master_mapper_search_to_find_department_name',
            python_callable=lambda:  next(iter(
                filter(lambda x: x['type'] == "Department", config.POLAND_MASTER_MAPPER)), {}).get('value', '')
        )

        log_required_department_uri = rail.PythonOperator(
            task_id='log_required_department_uri',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_departments'), 'name', rail.result('poland_master_mapper_search_to_find_department_name'), 'uri', "")
            if rail.result('poland_master_mapper_search_to_find_department_name') else None
        )

        user_import_log_master = rail.CreateLogOperator(
            task_id='user_import_log_master',
        )

        supervisor_log = rail.CreateLogOperator(
            task_id='supervisor_log',
        )

        query_inputfilerawdata_for_records_to_skip = rail.QueryCollectionOperator(
            task_id='query_inputfilerawdata_for_records_to_skip',
            name='records_to_skip',
            query="""SELECT * FROM inputfilerawdata WHERE NULLIF(LegalEntity, '') IS NULL or
                    NULLIF(OHRID, '') IS NULL or NULLIF(LegacyPayrollID, '') IS NULL """
        )

        add_skipped_records_to_user_log = rail.WriteLogOperator(
            task_id='add_skipped_records_to_user_log',
            items="{{ result('query_inputfilerawdata_for_records_to_skip') }}",
            log="{{ result('user_import_log_master')}}",
            message="na",
            severity="Skipped",
            properties=lambda item: {
                "OHRID": item['OHRID'],
                "action": "Validation",
                "status": "Skipped",
                "details": custom_methods.get_validation_log_details,
                "username": item['EmployeeFirstName'] + " " + item['EmployeeLastName']
            }
        )

        query_inputfilerawdata_for_records_to_process = rail.QueryCollectionOperator(
            task_id='query_inputfilerawdata_for_records_to_process',
            name='records_to_process',
            query="""SELECT * FROM inputfilerawdata WHERE NULLIF(LegalEntity, '') IS NOT NULL and
                    NULLIF(OHRID, '') IS NOT NULL and NULLIF(LegacyPayrollID, '') IS NOT NULL """
        )

        process_each_user = rail.trigger_parallel_dagrun(
            task_id='process_each_user',
            items="{{ result('query_inputfilerawdata_for_records_to_process') }}",
            trigger_dag_id=config.child_process_each_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.process_each_user_trigger_parallel_count,
            conf=lambda item: request_payload.get_process_each_user_payload(
                item)
        )

        get_process_each_user_dag_ids = rail.PythonOperator(
            task_id='get_process_each_user_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_user_{x+1}'), range(config.process_each_user_trigger_parallel_count))))),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_each_user_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        filter_ge_poland_supervisor_assignment_table_entries = rail.FilterLogEntriesOperator(
            task_id='filter_ge_poland_supervisor_assignment_table_entries',
            log="{{result('supervisor_log')}}",
            severity="queued",
            remove_filtered_entries=True
        )

        if_entries_in_supervisor_assignment_logs = rail.IfOperator(
            task_id='if_entries_in_supervisor_assignment_logs',
            test=lambda: rail.result(
                'filter_ge_poland_supervisor_assignment_table_entries', 'length') > 0,
            yes_task='process_supervisor_add',
            no_task='format_logs'
        )

        process_supervisor_add = rail.EmptyOperator(
            task_id='process_supervisor_add'
        )

        trigger_dag_run_ge_poland_child_add_foreign_supervisor_50 = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_ge_poland_child_add_foreign_supervisor_50',
            items="{{ result('filter_ge_poland_supervisor_assignment_table_entries') }}",
            trigger_dag_id=config.child_add_foreign_supervisor_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.child_add_foreign_supervisor_trigger_parallel_count,
            conf=lambda item: {
                "loginname": item['username'],
                'supervisorloginname': item['supervisorloginname'],
                'useruri': item['useruri'],
                'action': item['action'],
                'status': item['status'],
                'supervisoreffectivedate': item['supervisoreffectivedate'],
                'supervisorusername': item['supervisorusername'],
                'foreignsupervisordepartmenturi': rail.result('log_required_department_uri'),
                "integration_run_date": rail.result('log_integration_run_date')
            }
        )

        trigger_dag_run_ge_poland_child_add_supervisor_54 = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_ge_poland_child_add_supervisor_54',
            items="{{ result('filter_ge_poland_supervisor_assignment_table_entries') }}",
            trigger_dag_id=config.child_add_supervisor_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.child_add_supervisor_trigger_parallel_count,
            conf=lambda item: {
                "loginname": item['username'],
                'supervisorloginname': item['supervisorloginname'],
                'useruri': item['useruri'],
                'action': item['action'],
                'status': item['status'],
                'supervisoreffectivedate': item['supervisoreffectivedate'],
                'supervisorusername': item['supervisorusername'],
                "user_log": item['user_log'],
                "integration_run_date": rail.result('log_integration_run_date'),
                "supervisor_log": rail.result('supervisor_log')
            }
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'User Name',
                'Login Name',
                'Action',
                'Status',
                'Details',
                'JobId'
            ],
            row=[
                '{{ item.username }}',
                '{{ item.OHRID }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],
            footer=lambda: [
                'Number of records found:' +
                str(rail.result("format_logs", "total_record_count")),
                'Number of records processed:' + str(int(rail.result(
                    "format_logs", "exception_record_count")) + int(rail.result(
                        "format_logs", "error_record_count")) + int(rail.result("format_logs", "success_record_count"))),
                'Number of success records: ' +
                str(rail.result("format_logs", "success_record_count")),
                'Number of error records: ' +
                str(rail.result("format_logs", "error_record_count")),
                'Number of exception records: ' +
                str(rail.result("format_logs", "exception_record_count")),
                'Number of skipped records: ' +
                str(rail.result("format_logs", "skipped_record_count")),
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_log_filepath +
            '/Logs_{{ current_time_in_specified_tz("' + config.time_zone +
            '", "%H%M%S") }}_{{ result("new_file_sensor") | file_name }}.csv',
        )

        get_final_error_exception_count = rail.PythonOperator(
            task_id='get_final_error_exception_count',
            python_callable=lambda: {
                'final_error_record_count': int(rail.result("format_logs", key="error_record_count")),
                'final_exception_record_count':  int(rail.result("format_logs", key="exception_record_count"))
            }
        )

        generate_downloadlink_logs = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink_logs',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='Logs_{{ current_time_in_specified_tz("' + config.time_zone +
            '", "%H%M%S") }}_{{ result("new_file_sensor") | file_name }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_final_error_exception_count').final_error_record_count == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Poland User import {{""}} \
                {%- if result("get_final_error_exception_count").final_error_record_count > 0 -%} \
                    completed with errors  \
                {%- elif result("get_final_error_exception_count").final_exception_record_count > 0 -%} \
                    completed with exceptions  \
                {%- else -%} \
                    completed successfully \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("' + config.time_zone + '", "%m/%d/%YT%H:%M:%S") }}',
            html_content="templates/import_complete.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule='all_done',
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_file_pgp

        is_file_pgp >> rail.Label(
            'No') >> send_mail_for_incorrect_file_format >> finish
        is_file_pgp >> rail.Label('Yes') >> get_time_for_file

        get_time_for_file >> log_integration_run_date >> download_file >> was_new_file_found

        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file

        download_file >> decrypt_input_file >> log_file_name_to_use >> archive_decrypted_file_internal_sftp >> parse_csv_input


        parse_csv_input >> load_csv >> create_collection_from_csv

        create_collection_from_csv >> if_parse_csv_lines_less_than_1
        if_parse_csv_lines_less_than_1 >> rail.Label(
            'Yes') >> send_mail_send_email_for_blank_file_no_records >> finish
        if_parse_csv_lines_less_than_1 >> rail.Label(
            'No') >> trigger_dag_run_schedule_add

        trigger_dag_run_schedule_add >> wait_for_completion_trigger_dag_run_schedule_add \
            >> trigger_dag_run_suspend_assignment_category_custom_field \
            >> wait_for_completion_trigger_dag_run_suspend_assignment_category_custom_field \
            >> trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master \
            >> wait_for_completion_trigger_dag_run_ge_poland_legacy_payroll_id_service_centre_add_master \
            >> get_all_service_centers >> get_enabled_departments >> poland_master_mapper_search_to_find_department_name

        poland_master_mapper_search_to_find_department_name >> log_required_department_uri \
            >> user_import_log_master >> supervisor_log >> query_inputfilerawdata_for_records_to_skip \
            >> add_skipped_records_to_user_log >> query_inputfilerawdata_for_records_to_process >> process_each_user

        process_each_user >> get_process_each_user_dag_ids >> gather_user_logs >> filter_ge_poland_supervisor_assignment_table_entries >>\
            if_entries_in_supervisor_assignment_logs

        if_entries_in_supervisor_assignment_logs >> rail.Label(
            'No') >> format_logs
        if_entries_in_supervisor_assignment_logs >> rail.Label(
            'Yes') >> process_supervisor_add >> trigger_dag_run_ge_poland_child_add_foreign_supervisor_50 >>\
            trigger_dag_run_ge_poland_child_add_supervisor_54 >> format_logs

        format_logs >> render_logs_csv >> upload_log_to_sftp >> get_final_error_exception_count >> generate_downloadlink_logs >>\
            send_import_complete_email >> finish >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

        #     ge_supervisor_assignment_table_search_entries_41 >> \
        #     if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42
        # if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42 >> rail.Label('Yes') >> declare_list_dag_runs_43 >> \
        #     log_getalltheuniqsupervisor_43 >> trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049 >> \
        #     wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049 >> \
        #     trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53
        # if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42 >> rail.Label('No') >> \
        #     trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53 >> \
        #     wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53 >> \
        #     log_merge_54 >> create_csv_lines_55 >>\
        #     generate_downloadable_link >> upload_logs_57 >> get_logged_errors_58 >> get_logged_exception_59 >> email_subject_line_60 >> \
        #     send_log_mail_61 >> finish

    return dag


rail.for_each_instance(create_dag)
