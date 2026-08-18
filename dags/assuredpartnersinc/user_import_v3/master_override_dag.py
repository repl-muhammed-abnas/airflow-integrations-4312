from datetime import timedelta
from pendulum import now
import itertools
import rail
from assuredpartnersinc.user_import_v3.utils import request_payload, python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_override_dag_id,
        description=f'Assured Partners User Import Master Override {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_override_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='if_name_contains_changefile',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        if_name_contains_changefile = rail.IfOperator(
            task_id='if_name_contains_changefile',
            test=lambda: 'changefile' in rail.result("new_file_sensor"),
            yes_task="list_archive_files",
            no_task="if_name_downcase_ends_with_csv",
        )

        def get_file_name_to_pick_from_archive(archive_file_list, archive_filepath):
            override_changefile = rail.render_template(
                '{{result("new_file_sensor")| file_name}}')
            matching_file_name = rail.find_first_by_attr_and_get_attr(
                archive_file_list, 'name', override_changefile.split("_", 1)[-1], 'name', '')
            if not (matching_file_name):
                return null
            return archive_filepath + "/" + matching_file_name

        list_archive_files = rail.SFTPListFilesOperator(
            task_id='list_archive_files',
            paths=[config.sftp_archive_filepath]
        )

        required_file_path_for_matching_file_name_in_archive = rail.PythonOperator(
            task_id='required_file_path_for_matching_file_name_in_archive',
            python_callable=lambda: get_file_name_to_pick_from_archive(rail.result(
                'list_archive_files')[config.sftp_archive_filepath], config.sftp_archive_filepath)
        )

        if_matching_file_not_found_in_archive = rail.IfOperator(
            task_id='if_matching_file_not_found_in_archive',
            test="{{ result('required_file_path_for_matching_file_name_in_archive') | is_falsy}}",
            yes_task="fail_matching_file_not_found_in_archive",
            no_task="if_name_downcase_ends_with_csv",
        )

        fail_matching_file_not_found_in_archive = rail.FailOperator(
            task_id='fail_matching_file_not_found_in_archive',
            message="Matching file for changefile not found in archive"
        )

        if_name_downcase_ends_with_csv = rail.IfOperator(
            task_id='if_name_downcase_ends_with_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task="download_input_csv",
            no_task="send_mail_incorrect_file_format",
        )

        send_mail_incorrect_file_format = rail.EmailOperator(
            task_id='send_mail_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Incorrect file format received - '
            + '{{ current_time_in_specified_tz("' + config.time_zone + '", "%m/%d/%YT%H:%M:%S") }}',
            html_content="templates/incorrect_file_format.html"
        )

        download_input_csv = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
            remote_filepath="{{ result('required_file_path_for_matching_file_name_in_archive') or result('new_file_sensor')}}",
        )

        log_integration_run_date = rail.PythonOperator(
            task_id='log_integration_run_date',
            python_callable=lambda: now(config.time_zone).strftime(
                config.DATE_DEFAULT_FORMAT)
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.sftp_archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}''',
            existing_filename=config.sftp_input_override_filepath +
            '''/{{ result("new_file_sensor") | file_name }}''',
        )

        user_import_log = rail.CreateLogOperator(
            task_id="user_import_log"
        )

        supervisor_assignment_log = rail.CreateLogOperator(
            task_id="supervisor_assignment_log"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_input_csv') }}",
            encoding='utf-8'
        )

        create_csv_lines_input = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_input',
            source="{{ result('parse_csv') }}",
            header=[
                "EEStatus", "EmplID_Login", "FirstName", "LastName", "EEType", "JobCode", "JobTitle", "FLSAStatus", "ServiceDate",
                "TerminationDate", "Agency_Org2", "AgencyDescription", "SupervisorID", "SupervisorName", "E_Mail", "HourlyRate",
                "WeeklySTDHrs", "Schedule", "PTOSeniorityDate", "ProfitCenter", "ProfitCenterDescription", "CpnyCode", "PayGroupCode",
                "PayGroup", "PTO_1", "PTO_Bereavement", "PTO_JuryDuty", "HolidayType", "Illness", "ChangeEffectiveDate", "VTO", "EmergencySick",
                "PayRules", "TimesheetTemplate", "TimeOffTemplate", "HolidayCalendars", "TimeZone", "WorkWeek", "LocationCode_Work",
                "Dept_Org4", "Dept_Org4Desc", "CoreSupervisorID", "CoreSupervisorName", "LOASuspendPTOStart", "LOASuspendPTOEnd",
                "activity", "makeuptimepto", "punchentrypolicy", "PayrollGrouping", "PayrollPermission", "AdminPermission",
                "ConditionRestrict", "PayrollGroupingGroups", "ProfitCenterGroups", "AgencyGroups", "PayGroupGroups",
                "LocationGroups", "DepartmentGroups", "AdditionalTimeOffTypes", "RepliconTSDate",
                "DailyHours", "illnesspto", "AssignmentNumber", "sha_256"
            ],
            row=request_payload.row_data_for_input_file,
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        create_collection_from_input_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_input_csv',
            source="{{ result('create_csv_lines_input') }}",
            name="rawinput_assuredpartners",
        )

        if_input_lines_less_than_1 = rail.IfOperator(
            task_id='if_input_lines_less_than_1',
            test='''{{ result('create_collection_from_input_csv', 'length') < 1 }}''',
            yes_task="send_mail_blank_input_file",
            no_task="list_reference_file",
        )

        send_mail_blank_input_file = rail.EmailOperator(
            task_id='send_mail_blank_input_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Blank file received - '
            + '{{ current_time_in_specified_tz("' + config.time_zone + '", "%m/%d/%YT%H:%M:%S") }}',
            html_content="templates/blank_input.html",
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id='list_reference_file',
            paths=[config.sftp_reference_filepath],
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: rail.result('list_reference_file')[
                config.sftp_reference_filepath][0]['name']
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
            remote_filepath=config.sftp_reference_filepath +
            "/{{ result('get_reference_filename')}}"
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            delimiter=','
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('parse_reference_file') }}",
            name="referencefile",
        )

        query_list_changed_items_27 = rail.QueryCollectionOperator(
            task_id='query_list_changed_items_27',
            name='delta_records',
            query="""SELECT * FROM rawinput_assuredpartners WHERE  rawinput_assuredpartners.sha_256 NOT IN (SELECT DISTINCT referencefile.sha_256 FROM referencefile)""",
        )

        query_list_invalid_delta_records = rail.QueryCollectionOperator(
            task_id='query_list_invalid_delta_records',
            name='invalid_delta_records',
            query="""SELECT * FROM delta_records WHERE NULLIF(FirstName, '') IS NULL or
                    NULLIF(LastName, '') IS NULL or NULLIF(EEStatus, '') IS NULL or NULLIF(EmplID_Login, '') IS NULL or
                    NULLIF(ServiceDate, '') IS NULL or ServiceDate NOT LIKE '%/%' or (NULLIF(TerminationDate, '') IS NOT NULL and TerminationDate NOT LIKE '%/%') """
        )

        query_list_validated_delta_records = rail.QueryCollectionOperator(
            task_id='query_list_validated_delta_records',
            name='validated_delta_records',
            query="""SELECT * FROM delta_records WHERE NULLIF(FirstName, '') IS NOT NULL and
                    NULLIF(LastName, '') IS NOT NULL and NULLIF(EEStatus, '') IS NOT NULL and NULLIF(EmplID_Login, '') IS NOT NULL and
                    NULLIF(ServiceDate, '') IS NOT NULL and ServiceDate LIKE '%/%' and (NULLIF(TerminationDate, '') IS NULL or TerminationDate LIKE '%/%') """
        )

        query_list_unchangeditems_28 = rail.QueryCollectionOperator(
            task_id='query_list_unchangeditems_28',
            query="""SELECT * FROM rawinput_assuredpartners WHERE  rawinput_assuredpartners.sha_256 IN (SELECT DISTINCT referencefile.sha_256 FROM referencefile)""",
        )

        if_delta_records_less_than_1 = rail.IfOperator(
            task_id='if_delta_records_less_than_1',
            test='''{{ result('query_list_changed_items_27', 'length') < 1 }}''',
            yes_task="send_mail_no_delta",
            no_task="log_delta_list_size_to_percentage",
        )

        send_mail_no_delta = rail.EmailOperator(
            task_id='send_mail_no_delta',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - No new or changed records received - '
            + '{{ current_time_in_specified_tz("' + config.time_zone + '", "%m/%d/%YT%H:%M:%S") }}',
            html_content='''templates/no_delta.html'''
        )

        foreach_file_in_reference_folder = rail.ForEachOperator(
            task_id='foreach_file_in_reference_folder',
            items=lambda: rail.result('list_reference_file')[
                config.sftp_reference_filepath],
            start_task='if_file_name_ends_with_csv',
            end_task='foreach_file_in_reference_folder_end'
        )

        if_file_name_ends_with_csv = rail.IfOperator(
            task_id='if_file_name_ends_with_csv',
            test='''{{ result('foreach_file_in_reference_folder').name | ends_with('.csv') }}''',
            yes_task="rename_move_existing_reference_file_to_archive",
            no_task="foreach_file_in_reference_folder_end",
        )

        rename_move_existing_reference_file_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_move_existing_reference_file_to_archive',
            new_filename=config.sftp_archive_filepath +
            "/OLD_Reference{{ result('foreach_file_in_reference_folder').name }}",
            existing_filename=config.sftp_reference_filepath +
            "/{{ result('foreach_file_in_reference_folder').name }}",
        )

        foreach_file_in_reference_folder_end = rail.EmptyOperator(
            task_id='foreach_file_in_reference_folder_end',
        )

        upload_uploadnewreferencefile_34 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreferencefile_34',
            content='''{{ result('create_csv_lines_input') }}''',
            remote_filepath=config.sftp_reference_filepath +
            "/newreference_{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}",
        )

        log_delta_list_size_to_percentage = rail.PythonOperator(
            task_id='log_delta_list_size_to_percentage',
            python_callable=lambda:  (float(rail.result('query_list_changed_items_27', 'length')) / float(rail.result(
                'create_collection_from_input_csv', 'length'))) * 100
        )

        trigger_dag_run_assured_partners_child_groups_update_v3_044 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_child_groups_update_v3_044',
            retries=0,
            trigger_dag_id=config.child_groups_update_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf={
                "filename": "{{ result('new_file_sensor') | file_name }}",
                "filepath": "{{ result('new_file_sensor') }}",
                "integration_run_date": "{{ result('log_integration_run_date') }}"
            }
        )

        wait_for_dag_run_assured_partners_child_groups_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_run_assured_partners_child_groups_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_assured_partners_child_groups_update_v3_044") }}'
        )

        get_departmentdata_50 = rail.RepliconServiceOperator(
            task_id='get_departmentdata_50',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_department_data_payload,
            data_handler=python_callable.get_department_group_list
        )

        get_all_cost_centers_payroll_grouping_51 = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers_payroll_grouping_51',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_all_divisions_location_code_work_52 = rail.RepliconServiceOperator(
            task_id='get_all_divisions_location_code_work_52',
            endpoint="/services/DivisionService1.svc/GetAllDivisions"
        )

        get_all_employee_type_groups_dept_org4_desc_53 = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_groups_dept_org4_desc_53',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_locations_pay_group_code_54 = rail.RepliconServiceOperator(
            task_id='get_all_locations_pay_group_code_54',
            endpoint="/services/LocationService1.svc/GetAllLocations"
        )

        get_all_service_centers_profit_center_55 = rail.RepliconServiceOperator(
            task_id='get_all_service_centers_profit_center_55',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters"
        )

        get_all_custom_fields_get_all_custom_fields_56 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_get_all_custom_fields_56',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=python_callable.get_required_uris
        )

        get_all_office_schedules_57 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_57',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_time_zones_58 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_58',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        if_unchanged_records_greater_than_0_63 = rail.IfOperator(
            task_id='if_unchanged_records_greater_than_0_63',
            test='''{{ result('query_list_unchangeditems_28' , 'length') > 0 }}''',
            yes_task="create_csv_lines_unchanged_records_64",
            no_task="wait_for_dag_completion_variable",
        )

        create_csv_lines_unchanged_records_64 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_unchanged_records_64',
            source="{{ result('query_list_unchangeditems_28') }}",
            header=['User Name', 'Login Name',
                    'Action', 'Status', 'Details', 'JobID'],
            row=lambda item: [
                item['FirstName'] + " " + item['LastName'],
                item['EmplID_Login'],
                "No Change",
                "Skipped",
                "No change found from previous input file",
                rail.render_template("{{ dag_run_ecid() }}")
            ],
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        upload_logs_to_sftp_65 = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp_65',
            content='''{{ result('create_csv_lines_unchanged_records_64') }}''',
            remote_filepath=config.sftp_logs_filepath +
            '''/logs_{{ current_time_in_specified_tz("US/Pacific", "%m_%d_%YT%H_%M_%S") }}_{{ result('new_file_sensor') | file_name }}''',
        )

        wait_for_dag_completion_variable = rail.SetVariableOperator(
            task_id="wait_for_dag_completion_variable",
            name="dag_runs_to_wait_for",
            append=False,
            value=[]
        )

        if_query_invalid_delta_records_greater_than_0 = rail.IfOperator(
            task_id='if_query_invalid_delta_records_greater_than_0',
            test='''{{ result('query_list_invalid_delta_records', 'length') > 0 }}''',
            yes_task="validation_log_entry",
            no_task="if_query_validated_delta_records_greater_than_0_67",
        )

        validation_log_entry = rail.WriteLogOperator(
            task_id='validation_log_entry',
            items="{{ result('query_list_invalid_delta_records') }}",
            log="{{ result('user_import_log')}}",
            message="na",
            severity="Exception",
            properties=lambda item: {
                "action": "",
                "status": "Exception",
                "job_id": rail.render_template("{{dag_run_ecid()}}"),
                "details": python_callable.input_validation_logs(item),
                "username": item['FirstName'] if item['FirstName'] else "" + item['LastName'] if item['LastName'] else "",
                "loginname": item['EmplID_Login']
            }
        )

        if_query_validated_delta_records_greater_than_0_67 = rail.IfOperator(
            task_id='if_query_validated_delta_records_greater_than_0_67',
            test='''{{ result('query_list_validated_delta_records', 'length') > 0 }}''',
            yes_task="process_each_user_dummy",
            no_task="foreach_dir_user_importreferencefolder_19_96",
        )

        process_each_user_dummy = rail.EmptyOperator(
            task_id='process_each_user_dummy'
        )

        process_each_user_parallel_dagrun = rail.trigger_parallel_dagrun(
            task_id='process_each_user_parallel_dagrun',
            items="{{ result('query_list_validated_delta_records') }}",
            trigger_dag_id=config.process_each_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.process_each_user_trigger_parallel_count_master_override,
            conf=lambda item: request_payload.process_each_user_payload(item, rail.result(
                "get_all_custom_fields_get_all_custom_fields_56"))
        )

        get_process_each_user_dag_ids = rail.PythonOperator(
            task_id='get_process_each_user_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_user_parallel_dagrun_{x+1}'), range(config.process_each_user_trigger_parallel_count_master_override))))),
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

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.do_format_logs,
            show_return_value_in_logs=False
        )

        filter_assured_partners_supervisor_assignment_table_entries = rail.FilterLogEntriesOperator(
            task_id='filter_assured_partners_supervisor_assignment_table_entries',
            log="{{result('supervisor_assignment_log')}}",
            severity="queued",
            remove_filtered_entries=True
        )

        if_entries_in_supervisor_assignment_logs = rail.IfOperator(
            task_id='if_entries_in_supervisor_assignment_logs',
            test=lambda: rail.result(
                'filter_assured_partners_supervisor_assignment_table_entries', 'length') > 0,
            yes_task='create_supervisor_user_temp_logs',
            no_task='foreach_dir_user_importreferencefolder_19_96'
        )

        create_supervisor_user_temp_logs = rail.CreateLogOperator(
            task_id='create_supervisor_user_temp_logs'
        )

        trigger_dag_run_assured_partners_child_add_supervisor_86 = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_assured_partners_child_add_supervisor_86',
            items="{{ result('filter_assured_partners_supervisor_assignment_table_entries') }}",
            trigger_dag_id=config.child_add_supervisor_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.child_add_supervisor_trigger_parallel_count,
            conf=lambda item: {
                "parentjobid": item['properties']['job_id'],
                "useruri": item["properties"]["useruri"],
                "loginname": item["properties"]["username"],
                "supervisorloginname": item["properties"]["supervisorloginname"],
                "childjobid": item["properties"]["childjobid"],
                "action": item["properties"]["action"],
                "supervisoreffectivedate": item["properties"]["supervisoreffectivedate"],
                "supervisorusername": item["properties"]["supervisorusername"],
                "supervisor_assignment_log": rail.result('supervisor_assignment_log'),
                "user_temp_log": rail.result('create_supervisor_user_temp_logs'),
                "integration_run_date": rail.result('log_integration_run_date')
            }
        )

        update_user_logs_after_supervisor_assignment = rail.PythonOperator(
            task_id='update_user_logs_after_supervisor_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.update_user_log
        )

        foreach_dir_user_importreferencefolder_19_96 = rail.ForEachOperator(
            task_id='foreach_dir_user_importreferencefolder_19_96',
            items=lambda: rail.result('list_reference_file')[
                config.sftp_reference_filepath],
            start_task='if_foreach_dir_user_importreferencefolder_19_96_name_ends_with_csv_97',
            end_task='foreach_dir_user_importreferencefolder_19_96_end'
        )

        if_foreach_dir_user_importreferencefolder_19_96_name_ends_with_csv_97 = rail.IfOperator(
            task_id='if_foreach_dir_user_importreferencefolder_19_96_name_ends_with_csv_97',
            test='''{{ result('foreach_dir_user_importreferencefolder_19_96').name | ends_with('.csv') }}''',
            yes_task="rename_moveexistingreferencefileto_archive_98",
            no_task="foreach_dir_user_importreferencefolder_19_96_end",
        )

        rename_moveexistingreferencefileto_archive_98 = rail.SFTPMoveFileOperator(
            task_id='rename_moveexistingreferencefileto_archive_98',
            new_filename=config.sftp_archive_filepath +
            "/OLD_Reference_{{ result('foreach_dir_user_importreferencefolder_19_96').name }}",
            existing_filename=config.sftp_reference_filepath +
            "/{{ result('foreach_dir_user_importreferencefolder_19_96').name }}",
        )

        foreach_dir_user_importreferencefolder_19_96_end = rail.EmptyOperator(
            task_id='foreach_dir_user_importreferencefolder_19_96_end',
        )

        upload_uploadnewreferencefile_99 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreferencefile_99',
            content='''{{ result('create_csv_lines_input') }}''',
            remote_filepath=config.sftp_reference_filepath +
            "/newreference_{{dag_run_ecid()}}_{{result('new_file_sensor') | file_name}}",
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('update_user_logs_after_supervisor_assignment') if rail.result(
                'update_user_logs_after_supervisor_assignment') else rail.result('format_logs'),
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
                '{{ item.loginname }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.jobid }}'
            ],
            footer=lambda: [
                'Number of records found:' +
                    str(rail.result(
                        "update_user_logs_after_supervisor_assignment", "total_record_count")),
                'Number of records processed:' + str(int(rail.result(
                    "update_user_logs_after_supervisor_assignment", "exception_record_count")) + int(rail.result(
                        "format_logs", "error_record_count")) + int(rail.result(
                            "update_user_logs_after_supervisor_assignment", "success_record_count"))),
                'Number of success records: ' +
                    str(rail.result(
                        "update_user_logs_after_supervisor_assignment", "success_record_count")),
                'Number of error records: ' +
                    str(rail.result(
                        "update_user_logs_after_supervisor_assignment", "error_record_count")),
                'Number of exception records: ' +
                    str(rail.result(
                        "update_user_logs_after_supervisor_assignment", "exception_record_count")),
            ] if rail.result('update_user_logs_after_supervisor_assignment') else [
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
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_logs_filepath +
            '''/logs_{{ current_time_in_specified_tz("US/Pacific", "%m_%d_%YT%H_%M_%S") }}_{{ result('new_file_sensor') | file_name }}''',
        )

        get_final_error_exception_count = rail.PythonOperator(
            task_id='get_final_error_exception_count',
            python_callable=lambda: {
                'final_error_record_count': int(rail.result("update_user_logs_after_supervisor_assignment", key="error_record_count")) if rail.result(
                    "update_user_logs_after_supervisor_assignment") else int(rail.result("format_logs", key="error_record_count")),
                'final_exception_record_count':  int(rail.result("update_user_logs_after_supervisor_assignment", key="exception_record_count")) if rail.result(
                    "update_user_logs_after_supervisor_assignment") else int(rail.result("format_logs", key="exception_record_count"))
            }
        )

        generate_downloadlink_logs = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink_logs',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name="logs_{{ current_time_in_specified_tz('US/Pacific', '%m_%d_%YT%H_%M_%S') }}_{{ result('new_file_sensor') | file_name }}.csv",
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
            subject='{{ get_company_key() }} | User import {{""}} \
                {%- if result("get_final_error_exception_count").final_error_record_count > 0 -%} \
                    completed with errors  \
                {%- elif result("get_final_error_exception_count").final_exception_record_count > 0 -%} \
                    completed with exceptions  \
                {%- else -%} \
                    completed successfully \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("' + config.time_zone + '", "%m/%d/%YT%H:%M:%S") }}',
            html_content="/templates/import_complete.html",
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

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label(
            "No") >> delete_this_dagrun
        was_new_file_found >> rail.Label(
            "Yes") >> if_name_contains_changefile

        if_name_contains_changefile >> rail.Label(
            "No") >> if_name_downcase_ends_with_csv
        if_name_contains_changefile >> rail.Label(
            "Yes") >> list_archive_files >> required_file_path_for_matching_file_name_in_archive >> if_matching_file_not_found_in_archive

        if_matching_file_not_found_in_archive >> rail.Label(
            "No") >> if_name_downcase_ends_with_csv
        if_matching_file_not_found_in_archive >> rail.Label(
            "Yes") >> fail_matching_file_not_found_in_archive

        if_name_downcase_ends_with_csv >> rail.Label(
            'No') >> send_mail_incorrect_file_format >> finish
        if_name_downcase_ends_with_csv >> rail.Label(
            'Yes') >> download_input_csv

        download_input_csv >> log_integration_run_date >> archive_file >> user_import_log >> supervisor_assignment_log >> parse_csv \
            >> create_csv_lines_input >> create_collection_from_input_csv >> if_input_lines_less_than_1

        if_input_lines_less_than_1 >> rail.Label(
            'No') >> send_mail_blank_input_file >> finish
        if_input_lines_less_than_1 >> rail.Label(
            'Yes') >> list_reference_file >> get_reference_filename >> if_file_not_present_or_doesnt_end_with_csv

        if_file_not_present_or_doesnt_end_with_csv >> rail.Label(
            'No') >> download_reference_file
        if_file_not_present_or_doesnt_end_with_csv >> rail.Label(
            'Yes') >> fail_with_reference_file_missing >> finish

        download_reference_file >> parse_reference_file >> create_referencefile_collection \
            >> query_list_changed_items_27 >> query_list_invalid_delta_records >> query_list_validated_delta_records \
            >> query_list_unchangeditems_28 >> if_delta_records_less_than_1

        if_delta_records_less_than_1 >> rail.Label(
            'No') >> log_delta_list_size_to_percentage >> trigger_dag_run_assured_partners_child_groups_update_v3_044
        if_delta_records_less_than_1 >> rail.Label(
            'Yes') >> send_mail_no_delta >> foreach_file_in_reference_folder >> if_file_name_ends_with_csv

        if_file_name_ends_with_csv >> rail.Label(
            'No') >> foreach_file_in_reference_folder_end
        if_file_name_ends_with_csv >> rail.Label(
            'Yes') >> rename_move_existing_reference_file_to_archive >> foreach_file_in_reference_folder_end

        foreach_file_in_reference_folder >> foreach_file_in_reference_folder_end >> upload_uploadnewreferencefile_34 >> finish

        trigger_dag_run_assured_partners_child_groups_update_v3_044 >> wait_for_dag_run_assured_partners_child_groups_update \
            >> get_departmentdata_50

        get_departmentdata_50 >> get_all_cost_centers_payroll_grouping_51 >> get_all_divisions_location_code_work_52 \
            >> get_all_employee_type_groups_dept_org4_desc_53 >> get_all_locations_pay_group_code_54
        get_all_locations_pay_group_code_54 >> get_all_service_centers_profit_center_55 >> get_all_custom_fields_get_all_custom_fields_56 \
            >> get_all_office_schedules_57 >> get_all_time_zones_58 >> if_unchanged_records_greater_than_0_63

        if_unchanged_records_greater_than_0_63 >> rail.Label(
            'No') >> wait_for_dag_completion_variable
        if_unchanged_records_greater_than_0_63 >> rail.Label('Yes') >> create_csv_lines_unchanged_records_64 \
            >> upload_logs_to_sftp_65 >> wait_for_dag_completion_variable

        wait_for_dag_completion_variable >> if_query_invalid_delta_records_greater_than_0

        if_query_invalid_delta_records_greater_than_0 >> rail.Label(
            'Yes') >> validation_log_entry
        if_query_invalid_delta_records_greater_than_0 >> rail.Label(
            'No') >> if_query_validated_delta_records_greater_than_0_67

        validation_log_entry >> if_query_validated_delta_records_greater_than_0_67

        if_query_validated_delta_records_greater_than_0_67 >> rail.Label(
            'No') >> foreach_dir_user_importreferencefolder_19_96
        if_query_validated_delta_records_greater_than_0_67 >> rail.Label(
            'Yes') >> process_each_user_dummy >> process_each_user_parallel_dagrun >> get_process_each_user_dag_ids >> gather_user_logs \
            >> format_logs >> filter_assured_partners_supervisor_assignment_table_entries

        filter_assured_partners_supervisor_assignment_table_entries >> if_entries_in_supervisor_assignment_logs

        if_entries_in_supervisor_assignment_logs >> rail.Label(
            'Yes') >> create_supervisor_user_temp_logs >> trigger_dag_run_assured_partners_child_add_supervisor_86 \
            >> update_user_logs_after_supervisor_assignment >> foreach_dir_user_importreferencefolder_19_96
        if_entries_in_supervisor_assignment_logs >> rail.Label(
            'No') >> foreach_dir_user_importreferencefolder_19_96

        foreach_dir_user_importreferencefolder_19_96 >> if_foreach_dir_user_importreferencefolder_19_96_name_ends_with_csv_97

        if_foreach_dir_user_importreferencefolder_19_96_name_ends_with_csv_97 >> rail.Label(
            'No') >> foreach_dir_user_importreferencefolder_19_96_end
        if_foreach_dir_user_importreferencefolder_19_96_name_ends_with_csv_97 >> rail.Label('Yes') >> rename_moveexistingreferencefileto_archive_98 \
            >> foreach_dir_user_importreferencefolder_19_96_end

        foreach_dir_user_importreferencefolder_19_96 >> foreach_dir_user_importreferencefolder_19_96_end >> upload_uploadnewreferencefile_99 \
            >> render_logs_csv >> upload_log_to_sftp >> get_final_error_exception_count >> generate_downloadlink_logs \
            >> send_import_complete_email >> finish >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_dag)
