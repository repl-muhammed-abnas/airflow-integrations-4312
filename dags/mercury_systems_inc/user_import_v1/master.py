from datetime import timedelta
from pendulum import now
import rail
from mercury_systems_inc.user_import_v1.utils import request_payload, custom_methods
from mercury_systems_inc.user_import_v1.task_groups.pre_requisites import pre_requisites_task_group

null = None


def create_main_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'MercurySystemsInc User Import Master',
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
            path=config.sftp_input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='if_name_downcase_ends_with_csv',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

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
            subject='{{ get_company_key() }} | Replicon User Import from ADP HRIS - Incorrect file format received - '
            + '{{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/emails/incorrect_file_format.html"
        )

        archive_file_incorrect_file_format = rail.SFTPMoveFileOperator(
            task_id='archive_file_incorrect_file_format',
            new_filename=config.sftp_archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}''',
            existing_filename=config.sftp_input_filepath +
            '''/{{ result("new_file_sensor") | file_name }}''',
        )

        download_input_csv = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        log_integration_run_date = rail.PythonOperator(
            task_id='log_integration_run_date',
            python_callable=lambda: now(config.time_zone).strftime(
                config.DATE_FORMAT)
        )

        log_job_start_time = rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime("%Y-%m-%dT%H:%M:%S%z")
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.sftp_archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}''',
            existing_filename=config.sftp_input_filepath +
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
                'Employee_ID', 'First_Name', 'Last_Name', 'Preferred_Name', 'Email', 'Authentication_ID', 'Login_Name', 'Hire_Date',
                'Termination_Date', 'Pay_Group', 'Operating_Unit', 'Business_Unit', 'Chargeable_Flag', 'Job_Union_ID', 'Location_Class_Description',
                'Supervisor_ADP_Person_ID', 'Hourly_Cost', 'Pay_Type', 'Emp_Status', 'Work_Schedule', 'Flexible_Vacation_Eligible',
                'Expected_Time_Zone', 'VMS_ID', 'File_ID', 'Job_Code', 'Department', 'Full_Part_Time', 'FLSA',
                'Job_Function', 'Job_Family', 'Work_Location_State', 'Work_Location_Country', 'Work_Location_Name', 'Work_Location_Code',
                'Work_Schedule_Start_Time', 'Effective_Date', 'Employee_Classification', 'Job_Title', 'Manager_Type',
                'Punch_Entry_Policy', 'Timesheet_Template', 'Timesheet_Approval_Path', 'Timesheet_Period', 'Time_Off_Template',
                'Time_Off_Types', 'Holiday_Calendar', 'Pay_Rule', 'Office_Schedule', 'Work_Week', 'Permissions'
            ],
            row=request_payload.row_data_for_input_file,
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        create_collection_from_input_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_input_csv',
            source="{{ result('create_csv_lines_input') }}",
            name="rawinput_mercurysysinc_adp",
        )

        if_input_lines_less_than_1 = rail.IfOperator(
            task_id='if_input_lines_less_than_1',
            test='''{{ result('create_collection_from_input_csv', 'length') < 1 }}''',
            yes_task="send_mail_blank_input_file",
            no_task="query_invalid_records",
        )

        send_mail_blank_input_file = rail.EmailOperator(
            task_id='send_mail_blank_input_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Import from ADP HRIS - Blank file received - '
            + '{{ current_time_in_specified_tz("' + config.time_zone + '") }}',
            html_content="templates/emails/blank_input.html",
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name="invalid_records",
            query=f"""SELECT * FROM rawinput_mercurysysinc_adp WHERE NULLIF(Employee_ID, '') IS NULL
                or  NULLIF(First_Name, '') IS NULL or NULLIF(Last_Name, '') IS NULL
                or NULLIF(Email, '') IS NULL or NULLIF(Hire_Date, '') IS NULL or (Hire_Date == 'Invalid Date Format')
                or NULLIF(Pay_Group, '') IS NULL or NULLIF(Operating_Unit, '') IS NULL
                or NULLIF(Business_Unit, '') IS NULL or NULLIF(Chargeable_Flag, '') IS NULL
                or NULLIF(Job_Union_ID, '') IS NULL or NULLIF(Location_Class_Description, '') IS NULL
                or NULLIF(Employee_Classification, '') IS NULL or NULLIF(Supervisor_ADP_Person_ID, '') IS NULL
                or NULLIF(Pay_Type, '') IS NULL or NULLIF(Job_Function, '') IS NULL
                or NULLIF(FLSA, '') IS NULL or NULLIF(Work_Location_State, '') IS NULL
                or NULLIF(Job_Family, '') IS NULL or NULLIF(Work_Schedule, '') IS NULL
                or NULLIF(Full_Part_Time, '') IS NULL or NULLIF(Work_Location_Country, '') IS NULL
                or NULLIF(Emp_Status, '') IS NULL or NULLIF(Flexible_Vacation_Eligible, '') IS NULL
                or NULLIF(Expected_Time_Zone, '') IS NULL
                or NULLIF(Work_Location_Name, '') IS NULL or NULLIF(Job_Code, '') IS NULL
                or NULLIF(Department, '') IS NULL or  NULLIF(Work_Location_Code, '') IS NULL
                or NULLIF(Effective_Date, '') IS NULL or (Effective_Date == 'Invalid Date Format')
                or Termination_Date == 'Invalid Date Format'
                or NULLIF(Timesheet_Template, '') IS NULL
                or NULLIF(Timesheet_Period, '') IS NULL
                or NULLIF(Timesheet_Approval_Path, '') IS NULL
                or NULLIF(Work_Week, '') IS NULL
                or NULLIF(Office_Schedule, '') IS NULL"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('user_import_log')}}",
            message=lambda item: request_payload.get_mandatory_fields_exception_message(
                item, config),
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['Employee_ID'],
                'first_name': item['First_Name'],
                'last_name': item['Last_Name'],
                'action': 'Validation',
                'status': 'Exception',
                "details": request_payload.get_mandatory_fields_exception_message(item, config),
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_records',
            query=f"""SELECT * FROM rawinput_mercurysysinc_adp WHERE NULLIF(Employee_ID, '') IS NOT NULL
                and  NULLIF(First_Name, '') IS NOT NULL and NULLIF(Last_Name, '') IS NOT NULL
                and NULLIF(Email, '') IS NOT NULL and NULLIF(Hire_Date, '') IS NOT NULL and (Hire_Date != 'Invalid Date Format')
                and NULLIF(Pay_Group, '') IS NOT NULL and NULLIF(Operating_Unit, '') IS NOT NULL
                and NULLIF(Business_Unit, '') IS NOT NULL and NULLIF(Chargeable_Flag, '') IS NOT NULL
                and NULLIF(Job_Union_ID, '') IS NOT NULL and NULLIF(Location_Class_Description, '') IS NOT NULL
                and NULLIF(Employee_Classification, '') IS NOT NULL and NULLIF(Supervisor_ADP_Person_ID, '') IS NOT NULL
                and NULLIF(Pay_Type, '') IS NOT NULL and NULLIF(Job_Function, '') IS NOT NULL
                and NULLIF(FLSA, '') IS NOT NULL and NULLIF(Work_Location_State, '') IS NOT NULL
                and NULLIF(Job_Family, '') IS NOT NULL and NULLIF(Work_Schedule, '') IS NOT NULL
                and NULLIF(Full_Part_Time, '') IS NOT NULL and NULLIF(Work_Location_Country, '') IS NOT NULL
                and NULLIF(Emp_Status, '') IS NOT NULL and NULLIF(Flexible_Vacation_Eligible, '') IS NOT NULL
                and NULLIF(Expected_Time_Zone, '') IS NOT NULL
                and NULLIF(Work_Location_Name, '') IS NOT NULL and NULLIF(Job_Code, '') IS NOT NULL
                and NULLIF(Department, '') IS NOT NULL and  NULLIF(Work_Location_Code, '') IS NOT NULL
                and NULLIF(Effective_Date, '') IS NOT NULL and (Effective_Date != 'Invalid Date Format')
                and (NULLIF(Termination_Date, '') IS NULL or Termination_Date != 'Invalid Date Format')
                and NULLIF(Timesheet_Template, '') IS NOT NULL
                and NULLIF(Timesheet_Period, '') IS NOT NULL
                and NULLIF(Timesheet_Approval_Path, '') IS NOT NULL
                and NULLIF(Work_Week, '') IS NOT NULL
                and NULLIF(Office_Schedule, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='groups_log_table',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        groups_log_table = rail.CreateLogOperator(
            task_id='groups_log_table',
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.process_groups_dagid,
            retries=0,
            conf={
                "groups_log_table": "{{ result('groups_log_table') }}",
                "integration_run_date": "{{ result('log_integration_run_date') }}",
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_process_groups",
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        dummy_get_user_import_pre_requisites, get_user_pre_requisites = pre_requisites_task_group(
            config)

        create_collection_replicon_updated_locations = rail.CreateCollectionOperator(
            task_id='create_collection_replicon_updated_locations',
            source="{{ result('get_updated_location_grps') | to_json }}",
            columns={
                'name': 'Exception_Work_Location_Name',
                'full_path_code': 'work_location_code_fullpath'
            },
            name='replicon_updated_locations'
        )

        create_collection_replicon_updated_departments = rail.CreateCollectionOperator(
            task_id='create_collection_replicon_updated_departments',
            source="{{ result('get_updated_department_grps') | to_json }}",
            columns={
                'name': 'Department_Name',
                'full_path_code': 'department_code_fullpath'
            },
            name='replicon_updated_departments'
        )

        # Below tasks will filter out users where Level 1/Level 2 location OR department code fullpath is not present in replicon
        query_valid_records_where_department_or_parent_location_l1l2_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id='query_valid_records_where_department_or_parent_location_l1l2_not_present_in_replicon',
            name='valid_records_without_groups_in_replicon',
            query='''SELECT * ,
                (Work_Location_Country || '|' || Work_Location_State) AS LOCATION_GROUP_PARENT_HIERARCHY,
                ("MRCY" || '|' || Operating_Unit || '|' || Business_Unit || '|' || Chargeable_Flag || '|' || Job_Union_ID)
                    AS DEPARTMENT_GROUP_HIERARCHY
                FROM valid_records
                WHERE (LOCATION_GROUP_PARENT_HIERARCHY NOT IN (
                    SELECT work_location_code_fullpath FROM replicon_updated_locations)
                or DEPARTMENT_GROUP_HIERARCHY NOT IN (
                    SELECT department_code_fullpath FROM replicon_updated_departments)
                )''',
        )

        log_missing_location_group_l1_l2_records = rail.WriteLogOperator(
            task_id='log_missing_location_group_l1_l2_records',
            items='{{result("query_valid_records_where_department_or_parent_location_l1l2_not_present_in_replicon")}}',
            log="{{result('user_import_log')}}",
            message='Location/Department not present in Replicon',
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['Employee_ID'],
                'first_name': item['First_Name'],
                'last_name': item['Last_Name'],
                'action': 'Validation',
                'status': 'Exception',
                "details": custom_methods.get_validation_exception_for_group(item, rail.result(
                    'get_updated_location_grps'), rail.result('get_updated_department_grps'))
            }
        )

        query_final_valid_records_where_department_and_parent_location_groups_present_in_replicon = rail.QueryCollectionOperator(
            task_id='query_final_valid_records_where_department_and_parent_location_groups_present_in_replicon',
            name='valid_records_with_groups_in_replicon',
            query='''SELECT * , 
                (Work_Location_Country || '|' || Work_Location_State) AS LOCATION_GROUP_PARENT_HIERARCHY,
                ("MRCY" || '|' || Operating_Unit || '|' || Business_Unit || '|' || Chargeable_Flag || '|' || Job_Union_ID) 
                    AS DEPARTMENT_GROUP_HIERARCHY
                FROM valid_records 
                WHERE (LOCATION_GROUP_PARENT_HIERARCHY IN (
                    SELECT work_location_code_fullpath FROM replicon_updated_locations)
                and DEPARTMENT_GROUP_HIERARCHY IN (
                    SELECT department_code_fullpath FROM replicon_updated_departments)
                )''',
        )

        query_disable_user_records = rail.QueryCollectionOperator(
            task_id="query_disable_user_records",
            name='user_records_to_disable',
            query=f"""SELECT * FROM valid_records_with_groups_in_replicon WHERE Emp_Status IN {config.DISABLE_STATUS} or 
                (NULLIF(Termination_Date, '') IS NOT NULL and 
                    date(Termination_Date) <= date(:integration_run_date))""",
            query_params={
                'integration_run_date': "{{ result('log_integration_run_date') }}",
            }
        )

        if_query_disable_user_records_blank = rail.IfOperator(
            task_id='if_query_disable_user_records_blank',
            test='{{ result("query_disable_user_records", "length") < 1 }}',
            yes_task='query_final_valid_records_for_active_users',
            no_task='dummy_process_disable_users'
        )

        dummy_process_disable_users = rail.EmptyOperator(
            task_id='dummy_process_disable_users'
        )

        process_disable_users = rail.trigger_parallel_dagrun(
            task_id='process_disable_users',
            items="{{ result('query_disable_user_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_disabled_users,
            trigger_dag_id=config.process_each_user_payload_dagid,
            conf=lambda item: {
                **item,
                "starting_balance_set_to_script_uri": rail.result('get_timeoff_policy_starting_balance_set_to_script'),
                "prevent_balance_overdraw_script_uri": rail.result('get_timeoff_policy_prevent_balance_overdraw_script'),
                'integration_run_date': rail.result('log_integration_run_date'),
                'process': 'disable',
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_final_valid_records_for_active_users = rail.QueryCollectionOperator(
            task_id="query_final_valid_records_for_active_users",
            name='active_user_records',
            query=f"""SELECT * FROM valid_records_with_groups_in_replicon WHERE Emp_Status NOT IN {config.DISABLE_STATUS} and
                (NULLIF(Termination_Date, '') IS NULL or
                    (
                        NULLIF(Termination_Date, '') IS NOT NULL and
                        date(Termination_Date) > date(:integration_run_date)
                    )
                )""",
            query_params={
                'integration_run_date': "{{ result('log_integration_run_date') }}",
            }
        )

        # Query to identify supervisors from the active user records
        query_supervisors_with_subordinates_in_feed = rail.QueryCollectionOperator(
            task_id="query_supervisors_with_subordinates_in_feed",
            name='supervisors_in_feed',
            query="""SELECT * FROM active_user_records 
                WHERE Employee_ID IN (
                    SELECT DISTINCT Supervisor_ADP_Person_ID 
                    FROM active_user_records 
                    WHERE NULLIF(Supervisor_ADP_Person_ID, '') IS NOT NULL
                )""",
        )

        # Query to get non-supervisor active users
        query_non_supervisors_or_supervisors_without_subordinates_in_feed = rail.QueryCollectionOperator(
            task_id="query_non_supervisors_or_supervisors_without_subordinates_in_feed",
            name='non_supervisors_in_feed',
            query="""SELECT * FROM active_user_records 
                WHERE Employee_ID NOT IN (
                    SELECT DISTINCT Supervisor_ADP_Person_ID 
                    FROM active_user_records 
                    WHERE NULLIF(Supervisor_ADP_Person_ID, '') IS NOT NULL
                )""",
        )

        # Check if there are supervisors to process
        check_if_supervisors_with_subordinates_in_feed_exist = rail.IfOperator(
            task_id='check_if_supervisors_with_subordinates_in_feed_exist',
            test='{{ result("query_supervisors_with_subordinates_in_feed", "length") > 0 }}',
            yes_task='dummy_process_supervisors_with_subordinates_in_feed',
            no_task='dummy_process_remaining_active_users'
        )

        dummy_process_supervisors_with_subordinates_in_feed = rail.EmptyOperator(
            task_id='dummy_process_supervisors_with_subordinates_in_feed'
        )

        # Process supervisors first
        process_supervisors_with_subordinates_in_feed = rail.trigger_parallel_dagrun(
            task_id='process_supervisors_with_subordinates_in_feed',
            items="{{ result('query_supervisors_with_subordinates_in_feed') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_active_users,
            trigger_dag_id=config.process_each_user_payload_dagid,
            conf=lambda item: request_payload.get_process_each_user_payload(
                item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        dummy_process_remaining_active_users = rail.EmptyOperator(
            task_id='dummy_process_remaining_active_users'
        )

        # Process non-supervisor users
        process_remaining_active_users = rail.trigger_parallel_dagrun(
            task_id='process_remaining_active_users',
            items="{{ result('query_non_supervisors_or_supervisors_without_subordinates_in_feed') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_active_users,
            trigger_dag_id=config.process_each_user_payload_dagid,
            conf=lambda item: request_payload.get_process_each_user_payload(
                item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: custom_methods.get_process_each_user_payload_dag_ids(
                config.trigger_parallel_dagrun_count_process_active_users, config.trigger_parallel_dagrun_count_process_disabled_users,),
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
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda: {
                'total_records': rail.result('create_collection_from_input_csv', key='length'),
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('user_import_log'),
                'input_file_name': rail.render_template("{{ result('new_file_sensor') | file_name }}"),
                'job_start_time': rail.result('log_job_start_time'),
            }
        )

        wait_for_process_log_generation = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_log_generation',
            dag_runs="{{ result('process_log_generation') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
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
            "Yes") >> if_name_downcase_ends_with_csv

        if_name_downcase_ends_with_csv >> rail.Label(
            'No') >> send_mail_incorrect_file_format >> archive_file_incorrect_file_format >> finish
        if_name_downcase_ends_with_csv >> rail.Label(
            'Yes') >> download_input_csv

        download_input_csv >> log_integration_run_date >> log_job_start_time >> archive_file >> user_import_log >> supervisor_assignment_log >> parse_csv \
            >> create_csv_lines_input >> create_collection_from_input_csv >> if_input_lines_less_than_1

        if_input_lines_less_than_1 >> rail.Label(
            'Yes') >> send_mail_blank_input_file >> finish

        if_input_lines_less_than_1 >> rail.Label(
            'No') >> query_invalid_records >> log_invalid_records >> query_valid_records

        query_valid_records >> has_valid_records

        has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> process_log_generation
        has_valid_records >> rail.Label(
            'Yes') >> groups_log_table >> process_groups >> wait_process_groups

        wait_process_groups >> dummy_get_user_import_pre_requisites

        get_user_pre_requisites >> create_collection_replicon_updated_locations >> create_collection_replicon_updated_departments >>\
            query_valid_records_where_department_or_parent_location_l1l2_not_present_in_replicon >> log_missing_location_group_l1_l2_records >>\
            query_final_valid_records_where_department_and_parent_location_groups_present_in_replicon >> query_disable_user_records

        query_disable_user_records >> if_query_disable_user_records_blank

        if_query_disable_user_records_blank >> rail.Label(
            'Yes') >> query_final_valid_records_for_active_users

        if_query_disable_user_records_blank >> rail.Label(
            'No') >> dummy_process_disable_users >> process_disable_users >> query_final_valid_records_for_active_users

        query_final_valid_records_for_active_users >> query_supervisors_with_subordinates_in_feed >> query_non_supervisors_or_supervisors_without_subordinates_in_feed \
            >> check_if_supervisors_with_subordinates_in_feed_exist

        check_if_supervisors_with_subordinates_in_feed_exist >> rail.Label('Yes') >> dummy_process_supervisors_with_subordinates_in_feed \
            >> process_supervisors_with_subordinates_in_feed >> dummy_process_remaining_active_users
        check_if_supervisors_with_subordinates_in_feed_exist >> rail.Label(
            'No') >> dummy_process_remaining_active_users

        dummy_process_remaining_active_users >> process_remaining_active_users >> get_process_users_dag_ids >> gather_user_logs >> process_log_generation

        process_log_generation >> wait_for_process_log_generation >> finish

        finish >> can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
