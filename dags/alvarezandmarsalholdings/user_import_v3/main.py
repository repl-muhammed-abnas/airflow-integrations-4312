from datetime import timedelta
import rail
from pendulum import now
from alvarezandmarsalholdings.user_import_v3.utils import request_payload, custom_methods


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_master_dagid,
        description='Alvarez and Marsal Holdings User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_user_import_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test=lambda dag_run: bool(dag_run.conf['payload']),
            yes_task="create_exception_log",
            no_task="send_blank_payload_email"
        )

        create_exception_log = rail.CreateLogOperator(
            task_id="create_exception_log"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda dag_run: dag_run.conf['payload'],
            name="input_data_collection",
            columns={
                "Employee_ID": "employee_id",
                "Workday_User_Name": "workday_user_name",
                "Preferred_First_Name": "preferred_first_name",
                "Preferred_Last_Name": "preferred_last_name",
                "Suffix": "suffix",
                "Email": "email",
                "Event_Identifier": "event_identifier",
                "Worker_Status": "login_status",
                "Worker_Status_Effective_Date": "end_date",
                "Worker_Sub_Type": "employee_type",
                "Worker_Sub_Type_Effective_Date": "employee_type_effective_date",
                "Profile_Status": "profile_status",
                "Profile_Status_Effective_Date": "profile_status_effective_date",
                "Hire_Date": "start_date",
                "Reporting_Manager": "reporting_manager",
                "Reporting_Manager_Effective_Date": "reporting_manager_effective_date",
                "Office_Country": "office_country",
                "Office_State": "office_state",
                "Office_City": "office_city",
                "Office_Location_Code": "office_location_code",
                "Office_Location_Effective_Date": "office_location_effective_date",
                "Cost_Center_Code": "cost_center_code",
                "Cost_Center_Description": "cost_center_description",
                "Cost_Center_Effective_Date": "cost_center_effective_date",
                "Work_Schedule_Calendar": "schedule_type",
                "Work_Schedule_Calendar_Effective_Date": "schedule_type_effective_date",
                "Weekly_Working_Hours": "weekly_working_hours",
                "FTE_Percentage": "fte_percentage",
                "Job_Category": "job_category",
                "Job_Category_Code": "job_category_code",
                "Job_Category_Effective_Date": "job_category_effective_date",
                "Management_Level": "management_level",
                "Management_Level_Code": "management_level_code",
                "Pay_Rate_Type": "pay_rate_type",
                "Pay_Rate_Type_Effective_Date": "pay_rate_type_effective_date",
                "Job_Exempt": "job_exempt",
                "Job_Exempt_Effective_Date": "job_exempt_effective_date",
                "Performance_Manager": "performance_manager"
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_log',
            no_task='send_blank_payload_email'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | User Import - no records in payload - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data_collection WHERE NULLIF(employee_id, '') IS NULL or
              NULLIF(workday_user_name, '') IS NULL or NULLIF(preferred_first_name, '') IS NULL or
                NULLIF(preferred_last_name, '') IS NULL or NULLIF(email, '') IS NULL or NULLIF(login_status, '') IS NULL or
                  NULLIF(employee_type, '') IS NULL or NULLIF(employee_type_effective_date, '') IS NULL or
                    NULLIF(start_date, '') IS NULL or NULLIF(office_country, '') IS NULL or NULLIF(office_location_code, '') IS NULL or
                        NULLIF(office_location_effective_date, '') IS NULL or NULLIF(cost_center_code, '') IS NULL or
                          NULLIF(cost_center_description, '') IS NULL or NULLIF(cost_center_effective_date, '') IS NULL or
                            NULLIF(job_category, '') IS NULL or NULLIF(job_category_code, '') IS NULL or
                              NULLIF(job_category_effective_date, '') IS NULL or NULLIF(pay_rate_type, '') IS NULL or
                                NULLIF(pay_rate_type_effective_date, '') IS NULL or NULLIF(job_exempt, '') IS NULL or
                                  NULLIF(job_exempt_effective_date, '') IS NULL"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": request_payload.get_mandatory_fields_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='query_valid_users',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id, * FROM input_data_collection WHERE NULLIF(employee_id, '') IS NOT NULL
              AND NULLIF(workday_user_name, '') IS NOT NULL AND NULLIF(preferred_first_name, '') IS NOT NULL AND NULLIF(preferred_last_name, '') IS NOT NULL
                AND NULLIF(email, '') IS NOT NULL AND NULLIF(login_status, '') IS NOT NULL AND NULLIF(employee_type, '') IS NOT NULL
                  AND NULLIF(employee_type_effective_date, '') IS NOT NULL AND NULLIF(start_date, '') IS NOT NULL AND NULLIF(office_country, '') IS NOT NULL
                    AND NULLIF(office_location_code, '') IS NOT NULL
                      AND NULLIF(office_location_effective_date, '') IS NOT NULL AND NULLIF(cost_center_code, '') IS NOT NULL 
                      AND NULLIF(cost_center_description, '') IS NOT NULL AND NULLIF(cost_center_effective_date, '') IS NOT NULL
                        AND NULLIF(job_category, '') IS NOT NULL AND NULLIF(job_category_code, '') IS NOT NULL AND NULLIF(job_category_effective_date, '') IS NOT NULL
                          AND NULLIF(pay_rate_type, '') IS NOT NULL AND NULLIF(pay_rate_type_effective_date, '') IS NOT NULL AND NULLIF(job_exempt, '') IS NOT NULL
                            AND NULLIF(job_exempt_effective_date, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='create_custom_schedule',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        def generate_schedule():
            unique_schedule = set()
            unique_schedule_types = set()
            for each_record in rail.load_all_records(rail.result('query_valid_records')):
                if not each_record['schedule_type'] and each_record['weekly_working_hours']:
                    day_hour = round(
                        float(each_record['weekly_working_hours'])/5, 2)
                    unique_schedule.add("0.00|" +
                                        "|".join([str(day_hour) for _ in range(5)])+"|0.00")
                else:
                    unique_schedule_types.add(each_record['schedule_type'])
            return [{"schedule_type": schedule} for schedule in unique_schedule.union(unique_schedule_types)]

        create_custom_schedule = rail.PythonOperator(
            task_id='create_custom_schedule',
            python_callable=generate_schedule,
        )

        custom_schedule_collection = rail.CreateCollectionOperator(
            task_id='custom_schedule_collection',
            name='custom_schedule_collection',
            source="{{result('create_custom_schedule') | to_json}}",
            columns=[
                "schedule_type"
            ]

        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id='get_all_office_schedule',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        create_office_schedule_collection = rail.CreateCollectionOperator(
            task_id="create_office_schedule_collection",
            name="replicon_office_schedule",
            source="{{ result('get_all_office_schedule') | to_json }}"
        )

        query_new_schedules = rail.QueryCollectionOperator(
            task_id='query_new_schedules',
            query='''SELECT * FROM custom_schedule_collection
                    WHERE schedule_type NOT IN
                    (SELECT DISTINCT Displaytext FROM replicon_office_schedule)'''
        )

        process_new_schedules = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_schedules',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_new_schedules'),
            trigger_dag_id=config.schedule_add_dag_id,
            conf={
                'scheduletype': '{{ item.schedule_type }}',
            }
        )

        wait_for_process_new_schedules = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_schedules',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_schedules") }}',
        )

        get_updated_schedules = rail.RepliconServiceOperator(
            task_id='get_updated_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response: custom_methods.data_handler_for_cost_centers(
                response)
        )

        get_cost_centers_to_be_processed = rail.PythonOperator(
            task_id='get_cost_centers_to_be_processed',
            python_callable=custom_methods.get_cost_centers_to_be_created,
        )

        trigger_process_cost_centers = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_cost_centers',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('get_cost_centers_to_be_processed'),
            trigger_dag_id=config.process_cost_centers_dagid,
            conf=lambda item: {
                'type': item['type'],
                'name': item['name'],
                'code': item['code'],
                'updatedname': item.get('updatedname', None),
            }
        )
        wait_for_process_cost_centers = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_cost_centers',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_process_cost_centers") }}',
        )

        get_all_updated_cost_centers = rail.RepliconServiceOperator(
            task_id='get_all_updated_cost_centers',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response: custom_methods.data_handler_for_cost_centers(
                response)
        )

        get_all_enabled_location_groups = rail.RepliconServiceOperator(
            task_id='get_all_enabled_location_groups',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartments'
        )

        get_management_level_oef_uri = rail.RepliconServiceOperator(
            task_id='get_management_level_oef_uri',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:user"},
            data_handler=custom_methods.get_event_management_level_oef_uri
        )

        get_management_level_oef_values = rail.RepliconServiceOperator(
            task_id='get_management_level_oef_values',
            endpoint='/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch',
            data=lambda: {
                "page": "1",
                "pageSize": "10000",
                "objectExtensionTagDefinitionUri": rail.result('get_management_level_oef_uri'),
                "textSearch": None
            }
        )

        def get_create_management_level_code_and_name():
            management_level_list = []
            seen = set()

            existing_management_levels = [obj['displayText'].lower(
            ) for obj in rail.result('get_management_level_oef_values')]

            for each_record in rail.load_all_records(rail.result('query_valid_records')):
                if each_record['management_level'] and each_record['management_level_code']:
                    if each_record['management_level'].lower() not in existing_management_levels:
                        unique_key = (
                            each_record['management_level'], each_record['management_level_code'])
                        if unique_key not in seen:
                            seen.add(unique_key)
                            management_level_list.append({
                                "management_level": each_record['management_level'],
                                "management_level_code": each_record['management_level_code']
                            })

            return management_level_list

        create_management_level_code_and_name = rail.PythonOperator(
            task_id='create_management_level_code_and_name',
            python_callable=get_create_management_level_code_and_name,
        )

        process_new_management_level = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_management_level',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('create_management_level_code_and_name'),
            trigger_dag_id=config.update_oef_dropdown_dag_id,
            conf={
                'oefuri': "{{result('get_management_level_oef_uri')}}",
                'name': '{{ item.management_level }}',
                'code': '{{ item.management_level_code }}'
            }
        )

        wait_for_process_new_management_level = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_management_level',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_management_level") }}',
        )

        process_active_users = rail.trigger_parallel_dagrun(
            task_id='process_active_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf=lambda item: request_payload.get_process_users_conf(
                item, config.BATCH_COUNT),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: custom_methods.get_process_users_dag_ids(
                config.trigger_parallel_dagrun_count_process_users),
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

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('create_supervisor_log') }}",
            severity='Pending',
            remove_filtered_entries=True
        )

        is_supervisorcheck_queued_logs = rail.IfOperator(
            task_id='is_supervisorcheck_queued_logs',
            test="{{ result('get_supervisorcheck_queued_logs', 'length') > 0 }}",
            yes_task='process_supervisor_child_dag',
            no_task='process_log_generation'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.assign_supervisor_dag_id,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_log': rail.result('create_supervisor_log'),
                'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),
                                                                                  'displayText', config.GENERAL_MAPPER["supervisor_permission"], 'uri'),
                'report_user_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),
                                                                                   'displayText', config.GENERAL_MAPPER["end_user_with_report_permission"], 'uri'),
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dag_id,
            conf=lambda dag_run: {
                'total_records': rail.result('create_input_data_collection', key='length'),
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                'supervisorlogs': rail.result('create_supervisor_log'),
                'log_filename': "Log_"+rail.render_template('{{ get_company_key() }}') + "_user_import" +
                    now().strftime("%Y%m%dT%H%M%S") + ".csv"
            }
        )

        is_data_available >> rail.Label(
            'Yes') >> create_exception_log >> create_input_data_collection >> has_input_data
        is_data_available >> rail.Label(
            'No') >> send_blank_payload_email
        has_input_data >> rail.Label('Yes') >> create_log >> create_supervisor_log >>\
            query_invalid_records >> log_invalid_records >> query_valid_records >> has_valid_records
        has_valid_records >> rail.Label(
            'Yes') >> create_custom_schedule >> custom_schedule_collection >>\
            get_all_office_schedule >> create_office_schedule_collection >>\
            query_new_schedules >> process_new_schedules >> wait_for_process_new_schedules >>\
            get_updated_schedules >> get_all_permission_sets >> get_all_timeoff_types >> get_all_cost_centers >>\
            get_cost_centers_to_be_processed >> trigger_process_cost_centers >> wait_for_process_cost_centers >>\
            get_all_updated_cost_centers >>\
            get_all_enabled_location_groups >> get_management_level_oef_uri >> get_management_level_oef_values >> create_management_level_code_and_name >>\
            process_new_management_level >> wait_for_process_new_management_level >>\
            process_active_users >> get_process_users_dag_ids >> gather_user_logs >>\
            get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs
        is_supervisorcheck_queued_logs >> rail.Label(
            'Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_log_generation
        is_supervisorcheck_queued_logs >> rail.Label(
            'No') >> process_log_generation
        has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> process_log_generation
        has_input_data >> rail.Label('No') >> send_blank_payload_email

    return dag


rail.for_each_instance(create_child_dag)
