from datetime import timedelta
import rail

from crl.user_import_usa_v6.utils import request_payload, python_callable_methods
from crl.user_import_usa_v6.tasks.get_user_prereqs import get_user_prereqs_task_group

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_import_payload_dagid,
        description='CRL - User Import USA',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_user_import_payload,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda dag_run: dag_run.conf['payload'],
            name="input_data",
            columns={
                "Empl_ID": "emp_id",
                "First_Name": "first_name",
                "Last_Name": "last_name",
                "Work_Email": "email",
                "User_Name": "login_name",
                "Empl_Status": "emp_status",
                "Is_Contingent": "is_contingent",
                "Title": "title",
                "Bus_Seg_Unit": "buisness_unit_full_path",
                "Bus_Unit_Label": "buisness_unit_label",
                "Functional_Segment": "functional_segment",
                "Company": "company_code",
                "Location": "location_full_path",
                "Reg_Temp": "reg_temp",
                "Full_Part": "full_part",
                "Std_Hours": "std_hrs",
                "Supv_Empl_ID": "sup_emp_id",
                "Hire_Date": "start_date",
                "Adj_Hire_Date": "adjusted_hire_date",
                "is_HRBP": "is_hrbp",
                "Job_Code": "job_code",
                "Pay_Group": "pay_grp",
                "Pay_Type": "pay_type",
                "US_FLSA_Status": "us_flsa_status",
                "Cost_Center_Business_Area": "cost_center_full_path",
                "Cost_Center_Label": "cost_center_label",
                "Profit_Center": "profit_center",
                "Activity_Type": "activity_type",
                "Last_Worked_Day": "end_date",
                "Vacation_Exception": "us_vacation_exception",
                "US_Veterans_Status": "us_veterans_status",
                "SAP_Work_Schedule":"work_schedule",
                "Remote_Worker": "remote_worker",
                "Change_Effective_Date": "change_effective_date",
                "Event": "event",
                "Event_Reason_Code":"event_reason_code",
                "department":"department_name",
                "name":"department_code",
                "holidayCalendarCode":"holiday_calendar",
                "Home_Location": "home_location_full_path"
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
            subject='{{ get_company_key() }} | User Import USA - no records in payload - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data WHERE NULLIF(emp_id, '') IS NULL or
                    NULLIF(first_name, '') IS NULL or NULLIF(last_name, '') IS NULL
                    or NULLIF(email, '') IS NULL or NULLIF(login_name, '') IS NULL
                    or NULLIF(emp_status, '') IS NULL or NULLIF(buisness_unit_full_path, '') IS NULL or NULLIF(company_code, '') IS NULL
                    or NULLIF(location_full_path, '') IS NULL or (is_contingent ='N' and NULLIF(reg_temp, '') IS NULL)
                    or (is_contingent ='N' and NULLIF(full_part, '') IS NULL) or NULLIF(start_date, '') IS NULL
                    or (is_contingent ='N' and NULLIF(adjusted_hire_date, '') IS NULL) or ( is_contingent ='N' and NULLIF(job_code, '') IS NULL)
                    or (is_contingent ='N' and NULLIF(pay_type, '') IS NULL) or NULLIF(cost_center_full_path, '') IS NULL
                    or emp_status NOT IN ('Active','Unpaid Leave','Terminated','Suspended','Retired','Paid Leave','Furlough','Dormant','Discarted','Deceased')
                    or (is_contingent ='N' and pay_type NOT IN ('Hourly','Salaried','Exception Hourly')) or
                    (remote_worker="Y" and NULLIF(home_location_full_path, '') IS NULL )"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['emp_id'],
                'first_name': item['first_name'],
                'last_name': item['last_name'],
                'action':'Validation',
                'status': 'Exception',
                "details": request_payload.get_mandatory_fields_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_record',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM input_data WHERE NULLIF(emp_id, '') IS NOT NULL
                    and NULLIF(first_name, '') IS NOT NULL and NULLIF(last_name, '') IS NOT NULL and NULLIF(email, '') IS NOT NULL
                    and NULLIF(login_name, '') IS NOT NULL and NULLIF(emp_status, '') IS NOT NULL and NULLIF(buisness_unit_full_path, '') IS NOT NULL
                    and NULLIF(company_code, '') IS NOT NULL and NULLIF(location_full_path, '') IS NOT NULL
                    and (is_contingent ='Y' or NULLIF(reg_temp, '') IS NOT NULL) and (is_contingent ='Y' or NULLIF(full_part, '') IS NOT NULL)
                    and NULLIF(start_date, '') IS NOT NULL and (is_contingent ='Y' or NULLIF(adjusted_hire_date, '') IS NOT NULL)
                    and (is_contingent ='Y' or NULLIF(job_code, '') IS NOT NULL)
                    and (is_contingent ='Y' or NULLIF(pay_type, '') IS NOT NULL) and NULLIF(cost_center_full_path, '') IS NOT NULL
                    and emp_status IN ('Active','Unpaid Leave','Terminated','Suspended','Retired','Paid Leave','Furlough','Dormant','Discarted','Deceased') and
                    (is_contingent ='Y' or pay_type IN ('Hourly','Salaried','Exception Hourly')) and
                    ((remote_worker='Y' and NULLIF(home_location_full_path, '') IS NOT NULL) or remote_worker='N' or NULLIF(remote_worker, '') IS NULL)"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='process_groups',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.process_groups_dagid,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_process_groups",
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        dummy_get_user_prereqs, get_user_prereqs= get_user_prereqs_task_group(config)

        query_disable_user_records = rail.QueryCollectionOperator(
            task_id="query_disable_user_records",
            name='inactive_user_records',
            query=f"""SELECT * FROM valid_record WHERE emp_status IN {tuple(config.DISABLE_STATUS)}"""
        )

        dummy_process_disable_users = rail.EmptyOperator(
            task_id='dummy_process_disable_users'
        )

        process_disable_users = rail.trigger_parallel_dagrun(
            task_id='process_disable_users',
            items="{{ result('query_disable_user_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf= lambda item: request_payload.get_process_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_active_user_records = rail.QueryCollectionOperator(
            task_id="query_active_user_records",
            name='active_user_records',
            query=f"""SELECT * FROM valid_record WHERE emp_status IN {tuple(config.ACTIVE_STATUS)}"""
        )

        dummy_process_active_users = rail.EmptyOperator(
            task_id='dummy_process_active_users'
        )

        process_active_users = rail.trigger_parallel_dagrun(
            task_id='process_active_users',
            items="{{ result('query_active_user_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf= lambda item: request_payload.get_process_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_process_users_dag_ids',
            python_callable= lambda: python_callable_methods.get_process_users_dag_ids(config.trigger_parallel_dagrun_count_process_users),
            show_return_value_in_logs= False
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
            trigger_dag_id=config.process_supervisor_dagid,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_log': rail.result('create_supervisor_log'),
                'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                    'displayText', 'Supervisor', 'uri'),
                'report_user_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                    'displayText', 'Report User', 'uri'),
                "report_user_substitute_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                    'displayText', 'Report User with Substitute','uri'),
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
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run:{
                'total_records': rail.result('create_input_data_collection',key='length'),
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                'log_filename': dag_run.conf['log_filename']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "no_of_user_records_in_payload":  "{{result('create_input_data_collection','length')}}",
                "no_of_valid_user_records": "{{result('query_valid_records','length')}}",
                "no_of_invalid_user_records": "{{result('query_invalid_records','length')}}",
            }
        )


        create_input_data_collection >> has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label('Yes') >> create_log >> create_supervisor_log >> query_invalid_records >> log_invalid_records >> query_valid_records
        query_valid_records >> has_valid_records >> rail.Label('Yes') >> process_groups >> wait_process_groups >> dummy_get_user_prereqs
        get_user_prereqs >> query_disable_user_records >> dummy_process_disable_users >> process_disable_users >> query_active_user_records
        query_active_user_records >> dummy_process_active_users >> process_active_users >> get_process_users_dag_ids >> gather_user_logs
        gather_user_logs >> get_supervisorcheck_queued_logs
        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs >> rail.Label('No') >> process_log_generation
        has_valid_records >> rail.Label('No') >> no_valid_records_present >> process_log_generation
        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_log_generation
        process_log_generation >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
