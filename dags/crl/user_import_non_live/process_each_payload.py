from datetime import timedelta
import itertools
import rail

from crl.user_import_non_live.tasks.get_user_prereqs_other_locations import get_user_prereqs_task_group
from crl.user_import_non_live.utils import request_payload

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_import_payload_dagid,
        description='CRL - User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_non_live_location,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        MANDATORY_FIELDS = {
            "emp_id":"Empl_ID",
            "first_name":"First_Name",
            "last_name": "Last_Name",
            "email": "Work_Email",
            "login_name": "User_Name",
            'location_full_path': 'Location',
            'start_date': 'Hire_Date',
            'adjusted_start_date': 'Adj_Hire_Date'
        }

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda dag_run: dag_run.conf['payload'],
            name="input_data_collection_non_live",
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
                "Adj_Hire_Date": "adjusted_start_date",
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
                "Home_Location": "home_location"
                }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_log',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | User Import Others - no records in payload - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )


        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name="invalid_records_non_live",
            query="""SELECT * FROM input_data_collection_non_live WHERE NULLIF(emp_id, '') IS NULL or
                    NULLIF(first_name, '') IS NULL or NULLIF(last_name, '') IS NULL
                    or NULLIF(email, '') IS NULL or NULLIF(login_name, '') IS NULL or NULLIF(adjusted_start_date, '') IS NULL
                    or NULLIF(start_date, '') IS NULL or NULLIF(location_full_path, '') IS NULL"""
        )

        def get_mandatory_fields_exception_message(item):
            missing_fields = []
            for payload_key, log_value in MANDATORY_FIELDS.items():
                if not item[payload_key]:
                    missing_fields.append(f"{log_value} is not present in payload")

            return rail.smartjoin_by_delim(missing_fields, ";")

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message=get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['emp_id'],
                'first_name': item['first_name'],
                'last_name': item['last_name'],
                'action':'Validation',
                'status': 'Exception',
                "details": get_mandatory_fields_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_records_non_live',
            query="""SELECT * FROM input_data_collection_non_live WHERE NULLIF(emp_id, '') IS NOT NULL and
                    NULLIF(first_name, '') IS NOT NULL and NULLIF(last_name, '') IS NOT NULL
                    and NULLIF(email, '') IS NOT NULL and NULLIF(login_name, '') IS NOT NULL and NULLIF(adjusted_start_date, '') IS NOT NULL
                    and NULLIF(start_date, '') IS NOT NULL and NULLIF(location_full_path, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='dummy_get_user_prereqs',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        dummy_get_user_prereqs, get_user_prereqs= get_user_prereqs_task_group()

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf= lambda item: request_payload.get_process_other_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_users_{x+1}') if rail.result(
                    f'process_users_{x+1}') else []), range(config.trigger_parallel_dagrun_count_process_users))))),
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
        has_input_data >> rail.Label('Yes') >> create_log >> query_invalid_records >> log_invalid_records
        log_invalid_records >> query_valid_records >> has_valid_records >> rail.Label('No') >> no_valid_records_present
        has_valid_records >> rail.Label('Yes') >> dummy_get_user_prereqs
        get_user_prereqs >> process_users >> get_process_users_dag_ids >> gather_user_logs >> process_log_generation
        process_log_generation >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
