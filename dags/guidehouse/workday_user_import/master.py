from datetime import timedelta
import itertools
import rail
from guidehouse.workday_user_import.utils import custom_method
from guidehouse.workday_user_import.utils import request_payload
from airflow.models import Variable
from guidehouse.workday_user_import.task.get_user_prereqs import get_user_prereqs_task_group
from guidehouse.workday_user_import.task.get_updated_user_prereqs import get_updated_user_prereqs_task_group
# pylint: disable=too-many-statements


def create_main_dag(config):
    """
    Create the master DAG for Guidehouse Workday user import integration.

    Args:
        config: Configuration object containing instance-specific settings.

    Returns:
        airflow.DAG: Configured Airflow DAG object
    """
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'Guidehouse Workday User Import - Master DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_run_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv_pgp = rail.IfOperator(
            task_id='is_csv_pgp',
            test=lambda: rail.result("new_file_sensor").split('.')[-1] in ("csv", "pgp"),
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Workday User Sync - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
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

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='get_input_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        get_input_data = rail.PythonOperator(
            task_id="get_input_data",
            python_callable=lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file_var_name,
                            default_var='true').lower() == 'true' else rail.result('download_file'),
            show_return_value_in_logs=False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('get_input_data') }}",
            encoding="utf-8-sig"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                "Employee_ID": "employee_id",
                "Login_Name": "login_name",
                "First_Name": "first_name",
                "Last_Name": "last_name",
                "Email": "email",
                "Supervisor_ID": "supervisor_id",
                "Default_Location": "location",
                "Employee_Type": "employee_type",
                "Change_Effective_Date": "change_effective_date",
                "Schedule": "schedule",
                "Start_Date": "start_date",
                "Seniority_Date": "seniority_date",
                "End_Date": "end_date",
                "Job_code": "job_code",
                "Job_Description": "job_description",
                "Pay_Group": "pay_group",
                "Status": "user_status",
                "Company_Code": "company_code",
                "Company_Description": "company_description",
                "Cost_Center_Code": "cost_center_code",
                "Cost_Center_Description": "cost_center_description",
                "Financial_System": "financial_system",
                "Time_Profile_Name": "time_profile_name",
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='get_valid_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Workday User Sync - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        process_supervisor_log = rail.CreateLogOperator(
            task_id="process_supervisor_log"
        )

        get_valid_data = rail.QueryCollectionOperator(
            task_id="get_valid_data",
            name="valid_data",
            query="""SELECT * FROM inputdatacollection
            WHERE
                NULLIF("employee_id","") IS NOT NULL AND
                NULLIF("login_name","") IS NOT NULL AND
                NULLIF("first_name","") IS NOT NULL AND
                NULLIF("last_name","") IS NOT NULL AND
                NULLIF("location","") IS NOT NULL AND
                NULLIF("employee_type","") IS NOT NULL AND
                NULLIF("change_effective_date","") IS NOT NULL AND
                NULLIF("schedule","") IS NOT NULL AND
                NULLIF("start_date","") IS NOT NULL AND
                NULLIF("user_status","") IS NOT NULL AND
                NULLIF("company_code","") IS NOT NULL AND
                NULLIF("cost_center_description","") IS NOT NULL AND
                NULLIF("financial_system","") IS NOT NULL"""
        )

        get_invalid_data = rail.QueryCollectionOperator(
            task_id="get_invalid_data",
            name="invalid_data",
            query="""SELECT * FROM inputdatacollection
            WHERE
                NULLIF("employee_id", "") IS NULL OR
                NULLIF("login_name", "") IS NULL OR
                NULLIF("first_name", "") IS NULL OR
                NULLIF("last_name", "") IS NULL OR
                NULLIF("location", "") IS NULL OR
                NULLIF("employee_type", "") IS NULL OR
                NULLIF("change_effective_date", "") IS NULL OR
                NULLIF("schedule", "") IS NULL OR
                NULLIF("start_date", "") IS NULL OR
                NULLIF("user_status", "") IS NULL OR
                NULLIF("company_code", "") IS NULL OR
                NULLIF("cost_center_description", "") IS NULL OR
                NULLIF("financial_system", "") IS NULL"""
        )

        log_invalid_users = rail.WriteLogOperator(
            task_id='log_invalid_users',
            log='{{result("create_log")}}',
            items='{{ result("get_invalid_data") }}',
            severity='Exception',
            message=custom_method.get_mandatory_fields_exception_message,
            properties=lambda item: {
                "lastname": item['last_name'],
                "firstname": item['first_name'],
                "loginname": item['login_name'],
                "employeeid": item['employee_id'],
                "manager": item['supervisor_id'],
                "userstatus": item['user_status'],
                "company_description": item['company_description'],
                "cost_center_description": item['cost_center_description'],
                "location": item['location'],
                "action": "Validation",
                'status': 'Exception',
                'details': custom_method.get_mandatory_fields_exception_message(item),
            },
        )

        process_prerequisite = rail.EmptyOperator(
            task_id='process_prerequisite'
        )

        process_prerequisite_entry, process_prerequisite_exit = get_user_prereqs_task_group(config)

        process_updated_prereqs = rail.EmptyOperator(
            task_id='process_updated_prereqs'
        )

        process_updated_prereqs_entry, process_updated_prereqs_exit = get_updated_user_prereqs_task_group(config)

        get_unique_employee_id = rail.QueryCollectionOperator(
            task_id="get_unique_employee_id",
            name="unique_employee_id",
            query="""SELECT DISTINCT employee_id FROM valid_data"""
        )

        add_row_to_unique_employee_id = rail.QueryCollectionOperator(
            task_id='add_row_to_unique_employee_id',
            query="SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id, * FROM unique_employee_id"
        )

        dummy_process_each_user = rail.EmptyOperator(
            task_id='dummy_process_each_user'
        )

        def get_process_each_user_batch_dag_id(record_id):
            modulo = int(record_id) % config.PROCESS_USER_BATCH_COUNT
            return f'{config.process_each_user}_batch_{modulo+1}'

        process_each_user = rail.trigger_parallel_dagrun(
            task_id='process_each_user',
            items=lambda: rail.result('add_row_to_unique_employee_id'),
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=lambda item: get_process_each_user_batch_dag_id(item['record_id']),
            conf=lambda item: {
                **{"modulo": int(item['record_id']) % config.PROCESS_USER_BATCH_COUNT,},
                **custom_method.get_process_users_conf(item, config)
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        dummy_process_user_ids = rail.EmptyOperator(
            task_id='dummy_process_user_ids'
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_user_{x+1}'), range(config.trigger_parallel_dagrun_count_process_users))))),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs="{{ result('get_process_users_dag_ids') }}",
            dagrun_task_id='process_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('process_supervisor_log') }}",
            severity='Pending',
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
            items=lambda: rail.load_all_records(rail.result('get_supervisorcheck_queued_logs')),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.processs_supervisor,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'user_log': rail.result('create_log'),
                'supervisor_log': rail.result('process_supervisor_log'),
                'supervisor_permission_uri': custom_method.get_supervisor_permission_uri()
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
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'userlogs': rail.result('gather_user_logs') if rail.result('gather_user_logs') else [],
                'otherlogs': rail.result('create_log') if rail.result('create_log') else [],
                'log_filename': f"""log_{rail.render_template("{{ result('new_file_sensor') | file_name | replace('.csv', '') | replace('.pgp', '')}}")}_{rail.render_template("{{current_time_in_specified_tz(fmt='%Y-%m-%dT%H-%M-%S') | replace(':', '-')}}")}.csv"""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
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

        new_file_sensor >> is_csv_pgp >> rail.Label('Yes') >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        is_csv_pgp >> rail.Label('No') >> send_bad_file_format_email
        download_file >> can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> get_input_data
        can_decrypt_file >> rail.Label("No") >> get_input_data
        get_input_data >> load_data >> create_input_data_collection >> create_log >> process_supervisor_log >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email

        has_input_data >> rail.Label(
            'Yes') >> get_valid_data >> get_invalid_data >> log_invalid_users >> process_prerequisite >> process_prerequisite_entry
        process_prerequisite_exit >> process_updated_prereqs >> process_updated_prereqs_entry
        process_updated_prereqs_exit >> get_unique_employee_id >> add_row_to_unique_employee_id >> dummy_process_each_user >> process_each_user >> dummy_process_user_ids

        dummy_process_user_ids >> get_process_users_dag_ids >> gather_user_logs >> get_supervisorcheck_queued_logs >> \
        is_supervisorcheck_queued_logs >> rail.Label('No') >> process_log_generation
        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_log_generation
        process_log_generation >> log_to_sumo

        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
