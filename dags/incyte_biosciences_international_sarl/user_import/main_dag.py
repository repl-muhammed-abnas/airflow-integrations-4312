from datetime import timedelta
import itertools
from os import path
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

from incyte_biosciences_international_sarl.user_import.utils import request_payload
from incyte_biosciences_international_sarl.user_import.tasks.get_user_prereqs import get_user_prereqs_task_group

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='IBIS - User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
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

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='true').lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}",
            encoding='utf-8-sig'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="input_data_collection",
            columns={
                'First Name': 'first_name',
                'Last Name': 'last_name',
                'Email ID': 'email_id',
                'Emplid': 'employee_id',
                'Start Date': 'start_date',
                'End Date': 'end_date',
                'EE Login': 'login_name',
                'Supervisor Login': 'supervisor_login_name',
                'Business Title': 'business_title',
                'FTE': 'fte',
                'HR Manager ID': 'hr_manager_id',
                'Deptid': 'dept_full_path',
                'Empl Type': 'employee_type',
                'Country': 'country_name',
                'Countrys Code': 'country_code',
                'Full_PartTime': 'full_part_time',
                'Location': 'work_location_name',
                'Std Hours': 'standard_hours'
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
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalid_records',
            query="""SELECT * FROM input_data_collection WHERE NULLIF(first_name, '') IS NULL or
                    NULLIF(last_name, '') IS NULL or NULLIF(start_date, '') IS NULL or
                    NULLIF(login_name, '') IS NULL or NULLIF(country_name, '') IS NULL or
                    NULLIF(country_code, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="no_invalid_records_present"
        )

        no_invalid_records_present = rail.EmptyOperator(
            task_id='no_invalid_records_present'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_log') }}",
            items='{{result("query_invalid_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                "login_name": item['login_name'],
                "first_name": item['first_name'],
                "last_name": item['last_name'],
                "action": "Validation",
                "status": "Exception",
                'details': request_payload.get_mandatory_fields_exception_message(item),
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_records',
            query="""SELECT * FROM input_data_collection WHERE NULLIF(first_name, '') IS NOT NULL and
                    NULLIF(last_name, '') IS NOT NULL and NULLIF(start_date, '') IS NOT NULL and
                    NULLIF(login_name, '') IS NOT NULL and NULLIF(country_name, '') IS NOT NULL and
                    NULLIF(country_code, '') IS NOT NULL"""
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
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name}}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_process_groups",
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        dummy_get_user_prereqs, get_user_prereqs= get_user_prereqs_task_group()

        dummy_process_users = rail.EmptyOperator(
            task_id='dummy_process_users'
        )

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf= lambda item: request_payload.get_process_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_users_{x+1}'), range(config.trigger_parallel_dagrun_count_process_users))))),
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
            no_task='dummy_process_log_generation'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_supervisor_dagid,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'file_name':split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0]+'.csv',
                'supervisor_log': rail.result('create_supervisor_log'),
                'supervisor_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                    'displayText', 'Supervisor', 'uri'),
                'basic_user_permission_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_set'),
                    'displayText', 'Basic User with Reports', 'uri'),
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run:{
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0] }.csv'
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda: request_payload.get_task_state('delete_this_dagrun') != "success" and
                request_payload.get_task_state('download_file') == "success",
            yes_task="log_to_sumo",
            no_task="can_fail_dag"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
                "no_of_records": "{{ result('create_input_data_collection','length') }}"
            }
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

        new_file_sensor >> rail.Label('Yes') >> download_file >> can_decrypt_file

        can_decrypt_file >> rail.Label('No') >>  dummy_load_data
        can_decrypt_file >> rail.Label('Yes') >> decrypt_file >> dummy_load_data

        download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        dummy_load_data >> load_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label('Yes') >> create_log >> create_supervisor_log >> [ query_valid_records, query_invalid_records ]

        query_invalid_records >> has_invalid_records >> rail.Label('No') >> no_invalid_records_present >> dummy_process_log_generation
        has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> dummy_process_log_generation

        query_valid_records >> has_valid_records >> rail.Label('No') >> no_valid_records_present >> dummy_process_log_generation
        has_valid_records >> rail.Label('Yes') >> process_groups >> wait_process_groups >> dummy_get_user_prereqs
        get_user_prereqs >> dummy_process_users >> process_users

        process_users >> get_process_users_dag_ids >> gather_user_logs >> get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs
        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> dummy_process_log_generation
        is_supervisorcheck_queued_logs >> rail.Label('No') >> dummy_process_log_generation

        dummy_process_log_generation >> process_log_generation >> can_log_to_sumo >> rail.Label('Yes') >> log_to_sumo >> can_fail_dag
        can_log_to_sumo >> rail.Label('No') >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_dag)
