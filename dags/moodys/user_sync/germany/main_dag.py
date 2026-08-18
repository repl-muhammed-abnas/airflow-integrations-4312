from datetime import timedelta
import itertools
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

from moodys.user_sync.germany.utils import request_payload
from moodys.user_sync.germany.tasks.get_user_prereqs import get_user_prereqs_task_group

null = None

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='Moodys User Sync',
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

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            delimiter="|"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Country ID': 'countryid',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'Date of Birth': 'dateofbirth',
                'Rehire': 'rehire',
                'Start Date': 'startdate',
                'LastName': 'lastname',
                'FirstName': 'firstname',
                'Last day worked': 'lastdayworked',
                'EndDate': 'enddate',
                'Email ID': 'emailid',
                'Time Zone': 'timezone',
                'Language': 'language',
                'PN Flag': 'pnflag',
                'ADP File#': 'adpfile',
                'FTE%': 'ftepercent',
                'Employee Category': 'employeecategory',
                'Actual Working hours': 'actualworkinghrs',
                'Statutory Limit': 'statutorylimit',
                'Effective Date': 'effectivedate',
                'Employee Type Name': 'employeetypename',
                'Division Name': 'divisionname',
                'Location Name': 'locationname',
                'Location Code': 'locationcode',
                'Company Name': 'companyname',
                'Company Code': 'companycode',
                'Supervisor ID/Emp ID': 'supervisorid',
                'Supervisor First name': 'supervisorfirstname',
                'Supervisor Last name': 'supervisorlastname',
                'Supervisor Email ID': 'supervisoremailid',
                'Job Title': 'jobtitle'
            }
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log'
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='query_invalid_records',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Sync - Germany no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(countryid, '') IS NULL or
                    NULLIF(loginname, '') IS NULL or NULLIF(employeeid, '') IS NULL or NULLIF(startdate, '') IS NULL or
                    NULLIF(lastname, '') IS NULL or NULLIF(firstname, '') IS NULL or NULLIF(timezone, '') IS NULL or
                    NULLIF(effectivedate, '') IS NULL or NULLIF(employeetypename, '') IS NULL or
                    NULLIF(divisionname, '') IS NULL or NULLIF(locationname, '') IS NULL or NULLIF(locationcode, '') IS NULL or
                    NULLIF(companyname, '') IS NULL or NULLIF(companycode, '') IS NULL or
                    NULLIF(statutorylimit, '') IS NULL or NULLIF(ftepercent, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_log') }}",
            items='{{result("query_invalid_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                "countryid": item['countryid'],
                "loginname": item['loginname'],
                "lastname": item['lastname'],
                "firstname": item['firstname'],
                "action": "Validation",
                "status": "Exception",
                'details': request_payload.get_mandatory_fields_exception_message(item),
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(countryid, '') IS NOT NULL and
                    NULLIF(loginname, '') IS NOT NULL and NULLIF(employeeid, '') IS NOT NULL and NULLIF(startdate, '') IS NOT NULL and
                    NULLIF(lastname, '') IS NOT NULL and NULLIF(firstname, '') IS NOT NULL and NULLIF(timezone, '') IS NOT NULL and
                    NULLIF(effectivedate, '') IS NOT NULL and NULLIF(employeetypename, '') IS NOT NULL and
                    NULLIF(divisionname, '') IS NOT NULL and NULLIF(locationname, '') IS NOT NULL and NULLIF(locationcode, '') IS NOT NULL and
                    NULLIF(companyname, '') IS NOT NULL and NULLIF(companycode, '') IS NOT NULL and
                    NULLIF(statutorylimit, '') IS NOT NULL and NULLIF(ftepercent, '') IS NOT NULL"""
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
            trigger_dag_id=config.process_groups_dag_id,
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

        dummy_get_user_prereqs, get_user_prereqs = get_user_prereqs_task_group()

        dummy_process_users = rail.EmptyOperator(
            task_id='dummy_process_users'
        )

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users_dagid,
            conf=lambda item: request_payload.get_process_users_conf(
                item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_users_{x+1}'), range(config.trigger_parallel_dagrun_count_process_users))))),
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
            no_task='dummy_process_log_generation'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.processs_supervisor_dag_id,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_log': rail.result('create_supervisor_log'),
                'supervisorpermissionuri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_permission_set'), 'displayText', 'Supervisor', 'uri'),
                'enduserwithreportspermissionuri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_permission_set'), 'displayText', 'End user with reports', 'uri'),
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run: {
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                'log_filename': f'log_{get_dagrun_ecid(dag_run).replace(":", "-")}_{rail.result("new_file_sensor").split("/")[-1]}'
            }
        )

        new_file_sensor >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        download_file >> load_data >> create_input_data_collection >> create_log >> create_supervisor_log >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email

        has_input_data >> rail.Label(
            'Yes') >> query_invalid_records

        query_invalid_records >> has_invalid_records >> rail.Label(
            'No') >> query_valid_records
        has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> query_valid_records

        query_valid_records >> has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> dummy_process_log_generation
        has_valid_records >> rail.Label(
            'Yes') >> process_groups >> wait_process_groups >> dummy_get_user_prereqs
        get_user_prereqs >> dummy_process_users >> process_users

        process_users >> get_process_users_dag_ids >> gather_user_logs >> get_supervisorcheck_queued_logs

        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs >> rail.Label(
            'No') >> dummy_process_log_generation
        is_supervisorcheck_queued_logs >> rail.Label(
            'Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> dummy_process_log_generation

        dummy_process_log_generation >> process_log_generation

    return dag


rail.for_each_instance(create_main_dag)
