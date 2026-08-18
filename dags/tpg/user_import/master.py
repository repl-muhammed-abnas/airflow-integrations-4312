
from datetime import timedelta
import itertools
import rail
from tpg.user_import.utils import python_callable, request_payload
from tpg.user_import.tasks.get_user_prereqs import get_user_prereqs_task_group

null = None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.tpg_user_import_master,
        description=f'TPG User Import Master {config.instance}',
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
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_file',
            no_task='send_incorrect_fileformat_mail'
        )

        send_incorrect_fileformat_mail = rail.EmailOperator(
            task_id='send_incorrect_fileformat_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject="{{ get_company_key() }} | User import - incorrect file format recieved - {{result('log_current_date')}}",
            html_content="templates/emails/incorrect_fileformat_mail.html",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable=python_callable.get_current_date_time
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            new_filename=config.archive_filepath +
            "/{{'invalid_' if result('new_file_sensor') | file_ext | lower != 'csv' else '' }}{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}",
            existing_filename="{{ result('new_file_sensor') }}",
        )

        tpg_user_import_log = rail.CreateLogOperator(
            task_id = "tpg_user_import_log"
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log'
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.reference_filepath]
        )

        if_reference_file_present = rail.IfOperator(
            task_id='if_reference_file_present',
            test=lambda: bool(rail.result('list_reference_files') and rail.result(
                'list_reference_files')[config.reference_filepath]),
            yes_task='get_reference_filename',
            no_task='send_no_reference_file_mail'
        )

        send_no_reference_file_mail = rail.EmailOperator(
            task_id='send_no_reference_file_mail',
            to=config.internal_logs_email,
            bcc=config.bcc_email,
            subject="{{ get_company_key() }} | User import - no reference file present - {{result('log_current_date')}}",
            html_content="templates/emails/send_no_reference_file_mail.html",
        )

        get_reference_filename = rail.PythonOperator(
            task_id = "get_reference_filename",
            python_callable=lambda: python_callable.get_ref_file_name(config.reference_filepath)
        )

        parse_input_file_csv = rail.LoadCSVFileOperator(
            task_id="parse_input_file_csv",
            document='{{result("download_file")}}'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('parse_input_file_csv') }}",
            name="inputdata"
        )

        has_data_in_input_file = rail.IfOperator(
            task_id = "has_data_in_input_file",
            test = "{{result('create_input_data_collection', 'length') > 0 }}",
            yes_task = "input_file_with_md5",
            no_task = "send_no_data_to_import_mail"
        )

        input_file_with_md5 = rail.WriteCSVFileOperator(
            task_id="input_file_with_md5",
            source='{{result("parse_input_file_csv")}}',
            header=['firstname', 'lastname', 'loginname', 'employeeid', 'email',
                    'employeetype', 'authtype', 'costcenter', 'businessunitorgroup',
                    'isloginenable', 'startdate', 'enddate', 'level','manager', 'location',
                    'userpermission', 'supervisorpermission', 'teammanagerpermission',
                    'payrollmanagerpermission', 'administratorpermission','licenses','timesheettemplate',
                    'timesheetapprovalpath', 'timesheetperiod', 'schedule', 'md5'],
            row=request_payload.user_import_csv_data
        )

        send_no_data_to_import_mail = rail.EmailOperator(
            task_id='send_no_data_to_import_mail',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject="{{ get_company_key() }} | User import has been skipped - {{result('log_current_date')}}",
            html_content="templates/emails/no_records_in_file_mail.html",
        )

        create_collection_rawdatawithmd5 = rail.CreateCollectionOperator(
            task_id='create_collection_rawdatawithmd5',
            source="{{ result('input_file_with_md5') }}",
            name="rawdatawithmd5"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath= "{{ result('get_reference_filename')}}"
        )

        load_reference_csv = rail.LoadCSVFileOperator(
            task_id = "load_reference_csv",
            document="{{ result('download_reference_file') }}"
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id='create_reference_data_collection',
            source="{{ result('load_reference_csv') }}",
            name="userreferencedata",
            columns={
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'Email': 'email',
                'EmployeeType': 'employeetype',
                'Authentication Type': 'authtype',
                'Cost Center': 'costcenter',
                'Business Unit or Group': 'businessunitorgroup',
                'Is Login Enabled': 'isloginenable',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Level': 'level',
                'Manager': 'manager',
                'Location/Office': 'location',
                'User Permission': 'userpermission',
                'Supervisor Permission': 'supervisorpermission',
                'Team Manager Permission': 'teammanagerpermission',
                'Payroll Manager Permission': 'payrollmanagerpermission',
                'Administrator Permission': 'administratorpermission',
                'Licenses': 'licenses',
                'Timesheet Template': 'timesheettemplate',
                'Timesheet Approval Path': 'timesheetapprovalpath',
                'Timesheet Period': 'timesheetperiod',
                'Schedule': 'schedule',
                'MD5': 'md5'
            }
        )

        query_invalid_data = rail.QueryCollectionOperator(
            task_id="query_invalid_data",
            query="""SELECT * FROM rawdatawithmd5 WHERE
                    NULLIF(firstname, '') IS NULL OR
                     NULLIF(lastname, '') IS NULL OR
                     NULLIF(loginname, '') IS NULL OR
                     NULLIF(employeeid, '') IS NULL OR
                     NULLIF(authtype, '') IS NULL
                    """,
            name="invalid_records"
        )

        has_invalid_data = rail.IfOperator(
            task_id='has_invalid_data',
            test="{{result('query_invalid_data', 'length') > 0 }}",
            yes_task="log_invalids_records",
            no_task="query_valid_records",
        )

        log_invalids_records = rail.WriteLogOperator(
            task_id='log_invalids_records',
            log="{{ result('tpg_user_import_log') }}",
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            items='{{ result("query_invalid_data") }}',
            properties=lambda item: {
                'jobid': rail.render_template("{{dag_run_ecid()}}"),
                'lastname': item['lastname'],
                'firstname': item['firstname'],
                'loginname':  item['loginname'],
                'employeeid': item['employeeid'],
                'useruri': '',
                'manager': item['manager'],
                'action': 'Validation',
                'status': 'Exception',
                'details':request_payload.get_mandatory_fields_exception_message(item),
                'user_log': ''
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM rawdatawithmd5 WHERE
                    NULLIF(firstname, '') IS NOT NULL AND
                     NULLIF(lastname, '') IS NOT NULL AND
                     NULLIF(loginname, '') IS NOT NULL AND
                     NULLIF(employeeid, '') IS NOT NULL AND
                     NULLIF(authtype, '') IS NOT NULL AND md5 NOT IN
                     (SELECT md5 from userreferencedata)
                    """,
            name="validrecords"
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.process_groups,
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name}}",
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
            trigger_dag_id=config.process_users,
            conf=request_payload.get_process_users_conf,
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
            no_task='create_reference_file'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.processs_supervisor,
            conf=lambda item: {
                **item['properties'],
                'supervisor_log': rail.result('create_supervisor_log'),
                'supervisor_permission_uri': request_payload.get_supervisor_permission_uri
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('create_collection_rawdatawithmd5'),
            header=['First Name', 'Last Name', 'Login Name', 'Employee ID', 'Email', 'EmployeeType',
                    'Authentication Type', 'Cost Center', 'Business Unit or Group', 'Is Login Enabled',
                    'Start Date', 'End Date', 'Level', 'Manager', 'Location/Office', 'User Permission',
                    'Supervisor Permission', 'Team Manager Permission', 'Payroll Manager Permission',
                    'Administrator Permission', 'Timesheet Template', 'Timesheet Approval Path',
                    'Timesheet Period', 'Schedule', 'MD5'],
            row=[
                '{{item.firstname}}',
                '{{item.lastname}}',
                '{{item.loginname}}',
                '{{item.employeeid}}',
                '{{item.email}}',
                '{{item.employeetype}}',
                '{{item.authtype}}',
                '{{item.costcenter}}',
                '{{item.businessunitorgroup}}',
                '{{item.isloginenable}}',
                '{{item.startdate}}',
                '{{item.enddate}}',
                '{{item.level}}',
                '{{item.manager}}',
                '{{item.location}}',
                '{{item.userpermission}}',
                '{{item.supervisorpermission}}',
                '{{item.teammanagerpermission}}',
                '{{item.payrollmanagerpermission}}',
                '{{item.administratorpermission}}',
                '{{item.timesheettemplate}}',
                '{{item.timesheetapprovalpath}}',
                '{{item.timesheetperiod}}',
                '{{item.schedule}}',
                '{{item.md5}}'
            ]
        )

        archive_old_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_old_reference_file',
            new_filename=config.archive_filepath +"/archive_reference_{{ result('get_reference_filename').split('/')[-1] }}",
            existing_filename="{{ result('get_reference_filename') }}"
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content="{{ result('create_reference_file') }}",
            remote_filepath=config.reference_filepath + "/user_reference_file_{{ current_time('%d%m%YT%H%M%S') }}.csv",
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf={
                'userlogs': "{{result('gather_user_logs')}}",
                'otherlogs': "{{result('tpg_user_import_log')}}",
                'log_filename': 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}'
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda: request_payload.get_task_state('delete_this_dagrun') != "success" and
                request_payload.get_task_state('download_file') == "success",
            yes_task="log_to_sumo",
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

        new_file_sensor >> is_csv >> rail.Label("No") >> send_incorrect_fileformat_mail
        is_csv >> rail.Label("Yes") >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> log_current_date >> tpg_user_import_log >> create_supervisor_log >> \
        list_reference_files >> if_reference_file_present >> rail.Label(
            "Yes") >> get_reference_filename >> parse_input_file_csv
        if_reference_file_present >> rail.Label("No") >> send_no_reference_file_mail
        parse_input_file_csv >> create_input_data_collection >> has_data_in_input_file >> rail.Label("Yes") >> input_file_with_md5 >> \
        create_collection_rawdatawithmd5 >> download_reference_file
        has_data_in_input_file >> rail.Label("No") >> send_no_data_to_import_mail
        download_reference_file >> load_reference_csv >> create_reference_data_collection >> query_invalid_data >> \
        has_invalid_data >> rail.Label("Yes") >> log_invalids_records >> query_valid_records
        has_invalid_data >> rail.Label("No") >> query_valid_records >> process_groups >> \
        wait_process_groups >> dummy_get_user_prereqs

        get_user_prereqs >> dummy_process_users >> process_users

        process_users >> get_process_users_dag_ids >> gather_user_logs >> get_supervisorcheck_queued_logs
        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs >> rail.Label('No') >> create_reference_file
        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> create_reference_file

        create_reference_file >> archive_old_reference_file
        archive_old_reference_file >> upload_new_reference_file
        upload_new_reference_file >> process_log_generation >> can_log_to_sumo >> rail.Label('Yes') >> log_to_sumo

        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_dag)
