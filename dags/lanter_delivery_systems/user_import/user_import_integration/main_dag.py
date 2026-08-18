from datetime import timedelta, datetime
import itertools
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split

from lanter_delivery_systems.user_import.user_import_integration.utils import request_payload
from lanter_delivery_systems.user_import.user_import_integration.tasks.get_user_prereqs import get_user_prereqs_task_group

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='Lanter Delivery Systems User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.replicon_sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.process_users_input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Incorrect File Format - {{ current_time_in_specified_tz() }}',
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

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Login Name': 'loginname',
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Authentication Type': 'authtype',
                'Enabled': 'enabled',
                'Employee Type': 'employeetype',
                'Employee ID': 'employeeid',
                'Department': 'department',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Authentication ID': 'authid',
                'Password': 'password',
                'Licenses': 'licenses',
                'Initial Supervisor Login Name': 'supervisorusername',
                'Location': 'locationname',
                'Permission Sets': 'permisssionset',
                'Timesheet Template': 'timesheettemplate',
                'Timesheet Approval Path': 'timesheetapprovalpath',
                'Time Zone': 'timezone',
                'Initial Pay Rate Currency Name': 'currency',
                'Initial Pay Rate': 'payrate',
                'Punch Entry Policy': 'punchentrypolicy',
                'Initial Payrule Name': 'payrulename',
                'Custom Field : District': 'district',
                'Custom Field : Cost Center': 'costcenter',
                'Custom Field : CID': 'cid',
                'Custom Field : Location Address Line 1': 'locationaddress',
                'Custom Field : Location City': 'locationcity',
                'Custom Field : Location State/Territory': 'locationstate',
                'GL String': 'glstring',
                'accounting_code:gl_string': 'accountingcode',
                'work_type': 'worktype',
                'accounting_code:gl_description': 'accountingcodedescription',
                'Custom Field : Agency': 'agency',
                'Custom Field : markup %': 'markup',
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_md5',
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

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('create_input_data_collection')}}",
            columns=['loginname', 'firstname', 'lastname', 'authtype', 'enabled', 'employeetype', 'employeeid', 'department',
                     'startdate', 'enddate', 'authid', 'password', 'licenses', 'supervisorusername', 'locationname',
                     'permisssionset', 'timesheettemplate', 'timesheetapprovalpath', 'timezone',
                     'currency', 'payrate','punchentrypolicy', 'payrulename', 'district', 'costcenter','cid','locationaddress','locationcity',
                     'locationstate','glstring','accountingcode','worktype','accountingcodedescription','agency','markup','md5'],
            data=request_payload.get_create_md5_data
        )

        input_data_with_md5 = rail.CreateCollectionOperator(
            task_id="input_data_with_md5",
            name="input_data",
            source="{{result('create_md5')}}"
        )

        download_reference_file = rail.S3DownloadFileOperator(
            task_id='download_reference_file',
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_reference_filepath + '/user_import_reference_file.csv',
            aws_conn_id=config.aws_conn_id
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id="create_reference_data_collection",
            name="reference_data",
            source="{{result('parse_reference_file')}}"
        )

        get_delta_records = rail.QueryCollectionOperator(
            task_id="get_delta_records",
            query="""SELECT * FROM input_data WHERE md5 NOT IN (SELECT DISTINCT MD5 FROM reference_data)"""
        )

        has_any_changed_records = rail.IfOperator(
            task_id="has_any_changed_records",
            test="{{result('get_delta_records', 'length') > 0}}",
            yes_task=['query_valid_records', 'query_invalid_records'],
            no_task="no_changed_records"
        )

        no_changed_records = rail.EmptyOperator(
            task_id='no_changed_records'
        )

        get_unchanged_records = rail.QueryCollectionOperator(
            task_id="get_unchanged_records",
            query="""SELECT * FROM input_data WHERE md5 IN (SELECT DISTINCT MD5 FROM reference_data)"""
        )

        has_any_unchanged_records = rail.IfOperator(
            task_id="has_any_unchanged_records",
            test="{{result('get_unchanged_records', 'length') > 0}}",
            yes_task="log_unchanged_records",
            no_task="no_unchanged_records_present"
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id="log_unchanged_records",
            log="{{ result('create_log') }}",
            items="{{result('get_unchanged_records')}}",
            message="No changes Recieved",
            severity="Skipped",
            properties=lambda item: {
                "loginname": item['loginname'],
                "employeeid":item['employeeid'],
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "action":"Validation",
                'status': "Skipped",
                'details': "No changes Recieved"
            }
        )

        no_unchanged_records_present = rail.EmptyOperator(
            task_id='no_unchanged_records_present'
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM get_delta_records WHERE NULLIF(loginname, '') IS NOT NULL and
                    NULLIF(firstname, '') IS NOT NULL and NULLIF(lastname, '') IS NOT NULL and NULLIF(authtype, '') IS NOT NULL and
                    NULLIF(enabled, '') IS NOT NULL and NULLIF(employeetype, '') IS NOT NULL and NULLIF(employeeid, '') IS NOT NULL and
                    NULLIF(department, '') IS NOT NULL and NULLIF(startdate, '') IS NOT NULL and
                    NULLIF(password, '') IS NOT NULL and NULLIF(licenses, '') IS NOT NULL and NULLIF(locationname, '') IS NOT NULL"""
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

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM get_delta_records WHERE NULLIF(loginname, '') IS NULL or
                    NULLIF(firstname, '') IS NULL or NULLIF(lastname, '') IS NULL or NULLIF(authtype, '') IS NULL or
                    NULLIF(enabled, '') IS NULL or NULLIF(employeetype, '') IS NULL or NULLIF(employeeid, '') IS NULL or
                    NULLIF(department, '') IS NULL or NULLIF(startdate, '') IS NULL or
                    NULLIF(password, '') IS NULL or NULLIF(licenses, '') IS NULL or NULLIF(locationname, '') IS NULL"""
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
                "loginname": item['loginname'],
                "employeeid":item['employeeid'],
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "action": "Validation",
                'status': 'Exception',
                'details': request_payload.get_mandatory_fields_exception_message(item),
            }
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.process_groups_dagid,
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
            trigger_dag_id=config.process_users_dagid,
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
            trigger_dag_id=config.processs_supervisor_dagid,
            conf=lambda item: {
                **item['properties'],
                'supervisor_log': rail.result('create_supervisor_log'),
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
            source=lambda: rail.result('input_data_with_md5'),
            header=['Login Name', 'First Name', 'Last Name', 'Authentication Type', 'Enabled', 'Employee Type',
                    'Employee ID', 'Department', 'Start Date', 'End Date', 'Authentication ID', 'Password', 'Licenses',
                    'Initial Supervisor Login Name', 'Location', 'Permission Sets', 'Timesheet Template',
                    'Timesheet Approval Path', 'Time Zone', 'Initial Pay Rate Currency Name', 'Initial Pay Rate', 'Punch Entry Policy', 'Initial Payrule Name',
                    'Custom Field : District', 'Custom Field : Cost Center', 'Custom Field : CID',
                    'Custom Field : Location Address Line 1', 'Custom Field : Location City',
                    'Custom Field : Location State/Territory','GL String', 'accounting_code:gl_string', 'work_type', 'accounting_code:gl_description',
                    'Custom Field : Agency', 'Custom Field : markup %',  'MD5'],
            row=[
                '{{item.loginname}}',
                '{{item.firstname}}',
                '{{item.lastname}}',
                '{{item.authtype}}',
                '{{item.enabled}}',
                '{{item.employeetype}}',
                '{{item.employeeid}}',
                '{{item.department}}',
                '{{item.startdate}}',
                '{{item.enddate}}',
                '{{item.authid}}',
                '{{item.password}}',
                '{{item.licenses}}',
                '{{item.supervisorusername}}',
                '{{item.locationname}}',
                '{{item.permisssionset}}',
                '{{item.timesheettemplate}}',
                '{{item.timesheetapprovalpath}}',
                '{{item.timezone}}',
                '{{item.currency}}',
                '{{item.payrate}}',
                '{{item.punchentrypolicy}}',
                '{{item.payrulename}}',
                '{{item.district}}',
                '{{item.costcenter}}',
                '{{item.cid}}',
                '{{item.locationaddress}}',
                '{{item.locationcity}}',
                '{{item.locationstate}}',
                '{{item.glstring}}',
                '{{item.accountingcode}}',
                '{{item.worktype}}',
                '{{item.accountingcodedescription}}',
                '{{item.agency}}',
                '{{item.markup}}',
                '{{item.md5}}',
            ]
        )

        archive_old_reference_file = rail.S3MoveFileOperator(
            task_id='archive_old_reference_file',
            source_bucket_name=config.s3_bucket_name,
            existing_key_name=config.s3_reference_filepath + '/user_import_reference_file.csv',
            new_key_name=f'{config.s3_reference_archive_filepath}/user_import_reference_file_{(datetime.now()).strftime("%Y%m%d%H%M")}.csv',
            aws_conn_id=config.aws_conn_id
        )

        upload_new_reference_file = rail.S3UploadFileOperator(
            task_id='upload_new_reference_file',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_reference_filepath + '/user_import_reference_file.csv',
            source="{{ result('create_reference_file')}}"
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
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
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

        new_file_sensor >> is_csv >> rail.Label('Yes') >> download_file >> was_new_file_found
        is_csv >> rail.Label('No') >> send_bad_file_format_email
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        download_file >> load_data >> create_input_data_collection >> create_log >> create_supervisor_log >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email

        has_input_data >> rail.Label(
            'Yes') >> create_md5 >> input_data_with_md5 >> download_reference_file >> parse_reference_file

        parse_reference_file >> create_reference_data_collection
        create_reference_data_collection >> [get_delta_records, get_unchanged_records]
        get_delta_records >> has_any_changed_records >> rail.Label("Yes") >> [query_valid_records, query_invalid_records]

        has_any_changed_records >> rail.Label("No") >> no_changed_records >> create_reference_file
        get_unchanged_records >> has_any_unchanged_records >> rail.Label("Yes") >> log_unchanged_records >> create_reference_file
        has_any_unchanged_records >> rail.Label("No") >> no_unchanged_records_present >> create_reference_file

        query_invalid_records >> has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> create_reference_file
        has_invalid_records >> rail.Label('No') >> no_invalid_records_present >> create_reference_file

        query_valid_records >> has_valid_records
        has_valid_records >> rail.Label('No') >> no_valid_records_present >> create_reference_file
        has_valid_records >> rail.Label('Yes') >> process_groups >> wait_process_groups >> dummy_get_user_prereqs

        get_user_prereqs >> dummy_process_users >> process_users

        process_users >> get_process_users_dag_ids >> gather_user_logs >> get_supervisorcheck_queued_logs
        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs >> rail.Label('No') >> create_reference_file
        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> create_reference_file

        create_reference_file >> archive_old_reference_file
        archive_old_reference_file >> upload_new_reference_file >> dummy_process_log_generation
        dummy_process_log_generation >> process_log_generation >> can_log_to_sumo >> rail.Label('Yes') >> log_to_sumo

        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_dag)
