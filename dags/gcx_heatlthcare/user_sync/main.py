from datetime import timedelta
import json
import pendulum
import rail
from pendulum import datetime
from datetime import datetime as dt
from airflow.models import Variable
from gcx_heatlthcare.user_sync.utils import request_payload,response_payload


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_name,
        description='GCX USER IMPORT MASTER',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:
        

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: dt.now(pendulum.timezone(config.time_zone)).strftime('%Y-%m-%dT%H:%M:%S')
        )

        create_user_details_log = rail.CreateLogOperator(
            task_id='create_user_details_log'
        )

        get_access_token_data_to_pass = rail.PythonOperator(
            task_id='get_access_token_data_to_pass',
            python_callable=request_payload.get_token_data,
            op_args=[config]
        )

        get_access_token = rail.SimpleHttpOperator(
            task_id='get_access_token',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='/sts/v1/common/token?subscription-key=19315b29d4144730ae7f60e815f187f5',
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data="{{ result('get_access_token_data_to_pass')['access_token']}}",
            extra_options={
                'verify': False
            }

        )

        get_taiwan_users_details = rail.SimpleHttpOperator(
            task_id='get_taiwan_users_details',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='/v1/legalentities/163284/employees?workLocationName=Taiwan&include=All',
            headers={
                "Authorization": "Bearer " + "{{ result('get_access_token') | from_json | attr_or_default('access_token', 'none') }}",
                "Ocp-Apim-Subscription-Key": "{{ result('get_access_token_data_to_pass')['subscription_key'] }}"},
            data={},
            extra_options={
                'verify': False
            }

        )

        get_japan_users_details = rail.SimpleHttpOperator(
            task_id='get_japan_users_details',
            method='GET',
            http_conn_id=config.http_conn_id,
            endpoint='/v1/legalentities/163284/employees?workLocationName=Japan&include=All',
            headers={
                "Authorization": "Bearer " + "{{ result('get_access_token') | from_json | attr_or_default('access_token', 'none') }}",
                "Ocp-Apim-Subscription-Key": "{{ result('get_access_token_data_to_pass')['subscription_key']}}"},
            data={},
            extra_options={
                'verify': False
            }

        )

        get_all_data = rail.PythonOperator(
            task_id='get_all_data',
            python_callable=lambda: request_payload.get_user_data(json.loads(rail.result('get_japan_users_details'))['records']) + request_payload.get_user_data(json.loads(rail.result('get_taiwan_users_details'))['records'])
        )

        format_data = rail.WriteCSVFileOperator(
            task_id='format_data',
            source="{{result('get_all_data')| tojson}}",
            header=[
                'employee_id',
                'employee_first_name',
                'employee_last_name',
                'email',
                'start_date',
                'end_date',
                'manager',
                'work_location',
                'md5'
            ],
            row=request_payload.get_formated_user_row
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            name='inputfile',
            source="{{result('format_data')}}"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            delimiter=','
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id='create_reference_collection',
            name='referencefile',
            source="{{result('parse_reference_file')}}"
        )

        query_delta_records = rail.QueryCollectionOperator(
            task_id='query_delta_records',
            query="""SELECT * FROM  inputfile WHERE  inputfile.md5 NOT IN (SELECT DISTINCT  referencefile.md5 FROM  referencefile)""",
        )

        if_no_delta_records = rail.IfOperator(
            task_id='if_no_delta_records',
            test="{{result('query_delta_records','length') < 1}}",
            yes_task="send_mail_no_changes_found",
            no_task="create_delta_collection",
        )

        send_mail_no_changes_found = rail.EmailOperator(
            task_id='send_mail_no_changes_found',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | Replicon User Import skipped- No Changes in Values at {{ current_time_in_specified_tz(tz="America/New_York") }} ''',
            html_content='templates/no_change_mail.html',
        )

        create_delta_collection = rail.CreateCollectionOperator(
            task_id='create_delta_collection',
            name='delta_records',
            source="{{result('query_delta_records')}}"
        )

        get_all_user_details = rail.RepliconServiceOperator(
            task_id="get_all_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_all_manager_user_details,
            data_handler=response_payload.get_user_details
        )

        create_manager_user_collection = rail.CreateCollectionOperator(
            task_id='create_manager_user_collection',
            name='manager_records',
            source="{{result('get_all_user_details') | tojson}}"
        )

        query_new_manager_records = rail.QueryCollectionOperator(
            task_id='query_new_manager_records',
            query="""SELECT distinct(delta_records.manager) FROM  delta_records WHERE  delta_records.manager NOT IN (SELECT DISTINCT  manager_records.employee_id FROM  manager_records)"""
        )

        create_user_log = rail.CreateLogOperator(
            task_id='create_user_log'
        )

        process_new_manager_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_manager_records',
            trigger_dag_id=config.create_manager_child_dag_id,
            items="{{ result('query_new_manager_records') }}",
            conf=lambda item: {
                **dict(item.items()),
                "log": rail.result('create_user_log')
            }
        )

        wait_process_new_manager_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_manager_records",
             dag_runs="{{result('process_new_manager_records')}}"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_all_user_details,
            data_handler=response_payload.get_user_details
        )

        create_all_user_collection = rail.CreateCollectionOperator(
            task_id='create_all_user_collection',
            name='user_records',
            source="{{result('get_user_details') | tojson}}"
        )

        query_new_user_records = rail.QueryCollectionOperator(
            task_id='query_new_user_records',
            query="""SELECT * FROM  delta_records WHERE  delta_records.employee_id NOT IN (SELECT DISTINCT  user_records.employee_id FROM  user_records)"""
        )

        process_new_user_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_user_records',
            trigger_dag_id=config.create_new_user_child_dag_id,
            items="{{ result('query_new_user_records') }}",
            conf=lambda item: {
                **dict(item.items()),
                "log": rail.result('create_user_log')
            }
        )

        wait_process_new_user_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_new_user_records",
             dag_runs="{{result('process_new_user_records')}}"
        )

        query_update_user_records = rail.QueryCollectionOperator(
            task_id='query_update_user_records',
            query="""SELECT * FROM  delta_records WHERE  delta_records.employee_id IN (SELECT DISTINCT  user_records.employee_id FROM  user_records)"""
        )

        process_update_user_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_user_records',
            trigger_dag_id=config.update_user_child_dag_id,
            items="{{ result('query_update_user_records') }}",
            conf=lambda item: {
                **dict(item.items()),
                "log": rail.result('create_user_log')
            }
        )

        wait_process_update_user_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_update_user_records",
             dag_runs="{{result('process_update_user_records')}}"
        )

        rename_archive_referencefile=rail.SFTPMoveFileOperator(
            task_id='rename_archive_referencefile',
            new_filename=config.archive_filepath + '{{ dag_run_ecid() }}_reference.csv',
            sftp_conn_id=config.sftp_conn_id,
            existing_filename=config.reference_filepath,
        )

        upload_referencefile=rail.SFTPUploadFileOperator(
            task_id='upload_referencefile',
            content='''{{ result('format_data') }}''',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath= config.reference_filepath,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=request_payload.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['Employee ID', 'First name',
                    'Last name', 'Action', 'Status', 'Details', 'Job ID'],
            row=['{{ item.employeeid }}', '{{ item.first_name}}', '{{ item.last_name }}',
                 '{{ item.action }}', '{{ item.status }}', '{{ item.details }}', '{{ item.jobid }}'],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='log_{{dag_run_ecid() | replace(":", "-")}}_{{ current_time_in_specified_tz(tz="America/New_York") }}'+".csv",
            expires_in_seconds=7*24*60*60,
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('format_logs', 'error_record_count') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='send_completion_mail'
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Import -  completed successfully at {{ current_time_in_specified_tz(tz="America/New_York") }}',
            html_content="templates/import_complete.html"
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon User Import is Completed with Errors at {{ current_time_in_specified_tz(tz="America/New_York") }}',
            html_content="templates/import_with_error.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
            trigger_rule='all_done'
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )


        log_current_time >> create_user_details_log >> get_access_token_data_to_pass >> get_access_token >> get_taiwan_users_details\
        >> get_japan_users_details >> get_all_data >> format_data >> create_user_collection >> download_reference_file\
        >> parse_reference_file >> create_reference_collection >> query_delta_records >> if_no_delta_records >> rail.Label("Yes") >> send_mail_no_changes_found

        if_no_delta_records >> rail.Label("No") >> create_delta_collection >> get_all_user_details >> create_manager_user_collection >> query_new_manager_records >> create_user_log >> process_new_manager_records >> wait_process_new_manager_records\
        >> get_user_details >> create_all_user_collection >> query_new_user_records >> process_new_user_records >> wait_process_new_user_records\
        >> query_update_user_records >> process_update_user_records >> wait_process_update_user_records >> rename_archive_referencefile >> upload_referencefile\
        >> format_logs >> render_logs_csv >> generate_download_link >>any_records_failed >> rail.Label("Yes") >> send_completion_error_mail >> log_to_sumo >> can_fail_dag >> fail_dagrun
        any_records_failed >> rail.Label("No") >> send_completion_mail >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_main_airflow_dag)