
from datetime import timedelta, datetime
import rail
from zaloragroup.user_import_v1.utils import python_callable_method, request_payload
from zaloragroup.user_import_v1.utils.python_callable_method import get_today

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_userimport_master_{config.instance}_v1',
        description=f'zaloragroup_userimport_master_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_master,
            soft_fail_timeout=timedelta(seconds=10)
        )

        get_current_time = rail.PythonOperator(
            task_id = "get_current_time",
            python_callable=lambda: datetime.now().strftime("%m_%d_%Y_T%H_%M_%S")
        )

        logger_list = rail.CreateLogOperator(
            task_id = "logger_list"
        )

        supervisor_mapper_list = rail.CreateLogOperator(
            task_id = "supervisor_mapper_list"
        )

        get_file_name = rail.PythonOperator(
            task_id = "get_file_name",
            python_callable=python_callable_method.get_filename
        )

        if_file_name_ends_with_csv = rail.IfOperator(
            task_id='if_file_name_ends_with_csv',
            test="{{result('new_file_sensor_to_process') | file_ext | lower == 'csv'}}",
            yes_task="download_sftp_file",
            no_task="archive_invalid_file_in_sftp",
        )

        archive_invalid_file_in_sftp = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file_in_sftp',
            new_filename=config.archive_filepath + "/{{ result('get_file_name') }}",
            existing_filename=config.input_filepath_master +'/{{ result("new_file_sensor_to_process") | file_name }}',
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        archive_file_in_sftp = rail.SFTPMoveFileOperator(
            task_id='archive_file_in_sftp',
            new_filename=config.archive_filepath + "/{{ result('get_file_name') }}",
            existing_filename=config.input_filepath_master +'/{{ result("new_file_sensor_to_process") | file_name }}',
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id = "load_csv",
            delimiter="|",
            document="{{ result('download_sftp_file') }}",
            headers=["LOGIN_NAME", "FIRST_NAME", "LAST_NAME", "EMPLOYEE_TYPE", "DEPARTMENT", "ENABLED",
                     "EMPLOYEE_ID", "START_DATE", "END_DATE", "EMAIL_ADDRESS", "INITIAL_SUPERVISOR_LOGINNAME", 
                     "HOLIDAY_CALENDAR", "SUB_DEPARTMENT", "JOB_FAMILY", "JOB_NAME","GRADE_NAME","LEGAL_ENTITY",
                     "DO_NOT_USE"]
        )

        check_csv_has_data = rail.IfOperator(
            task_id = "check_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('load_csv'))) > 0,
            yes_task = "process_each_user_from_csv",
            no_task = "send_no_data_to_import_mail"
        )

        process_each_user_from_csv = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_user_from_csv",
            items = "{{ result('load_csv')}}",
            trigger_dag_id = f'zaloragroup_user_import_process_each_user_from_csv_child_{config.instance}_v1',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = request_payload.process_user_data
        )

        wait_for_process_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_user',
            dag_runs='{{ result("process_each_user_from_csv") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        send_no_data_to_import_mail = rail.EmailOperator(
            task_id='send_no_data_to_import_mail',
            to=config.to_email,
            bcc=config.bcc_email,
            subject="{{ get_company_key() }} | User Import completed on " + get_today(),
            html_content="templates/email/no_data_to_import_email.html",
            params={
                "filepath": config.input_filepath_master,
                "today": get_today()
            }
        )

        write_supervisor_mapper_log_file = rail.WriteCSVFileOperator(
            task_id="write_supervisor_mapper_log_file",
            source=lambda: rail.result('supervisor_mapper_list'),
            header=['loginname', 'supervisorid', 'username', 'status'],
            row=lambda item: [
                item['properties']['loginname'],
                item['properties']['supervisorid'],
                item['properties']['username'],
                item['properties']['status']
            ]
        )

        load_csv_supervisor_mapper = rail.LoadCSVFileOperator(
            task_id='load_csv_supervisor_mapper',
            document="{{ result('write_supervisor_mapper_log_file') }}",
        )

        check_supervisor_mapper_csv_has_data = rail.IfOperator(
            task_id = "check_supervisor_mapper_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('load_csv_supervisor_mapper'))) > 0 ,
            yes_task = "process_each_mapper_data",
            no_task = "write_log_file"
        )

        process_each_mapper_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_mapper_data',
            items = "{{ result('load_csv_supervisor_mapper')}}",
            trigger_dag_id=f'zaloragroup_user_import_update_supervisor_from_logs_child_{config.instance}_v1',
            conf=request_payload.process_supervisor_mapper_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_mapper_data_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_mapper_data_process',
            dag_runs='{{ result("process_each_mapper_data") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        write_log_file = rail.WriteCSVFileOperator(
            task_id="write_log_file",
            source=lambda: rail.result('logger_list'),
            header=['login_name', 'status', 'failure_reason', 'child_job_id'],
            row=lambda item: [
                item['properties']['login_name'],
                item['properties']['status'],
                item['properties']['failure_reason'],
                item['ecid']
            ]
        )

        create_collection_from_csv_logs = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv_logs',
            source="{{ result('write_log_file') }}",
            name="error_statuslist",
            columns={
                'login_name': 'login_name',
                'status': 'status',
                'failure_reason': 'failure_reason',
                'child_job_id': 'child_job_id'
            }
        )

        query_error_status_data = rail.QueryCollectionOperator(
            task_id='query_error_status_data',
            query="SELECT * FROM  error_statuslist WHERE  status = 'Error'",
        )

        is__error_logger_csv_data_present = rail.IfOperator(
            task_id='is__error_logger_csv_data_present',
            test='{{ result("query_error_status_data", "length") > 0 }}',
            yes_task='compose_error_log_csv_with_header',
            no_task='load_csv_log'
        )

        compose_error_log_csv_with_header = rail.WriteCSVFileOperator(
            task_id="compose_error_log_csv_with_header",
            source="{{ result('query_error_status_data') }}",
            header=['login_name', 'status', 'failure_reason', 'child_job_id'],
            row=lambda item: [
                item['login_name'],
                item['status'],
                item['failure_reason'],
                item['child_job_id']
            ]
        )

        generate_error_logdownload_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_error_logdownload_link',
            artifact_name="{{ result('compose_error_log_csv_with_header')}}",
            output_file_name= "Userimportlogs_" + "{{ result('get_current_time') }}" + ".csv",
            expires_in_seconds=7*24*60*60,
        )

        send_error_mail_with_cshare = rail.EmailOperator(
            task_id='send_error_mail_with_cshare',
            to=config.to_email,
            bcc=config.alert_email,
            subject="{{ get_company_key() }}  | User Import completed with errors on  " + get_today(),
            html_content="templates/email/user_import_completed_with_error_email.html",
            params={
                "today" : get_today()
            },
        )

        load_csv_log = rail.LoadCSVFileOperator(
            task_id='load_csv_log',
            document="{{ result('write_log_file') }}",
        )

        check_logger_csv_has_data = rail.IfOperator(
            task_id = "check_logger_csv_has_data",
            test = lambda: len(rail.read_artifact(rail.result('load_csv_log'))) > 0 ,
            yes_task = "generate_download_link",
            no_task = "log_to_sumo"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_log_file')}}",
            output_file_name= "Userimportlogs_" + "{{ result('get_current_time') }}" + ".csv",
            expires_in_seconds=7*24*60*60,
        )

        send_success_mail_with_cshare = rail.EmailOperator(
            task_id='send_success_mail_with_cshare',
            to=config.to_email,
            bcc=config.bcc_email,
            subject="{{ get_company_key() }}  | User Import completed on  " + get_today(),
            html_content="templates/email/user_import_process_completion_email.html",
            params={
                "today" : get_today()
            },
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('load_csv_log') }}",
            remote_filepath=config.log_filepath + '/Userimportlogs_' + (datetime.now()).strftime("%m%d%Y%H%M%S") + '.csv',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
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

        new_file_sensor_to_process >> get_current_time >> logger_list >> supervisor_mapper_list >> \
            get_file_name >> if_file_name_ends_with_csv

        if_file_name_ends_with_csv >> rail.Label('No') >> archive_invalid_file_in_sftp >> log_to_sumo
        if_file_name_ends_with_csv >> rail.Label(
            'Yes') >> download_sftp_file >> archive_file_in_sftp >> load_csv >> check_csv_has_data

        check_csv_has_data >> rail.Label('Yes') >> process_each_user_from_csv >> wait_for_process_user >> \
            write_supervisor_mapper_log_file >> load_csv_supervisor_mapper >> check_supervisor_mapper_csv_has_data

        check_supervisor_mapper_csv_has_data >> rail.Label(
            'Yes') >> process_each_mapper_data >> wait_for_mapper_data_process >> write_log_file
        check_supervisor_mapper_csv_has_data >> rail.Label(
            'No') >> write_log_file

        write_log_file >> create_collection_from_csv_logs >> query_error_status_data >> is__error_logger_csv_data_present

        is__error_logger_csv_data_present >> rail.Label(
            'Yes') >> compose_error_log_csv_with_header >> generate_error_logdownload_link >> send_error_mail_with_cshare >> upload_log_to_sftp >> log_to_sumo

        is__error_logger_csv_data_present >> rail.Label(
            'No') >> load_csv_log >> check_logger_csv_has_data

        check_logger_csv_has_data >> rail.Label(
            'No') >> log_to_sumo
        check_logger_csv_has_data >> rail.Label(
            'Yes') >> generate_download_link >> send_success_mail_with_cshare >> upload_log_to_sftp >> log_to_sumo

        check_csv_has_data >> rail.Label('No') >> send_no_data_to_import_mail >> log_to_sumo

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
