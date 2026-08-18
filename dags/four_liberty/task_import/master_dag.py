
from datetime import timedelta
import os
from rail.lib.ecid import get_dagrun_ecid
import chardet
from rail.lib.artifact import existing_artifact
import rail
from four_liberty.task_import.utils import custom_methods, python_callable_method, request_payload, response_filter


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'4liberty_taskimporttoreplicon_masterv20_{config.instance}',
        description=f'4liberty _ Task import to Replicon _ Master V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        column_names = ['taskuri', 'taskname', 'isenabled']

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task import - skipped {{ current_time_in_specified_tz() }}',
            html_content='templates/email/bad_file_format.html',
        )

        archive_invalid_file = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding = rail.PythonOperator(
            task_id = "find_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_file.task_id]
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ (get_task_state("new_file_sensor") == "success") and (result("new_file_sensor") | file_ext | lower == "csv") }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.processing_filepath +
            "/processing_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_task_import_data = rail.LoadCSVFileOperator(
            task_id='load_task_import_data',
            document="{{ result('download_file') }}",
            encoding="{{ result('find_file_encoding')[0].encoding}}"
        )

        create_task_import_collection = rail.CreateCollectionOperator(
            task_id='create_task_import_collection',
            source="{{ result('load_task_import_data') }}",
            name="input_data"
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_task_import_collection', 'length') > 0 }}",
            yes_task='process_records',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task import - skipped {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_file.html"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        project_file_name = rail.PythonOperator(
            task_id='project_file_name',
            python_callable=lambda: os.path.basename(
                rail.result('new_file_sensor')).split("_")[0]
        )

        create_task_import_log = rail.CreateLogOperator(
            task_id='create_task_import_log'
        )

        create_input_data_csv = rail.WriteCSVFileOperator(
            task_id='create_input_data_csv',
            source="{{ result('create_task_import_collection') }}",
            header=['Task_Name',
                    'Budget_Code_Name',
                    'Budget_Code',
                    'Work_Order',
                    'Substation___Work_Order_Name',
                    'Internal_Order',
                    'FERC_Code',
                    'System_Status',
                    'Work_Order_Status',
                    'Open_Date',
                    'TECO_Date',
                    'Close_Date',
                    'Md5'],
            row=custom_methods.get_csv_rows
        )

        load_input_data_csv = rail.LoadCSVFileOperator(
            task_id="load_input_data_csv",
            document="{{ result('create_input_data_csv')}}",
        )

        create_collection_from_input_data = rail.CreateCollectionOperator(
            task_id='create_collection_from_input_data',
            source="{{ result('load_input_data_csv') }}",
            name="inputdata",
        )

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            sftp_conn_id=config.sftp_conn_id2,
            paths=[config.reference_filepath]
        )

        reference_file = rail.PythonOperator(
            task_id='reference_file',
            python_callable=python_callable_method.get_reference_file,
            op_args=['list_sftp_files', config.reference_filepath]
        )

        has_reference_file = rail.IfOperator(
            task_id='has_reference_file',
            test=lambda: bool(rail.result("reference_file") and rail.result(
                "reference_file")[0]["size"] > 0),
            yes_task="download_reference_file",
            no_task="create_blank_reference_data_csv",
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id2,
            remote_filepath=config.reference_filepath +
            '/{{ result("reference_file") | first_or_default | attr_or_default("filename") }}'
        )

        find_reference_file_encoding = rail.PythonOperator(
            task_id = "find_reference_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_reference_file.task_id]
        )

        load_reference_data_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_data_csv",
            document="{{ result('download_reference_file') }}",
            encoding="{{ result('find_reference_file_encoding')[0].encoding}}"
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id='create_reference_data_collection',
            source="{{ result('load_reference_data_csv') }}"
        )

        create_blank_reference_data_csv = rail.WriteCSVFileOperator(
            task_id='create_blank_reference_data_csv',
            source = [],
            header=['Task_Name',
                    'Budget_Code_Name',
                    'Budget_Code',
                    'Work_Order',
                    'Substation___Work_Order_Name',
                    'Internal_Order',
                    'FERC_Code',
                    'System_Status',
                    'Work_Order_Status',
                    'Open_Date',
                    'TECO_Date',
                    'Close_Date',
                    'Md5'],
            row=[]
        )

        create_reference_data_csv = rail.WriteCSVFileOperator(
            task_id='create_reference_data_csv',
            source=lambda : rail.result('create_reference_data_collection') if (rail.result("reference_file") and rail.result(
                "reference_file")[0]["size"] > 0) else rail.result('create_blank_reference_data_csv'),
            header=['Task_Name',
                    'Budget_Code_Name',
                    'Budget_Code',
                    'Work_Order',
                    'Substation___Work_Order_Name',
                    'Internal_Order',
                    'FERC_Code',
                    'System_Status',
                    'Work_Order_Status',
                    'Open_Date',
                    'TECO_Date',
                    'Close_Date',
                    'Md5'],
            row=custom_methods.get_csv_rows
        )

        create_collection_from_reference_data_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_reference_data_csv',
            source="{{ result('create_reference_data_csv') }}",
            name="referencedata",
        )

        query_list_records_to_ignore = rail.QueryCollectionOperator(
            task_id='query_list_records_to_ignore',
            # pylint: disable=line-too-long
            query="""SELECT * FROM inputdata WHERE Md5 IN (SELECT Md5 FROM referencedata) OR Work_Order_Status NOT IN ('CLOSED','LOCKED','OPEN','TECO') OR NULLIF(Task_Name, '') IS NULL OR NULLIF(Internal_Order, '') IS NULL""",
        )

        query_list_delta_records = rail.QueryCollectionOperator(
            task_id='query_list_delta_records',
            # pylint: disable=line-too-long
            query="""SELECT * FROM inputdata WHERE Md5 NOT IN (SELECT Md5 FROM referencedata) AND Work_Order_Status IN ('CLOSED','LOCKED','OPEN','TECO') AND NULLIF(Task_Name, '') IS NOT NULL AND NULLIF(Internal_Order, '') IS NOT NULL""",
        )

        query_list_deltarecordstoignorewheretasknameinternalorderorworkorderisnotincorrectformat = rail.QueryCollectionOperator(
            task_id='query_list_deltarecordstoignorewheretasknameinternalorderorworkorderisnotincorrectformat',
            # pylint: disable=line-too-long
            query="""SELECT * FROM inputdata WHERE Md5 NOT IN (SELECT Md5 FROM referencedata) AND (Work_Order_Status NOT IN ('CLOSED','LOCKED','OPEN','TECO') OR NULLIF(Task_Name, '') IS NULL OR NULLIF(Internal_Order, '') IS NULL)""",
        )

        log_task_name_internalorder_workordertatus_blank = rail.WriteLogOperator(
            task_id='log_task_name_internalorder_workordertatus_blank',
            log='{{ result("create_task_import_log") }}',
            items='{{ result("query_list_deltarecordstoignorewheretasknameinternalorderorworkorderisnotincorrectformat") }}',
            message='Either Task Name or Internal Order are blank / Work Order status is not in the expected format.',
            properties={
                "projectname": "{{ result('project_file_name') }}",
                "taskname": "{{ item.Task_Name }}",
                "budgetcodename": "{{ item.Budget_Code_Name }}",
                "substationworkordername": "{{ item.Substation___Work_Order_Name }}",
                "internal": "{{ item.Internal_Order }}",
                "status": "Skipped",
                "details": "Either Task Name or Internal Order are blank / Work Order status is not in the expected format.",
                "parentjobid": "{{ dag_run_ecid() }}",
                "childjobid": ""
            }
        )

        has_delta_records_to_process = rail.IfOperator(
            task_id='has_delta_records_to_process',
            test='{{ result("query_list_delta_records", "length") > 0 }}',
            yes_task="query_tasks_count_and_records",
            no_task="upload_uploadnewreferencefile_76",
        )

        query_tasks_count_and_records = rail.QueryCollectionOperator(
            task_id='query_tasks_count_and_records',
            # pylint: disable=line-too-long
            query="""SELECT Task_Name, COUNT(*) FROM inputdata WHERE Md5 NOT IN (SELECT Md5 FROM referencedata) AND Work_Order_Status IN ('CLOSED','LOCKED','OPEN','TECO') AND NULLIF(Task_Name, '') IS NOT NULL AND NULLIF(Internal_Order, '') IS NOT NULL
GROUP BY Task_Name""",
            name='Tasknamebycount'
        )

        query_unique_taskname_deltarecords_toprocess = rail.QueryCollectionOperator(
            task_id='query_unique_taskname_deltarecords_toprocess',
            # pylint: disable=line-too-long
            query="""SELECT * FROM inputdata WHERE Md5 NOT IN (SELECT Md5 FROM referencedata) AND Task_Name IN (SELECT Task_Name FROM Tasknamebycount WHERE COUNT___ = 1)""",
            name='recordstoupdateorcreate'
        )

        query_duplicate_taskname_deltarecords_toignore = rail.QueryCollectionOperator(
            task_id='query_duplicate_taskname_deltarecords_toignore',
            # pylint: disable=line-too-long
            query="""SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Task_Name ORDER BY Md5) AS rn FROM inputdata WHERE Md5 NOT IN (SELECT Md5 FROM referencedata) AND Task_Name IN (SELECT Task_Name FROM Tasknamebycount WHERE COUNT___ > 1 )) AS sub WHERE rn = 1""",
        )

        log_duplicate_task_name_received = rail.WriteLogOperator(
            task_id='log_duplicate_task_name_received',
            log='{{ result("create_task_import_log") }}',
            items='{{ result("query_duplicate_taskname_deltarecords_toignore") }}',
            message='Duplicate records received for the given task name.',
            properties={
                "projectname": "{{ result('project_file_name') }}",
                "taskname": "{{ item.Task_Name }}",
                "budgetcodename": "{{ item.Budget_Code_Name }}",
                "substationworkordername": "{{ item.Substation___Work_Order_Name }}",
                "internal": "{{ item.Internal_Order }}",
                "status": "Skipped",
                "details": "Duplicate records received for the given task name.",
                "parentjobid": "{{ dag_run_ecid() }}",
                "childjobid": ""
            }
        )

        has_unique_taskname_deltarecords_toprocess = rail.IfOperator(
            task_id='has_unique_taskname_deltarecords_toprocess',
            test='{{ result("query_unique_taskname_deltarecords_toprocess", "length") > 0 }}',
            yes_task='bulk_get_project_details3',
            no_task='upload_uploadnewreferencefile_76',
        )

        bulk_get_project_details3 = rail.RepliconServiceOperator(
            task_id='bulk_get_project_details3',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=request_payload.get_project_details_payload,
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        is_projectdetails_uri_present = rail.IfOperator(
            task_id='is_projectdetails_uri_present',
            test='{{ result("bulk_get_project_details3").uri | is_truthy }}',
            yes_task="get_taskdetails",
            no_task="send_mail_no_project_found",
        )

        get_taskdetails = rail.RepliconServiceOperator(
            task_id='get_taskdetails',
            endpoint="/services/TaskListService1.svc/GetData",
            data=request_payload.get_task_details_payload,
            response_filter=response_filter.get_task_list_data
        )

        existing_task_collection = rail.CreateCollectionOperator(
            task_id='existing_task_collection',
            source='{{result("get_taskdetails") | to_json }}',
            columns=column_names,
            name='existingtasklist'
        )

        query_existing_task_toupdate = rail.QueryCollectionOperator(
            task_id='query_existing_task_toupdate',
            query="""SELECT * FROM recordstoupdateorcreate WHERE Task_Name IN (SELECT taskname FROM existingtasklist)""",
        )

        query_task_tocreate = rail.QueryCollectionOperator(
            task_id='query_task_tocreate',
            query="""SELECT * FROM recordstoupdateorcreate WHERE Task_Name NOT IN (SELECT taskname FROM existingtasklist)""",
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:task"}
        )

        has_task_to_create = rail.IfOperator(
            task_id='has_task_to_create',
            test='{{ result("query_task_tocreate", "length")> 0 }}',
            yes_task="trigger_dag_run_4liberty_process_tasks_create_child",
            no_task="trigger_dag_run_4liberty_process_tasks_update_child",
        )        

        trigger_dag_run_4liberty_process_tasks_create_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_4liberty_process_tasks_create_child',
            retries=0,
            items="{{ result('query_task_tocreate') }}",
            trigger_dag_id=f'4liberty_processtaskschildv4_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_task_create_conf
        )

        wait_for_completion_trigger_dag_run_4liberty_process_tasks_create_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_4liberty_process_tasks_create_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_4liberty_process_tasks_create_child") }}'
        )

        trigger_dag_run_4liberty_process_tasks_update_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_4liberty_process_tasks_update_child',
            retries=0,
            items="{{ result('query_existing_task_toupdate') }}",
            trigger_dag_id=f'4liberty_processtaskschildv4_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_task_update_conf
        )

        wait_for_completion_trigger_dag_run_4liberty_process_tasks_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_4liberty_process_tasks_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_4liberty_process_tasks_update_child") }}'
        )

        rename_and_move_processing_file_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_and_move_processing_file_to_archive',
            existing_filename=config.processing_filepath +
            '/processing_{{ result("new_file_sensor") | file_name }}',
            new_filename=config.archive_filepath +
            '/{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}'
        )

        has_reference_file_to_archive = rail.IfOperator(
            task_id='has_reference_file_to_archive',
            test=lambda: bool(rail.result("reference_file") and rail.result(
                "reference_file")[0]["size"] > 0),
            yes_task="rename_and_move_reference_file_to_archive",
            no_task="upload_new_reference_file",
        )

        rename_and_move_reference_file_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_and_move_reference_file_to_archive',
            sftp_conn_id=config.sftp_conn_id2,
            existing_filename=config.reference_filepath +
            '/{{ result("reference_file")[0]["filename"] }}',
            new_filename=config.reference_archive_filepath +
            '/{{ result("reference_file")[0]["filename"] }}'
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            sftp_conn_id=config.sftp_conn_id2,
            content='{{ result("download_file") }}',
            remote_filepath=config.reference_filepath +
            '/{{ result("project_file_name")}}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        is_create_logs_exist = rail.IfOperator(
            task_id='is_create_logs_exist',
            test=lambda: rail.result(
                'trigger_dag_run_4liberty_process_tasks_create_child'),
            yes_task='gather_task_create_logs',
            no_task='gather_task_update_logs'
        )

        gather_task_create_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_task_create_logs',
            dag_runs='{{ result("trigger_dag_run_4liberty_process_tasks_create_child") }}',
            dagrun_task_id='create_task_create_update_log',
            flatten=True
        )

        gather_task_update_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_task_update_logs',
            dag_runs='{{ result("trigger_dag_run_4liberty_process_tasks_update_child") }}',
            dagrun_task_id='create_task_create_update_log',
            flatten=True
        )

        trigger_task_import_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_task_import_log_generation',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'4liberty_taskimport_loggeneration_{config.instance}',
            conf=lambda: {
                'task_import_logs': rail.result('create_task_import_log'),
                'task_create_logs': rail.result('gather_task_create_logs') if rail.result('gather_task_create_logs') else null,
                'task_update_logs': rail.result('gather_task_update_logs') if rail.result('gather_task_update_logs') else null,
                'file_name': rail.result("new_file_sensor").rsplit('/', 1)[-1],
                'parentjobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        send_mail_no_project_found = rail.EmailOperator(
            task_id='send_mail_no_project_found',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon task import - skipped {{ current_time_in_specified_tz() }}',
            html_content='templates/email/no_project_found.html'
        )

        rename_archivetheinputfile_72 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_72',
            existing_filename=config.processing_filepath +
            '/processing_{{ result("new_file_sensor") | file_name }}',
            new_filename=config.archive_filepath +
            '/{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}'
        )

        rename_archivereferencefile_73 = rail.SFTPMoveFileOperator(
            task_id='rename_archivereferencefile_73',
            sftp_conn_id=config.sftp_conn_id2,
            existing_filename=config.reference_filepath +
            '/{{ result("reference_file")[0]["filename"] }}',
            new_filename=config.reference_archive_filepath +
            '/{{ result("reference_file")[0]["filename"] }}'
        )

        upload_uploadnewreferencefile_74 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreferencefile_74',
            sftp_conn_id=config.sftp_conn_id2,
            content='{{ result("download_file") }}',
            remote_filepath=config.reference_filepath +
            '/{{ result("project_file_name")}}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        upload_uploadnewreferencefile_76 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreferencefile_76',
            sftp_conn_id=config.sftp_conn_id2,
            content='{{ result("download_file") }}',
            remote_filepath=config.reference_filepath +
            '/{{ result("project_file_name")}}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        rename_archivereferencefile_77 = rail.SFTPMoveFileOperator(
            task_id='rename_archivereferencefile_77',
            sftp_conn_id=config.sftp_conn_id2,
            existing_filename=config.reference_filepath +
            '/{{ result("reference_file")[0]["filename"] }}',
            new_filename=config.reference_archive_filepath +
            '/{{ result("reference_file")[0]["filename"] }}'
        )

        upload_uploadnewreferencefile_78 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreferencefile_78',
            sftp_conn_id=config.sftp_conn_id2,
            content='{{ result("download_file") }}',
            remote_filepath=config.reference_filepath +
            '/{{ result("project_file_name")}}_{{ dag_run_ecid() | replace(":", "-") }}.csv',
        )

        send_mail_no_changed_record = rail.EmailOperator(
            task_id='send_mail_no_changed_record',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Replicon task import - skipped {{ current_time_in_specified_tz() }} ''',
            html_content='templates/email/no_changed_records.html'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=lambda: {
                "project_name": rail.result("project_file_name") if rail.result("project_file_name") else '',
                "file_name": os.path.split(rail.result("new_file_sensor"))[1] if rail.result("new_file_sensor") else '',
            }
        )

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_bad_file_format_email >> archive_invalid_file >> finish

        is_csv >> rail.Label("Yes") >> download_file >> rail.Label(
            "Always") >> find_file_encoding >> was_new_file_found >> rail.Label("Yes") >> archive_file >> load_task_import_data
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        load_task_import_data >> create_task_import_collection\
            >> has_any_records >> rail.Label("No") >> send_blank_payload_email >> finish

        has_any_records >> rail.Label(
            "Yes") >> process_records >> project_file_name >> create_task_import_log >> create_input_data_csv >> load_input_data_csv \
            >> create_collection_from_input_data >> list_sftp_files >> reference_file >> has_reference_file

        has_reference_file >> rail.Label(
            'Yes') >> download_reference_file >> find_reference_file_encoding >> load_reference_data_csv
        has_reference_file >> rail.Label(
            'No') >> create_blank_reference_data_csv >> create_reference_data_csv

        load_reference_data_csv >> create_reference_data_collection >> create_reference_data_csv 
        
        create_reference_data_csv >> create_collection_from_reference_data_csv >> query_list_records_to_ignore >> query_list_delta_records \
            >> query_list_deltarecordstoignorewheretasknameinternalorderorworkorderisnotincorrectformat \
            >> log_task_name_internalorder_workordertatus_blank >> has_delta_records_to_process

        has_delta_records_to_process >> rail.Label(
            'Yes') >> query_tasks_count_and_records >> query_unique_taskname_deltarecords_toprocess \
            >> query_duplicate_taskname_deltarecords_toignore >> log_duplicate_task_name_received >> has_unique_taskname_deltarecords_toprocess
        has_delta_records_to_process >> rail.Label(
            'No') >> upload_uploadnewreferencefile_76

        has_unique_taskname_deltarecords_toprocess >> rail.Label(
            'Yes') >> bulk_get_project_details3 >> is_projectdetails_uri_present
        has_unique_taskname_deltarecords_toprocess >> rail.Label(
            'No') >> upload_uploadnewreferencefile_76 >> rename_archivereferencefile_77 >> upload_uploadnewreferencefile_78 \
            >> send_mail_no_changed_record >> finish

        is_projectdetails_uri_present >> rail.Label(
            'Yes') >> get_taskdetails >> existing_task_collection >> query_existing_task_toupdate \
            >> query_task_tocreate >> get_all_custom_fields >> has_task_to_create
        is_projectdetails_uri_present >> rail.Label(
            'No') >> send_mail_no_project_found >> rename_archivetheinputfile_72 \
            >> rename_archivereferencefile_73 >> upload_uploadnewreferencefile_74 >> finish

        has_task_to_create >> rail.Label(
            'Yes') >> trigger_dag_run_4liberty_process_tasks_create_child >> wait_for_completion_trigger_dag_run_4liberty_process_tasks_create_child \
                >> trigger_dag_run_4liberty_process_tasks_update_child

        has_task_to_create >> rail.Label(
            'No') >> trigger_dag_run_4liberty_process_tasks_update_child
        
        trigger_dag_run_4liberty_process_tasks_update_child >> wait_for_completion_trigger_dag_run_4liberty_process_tasks_update_child \
            >> rename_and_move_processing_file_to_archive >> has_reference_file_to_archive
        
        has_reference_file_to_archive >> rail.Label(
            'No') >> upload_new_reference_file >> is_create_logs_exist
        has_reference_file_to_archive >> rail.Label(
            'Yes') >> rename_and_move_reference_file_to_archive >> upload_new_reference_file >> is_create_logs_exist

        is_create_logs_exist >> rail.Label(
            'Yes') >> gather_task_create_logs >> gather_task_update_logs
        is_create_logs_exist >> rail.Label(
            'No') >> gather_task_update_logs >> trigger_task_import_log_generation >> finish

        finish >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
