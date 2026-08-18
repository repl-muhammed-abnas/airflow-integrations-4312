
from datetime import timedelta
from sunovion.project_task_import.utils import request_payload
from sunovion.project_task_import.utils import custom_methods
from sunovion.project_task_import.task.send_logs import get_send_logs
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_task_import_update_master_{config.instance}',
        description=f'Sunovion Project Task Import/Update Master 1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            sftp_conn_id=config.sftp_conn_id2,
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id2,
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
            sftp_conn_id=config.sftp_conn_id2,
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_input_filepath + '/{{ result("get_logging_details").input_archive_filename }}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            delimiter='|'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Project Description': 'projectdescription',
                'Status': 'status',
                'Allow Time Entry': 'allowtimeentry',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Project Manager': 'projectmanager',
                'Cost Type': 'costtype',
                'Project Leader Approval Required': 'projectleaderapprovalrequired',
                'Invoice Currency': 'invoicecurrency',
                'Task Name Level 1': 'tasknamelevel1',
                'Task Code': 'taskcode',
                'Task Status': 'taskstatus',
                'Custom Field: Registration': 'customfieldregistration'
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_md5',
            no_task='fail_with_no_records'
        )

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('create_input_data_collection')}}",
            columns=['projectname', 'projectcode', 'projectdescription', 'status', 'allowtimeentry', 'startdate', 'enddate', 'projectmanager',
                     'costtype', 'projectleaderapprovalrequired', 'invoicecurrency', 'tasknamelevel1', 'taskcode', 'taskstatus', 'customfieldregistration', 'md5'],
            data=request_payload.get_create_md5_data
        )

        fail_with_no_records = rail.FailOperator(
            task_id='fail_with_no_records',
            message='Error: Invalid file provided'
        )

        input_data_with_md5 = rail.CreateCollectionOperator(
            task_id="input_data_with_md5",
            name="input_data",
            source="{{result('create_md5')}}"
        )

        list_reference_file = rail.SFTPListFilesOperator(
            task_id="list_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            paths=[config.reference_file_path]
        )

        has_files = rail.IfOperator(
            task_id='has_files',
            test=lambda: bool(rail.result('list_reference_file').get(
                config.reference_file_path)),
            yes_task='get_reference_file_name'
        )

        get_reference_file_name= rail.PythonOperator(
            task_id= 'get_reference_file_name',
            python_callable=lambda: rail.result("list_reference_file")[config.reference_file_path][0]['name'] if rail.result(
                "list_reference_file") else None
        )

        get_reference_file = rail.SFTPDownloadFileOperator(
            task_id="get_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_file_path +
            "/{{ result('get_reference_file_name') }}",
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('get_reference_file')}}",
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id="create_reference_data_collection",
            name="reference_data",
            source="{{result('parse_reference_file')}}"
        )

        get_delta_records = rail.QueryCollectionOperator(
            task_id="get_delta_records",
            query="""SELECT * FROM input_data WHERE md5 NOT IN (SELECT DISTINCT md5 FROM reference_data)"""
        )

        get_unchanged_records = rail.QueryCollectionOperator(
            task_id="get_unchanged_records",
            query="""SELECT * FROM input_data WHERE md5 IN (SELECT DISTINCT md5 FROM reference_data)"""
        )

        log_skipped_records = rail.WriteLogOperator(
            task_id="log_skipped_records",
            log='{{ result("create_log") }}',
            items='{{ result("get_unchanged_records") }}',
            message='No change in record',
            severity='Skipped',
            properties={
                'projectcode': '{{ item.projectcode }} / {{ item.projectname }}',
                'taskcode': '{{ item.taskcode }} / {{ item.tasknamelevel1 }}',
                'status': 'Skipped',
                'details': 'No change in record'
            }
        )

        has_any_changed_records = rail.IfOperator(
            task_id="has_any_changed_records",
            test="{{result('get_delta_records', 'length') > 0}}",
            yes_task='query_distinct_project_code',
            no_task='process_logs'
        )

        query_distinct_project_code = rail.QueryCollectionOperator(
            task_id='query_distinct_project_code',
            query="""SELECT DISTINCT(projectcode) FROM  get_delta_records""",
        )

        process_each_project_code = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_project_code',
            items="{{ result('query_distinct_project_code') }}",
            trigger_dag_id=f'sunovion_project_sync_process_each_code_child_{config.instance}',
            conf=request_payload.get_process_each_code_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_project_code = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_project_code',
            dag_runs='{{ result("process_each_project_code") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('input_data_with_md5'),
            header=['projectname', 'projectcode', 'projectdescription', 'status', 'allowtimeentry', 'startdate', 'enddate', 'projectmanager',
                    'costtype', 'projectleaderapprovalrequired', 'invoicecurrency', 'tasknamelevel1', 'taskcode', 'taskstatus', 'customfieldregistration', 'md5'],
            row=[
                '{{item.projectname}}',
                '{{item.projectcode}}',
                '{{item.projectdescription}}',
                '{{item.status}}',
                '{{item.allowtimeentry}}',
                '{{item.startdate}}',
                '{{item.enddate}}',
                '{{item.projectmanager}}',
                '{{item.costtype}}',
                '{{item.projectleaderapprovalrequired}}',
                '{{item.invoicecurrency}}',
                '{{item.tasknamelevel1}}',
                '{{item.taskcode}}',
                '{{item.taskstatus}}',
                '{{item.customfieldregistration}}',
                '{{item.md5}}'
            ]
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            new_filename=config.archive_filepath + '/Old_{{ result("get_reference_file_name") }}_{{ result("get_logging_details").current_time }}.csv',
            existing_filename=config.reference_file_path + '/{{ result("get_reference_file_name") }}'
        )

        update_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="update_new_reference_file",
            sftp_conn_id=config.sftp_conn_id,
            content="{{result('create_reference_file')}}",
            remote_filepath=config.reference_file_path + '/Ref_{{ result("new_file_sensor") | file_base }}_{{ result("get_logging_details").current_time }}.csv'
        )

        process_logs = rail.EmptyOperator(
            task_id='process_logs'
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file_name": "{{ result('get_logging_details').input_archive_filename }}",
                "log_file_name": "{{ result('get_logging_details').log_filename }}"
            }
        )

        new_file_sensor >> create_log >> get_logging_details >> is_csv >> rail.Label('Yes') >> download_file
        download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> load_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label("Yes") >> create_md5
        has_input_data >> rail.Label("No") >> fail_with_no_records
        create_md5 >> input_data_with_md5 >> list_reference_file >> has_files
        has_files >> rail.Label("Yes") >> get_reference_file_name >> get_reference_file >> parse_reference_file >> create_reference_data_collection
        create_reference_data_collection >> get_delta_records >> get_unchanged_records >> log_skipped_records >> has_any_changed_records
        has_any_changed_records >> rail.Label(
            "Yes") >> query_distinct_project_code >> process_each_project_code >> wait_for_process_each_project_code >> create_reference_file
        has_any_changed_records >> rail.Label(
            "No") >> process_logs
        create_reference_file >> archive_reference_file >> update_new_reference_file >> process_logs >> send_logs_enter
        send_logs_end >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
