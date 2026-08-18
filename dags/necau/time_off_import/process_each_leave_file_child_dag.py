from datetime import timedelta
import rail
from necau.time_off_import.utils import request_payload
null = None

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_process_each_leave_file_child_{config.instance}',
        description=f'NECAU - process_each_leave_file_child_v3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        download_leave_file = rail.SFTPDownloadFileOperator(
            task_id='download_leave_file',
            remote_filepath=config.processing_file_directory +
            '/' + "{{ dag_run.conf.file_name }}",
        )

        load_leave_data = rail.LoadCSVFileOperator(
            task_id='load_leave_data',
            document="{{ result('download_leave_file') }}"
        )

        create_leave_data_collection = rail.CreateCollectionOperator(
            task_id='create_leave_data_collection',
            source="{{ result('load_leave_data') }}",
            name="inputdatacollection",
            columns={'detnumber': 'staff_member',
                     'detsurname': 'surname',
                     'detprefnm': 'preferred_name',
                     'detg1name1': 'preferred_name_1',
                     'lphlap': 'form_code',
                     'wfhscnmne': 'form_code_1',
                     'wfhscnmne.trn': 'form_description',
                     'lapkey': 'request_key',
                     'lphkey': 'request_key_1',
                     'wfhkey': 'request_key_2',
                     'wfhcreation': 'creation_date',
                     'wfhtime': 'creation_time',
                     'lapseq': 'seq_no',
                     'lphseqn': 'seq_no_1',
                     'wfhsequence': 'seq_no_2',
                     'laptypecd': 'leave_type',
                     'lphtypecd': 'leave_type_1',
                     'wfhcode': 'leave_type_2',
                     'laptypecd.trn': 'leave_description',
                     'lphtypecd.trn': 'leave_description_1',
                     'wfhdescr': 'leave_description_2',
                     'lapstart': 'start_date',
                     'lphstart': 'start_date_1',
                     'wfhstartdt': 'start_date_2',
                     'lapend': 'end_date',
                     'lphend': 'end_date_1',
                     'wfhenddate': 'end_date_2',
                     'lapworkflow.trn': 'action_status',
                     'lphapproved': 'action_status_1',
                     'wfhactstat.trn': 'action_status_2',
                     'lapdaytake': 'days_taken',
                     'lphdaytake': 'days_taken_1',
                     'laphrstake': 'hours_taken',
                     'lphhrstake': 'hours_taken_1'
                     }
        )

        query_timeoff_data = rail.QueryCollectionOperator(
            task_id='query_timeoff_data',
            query='SELECT * FROM inputdatacollection Where staff_member != "Staff Member"'
        )

        query_timeoff_has_data = rail.IfOperator(
            task_id="query_timeoff_has_data",
            test="{{ result('query_timeoff_data','length') > 0 }}",
            yes_task='create_shift_assignment_log',
            no_task='send_no_data_email'
        )

        create_shift_assignment_log = rail.CreateLogOperator(
            task_id='create_shift_assignment_log',
        )

        create_file_processing_log = rail.CreateLogOperator(
            task_id='create_file_processing_log',
        )

        process_leaves = rail.TriggerDagRunForEachItemOperator(
            task_id='process_leaves',
            retries=0,
            items="{{ result('query_timeoff_data') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_timeoff_import_child_{config.instance}',
            conf=request_payload.get_user_and_time_off_info
        )

        wait_for_process_leaves = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_leaves',
            dag_runs='{{ result("process_leaves") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        move_files_to_archive = rail.SFTPMoveFileOperator(
            task_id='move_files_to_archive',
            existing_filename=config.processing_file_directory +
            '/{{ dag_run.conf.file_name }}',
            new_filename=config.archive_file_directory +
            '/{{ dag_run.conf.file_name }}'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_file_processing_log') }}",
            header=[
                'jobid',
                'Staff Member',
                'Status',
                'reason',
                'Request Key'],
            row=[
                '{{ item.properties | attr_or_default("jobid", "") }}',
                '{{ item.properties | attr_or_default("Staff Member", "") }}',
                '{{ item.properties | attr_or_default("Status", "")}}',
                '{{ item.properties | attr_or_default("reason", "") }}',
                '{{ item.properties | attr_or_default("Request Key", "") }}'],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            log="{{ result('create_file_processing_log') }}",
            task_id="get_errored_logs",
            properties={'Status': 'Error'}
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Time Off Import Completed {{ dag_run.conf.file_name }}',
            html_content="templates/email/no_data.html",
        )

        download_leave_file >> load_leave_data >> create_leave_data_collection >> \
            query_timeoff_data >> query_timeoff_has_data
        query_timeoff_has_data >> rail.Label(
            "Yes") >> create_shift_assignment_log >> create_file_processing_log >> process_leaves >> wait_for_process_leaves >> \
            render_logs_csv >> generate_download_link >> get_errored_logs >> move_files_to_archive
        query_timeoff_has_data >> rail.Label(
            "No") >> send_no_data_email >> move_files_to_archive
    return dag


rail.for_each_instance(create_dag)
