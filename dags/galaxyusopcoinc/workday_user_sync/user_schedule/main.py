from datetime import timedelta
import os
import rail
from galaxyusopcoinc.workday_user_sync.user_schedule.utils import request_payload
# pylint: disable=too-many-statements

def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_user_schedule_master{dag_id_postfix}',
        description=f'VialtoPartners_User Schedule Master V1.0 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
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
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon User Schedule Sync - Incorrect Format - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        has_decrypted_file = rail.IfOperator(
            task_id='has_decrypted_file',
            test=lambda: rail.result('decrypt_file'),
            yes_task='has_file_content',
            no_task='fail_decryption_file'
        )

        fail_decryption_file = rail.FailOperator(
            task_id="fail_decryption_file",
            message="File Decryption Failed",
        )

        def do_has_file_content():
            with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0
        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=do_has_file_content,
            yes_task='load_user_schedule_data',
            no_task='send_blank_payload_email'
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

        load_user_schedule_data = rail.LoadCSVFileOperator(
            task_id='load_user_schedule_data',
            document="{{ result('decrypt_file') }}",
            delimiter="|"
        )

        create_user_schedule_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_schedule_data_collection',
            source="{{ result('load_user_schedule_data') }}",
            name="userscheduledata",
        )

        has_user_schedule_data = rail.IfOperator(
            task_id='has_user_schedule_data',
            test="{{ result('create_user_schedule_data_collection','length') > 0 }}",
            yes_task='query_user_schedule_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Schedule Sync - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/blank_payload.html"
        )

        query_user_schedule_data = rail.QueryCollectionOperator(
            task_id='query_user_schedule_data',
            query="""SELECT * FROM userscheduledata
                    WHERE  NULLIF(EmployeeId, '') IS NOT NULL""",
            name="queryuserscheduledata"
        )

        add_replicon_schedule_name_data = rail.QueryCollectionOperator(
            task_id='add_replicon_schedule_name_data',
            query="""SELECT *, (e.MondayHours || "|" || e.TuesdayHours || "|" || e.WednesdayHours || "|" || e.ThursdayHours || "|" || \
                e.FridayHours || "|" || e.SaturdayHours || "|" || e.SundayHours) AS replicon_schedule_name FROM queryuserscheduledata e""",
            name="addrepliconschedulenamedata"
        )

        query_distinct_schedule = rail.QueryCollectionOperator(
            task_id='query_distinct_schedule',
            query="SELECT DISTINCT replicon_schedule_name FROM addrepliconschedulenamedata WHERE replicon_schedule_name IS NOT NULL"
        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id='get_all_office_schedule',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        create_office_schedule_collection = rail.CreateCollectionOperator(
            task_id="create_office_schedule_collection",
            name="replicon_office_schedule",
            source="{{ result('get_all_office_schedule') | to_json }}"
        )

        query_new_schedules = rail.QueryCollectionOperator(
            task_id='query_new_schedules',
            query='''SELECT * FROM query_distinct_schedule
                    WHERE replicon_schedule_name IS NOT NULL AND replicon_schedule_name NOT IN
                    (SELECT DISTINCT Displaytext FROM replicon_office_schedule)'''
        )

        process_new_schedules = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_schedules',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_new_schedules'),
            trigger_dag_id=f'vialtopartners_new_schedule_child_dag_{config.instance}',
            conf={
                'scheduletype': '{{ item.replicon_schedule_name }}'
            }
        )

        wait_for_process_new_schedules = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_schedules',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_schedules") }}',
        )

        process_each_user_schedule_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_user_schedule_records',
            retries=0,
            items="{{ result('add_replicon_schedule_name_data') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'vialtopartners_user_schedule_child_dag{dag_id_postfix}',
            conf=request_payload.get_process_each_user_schedule_records_conf
        )

        wait_for_process_each_user_schedule_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_user_schedule_records',
            dag_runs='{{ result("process_each_user_schedule_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_successful_logs = rail.FilterLogEntriesOperator(
            task_id='get_successful_logs',
            properties={'status': 'Success'}
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_logs',
            properties={'status': 'Exception'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'Schedule',
                'Enployee ID',
                'Status',
                'Details',
                'Job ID'],
            row=[
                '{{ item.properties.schedulename }}',
                '{{ item.properties.employeeid }}',
                '{{ item.properties.status }}',
                '{{ item.properties.message }}',
                '{{ item.ecid }}'],
            footer=[
                'Number of Records Processed Successfully: {{result("get_successful_logs", key="length")}}',
                'Number of Records with Error: {{ result("get_errored_logs", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_logs", key="length") }}',
                '',
                ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv')

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Schedule sync - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        new_file_sensor >> is_csv
        is_csv >> rail.Label("No") >> send_bad_file_format_email
        is_csv >> rail.Label("Yes") >> download_file
        download_file >> decrypt_file >> has_decrypted_file >> rail.Label("Yes") >> has_file_content >> rail.Label(
            "Yes") >> load_user_schedule_data
        has_decrypted_file >> rail.Label("No") >> fail_decryption_file
        has_file_content >> rail.Label("No") >> send_blank_payload_email
        load_user_schedule_data >> create_user_schedule_data_collection >> has_user_schedule_data
        has_user_schedule_data >> rail.Label("No") >> send_blank_payload_email
        has_user_schedule_data >> rail.Label("Yes") >> query_user_schedule_data
        query_user_schedule_data >> add_replicon_schedule_name_data >> query_distinct_schedule
        query_distinct_schedule >> get_all_office_schedule >> create_office_schedule_collection
        create_office_schedule_collection >> query_new_schedules
        query_new_schedules >> process_new_schedules >> wait_for_process_new_schedules
        wait_for_process_new_schedules >> process_each_user_schedule_records >> wait_for_process_each_user_schedule_records >> generate_output_log
        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        generate_output_log >> [
            get_successful_logs, get_errored_logs, get_exception_logs] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

        return dag


rail.for_each_instance(create_main_dag)
