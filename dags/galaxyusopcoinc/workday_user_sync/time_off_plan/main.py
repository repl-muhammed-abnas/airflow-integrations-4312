from datetime import timedelta
import os
import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan.utils import custom_method
from galaxyusopcoinc.workday_user_sync.time_off_plan.utils import request_payload
# pylint: disable=too-many-statements


def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_time_off_plan_master{dag_id_postfix}',
        description=f'VialtoPartners_Time Off Plan Master V1.0 - SFTP {config.instance}',
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
            subject='{{ get_company_key() }} | Replicon Time Off Plan Sync - Incorrect Format - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
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
            yes_task='load_time_off_plan_data',
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

        load_time_off_plan_data = rail.LoadCSVFileOperator(
            task_id='load_time_off_plan_data',
            document="{{ result('decrypt_file') }}",
            delimiter="|"
        )

        create_time_off_plan_data_collection = rail.CreateCollectionOperator(
            task_id='create_time_off_plan_data_collection',
            source="{{ result('load_time_off_plan_data') }}",
            name="timeoffplandata",
        )

        has_time_off_plan_data = rail.IfOperator(
            task_id='has_time_off_plan_data',
            test="{{ result('create_time_off_plan_data_collection','length') > 0 }}",
            yes_task='query_time_off_plan_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Time Off Plan Sync - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/blank_payload.html"
        )

        query_time_off_plan_data = rail.QueryCollectionOperator(
            task_id='query_time_off_plan_data',
            query="""SELECT * FROM timeoffplandata
                    WHERE  NULLIF(TimeOffPlan, '') IS NOT NULL AND NULLIF(UnitOfTime, '') IS NOT NULL AND NULLIF(Country, '') IS NOT NULL
                    AND NULLIF(ReferenceID, '') IS NOT NULL""",
            name="querytimeoffplandata"
        )

        get_all_time_off_types_uri = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_uri',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            response_filter=custom_method.map_time_off_uri
        )

        get_all_time_off_type_description = rail.RepliconServiceOperator(
            task_id='get_all_time_off_type_description',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {
                "timeOffTypeUris": rail.result("get_all_time_off_types_uri")
            },
            response_filter=custom_method.map_time_off_uri_description
        )

        create_time_off_type_collection = rail.CreateCollectionOperator(
            task_id="create_Time_off_type_collection",
            name="replicon_time_off_type",
            source="{{ result('get_all_time_off_type_description') | to_json }}"
        )

        get_all_disabled_time_off_type_feed = rail.QueryCollectionOperator(
            task_id='get_all_disabled_time_off_type_feed',
            query="""SELECT querytimeoffplandata.ReferenceID, replicon_time_off_type.enabled , replicon_time_off_type.uri
                    FROM querytimeoffplandata
                    INNER JOIN replicon_time_off_type
                    ON querytimeoffplandata.ReferenceID = replicon_time_off_type.description AND replicon_time_off_type.enabled=0 """,
            name="querydisabledtimeoff"
        )

        process_enable_time_off_types = rail.TriggerDagRunForEachItemOperator(
            task_id='process_enable_time_off_types',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('get_all_disabled_time_off_type_feed'),
            trigger_dag_id=f'vialtopartners_time_off_enable_create_child_{config.instance}',
            conf=request_payload.get_enabled_time_off_type_conf
        )

        wait_for_process_enable_time_off_types = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_enable_time_off_types',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_enable_time_off_types") }}',
        )

        get_all_create_time_off_type_feed = rail.QueryCollectionOperator(
            task_id='get_all_create_time_off_type_feed',
            query="""SELECT TimeOffPlan, UnitOfTime, Country, ReferenceID
                    FROM querytimeoffplandata
                    WHERE ReferenceID NOT IN (SELECT description from replicon_time_off_type WHERE description IS NOT NULL)
                     """,
            name="querycreatetimeoff"
        )

        process_create_time_off_types = rail.TriggerDagRunForEachItemOperator(
            task_id='process_create_time_off_types',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('get_all_create_time_off_type_feed'),
            trigger_dag_id=f'vialtopartners_time_off_enable_create_child_{config.instance}',
            conf=request_payload.get_create_time_off_type_conf
        )

        wait_for_process_create_time_off_types = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_create_time_off_types',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_create_time_off_types") }}',
        )

        get_distinct_country_from_created_time_off_type = rail.QueryCollectionOperator(
            task_id='get_distinct_country_from_created_time_off_type',
            query="""SELECT DISTINCT Country FROM querycreatetimeoff""",
            name="querycountry"
        )

        process_each_country_time_off = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_country_time_off',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result(
                'get_distinct_country_from_created_time_off_type'),
            trigger_dag_id=f'vialtopartners_update_user_sync_mapper_child_{config.instance}',
            conf={
                "country": "{{ item.Country }}"
            }
        )

        wait_for_process_each_country_time_off = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_country_time_off',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_country_time_off") }}',
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
                'Time Off Type Description',
                'Time Off Type Name',
                'Unit Of Time',
                'Country',
                'Status',
                'Details',
                'Job ID'],
            row=[
                '{{ item.properties.time_off_type_desc }}',
                '{{ item.properties.time_off_type_name }}',
                '{{ item.properties.unit_of_time }}',
                '{{ item.properties.country }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
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
            subject='{{ get_company_key() + " | Replicon Time Off Plan Integration - " }} \
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
            "Yes") >> load_time_off_plan_data
        has_decrypted_file >> rail.Label("No") >> fail_decryption_file
        has_file_content >> rail.Label("No") >> send_blank_payload_email
        load_time_off_plan_data >> create_time_off_plan_data_collection >> has_time_off_plan_data
        has_time_off_plan_data >> rail.Label("No") >> send_blank_payload_email
        has_time_off_plan_data >> rail.Label("Yes") >> query_time_off_plan_data
        query_time_off_plan_data >> get_all_time_off_types_uri >> get_all_time_off_type_description
        get_all_time_off_type_description >> create_time_off_type_collection >> get_all_disabled_time_off_type_feed
        get_all_disabled_time_off_type_feed >> process_enable_time_off_types >> wait_for_process_enable_time_off_types
        wait_for_process_enable_time_off_types >> get_all_create_time_off_type_feed
        get_all_create_time_off_type_feed >> process_create_time_off_types >> wait_for_process_create_time_off_types
        wait_for_process_create_time_off_types >> get_distinct_country_from_created_time_off_type
        get_distinct_country_from_created_time_off_type >> process_each_country_time_off >> wait_for_process_each_country_time_off >> generate_output_log
        generate_output_log >> [
            get_successful_logs, get_errored_logs, get_exception_logs] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)
