from datetime import timedelta
import os
import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v2.utils import custom_method
from galaxyusopcoinc.workday_user_sync.time_off_plan_v2.utils import request_payload
# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'VialtoPartners_Time Off Plan Master V1.0 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
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

        # update this logic this doesn't work
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
            # yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            # trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_time_off_plan_data = rail.LoadCSVFileOperator(
            task_id='load_time_off_plan_data',
            document="{{ result('decrypt_file') }}",
            delimiter="|",
            encoding="utf-8-sig"
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
            query="""SELECT * FROM timeoffplandata WHERE
                    NULLIF(TimeOffPlan, '') IS NOT NULL AND
                    NULLIF(UnitOfTime, '') IS NOT NULL AND
                    NULLIF(Country, '') IS NOT NULL AND
                    NULLIF(ReferenceID, '') IS NOT NULL""",
            name="query_timeoff_plan_data"
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

        get_timeoff_balance_validation_script_uri = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_validation_script_uri',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response:{
                "only_admin_can_book_to_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Only Admin can book time off', 'uri')
            }
        )

        def get_oef_values(response, oefs_name):
            return list(filter(lambda x: x['oef_name'] in oefs_name , map(lambda row: {
                "oef_name": row['cells'][0]['textValue'],
                "oef_uri": row['cells'][1]['uri'],
            }, response['rows'])))

        get_booking_reference_id_oef_values= rail.RepliconServiceOperator(
            task_id='get_booking_reference_id_oef_values',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:object-extension-tag-definition-list-column:name",
                    "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response: get_oef_values(response, config.BOOKING_REFERENCE_ID_OEF_NAME)
        )

        create_time_off_type_collection = rail.CreateCollectionOperator(
            task_id="create_Time_off_type_collection",
            name="replicon_time_off_type",
            source="{{ result('get_all_time_off_type_description') | to_json }}"
        )

        get_all_disabled_time_off_type_feed = rail.QueryCollectionOperator(
            task_id='get_all_disabled_time_off_type_feed',
            query="""SELECT * FROM query_timeoff_plan_data
                    INNER JOIN replicon_time_off_type
                    ON query_timeoff_plan_data.ReferenceID = replicon_time_off_type.description AND replicon_time_off_type.enabled=0
                    AND (LOWER(query_timeoff_plan_data.TimeOffPlan) not LIKE "zdnu%")""",
            name="query_disabled_timeoff"
        )

        log_disabled_timeoff_types = rail.WriteLogOperator(
            task_id='log_disabled_timeoff_types',
            items= lambda: rail.result("get_all_disabled_time_off_type_feed"),
            message="Time Off Type is disabled in Replicon",
            severity='Exception',
            properties=lambda item:{
                'time_off_type_desc': item["ReferenceID"],
                'time_off_type_name': item["TimeOffPlan"],
                'unit_of_time': item["UnitOfTime"],
                'country': item['Country'],
                'status': 'Exception'
            }
        )

        get_all_create_time_off_type_feed = rail.QueryCollectionOperator(
            task_id='get_all_create_time_off_type_feed',
            query="""SELECT TimeOffPlan, UnitOfTime, Country, ReferenceID
                    FROM query_timeoff_plan_data
                    WHERE ReferenceID NOT IN (SELECT description from replicon_time_off_type WHERE description IS NOT NULL)
                     """,
            name="query_create_timeoff"
        )

        process_create_time_off_types = rail.TriggerDagRunForEachItemOperator(
            task_id='process_create_time_off_types',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('get_all_create_time_off_type_feed'),
            trigger_dag_id=config.create_timeoff_type_dag_id,
            conf=lambda item :request_payload.get_create_time_off_type_conf(item, config)
        )

        wait_for_process_create_time_off_types = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_create_time_off_types',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_create_time_off_types") }}',
        )

        query_timeoff_to_disable = rail.QueryCollectionOperator(
            task_id = "query_timeoff_to_disable",
            query="""SELECT * FROM query_timeoff_plan_data qct, replicon_time_off_type rtot
                    WHERE qct.ReferenceID == rtot.description
                    and (LOWER(qct.TimeOffPlan) LIKE "zdnu%")
                    and rtot.enabled == 1""",
            name= "query_timeoff_to_disable"
        )

        process_timeoff_disable = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_timeoff_disable",
            trigger_dag_id=config.disable_timeoff_type_dag_id,
            items = "{{ result('query_timeoff_to_disable') }}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name }}",
                "timeoff_name": "{{ item.name }}",
                "timeoff_description": "{{ item.ReferenceID }}",
                "unit_of_time": "{{ item.UnitOfTime }}",
                "timeoff_uri": "{{ item.uri }}",
                "feed_timeoff_name": "{{ item.TimeOffPlan }}",
                "country": "{{item.Country}}",
                "measurement_unit_uri":"{{ item.measurement_unit_uri }}",
                "minimum_timeoff_increment_policy_uri": "{{ item.minimum_timeoff_increment_policy_uri }}",
                "startEnd_time_specification_requirement_uri":" {{ item.startEnd_time_specification_requirement_uri }}",
                "timeoff_balance_tracking_option_uri": "{{ item.timeoff_balance_tracking_option_uri }}",
                "timeoff_display_format": "{{ item.timeoff_display_format }}",
                "current_timeoff_status": "{{ item.enabled }}",
                "action": 'disable'
            }

        )

        wait_for_process_timeoff_disable = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoff_disable',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timeoff_disable") }}',
        )


        query_timeoff_to_update = rail.QueryCollectionOperator(
            task_id = "query_timeoff_to_update",
            query="""SELECT * FROM query_timeoff_plan_data qct, replicon_time_off_type rtot WHERE qct.ReferenceID == rtot.description
                    AND (qct.TimeOffPlan != rtot.name)
                    AND qct.ReferenceID NOT IN (SELECT ReferenceID FROM query_timeoff_to_disable qttd
                        UNION
                        SELECT ReferenceID FROM query_disabled_timeoff qdt)""",
            name= "query_timeoff_to_update"
        )

        process_update_timeoff_name = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_update_timeoff_name",
            trigger_dag_id=config.update_timeoff_type_dag_id,
            items = "{{ result('query_timeoff_to_update') }}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name }}",
                "timeoff_name": "{{ item.name }}",
                "time_off_type_name": "{{ item.TimeOffPlan }}",
                "timeoff_description": "{{ item.ReferenceID }}",
                "timeoff_uri": "{{ item.uri }}",
                "feed_timeoff_name": "{{ item.TimeOffPlan }}",
                "unit_of_time":"{{item.UnitOfTime}}",
                "country": "{{item.Country}}",
                "measurement_unit_uri":"{{ item.measurement_unit_uri }}",
                "minimum_timeoff_increment_policy_uri": "{{ item.minimum_timeoff_increment_policy_uri }}",
                "startEnd_time_specification_requirement_uri":" {{ item.startEnd_time_specification_requirement_uri }}",
                "timeoff_balance_tracking_option_uri": "{{ item.timeoff_balance_tracking_option_uri }}",
                "timeoff_display_format": "{{ item.timeoff_display_format }}",
                "current_timeoff_status": "{{ item.enabled }}",
                "action": "update"
            }
        )

        wait_for_process_timeoff_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timeoff_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_update_timeoff_name") }}',
        )

        # get_distinct_country_from_created_time_off_type = rail.QueryCollectionOperator(
        #     task_id='get_distinct_country_from_created_time_off_type',
        #     query="""SELECT DISTINCT Country FROM query_create_timeoff""",
        #     name="query_distinct_country"
        # )

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
        download_file >> archive_file >> decrypt_file >> has_decrypted_file >> rail.Label("Yes") >> has_file_content >> rail.Label(
            "Yes") >> load_time_off_plan_data
        has_decrypted_file >> rail.Label("No") >> fail_decryption_file
        has_file_content >> rail.Label("No") >> send_blank_payload_email
        load_time_off_plan_data >> create_time_off_plan_data_collection >> has_time_off_plan_data
        has_time_off_plan_data >> rail.Label("No") >> send_blank_payload_email
        has_time_off_plan_data >> rail.Label("Yes") >> query_time_off_plan_data
        query_time_off_plan_data >> get_all_time_off_types_uri >> get_all_time_off_type_description
        get_all_time_off_type_description >> get_timeoff_balance_validation_script_uri >> get_booking_reference_id_oef_values
        get_booking_reference_id_oef_values >> create_time_off_type_collection >> [get_all_disabled_time_off_type_feed,
             get_all_create_time_off_type_feed, query_timeoff_to_disable]
        get_all_disabled_time_off_type_feed >> log_disabled_timeoff_types
        log_disabled_timeoff_types >> query_timeoff_to_update >> process_update_timeoff_name >> wait_for_process_timeoff_update\
         >> generate_output_log
        get_all_create_time_off_type_feed >> process_create_time_off_types >> wait_for_process_create_time_off_types
        query_timeoff_to_disable >> process_timeoff_disable >> wait_for_process_timeoff_disable >> query_timeoff_to_update
        wait_for_process_create_time_off_types >> query_timeoff_to_update
        generate_output_log >> [
            get_successful_logs, get_errored_logs, get_exception_logs] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        download_file >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)
