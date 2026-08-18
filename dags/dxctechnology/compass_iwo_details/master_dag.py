from datetime import timedelta
import rail
from dxctechnology.compass_iwo_details.utils import request_payload
from dxctechnology.compass_iwo_details.utils import response_filter
from dxctechnology.compass_iwo_details.utils import python_callable_method
from dxctechnology.compass_iwo_details.task.generate_report_batch import report_batch

null = None

# pylint: disable=too-many-statements


def create_iwo_details_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_details_master_{config.dag_id_postfix}',
        description=f'DXC_COMPASS_IWO_Details - Master V2.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }}|Replicon resource assignment sync for Compass IWO Details - Incorrect File Format {{ current_time("%d%m%YT%H%M%S") }}',
            html_content='templates/email/bad_file_format.html',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath='{{ result("new_file_sensor") }}',
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
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            '/{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document='{{ result("download_file") }}',
            xsd_document='./dags/dxctechnology/compass_iwo_details/xsdschema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records/Header") | length > 0 }}',
            yes_task='generate_report',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon resource assignment sync for Compass IWO Details - No records to process {{ current_time("%d%m%YT%H%M%S") }}',
            html_content='templates/email/blank_payload.html',
        )

        generate_report = rail.EmptyOperator(task_id='generate_report')

        load_report, create_report_collection = report_batch(config)

        get_all_object_extension_field = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={
                "bindingContextUri": "urn:replicon:object-type:project"},
            response_filter=response_filter.get_oef_details
        )

        def map_assignments(json):
            assignments = json['Assignments']
            return list(map(lambda x: {
                'compasspersonnelnumber':  x['CompassPersonnelNumber'][0].get('#text'),
                'assignmentstartdate':  x['AssignmentStart'][0].get('#text'),
                'assignmentenddate': x['AssignmentEnd'][0].get('#text')
            }, assignments)) if assignments else []

        get_records_from_xml = rail.XMLAdaptorOperator(
            task_id='get_records_from_xml',
            source='{{  result("parse_xml") }}',
            target='result',
            adaptor=[
                'Records',
                {
                    'wbs': 'Header/WBS/text()',
                    'parentcompanycode': 'Header/ParentCompanyCode/text()',
                    'parentwbs': 'Header/ParentWBS/text()',
                    'parentserviceorder': 'Header/ParentServiceOrder/text()',
                    'parentproject': 'Header/ParentProject/text()',
                    'assignments': map_assignments
                }
            ],
        )

        get_unique_users_from_feed = rail.PythonOperator(
            task_id="get_unique_users_from_feed",
            python_callable=python_callable_method.unique_users_from_feed
        )

        query_required_users = rail.QueryCollectionOperator(
            task_id="query_required_users",
            query="""SELECT * FROM userdatafromreplicon WHERE NULLIF(employeeid, '') IS NOT NULL AND employeeid IN ({{result('get_unique_users_from_feed')}})"""
        )

        get_query_required_users = rail.DataAdaptorOperator(
            task_id="get_query_required_users",
            source='{{result("query_required_users")}}',
            columns=['employeeid', 'uri', 'userstatus', 'userenddate'],
            data=python_callable_method.get_query_required_users
        )

        process_iwo_wbs_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_iwo_wbs_update',
            retries=0,
            items=lambda: rail.result('get_records_from_xml'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_iwo_wbs_update_child_{config.dag_id_postfix}',
            conf=request_payload.get_iwo_wbs_update,
        )

        wait_for_process_iwo_wbs_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_iwo_wbs_update',
            dag_runs='{{ result("process_iwo_wbs_update") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_logs',
            properties={'status': 'Exception'}
        )

        get_success_logs = rail.FilterLogEntriesOperator(
            task_id='get_success_logs',
            properties={'status': 'Success'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ get_master_log() }}',
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("get_records_from_xml", key="length") }}',
                'COMPASS IWO Details Inbound',
                '',
                ''],
            row=[
                '{{ item.properties.wbs }}',
                '{{ item.properties.employeeid }}',
                '{{ item.properties.status }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'],
            footer=[
                # pylint: disable=line-too-long
                'Number of Records Processed Successfully: {{ result("get_success_logs", key="length") }}',
                'Number of Records with Error: {{ result("get_errored_logs", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_logs", key="length") }}',
                '',
                ''],
        )

        def file_upload_failed(context):
            # pylint: disable=line-too-long
            subject = '{{ get_company_key() }} | Replicon resource assignment sync for Compass IWO Details - Uploading Logs to SFTP failed {{ current_time("%d%m%YT%H%M%S") }}'
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.tenant_email,
                bcc=config.alert_email,
                subject=subject,
                html_content='templates/email/sftp_upload_failed.html',
                params={
                    'dag_id': f'dxctechnology_compass_iwo_details_master_{config.dag_id_postfix}'
                },
                files=[
                    ('{{ result("render_logs_csv") }}')
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("render_logs_csv") }}',
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
            on_failure_callback=file_upload_failed
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon resource assignment sync for Compass IWO Details -  " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%d%m%YT%H%M%S") }}',
            html_content='templates/email/import_complete.html',
            params={
                'log_filepath': config.log_filepath,
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            trigger_rule='all_done',
            test='{{ result("new_file_sensor") | is_truthy and result("parse_xml") | is_truthy }}',
            yes_task='log_to_sumo'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename ': '{{ result("new_file_sensor") | file_name }}',
                'recordcount': '{{ result("get_records_from_xml") | length  if result("get_records_from_xml")}}'
            }
        )

        new_file_sensor >> is_xml

        is_xml >> rail.Label(
            'Yes') >> download_file >> parse_xml >> has_data
        is_xml >> rail.Label("No") >> send_bad_file_format_email

        has_data >> rail.Label(
            'Yes') >> generate_report >> load_report >> create_report_collection \
            >> get_all_object_extension_field >> get_records_from_xml \
            >> get_unique_users_from_feed >> query_required_users >> get_query_required_users \
            >> process_iwo_wbs_update >> wait_for_process_iwo_wbs_update >> generate_output_log \
            >> [get_errored_logs, get_exception_logs, get_success_logs] \
            >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email >> finish

        has_data >> rail.Label('No') >> send_blank_payload_email

        download_file >> rail.Label(
            'Always') >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        finish >> catch_and_log_errors >> check_if_new_file_found >> log_to_sumo

    return dag


rail.for_each_instance(create_iwo_details_master_dag)
