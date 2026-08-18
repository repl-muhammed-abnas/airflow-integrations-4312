from datetime import timedelta
import rail
from dxctechnology.compass_attributes_1_and_2.utils import request_payload
from dxctechnology.compass_attributes_1_and_2.utils import python_callable_method


null = None

# pylint: disable=too-many-statements


def create_attribute_2_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_2_master_{config.dag_id_postfix}',
        description=f'DXC_Compass_Attribute 2 Master V1.0 {config.dag_id_postfix}',
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
            path=config.input_filepath_attr2,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        is_attributes_2_file = rail.IfOperator(
            task_id='is_attributes_2_file',
            test='{{ result("new_file_sensor") | file_base | matches("Attributes_2")}}',
            yes_task='is_xml',
            no_task='send_bad_file_name_email'
        )

        send_bad_file_name_email = rail.EmailOperator(
            task_id='send_bad_file_name_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon task sync for Compass Attribute 2 - Incorrect File Name - {{ current_time() }}',
            html_content='templates/email/bad_file_format.html',
            params={'attributenumber': '2'}
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
            subject='{{ get_company_key() }} | Replicon task sync for Compass Attribute 2 - Incorrect file format {{ current_time() }}',
            html_content='templates/email/bad_file_format.html',
            params={'attributenumber': '2'}
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
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
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document='{{ result("download_file") }}',
            xsd_document='./dags/dxctechnology/compass_attributes_1_and_2/xsdschema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='get_all_customfields',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task sync for Compass Attribute 2 - Blank Payload  {{ current_time() }}',
            html_content='templates/email/blank_payload.html',
            params={'attributenumber': '2'}
        )

        get_all_customfields = rail.RepliconServiceOperator(
            task_id='get_all_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:task"}
        )

        get_all_customfield_drop_down_options = rail.RepliconServiceOperator(
            task_id='get_all_customfield_drop_down_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{ result("get_all_customfields") | find_first_by_attr_and_get_attr("displayText","Task Type","uri") }}'
            }
        )

        def map_attributes(json):
            attributes = json['Attributes']
            if not attributes:
                return null
            return list(map(lambda x: {
                'attributenumber':  x['AttributeNumber'][0].get('#text'),
                'attribute':  x['Attribute'][0].get('#text'),
                'description': x['Description'][0].get('#text'),
                'enddate': x['EndDate'][0].get('#text')
            }, attributes))

        get_wbs_records_from_xml = rail.XMLAdaptorOperator(
            task_id='get_wbs_records_from_xml',
            source='{{  result("parse_xml") }}',
            target='result',
            adaptor=[
                'Records',
                {
                    'wbs': 'WBS/text()',
                    'attributes': map_attributes
                }
            ],
        )

        filter_valid_wbs_records = rail.PythonOperator(
            task_id='filter_valid_wbs_records',
            python_callable=python_callable_method.get_valid_wbs_records,
            op_args=[True, config.wbs_skiplist]
        )

        filter_blank_wbs_records = rail.PythonOperator(
            task_id='filter_blank_wbs_records',
            python_callable=python_callable_method.get_blank_wbs_records
        )

        get_wbs_attributes_count = rail.PythonOperator(
            task_id='get_wbs_attributes_count',
            python_callable=python_callable_method.get_attributescount
        )

        check_all_wbs_blank = rail.IfOperator(
            task_id='check_all_wbs_blank',
            test=lambda: len(rail.result('filter_blank_wbs_records')) > 0
                    and len(rail.result('filter_valid_wbs_records')) == 0,
            yes_task='log_wbs_name_blank',
            no_task=['has_valid_wbs_records', 'has_blank_wbs_records']
        )

        has_valid_wbs_records = rail.IfOperator(
            task_id='has_valid_wbs_records',
            test=lambda: len(rail.result('filter_valid_wbs_records')) > 0,
            yes_task='process_each_wbs_attribute',
            no_task='generate_output_log',
        )

        has_blank_wbs_records = rail.IfOperator(
            task_id='has_blank_wbs_records',
            test=lambda: len(rail.result('filter_blank_wbs_records')) > 0,
            yes_task='log_wbs_name_blank',
            no_task='generate_output_log',
        )

        log_wbs_name_blank = rail.WriteLogOperator(
            task_id='log_wbs_name_blank',
            message='WBS name is blank',
            items='{{ result("filter_blank_wbs_records") | to_json }}',
            properties={
                'wbs': 'na',
                'attributename': '',
                'attributenumber': '',
                'action': null,
                'status': 'skipped',
                'details': 'WBS name is blank',
                'recordcount': '{{ item.attributes | length }}'
            }
        )

        process_each_wbs_attribute = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_wbs_attribute',
            retries=0,
            items='{{ result("filter_valid_wbs_records") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_compass_attribute_2_process_wbs_child_{config.dag_id_postfix}',
            conf=lambda item: request_payload.get_process_each_wbs(
                item, 'Attribute 2')
        )

        wait_for_process_each_wbs_attribute = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_wbs_attribute',
            dag_runs='{{ result("process_each_wbs_attribute") }}',
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
            source="{{ get_master_log() }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("get_wbs_records_from_xml", key="length") }}',
                'COMPASS WBS Attributes 1 & 2 Inbound',
                '',
                ''],
            row=[
                '{{ item.properties | attr_or_default("wbs", "") }}',
                '{{ item.properties | attr_or_default("attributename", "") }}',
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
            subject = '{{ get_company_key() }} | Replicon task sync for Compass Attributes 2 - Uploading Logs to SFTP failed  {{ current_time() }}'
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.tenant_email,
                bcc=config.alert_email,
                subject=subject,
                html_content='templates/email/sftp_upload_failed.html',
                params={
                    'dag_id': f'dxctechnology_compass_attribute_2_master_{config.dag_id_postfix}',
                    'attributenumber': '2'
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
            subject='{{ get_company_key() + " | Replicon task sync for Compass Attributes 2 -  " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content='templates/email/import_complete.html',
            params={
                'log_filepath': config.log_filepath,
                'attributenumber': '2'
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
            test='{{ result("new_file_sensor") | is_truthy }}',
            yes_task='log_to_sumo'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'filename ': '{{ result("new_file_sensor") | file_name }}',
                'wbscount': '{{ result("filter_valid_wbs_records") | length }}',
                'attributecount': '{{ result("get_wbs_attributes_count") }}',
                'attributenumber': '2'
            }
        )

        new_file_sensor >> is_attributes_2_file >> rail.Label(
            'Yes') >> is_xml
        is_attributes_2_file >> rail.Label(
            'No') >> send_bad_file_name_email

        is_xml >> rail.Label(
            'Yes') >> download_file >> parse_xml >> has_data
        is_xml >> rail.Label('No') >> send_bad_file_format_email

        has_data >> rail.Label(
            'Yes') >> get_all_customfields >> get_all_customfield_drop_down_options >> \
            get_wbs_records_from_xml >> filter_valid_wbs_records >> filter_blank_wbs_records >> \
            get_wbs_attributes_count >> check_all_wbs_blank

        check_all_wbs_blank >> rail.Label(
            'Yes') >> log_wbs_name_blank >> generate_output_log
        check_all_wbs_blank >> rail.Label(
            'No') >> [has_valid_wbs_records, has_blank_wbs_records]

        has_valid_wbs_records >> rail.Label(
            'Yes') >> process_each_wbs_attribute >> wait_for_process_each_wbs_attribute >> generate_output_log
        has_valid_wbs_records >> rail.Label(
            'No') >> generate_output_log >> [get_errored_logs, get_exception_logs, get_success_logs] >> render_logs_csv \
            >> upload_log_to_sftp >> send_import_complete_email >> finish

        has_blank_wbs_records >> rail.Label(
            'Yes') >> log_wbs_name_blank >> generate_output_log
        has_blank_wbs_records >> rail.Label(
            'No') >> generate_output_log

        has_data >> rail.Label('No') >> send_blank_payload_email
        # was_new_file_found has trigger_rule = 'all_done', so it will execute whenever download_file is done, regardless of whether it
        # succeeded, failed, or was skipped
        download_file >> rail.Label(
            'Always') >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        finish >> catch_and_log_errors >> check_if_new_file_found
        check_if_new_file_found >> rail.Label(
            'Yes') >> log_to_sumo

    return dag


rail.for_each_instance(create_attribute_2_master_dag)
