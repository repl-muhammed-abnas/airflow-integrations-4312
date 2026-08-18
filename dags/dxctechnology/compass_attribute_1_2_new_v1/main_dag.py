from datetime import timedelta
import rail
from dxctechnology.compass_attribute_1_2_new_v1 import request_payload
from dxctechnology.compass_attribute_1_2_new_v1.task.get_system_level_attribute import get_system_level_attribute
# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_attribute_1_2_master_v1_new_{config.instance}_{config.sub_erp}_{config.attribute}',
        description=f'DXC_Compass_Attribute 1 & 2 V2.0 {config.instance}',
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

        is_attributes_1_2_file = rail.IfOperator(
            task_id='is_attributes_1_2_file',
            test="{{ result('new_file_sensor') | file_base | matches(['Attributes_1', 'Attributes_2'])}}",
            yes_task='is_xml',
            no_task='send_bad_file_name_email'
        )

        send_bad_file_name_email = rail.EmailOperator(
            task_id='send_bad_file_name_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon project field sync - Incorrect File Name - {{ current_time() }}',
            html_content="email_bad_file_name.html",
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon project field sync for compass attribute {{ 1 if "Attributes_1" in result("new_file_sensor") else 2 }} - Incorrect file format - {{ current_time() }}',
            html_content="email_bad_file_format.html",
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
            "/{{ dag_run.run_id | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}")

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}",
            xsd_document='./dags/dxctechnology/compass_attribute_1_2_new/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='get_records_missing_wbs_from_xml',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project field sync for compass attribute {{ 1 if "Attributes_1" in result("new_file_sensor") \
                else 2 }} - Blank Payload - {{ current_time() }}',
            html_content="email_blank_payload.html",
        )

        get_records_missing_wbs_from_xml = rail.XMLAdaptorOperator(
            task_id="get_records_missing_wbs_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/Attributes[not(../WBS/text())]',
                {
                    'WBS': '../WBS/text()',
                    'AttributeNumber': 'AttributeNumber/text()',
                    'Attribute': 'Attribute/text()',
                    'Description': 'Description/text()',
                    'EndDate': 'EndDate/text()',
                },
            ],
        )

        create_missing_wbs_collection = rail.CreateCollectionOperator(
            task_id="create_missing_wbs_collection",
            source='{{ result("get_records_missing_wbs_from_xml") }}',
            columns=[
                'WBS',
                'AttributeNumber',
                'Attribute',
                'Description',
                'EndDate'],
        )

        check_any_blank_wbs_present = rail.IfOperator(
            task_id='check_any_blank_wbs_present',
            test="{{ result('create_missing_wbs_collection','length') > 0 }}",
            yes_task='log_blank_wbs',
            no_task='get_wbs_records_from_xml',
        )

        log_blank_wbs = rail.WriteLogOperator(
            task_id='log_blank_wbs',
            message="WBS name is blank",
            properties={
                'Level': "File",
                'wbs': "na",
                'attributename': "",
                'attributenumber': "",
                'action': 'na',
                'status': "Exception",
                'recordcount': '{{ result("create_missing_wbs_collection","length")}}',
            }
        )

        get_wbs_records_from_xml = rail.XMLAdaptorOperator(
            task_id="get_wbs_records_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/Attributes[../WBS/text()]',
                {
                    'WBS': '../WBS/text()',
                    'AttributeNumber': 'AttributeNumber/text()',
                    'Attribute': 'Attribute/text()',
                    'Description': 'Description/text()',
                    'EndDate': 'EndDate/text()',
                },
            ],
        )

        create_wbs_record_collection = rail.CreateCollectionOperator(
            task_id="create_wbs_record_collection",
            source='{{ result("get_wbs_records_from_xml") }}',
            columns=[
                'WBS',
                'AttributeNumber',
                'Attribute',
                'Description',
                'EndDate'],
            name="xmlwbsrecords"
        )

        query_wbs_record_collection = rail.QueryCollectionOperator(
            task_id="query_wbs_record_collection",
            name="uniquewbsrecords",
            query="""SELECT DISTINCT WBS FROM xmlwbsrecords"""
        )

        get_system_level_attribute_1_2 = get_system_level_attribute()

        sync_unique_atrribute_system_level = rail.TriggerDagRunForEachItemOperator(
            task_id='sync_unique_atrribute_system_level',
            retries=0,
            items="{{ result('get_unique_name_attribute_1') if 'Attributes_1' in result('new_file_sensor') else result('get_unique_name_attribute_2') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_child_sync_attribute_system_level_v1_{config.instance}_{config.sub_erp}_{config.attribute}',
            conf=lambda item: {
                'attribute_name': item['NAME'],
                'attribute_1_2_uri': rail.result('get_attribute_1_uri')[0]['uri'] if 'Attributes_1' in rail.result('new_file_sensor')
                    else rail.result('get_attribute_2_uri')[0]['uri'],
                'attribute_number': "1" if 'Attributes_1' in rail.result('new_file_sensor') else "2",
                'attribute_value': item['Attribute']
            }
        )

        wait_for_sync_unique_atrribute_system_level = rail.WaitForDagRunsSensor(
            task_id='wait_for_sync_unique_atrribute_system_level',
            dag_runs='{{ result("sync_unique_atrribute_system_level") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        is_wbs_xml_present = rail.IfOperator(
            task_id='is_wbs_xml_present',
            test='{{ result("query_wbs_record_collection", "length") > 0 }}',
            yes_task='sync_attribute_1_2_file',
            no_task='generate_output_log'
        )

        sync_attribute_1_2_file = rail.TriggerDagRunForEachItemOperator(
            task_id='sync_attribute_1_2_file',
            retries=0,
            items="{{ result('query_wbs_record_collection') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_child_sync_attributes_1_2_file_v1_{config.instance}_{config.sub_erp}_{config.attribute}',
            conf=request_payload.attribute_payload
        )

        wait_for_sync_attribute_1_2_file = rail.WaitForDagRunsSensor(
            task_id='wait_for_sync_attribute_1_2_file',
            dag_runs='{{ result("sync_attribute_1_2_file") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_successful_attribute_logs = rail.FilterLogEntriesOperator(
            task_id='get_successful_attribute_logs',
            properties={'status': 'Success'}
        )

        get_errored_attribute_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_attribute_logs',
            properties={'status': 'Error'}
        )

        get_exception_attribute_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_attribute_logs',
            properties={'status': 'Exception'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'Level',
                'WBS',
                'Attribute name',
                'Status',
                'Details',
                'Record Count',
                'Job ID'],
            row=[
                '{{item.properties.Level}}',
                '{{ item.properties.wbs }}',
                '{{ item.properties.attributename }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.properties.recordcount }}',
                '{{ item.ecid }}'],
            footer=[
                'Number of Records Processed Successfully: {{result("get_successful_attribute_logs", key="length")}}',
                'Number of Records with Error: {{ result("get_errored_attribute_logs", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_attribute_logs", key="length") }}',
                '',
                ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run.run_id | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv')

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_attribute_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " |  Replicon project field sync for compass attribute " + (" 1 " if "Attributes_1" in result("new_file_sensor") \
                else " 2 ") }} \
                {%- if result("get_errored_attribute_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_attribute_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> is_attributes_1_2_file >> rail.Label(
            "Yes") >> is_xml
        is_attributes_1_2_file >> rail.Label("No") >> send_bad_file_name_email
        is_xml >> rail.Label("Yes") >> download_file >> rail.Label(
            "ALWAYS") >> was_new_file_found
        is_xml >> rail.Label("No") >> send_bad_file_format_email
        was_new_file_found >> rail.Label("YES") >> archive_file
        was_new_file_found >> rail.Label("NO") >> delete_this_dagrun
        download_file >> parse_xml
        parse_xml >> has_data
        has_data >> rail.Label("NO") >> send_blank_payload_email
        has_data >> rail.Label('YES') >> get_records_missing_wbs_from_xml
        get_records_missing_wbs_from_xml >> create_missing_wbs_collection >> check_any_blank_wbs_present >> rail.Label(
            "Yes") >> log_blank_wbs
        log_blank_wbs >> get_wbs_records_from_xml
        check_any_blank_wbs_present >> rail.Label(
            "No") >> get_wbs_records_from_xml
        get_wbs_records_from_xml >> create_wbs_record_collection >> query_wbs_record_collection
        query_wbs_record_collection >> get_system_level_attribute_1_2 >> sync_unique_atrribute_system_level
        sync_unique_atrribute_system_level >> wait_for_sync_unique_atrribute_system_level
        wait_for_sync_unique_atrribute_system_level >> is_wbs_xml_present
        is_wbs_xml_present >> rail.Label("Yes") >> sync_attribute_1_2_file
        sync_attribute_1_2_file >> wait_for_sync_attribute_1_2_file >> generate_output_log
        is_wbs_xml_present >> rail.Label("No") >> generate_output_log
        generate_output_log >> [get_successful_attribute_logs,
                                get_errored_attribute_logs, get_exception_attribute_logs] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_dag)
