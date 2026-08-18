from datetime import datetime, timedelta
import os

from airflow.utils.edgemodifier import Label

from rail.task_groups.batch_execution import batch_execution
import rail
from dxctechnology.ppmc_project_and_tasks_import import request_payload

# pylint: disable=too-many-statements

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py

def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_master{dag_id_postfix}',
        description=f'PPMC - Project and Tasks Master V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log',
        )

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        is_xml_gpg = rail.IfOperator(
            task_id='is_xml_gpg',
            test='{{ result("new_file_sensor") | lower | ends_with("xml.gpg") }}',
            yes_task='list_ftp_files',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | PPMC - Porject and Task Import - Incorrect file format - {{ current_time() }}',
            html_content="email_bad_file_format.html",
        )

        list_ftp_files = rail.SFTPListFilesOperator(
            task_id='list_ftp_files',
            paths=[config.input_filepath]
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        is_empty_file_decrypt_error = rail.IfOperator(
            task_id='is_empty_file_decrypt_error',
            trigger_rule='one_failed',
            test=lambda: rail.result('decrypt_file', 'error')['exc_message'] == 'decryption failed' and
            next(
                filter(
                    lambda x: x['size'] < config.pgp_decrypt_empty_file_size_in_bytes and x['name'] ==
                    os.path.basename(rail.result('new_file_sensor')),
                    rail.result('list_ftp_files')[config.input_filepath]
                ),
                None),
            no_task='decrypt_fail'
        )

        has_decrypted_file = rail.IfOperator(
            task_id='has_decrypted_file',
            trigger_rule='all_done',
            test=lambda: rail.result('decrypt_file'),
            yes_task='has_file_content'
        )

        def do_has_file_content():
            with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0
        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=do_has_file_content,
            yes_task='parse_xml',
            no_task='send_blank_payload_email'
        )

        decrypt_fail = rail.FailOperator(
            task_id='decrypt_fail',
            message="{{ result('decrypt_file','error') }}"
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
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('decrypt_file') }}",
            xsd_document='./dags/dxctechnology/ppmc_project_and_tasks_import/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Proj") | length > 0 }}',
            yes_task='create_exception_log',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | PPMC - Project and Task Import - Blank Payload - {{ current_time() }}',
            html_content="email_blank_payload.html",
        )

        map_assn_data = rail.XMLAdaptorOperator(
            task_id="map_assn_data",
            source='{{ result("parse_xml") }}',
            adaptor=[
                    'Proj/TAssn/Assn[WBSE/text()]',
                    {
                        "wbs": 'WBSE/text()',
                        "task1name": '../../PName/text()',
                        "task1code": '../../PID/text()',
                        "task1status": '../../PStatus/text()',
                        "task1startdate": '../../PSDate/text()',
                        "task1enddate": '../../PEDate/text()',
                        "task2name": 'TName/text()',
                        "task2code": 'TID/text()',
                        "task2estimatedhours": 'RemWork/text()',
                        "task2startdate": 'TSDate/text()',
                        "task2enddate": 'TEDate/text()',
                        "aid": 'AID/text()',
                        "eidresource": 'EID/text()',
                        "systemid": '../../../SystemID/text()',
                    },
            ],
            target='result'
        )

        create_project_collection = rail.CreateCollectionOperator(
            task_id='create_project_collection',
            name="inputlist",
            source=lambda: rail.result('map_assn_data')
        )

        query_invalid_data = rail.QueryCollectionOperator(
            task_id="query_invalid_data",
            name="invaliddata",
            query="""SELECT * FROM inputlist
                    WHERE (
                            Systemid IS NULL OR
                            Task1name IS NULL OR
                            Task1startdate IS NULL OR
                            Task1enddate IS NULL OR
                            Eidresource IS NULL OR
                            Task2name IS NULL OR
                            Task2startdate IS NULL OR
                            Task2enddate IS NULL OR
                            Wbs IS NULL OR
                            Systemid ="" OR
                            Task1name ="" OR
                            Task1startdate ="" OR
                            Task1enddate ="" OR
                            Eidresource ="" OR
                            Task2name ="" OR
                            Task2startdate ="" OR
                            Task2enddate ="" OR
                            Wbs ="")
                    """
        )

        has_invalid_data = rail.IfOperator(
            task_id="has_invalid_data",
            test="{{ result('query_invalid_data','length') > 0 }}",
            yes_task='log_validation_error',
            no_task='query_valid_data'
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            message='One or more mandatory fields missing',
            items=lambda:  rail.load_all_records(
                rail.result('query_invalid_data')),
            properties={
                'wbs':  '{{ item.wbs}}',
                        'task': '{{ item.task1name}}' + '|' + '{{ item.task2name}}',
                        'status': 'Exception',
                        'message': 'One or more mandatory fields missing',
            }
        )

        query_valid_data = rail.QueryCollectionOperator(
            task_id="query_valid_data",
            name="validatedinput",
            query="""SELECT * FROM inputlist
                    WHERE (
                            Systemid IS NOT NULL OR
                            Task1name IS NOT NULL OR
                            Task1startdate IS NOT NULL OR
                            Task1enddate IS NOT NULL OR
                            Eidresource IS NOT NULL OR
                            Task2name IS NOT NULL OR
                            Task2startdate IS NOT NULL OR
                            Task2enddate IS NOT NULL OR
                            Wbs IS NOT NULL OR
                            Systemid !="" OR
                            Task1name !="" OR
                            Task1startdate !="" OR
                            Task1enddate !="" OR
                            Eidresource !="" OR
                            Task2name !="" OR
                            Task2startdate !="" OR
                            Task2enddate !="" OR
                            Wbs !="")
                    """
        )

        log_file_summary_to_sumo = rail.SendToSumoOperator(
            task_id='log_file_summary_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            data={
                'sftp_file_path': config.input_filepath,
                'file_name': '{{ result("new_file_sensor") | file_name  }}',
                'file_size': "{{ result('list_ftp_files')['" + config.input_filepath
                + "'] | find_first_by_attr_and_get_attr('name',result('new_file_sensor') | file_name ,'size') }}",
                'record_count': "{{ result('create_project_collection', 'length') or 0 }}",
                'file_modified_datetime':  "{{ result('list_ftp_files')['" + config.input_filepath
                + "'] | find_first_by_attr_and_get_attr('name',result('new_file_sensor') | file_name ,'modify') }}",
            }
        )

        query_unique_projects = rail.QueryCollectionOperator(
            task_id="query_unique_projects",
            name="query_unique_projects",
            query="""SELECT DISTINCT
                        Wbs
                        FROM
                        validatedinput
                    """
        )

        get_enabled_department_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups',
            endpoint='/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups'
        )

        get_enabled_divsions = rail.RepliconServiceOperator(
            task_id='get_enabled_divsions',
            endpoint='/services/DivisionService1.svc/GetEnabledDivisions'
        )

        get_task_custom_fields = rail.RepliconServiceOperator(
            task_id='get_task_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                "objectUri": "urn:replicon:object-type:task"
            }
        )

        get_task_type_udf_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_task_type_udf_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_task_custom_fields'), 'displayText', "Task Type", 'uri')
            }
        )

        get_project_oefs = rail.RepliconServiceOperator(
            task_id='get_project_oefs',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            }
        )

        get_task_required_oef_details = rail.RepliconServiceOperator(
            task_id='get_task_required_oef_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_oefs'), 'name', "PPMC Task Required", 'uri')
            }
        )

        get_task_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_task_user_report_details",
            report_name=config.task_user_details_report_name
        )

        create_task_user_report_generation_batch = rail.RepliconServiceOperator(
            task_id="create_task_user_report_generation_batch",
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=request_payload.get_task_user_report_generation_batch_param
        )

        batchuri = "{{ result('create_task_user_report_generation_batch') }}"

        process_report_batch = batch_execution(
            group_id='execute_report_generation_batch',
            creation_task_id=create_task_user_report_generation_batch.task_id
        )

        payload = {
            "reportGenerationBatchUri": batchuri
        }

        get_report_batch_result = rail.RepliconServiceOperator(
            task_id="get_report_batch_result",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data=payload
        )

        load_user_csv_data = rail.LoadCSVFileOperator(
            task_id="load_user_csv_data",
            document="{{ result('get_report_batch_result').reportGenerationResults[0].payload }}"
        )

        create_user_data_collection = rail.CreateCollectionOperator(
            task_id="create_user_data_collection",
            name="userdatafromreplicon",
            source="{{ result('load_user_csv_data') }}"

        )

        query_user_with_empid = rail.QueryCollectionOperator(
            task_id="query_user_with_empid",
            name="query_user_with_empid",
            query="""SELECT *
                        FROM
                        userdatafromreplicon
                        WHERE Employee_Id IS NOT NULL
                    """
        )

        process_project_task_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_project_task_records',
            retries=0,
            items="{{ result('query_unique_projects') }}",
            trigger_dag_id=f'dxctechnology_ppmc_project_task_import_child_project_process{dag_id_postfix}',
            execution_timeout=timedelta(days=14),
            conf=request_payload.get_project_task_child_dag_confg
        )

        wait_for_process_project_task_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_project_task_records',
            dag_runs='{{ result("process_project_task_records") }}',
            execution_timeout=timedelta(days=7),
        )

        get_exception_records = rail.FilterLogEntriesOperator(
            task_id='get_exception_records',
            properties={'status': 'Exception'}
        )

        get_success_records = rail.FilterLogEntriesOperator(
            task_id='get_success_records',
            properties={'status': 'Success'}
        )

        get_errored_records = rail.FilterLogEntriesOperator(
            task_id='get_errored_records',
            properties={'status': 'Error'}
        )
        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("create_project_collection", key="length") }}',
                'Function: PPMC Project Task Import',
                '',
                ''],
            row=[
                '{{ item.properties | attr_or_default("wbs","") }}',
                '{{ item.properties | attr_or_default("task","") }}',
                '{{ item.properties | attr_or_default("status","") }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
            footer=[
                'Number of Records Errored: {{ result("get_errored_records", key="length") }}',
                'Number of Records Processed Successfully: {{ result("get_success_records", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_records", key="length") }}',
                '',
                ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv')

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_records', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " |  Replicon task sync for PPMC projects and tasks - " }} \
                    {%- if result("get_errored_records", key="length") > 0 -%} \
                        completed with errors  \
                    {%- else -%} \
                        {%- if result("get_exception_records", key="length") > 0 -%} \
                            completed with exceptions  \
                        {%- else -%} \
                            completed successfully  \
                        {%- endif -%} \
                    {%- endif -%} \
                    {{ " - " + current_time() }}',
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )
        new_file_sensor >> is_xml_gpg

        is_xml_gpg >> Label("No") >> send_bad_file_format_email >> archive_file
        is_xml_gpg >> Label('Yes') >> list_ftp_files >> download_file

        download_file >> decrypt_file >> [
            is_empty_file_decrypt_error, has_decrypted_file]
        is_empty_file_decrypt_error >> rail.Label('No') >> decrypt_fail
        is_empty_file_decrypt_error >> rail.Label('Yes') >> log_file_summary_to_sumo
        has_decrypted_file >> rail.Label('Yes') >> has_file_content
        has_file_content >> rail.Label('yes') >> parse_xml
        has_file_content >> rail.Label('no') >> send_blank_payload_email >> log_file_summary_to_sumo
        parse_xml >> has_data

        has_data >> Label("No") >> send_blank_payload_email
        has_data >> Label('Yes') >> create_exception_log >> map_assn_data

        map_assn_data >> create_project_collection >> log_file_summary_to_sumo >> query_invalid_data >> has_invalid_data

        has_invalid_data >> Label("No") >> query_valid_data
        has_invalid_data >> Label(
            'Yes') >> log_validation_error >> query_valid_data

        query_valid_data >> query_unique_projects >> \
            [get_enabled_department_groups, get_enabled_divsions, get_project_oefs, get_task_custom_fields] >> \
            get_task_type_udf_dropdown_options >> get_task_required_oef_details >>  \
            get_task_user_report_details >> create_task_user_report_generation_batch >> \
            process_report_batch >> get_report_batch_result >> load_user_csv_data >> create_user_data_collection >> \
            query_user_with_empid >> process_project_task_records

        process_project_task_records >> wait_for_process_project_task_records >> \
            [get_exception_records, get_success_records,
                get_errored_records] >> render_logs_csv >> upload_log_to_sftp
        upload_log_to_sftp >> send_import_complete_email

        download_file >> Label("Always") >> was_new_file_found >> Label(
            "Yes") >> archive_file
        was_new_file_found >> Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
