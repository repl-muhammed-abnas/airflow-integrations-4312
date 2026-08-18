from datetime import timedelta
import rail
from dxctechnology.adhoc.gsap_task_project_fields_import.utils.response_filter import map_gsap_task_uri

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adhoc_dxctechnology_gsap_project_field_task_import_master_{config.instance}',
        description=f'DXC GSAP Task import as Project fields {config.instance}',
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
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='fail_bad_file_format',
        )

        fail_bad_file_format = rail.FailOperator(
            task_id = "fail_bad_file_format",
            message= "File not in correct extension"
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
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}")

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}",
            xsd_document='./dags/dxctechnology/gsap_task_import_project_fields/xsdschema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='load_input_data',
            no_task='finish_no_data',
        )

        finish_no_data = rail.EmptyOperator(
            task_id='finish_no_data',
        )

        load_input_data = rail.XMLAdaptorOperator(
            task_id="load_input_data",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records',
                {
                    'wbs': "WBS_Name/text()",
                    'task_name': "Task_Name/text()",
                    "task_code": "Task_Code/text()",
                    "task_start_date": "Task_Start_Date/text()",
                    "task_end_date": "Task_End_Date/text()"
                }
            ]
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id="create_input_data_collection",
            source="{{result('load_input_data')}}",
            name= "input_data"
        )

        create_wbs_record_collection = rail.QueryCollectionOperator(
            task_id="create_wbs_record_collection",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NOT NULL
            AND NULLIF(task_name, '') IS NOT NULL
            AND NULLIF(task_start_date, '') IS NOT NULL
            AND NULLIF(task_end_date, '') IS NOT NULL""",
            name="valid_input_records"
        )

        get_gsap_task_uri = rail.RepliconServiceOperator(
            task_id="get_gsap_task_uri",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings",
            data=lambda: {
                    "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            response_filter=map_gsap_task_uri
        )

        get_unique_task_name = rail.QueryCollectionOperator(
            task_id="get_unique_task_name",
            name="uniqueattribute2names",
            query="""Select DISTINCT task_name, task_code from valid_input_records"""
        )


        def get_formatted_data(response):
            if not response['rows']:
                return []
            return list(map(lambda item : {
                "name": item['cells'][0]['textValue'],
                "code": item['cells'][1].get('textValue'),
                "uri": item['cells'][3]['uri'],
                "task_name": item['cells'][3]['textValue'],
                "is_enabled": item['cells'][4]['textValue']

                }, response['rows']))

        get_gsap_task_from_replicon = rail.RepliconServiceOperator(
            task_id="get_gsap_task_from_replicon",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_gsap_task_uri')[0].uri }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler= get_formatted_data
        )

        create_replicon_task_collection = rail.CreateCollectionOperator(
            task_id = "create_replicon_task_collection",
            source="{{ result('get_gsap_task_from_replicon') | to_json }}"
        )

        query_records_not_present = rail.QueryCollectionOperator(
            task_id = "query_records_not_present",
            query="""SELECT * FROM uniqueattribute2names vir WHERE
            (vir.task_name || ' - ' ||  vir.task_code) NOT IN (SELECT crtc.name FROM create_replicon_task_collection crtc)""",
            name="task_to_create"
        )

        sync_unique_gsap_task = rail.trigger_parallel_dagrun(
            task_id='sync_unique_gsap_task',
            items="{{ result('query_records_not_present') }}",
            parallel_count=config.parallel_dag_run_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'adhoc_dxctechnology_gsap_project_field_task_import_add_gsap_task_{config.instance}',
            conf=lambda item: {
                'task_name': item['task_name'],
                'gsap_task_uri': rail.result('get_gsap_task_uri')[0]['uri'],
                'attribute_type': "GSAP Task",
                'task_code': item['task_code'] if item['task_code'] else None
            }
        )

        get_errored_attribute_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_attribute_logs',
            properties={'status': 'Error'}
        )

        has_any_errored_logs = rail.IfOperator(
            task_id = "has_any_errored_logs",
            test="{{ result('get_errored_attribute_logs' , 'length') > 0 }}",
            yes_task= "render_logs_csv"
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'Level',
                'WBS',
                'Task name',
                'Status',
                'Details',
                'Job ID'],
            row=[
                '{{item.properties.Level}}',
                '{{ item.properties.wbs }}',
                '{{ item.properties.task_name }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            "/log_{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_base }}.csv"
        )

        send_failure_email = rail.EmailOperator(
            task_id = "send_failure_email",
            to= '{{ var.value.dagrun_failure_alert_email }}',
            bcc= '{{ var.value.dagrun_internal_testing_email }}',
            subject="GSAP TASK Import Adhoc completed with Errors",
            html_content="task_import_failed_email_template.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> rail.Label(
            "Yes") >> is_xml
        is_xml >> rail.Label("Yes") >> download_file >> rail.Label(
            "ALWAYS") >> was_new_file_found
        is_xml >> rail.Label("No") >> fail_bad_file_format
        was_new_file_found >> rail.Label("YES") >> archive_file
        was_new_file_found >> rail.Label("NO") >> delete_this_dagrun
        download_file >> parse_xml
        parse_xml >> has_data
        has_data >> rail.Label("NO") >> finish_no_data
        has_data >> rail.Label('YES') >> load_input_data >> create_input_data_collection
        create_input_data_collection >> create_wbs_record_collection >> get_gsap_task_uri >>\
        get_unique_task_name >> get_gsap_task_from_replicon >> create_replicon_task_collection >> query_records_not_present\
            >> sync_unique_gsap_task >> get_errored_attribute_logs >> has_any_errored_logs\
            >> rail.Label("Yes") >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_failure_email

    return dag


rail.for_each_instance(create_dag)
