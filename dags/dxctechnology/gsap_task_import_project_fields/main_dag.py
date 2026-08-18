from datetime import timedelta
import itertools
from os import path
import rail
from dxctechnology.gsap_task_import_project_fields.utils import request_payload
from dxctechnology.gsap_task_import_project_fields.task.get_system_level_attribute import get_system_level_attribute
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
# pylint: disable=too-many-statements
null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_project_field_task_import_master_{config.instance}',
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
            subject='{{ get_company_key() }} | Replicon project field sync for GSAP Task - Incorrect file format - {{ current_time() }}',
            html_content="templates/emails/email_bad_file_format.html",
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

        master_dag_log = rail.CreateLogOperator(
            task_id = "master_dag_log"
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='load_input_data',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon project field sync for GSAP Task - Blank Payload - {{ current_time() }}',
            html_content="templates/emails/email_bad_file_format.html",
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

        query_missing_fields = rail.QueryCollectionOperator(
            task_id="query_missing_fields",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NULL
            OR NULLIF(task_name, '') IS NULL
            OR NULLIF(task_start_date, '') IS NULL
            OR NULLIF(task_end_date, '') IS NULL"""
        )

        has_any_missing_fields = rail.IfOperator(
            task_id='has_any_missing_fields',
            test="{{ result('query_missing_fields','length') > 0 }}",
            yes_task='log_missing_fields',
            no_task='create_wbs_record_collection',
        )

        def get_missing_fields_details(item):
            log = []
            missing_fields = [('wbs', 'WBS'),('task_name', 'Task'), ('task_start_date', 'Task Start date'),('task_end_date', 'Task End date')]
            for field, log_message in missing_fields:
                if not item[field]:
                    log.append(log_message)
            return ";".join(log) + f" value{'s'if len(log)>0 else ''} missing"

        log_missing_fields = rail.WriteLogOperator(
            task_id='log_missing_fields',
            log = "{{result('master_dag_log')}}",
            message="Mandatory fields blank",
            items= "{{result('query_missing_fields')}}",
            properties=lambda item:{
                'Level': "File",
                'wbs': item['wbs'] if item['wbs'] else 'NA',
                'task_name': item['task_name'] if item['task_name'] else 'NA',
                'task_code': item['task_code'],
                'action': 'Validation',
                'status': "Skipped",
                'details': get_missing_fields_details(item)
            }
        )

        create_wbs_record_collection = rail.QueryCollectionOperator(
            task_id="create_wbs_record_collection",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NOT NULL
            AND NULLIF(task_name, '') IS NOT NULL
            AND NULLIF(task_start_date, '') IS NOT NULL
            AND NULLIF(task_end_date, '') IS NOT NULL""",
            name="mandatory_valid_input_records"
        )

        query_data_end_date_in_past = rail.QueryCollectionOperator(
            task_id = "query_data_end_date_in_past",
            query="""SELECT *
                    FROM input_data 
                    WHERE date(substr(task_end_date, 7, 4) || '-' || substr(task_end_date, 4, 2) || '-' || substr(task_end_date, 1, 2), 'start of day') 
                    < date('now', 'start of day')
                    """,
            name="end_date_is_past_date"
        )

        log_past_end_date_records = rail.WriteLogOperator(
            task_id = "log_past_end_date_records",
            log = "{{result('master_dag_log')}}",
            message="End date is prior to today",
            items= "{{result('query_data_end_date_in_past')}}",
            properties=lambda item:{
                'Level': "File",
                'wbs': item['wbs'] if item['wbs'] else 'NA',
                'task_name': item['task_name'] if item['task_name'] else 'NA',
                'task_code': item['task_code'],
                'action': 'Validation',
                'status': "Skipped",
                'details': "End date is prior to today"
            }
        )

        query_data_end_date_not_in_past  = rail.QueryCollectionOperator(
            task_id = "query_data_end_date_not_in_past",
            query="""SELECT *
                    FROM input_data 
                    WHERE date(substr(task_end_date, 7, 4) || '-' || substr(task_end_date, 4, 2) || '-' || substr(task_end_date, 1, 2), 'start of day') 
                    > date('now', '-1 day')
                    """,
            name="valid_input_records"
        )

        has_any_valid_data = rail.IfOperator(
            task_id = "has_any_valid_data",
            test="{{result('query_data_end_date_not_in_past', 'length') > 0 }}",
            yes_task='query_unique_wbs',
            no_task='generate_output_log'
        )

        query_unique_wbs = rail.QueryCollectionOperator(
            task_id="query_unique_wbs",
            name="uniquewbsrecords",
            query="""SELECT DISTINCT wbs FROM valid_input_records"""
        )

        get_system_level_gsap_task_import = get_system_level_attribute()

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
            trigger_dag_id=f'dxctechnology_gsap_project_field_task_import_add_gsap_task_{config.instance}',
            conf=lambda item: {
                'task_name': item['task_name'],
                "master_dag_log": rail.result('master_dag_log'),
                'gsap_task_uri': rail.result('get_gsap_task_uri')[0]['uri'],
                'attribute_type': "GSAP Task",
                'task_code': item['task_code'] if item['task_code'] else None
            }
        )

        is_wbs_xml_present = rail.IfOperator(
            task_id='is_wbs_xml_present',
            test='{{ result("query_unique_wbs", "length") > 0 }}',
            yes_task='sync_gsap_task_import_file_parallel',
            no_task='generate_output_log'
        )

        sync_gsap_task_import_file_parallel = rail.EmptyOperator(
            task_id= "sync_gsap_task_import_file_parallel"
        )
        sync_gsap_task_import_file = rail.trigger_parallel_dagrun(
            task_id="sync_gsap_task_import_file",
            items="{{ result('query_unique_wbs') }}",
            trigger_dag_id=f'dxctechnology_gsap_project_field_task_import_process_each_wbs_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_dag_run_count,
            conf= request_payload.attribute_payload
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_sync_gsap_wbs_child_dag_ids =rail.PythonOperator(
            task_id= 'get_sync_gsap_wbs_child_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'sync_gsap_task_import_file_{x+1}') if rail.result(
                    f'sync_gsap_task_import_file_{x+1}') else []), range(config.parallel_dag_run_count))))),
            show_return_value_in_logs= False
        )

        gather_each_wbs_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_each_wbs_logs',
            dag_runs='{{ result("get_sync_gsap_wbs_child_dag_ids") }}',
            dagrun_task_id='create_wbs_log',
            execution_timeout=timedelta(
                hours=config.execution_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_project_field_task_import_child_process_log_generation_{config.instance}',
            conf=lambda dag_run:{
                'wbs_logs': rail.result('gather_each_wbs_logs'),
                "master_dag_log": rail.result('master_dag_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0] }.csv'
            }
        )

        new_file_sensor >> rail.Label(
            "Yes") >> is_xml
        is_xml >> rail.Label("Yes") >> download_file >> rail.Label(
            "ALWAYS") >> was_new_file_found
        is_xml >> rail.Label("No") >> send_bad_file_format_email
        was_new_file_found >> rail.Label("YES") >> archive_file
        was_new_file_found >> rail.Label("NO") >> delete_this_dagrun
        download_file >> parse_xml
        parse_xml >> has_data
        has_data >> rail.Label("NO") >> send_blank_payload_email
        has_data >> rail.Label('YES') >> load_input_data >> create_input_data_collection
        create_input_data_collection >> master_dag_log >> query_missing_fields >> has_any_missing_fields >> rail.Label(
            "Yes") >> log_missing_fields
        has_any_missing_fields >> rail.Label("No") >> create_wbs_record_collection >> query_data_end_date_in_past >> log_past_end_date_records
        log_past_end_date_records >> query_data_end_date_not_in_past >> has_any_valid_data >> rail.Label("Yes") >> query_unique_wbs
        has_any_valid_data >> rail.Label("No") >> generate_output_log
        log_missing_fields >> create_wbs_record_collection
        query_unique_wbs >> get_system_level_gsap_task_import\
            >> get_gsap_task_from_replicon >> create_replicon_task_collection >> query_records_not_present >> sync_unique_gsap_task
        sync_unique_gsap_task >> is_wbs_xml_present
        is_wbs_xml_present >> rail.Label("Yes") >> sync_gsap_task_import_file_parallel >> sync_gsap_task_import_file >> generate_output_log
        is_wbs_xml_present >> rail.Label("No") >> generate_output_log
        generate_output_log >> get_sync_gsap_wbs_child_dag_ids >> gather_each_wbs_logs >> process_log_generation

    return dag


rail.for_each_instance(create_dag)
