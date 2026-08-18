
from datetime import timedelta, timezone
import datetime
import os
import rail
from rail.lib.log import get_master_log_artifact_name
null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_master_sftp_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_COMPASS_Labour Types and Task_Automation Master - SFTP - {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        list_ftp_files = rail.SFTPListFilesOperator(
            task_id='list_ftp_files',
            paths=[config.input_filepath]
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
            subject='{{ get_company_key() }} | Replicon task and billing assignment sync for Compass Labour type and task  - Incorrect Format - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_bad_file_format.html",
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
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}",
            xsd_document='./dags/dxctechnology/compass_labor_types_and_tasks/xml_schema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='get_all_billing_rates',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task and billing assignment sync for Compass Labour type and task - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_blank_payload.html",
        )

        def map_billing_rates(response):
            data = response.json()['d']
            return list(map(lambda item: {
                "displayText": item['displayText'],
                "name": item['name'].replace("|Billable", "").replace("|Non-Billable", "").strip(),
                "uri": item['uri']
            }, data))

        get_all_billing_rates = rail.RepliconServiceOperator(
            task_id='get_all_billing_rates',
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=map_billing_rates
        )

        create_billing_rate_collection = rail.CreateCollectionOperator(
            task_id='create_billing_rate_collection',
            name='replicon_billing_rate',
            source="{{ result('get_all_billing_rates') | to_json}}",
        )

        get_task_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_task_details_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/Tasks',
                {
                    "wbs": "../WBS/text()",
                    "task": "Task/text()",
                    "code": "Description/text()",
                    "startdate": "StartDate/text()",
                    "enddate": "EndDate/text()",
                },
            ],
        )

        create_task_collection = rail.CreateCollectionOperator(
            task_id='create_task_collection',
            name='task',
            source="{{ result('get_task_details_from_xml')}}",
        )

        query_list_invalid_task = rail.QueryCollectionOperator(
            task_id='query_list_invalid_task',
            query='''SELECT * FROM task WHERE wbs IS NULL OR task IS NULL''',
        )

        query_list_valid_task = rail.QueryCollectionOperator(
            task_id='query_list_valid_task',
            query='''SELECT * FROM task WHERE wbs IS NOT NULL AND task IS NOT NULL''',
        )

        get_labourtype_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_labourtype_details_from_xml",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'Records/LaborTypes',
                {
                    "wbs": "../WBS/text()",
                    "labourtypes": "LaborType/text()",
                    "name": "LaborType/text()",
                    "personnelnumber": "PersonnelNumber/text()",
                    "taskassignmentstartdate": "StartDate/text()",
                    "taskassignmentenddate": "EndDate/text()",
                    "billabledefault": "BillableDefault/text()",
                },
            ],
        )

        create_labourtype_collection = rail.CreateCollectionOperator(
            task_id='create_labourtype_collection',
            name='labourtype',
            source="{{ result('get_labourtype_details_from_xml')}}",
        )

        query_list_all_distinct_labor_types = rail.QueryCollectionOperator(
            task_id='query_list_all_distinct_labor_types',
            name='feedlabourtypes',
            query='''SELECT DISTINCT  labourtypes FROM labourtype WHERE labourtypes IS NOT NULL''',
        )

        query_list_distinctlabourtypesnotavailablein_replicon = rail.QueryCollectionOperator(
            task_id='query_list_distinctlabourtypesnotavailablein_replicon',
            query='''SELECT * FROM feedlabourtypes WHERE LOWER(labourtypes) NOT IN (SELECT DISTINCT LOWER(name) FROM replicon_billing_rate)''',
        )

        has_new_billingrates = rail.IfOperator(
            task_id='has_new_billingrates',
            test="{{ result('query_list_distinctlabourtypesnotavailablein_replicon','length') > 0 }}",
            yes_task="create_billing_rates",
            no_task="query_list_distinct_projectstask",
        )

        create_billing_rates = rail.TriggerDagRunForEachItemOperator(
            task_id='create_billing_rates',
            retries=0,
            items="{{ result('query_list_distinctlabourtypesnotavailablein_replicon') }}",
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_child_createbillingrate_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "name": item['labourtypes']
            }
        )

        wait_for_create_billing_rates = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_billing_rates',
            dag_runs='{{ result("create_billing_rates") }}',
            execution_timeout=timedelta(days=14),
        )

        get_all_updated_billing_rates = rail.RepliconServiceOperator(
            task_id='get_all_updated_billing_rates',
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=map_billing_rates
        )

        query_list_distinct_projectstask = rail.QueryCollectionOperator(
            task_id='query_list_distinct_projectstask',
            query='''SELECT DISTINCT wbs FROM task ''',
        )

        query_list_distinct_projectslabour = rail.QueryCollectionOperator(
            task_id='query_list_distinct_projectslabour',
            query='''SELECT DISTINCT labourtype.wbs FROM labourtype WHERE labourtype.wbs NOT IN (SELECT DISTINCT task.wbs FROM task)''',
        )

        query_list_mergedata = rail.QueryCollectionOperator(
            task_id='query_list_mergedata',
            query='''SELECT wbs FROM query_list_distinct_projectstask
                        UNION
                        SELECT wbs FROM query_list_distinct_projectslabour''',
        )

        has_merged_list_row = rail.IfOperator(
            task_id='has_merged_list_row',
            test="{{ result('query_list_mergedata','length') > 0 }}",
            yes_task="create_billing_rates_collection",
        )

        create_billing_rates_collection = rail.CreateCollectionOperator(
            task_id='create_billing_rates_collection',
            name='billingratesinreplicon',
            source="{{ (result('get_all_updated_billing_rates') if result('get_all_updated_billing_rates') else result('get_all_billing_rates')) | to_json }}",
        )

        process_labour_types_and_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id='process_labour_types_and_tasks',
            retries=0,
            items="{{ result('query_list_mergedata') }}",
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                    "wbs": item['wbs'],
            }
        )

        wait_for_process_labour_types_and_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_labour_types_and_tasks',
            dag_runs='{{ result("process_labour_types_and_tasks") }}',
            execution_timeout=timedelta(days=14),
        )

        get_errored_master_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_master_logs',
            properties={'status': 'Error'}
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('process_labour_types_and_tasks') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        def do_format_logs():
            logs = []
            logs.extend(rail.load_all_records(
                get_master_log_artifact_name(rail.get_current_context())))
            for log in rail.result('gather_logs'):
                log_records = rail.load_all_records(log)
                if log_records:
                    logs.extend(log_records)
            return logs

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=14),
            python_callable=do_format_logs
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: len(list(filter(lambda x: x['properties'].get('status') == 'Error',
                                                    rail.result('format_logs'))))
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: len(list(filter(lambda x: x['properties'].get('status') == 'Exception',
                                                    rail.result('format_logs'))))
        )

        write_csv_log = rail.WriteCSVFileOperator(
            task_id='write_csv_log',
            source="{{ result('format_logs') | to_json }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("create_task_collection", key="length") }}',
                'Function: COMPASS Labor Type and Tasks inbound',
                '',
                '',
                ''
            ],
            row=[
                '{{ item.properties | attr_or_default("wbs", "") }}',
                '{{ item.properties | attr_or_default("task", "") }}',
                '{{ item.properties | attr_or_default("billingrate", "") }}',
                '{{ item.properties.status }}',
                '{{ item.properties | attr_or_default("message", "") }}',
                '{{ item.ecid }}'
            ],
            footer=[
                # pylint: disable=line-too-long
                'Number of Records Processed Successfully: {{ result("create_task_collection", key="length") - result("get_errored_logs")  - result("get_exception_logs") }}',
                'Number of Records with Error: {{  result("get_errored_logs")  }}',
                'Number of Records with Exception: {{  result("get_exception_logs")  }}',
                '',
                '',
                ''
            ],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('write_csv_log') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon task and billing assignment sync for Compass Labour type and task - " }} \
                {%- if result("get_errored_logs") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs") > 0 -%} \
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

        def get_extra_info():
            # to handle all the failure scenarios
            if not rail.result('new_file_sensor'):
                return None
            file_name = os.path.basename(rail.result('new_file_sensor'))
            file_info = rail.find_first_by_attr_and_get_attr(rail.result('list_ftp_files').get(
                config.input_filepath), 'name', file_name)
            file_modified_datetime = datetime.datetime.strptime(
                file_info.get('modify'), '%Y%m%d%H%M%S')
            return {
                "file_name": file_name,
                "sftp_file_path": config.input_filepath,
                "file_size": file_info.get('size'),
                "record_count": rail.result('create_task_collection', 'length'),
                "file_modified_datetime": datetime.datetime(
                    file_modified_datetime.year,
                    file_modified_datetime.month,
                    file_modified_datetime.day,
                    file_modified_datetime.hour,
                    file_modified_datetime.minute,
                    file_modified_datetime.second,
                    tzinfo=timezone.utc).isoformat(),
            }

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=get_extra_info,
        )

        new_file_sensor >> list_ftp_files >> is_xml
        is_xml >> rail.Label('yes') >> download_file
        is_xml >> rail.Label('no') >> send_bad_file_format_email
        download_file >> rail.Label(
            'Always') >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> parse_xml >> has_data
        has_data >> rail.Label('yes') >> get_all_billing_rates
        has_data >> rail.Label('no') >> send_blank_payload_email
        get_all_billing_rates >> create_billing_rate_collection >> get_task_details_from_xml >> create_task_collection >> query_list_invalid_task >> \
            query_list_valid_task >> get_labourtype_details_from_xml >> create_labourtype_collection >> query_list_all_distinct_labor_types >> \
            query_list_distinctlabourtypesnotavailablein_replicon >> has_new_billingrates
        has_new_billingrates >> rail.Label(
            'yes') >> create_billing_rates >> wait_for_create_billing_rates >> get_all_updated_billing_rates >> query_list_distinct_projectstask
        has_new_billingrates >> rail.Label(
            'no') >> query_list_distinct_projectstask
        query_list_distinct_projectstask >> query_list_distinct_projectslabour >> query_list_mergedata >> has_merged_list_row
        has_merged_list_row >> rail.Label(
            'yes') >> create_billing_rates_collection >> process_labour_types_and_tasks >> wait_for_process_labour_types_and_tasks >> get_errored_master_logs >> gather_logs
        gather_logs >> format_logs >> get_errored_logs >> get_exception_logs >> write_csv_log >> \
            upload_log_to_sftp >> send_import_complete_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
