from datetime import timedelta
import rail
from dxctechnology.c1_iwo_leanstaffing import response_filter
from dxctechnology.c1_iwo_leanstaffing import request_payload
from dxctechnology.c1_iwo_leanstaffing import python_callable_method
# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_iwo_leanstaffing_master_{config.instance}',
        description=f'Live | DXC_C1_Lean Staffing_Automation Master V3.0 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
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

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon resource and billing assignment sync for C1 IWO Lean Staffing - Incorrect File Format - {{ current_time() }}',
            html_content="email_bad_file_format.html",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}")

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}"
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Header") | length > 0 }}',
            yes_task='get_all_billing_rates',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon resource and billing assignment sync for C1 IWO Lean Staffing - Blank File - {{ current_time() }}',
            html_content="email_blank_payload.html",
        )

        get_all_billing_rates = rail.RepliconServiceOperator(
            task_id="get_all_billing_rates",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=response_filter.map_billing_rates
        )

        existing_billing_rates_in_replicon = rail.CreateCollectionOperator(
            task_id="existing_billing_rates_in_replicon",
            source="{{ result('get_all_billing_rates') | to_json }}",
            name="existingbillingrates"
        )

        create_labour_type_item_data_collection = rail.CreateCollectionOperator(
            task_id='create_labour_type_item_data_collection',
            source="{{ result('parse_xml') | xpath('Header/Item')}}",
            name="labourtypesitemdata",
        )

        query_all_distinct_labour_type = rail.QueryCollectionOperator(
            task_id='query_all_distinct_labour_type',
            query="""SELECT DISTINCT LaborType
                    FROM labourtypesitemdata
                    WHERE LaborType IS NOT NULL
                    """
        )

        create_feed_labour_type_collection = rail.CreateCollectionOperator(
            task_id="create_feed_labour_type_collection",
            source="{{ result('query_all_distinct_labour_type') }}",
            name="feed_labour_type"
        )

        query_distinct_labour_type_not_in_replicon = rail.QueryCollectionOperator(
            task_id="query_distinct_labour_type_not_in_replicon",
            query="""SELECT * FROM feed_labour_type WHERE LOWER(LaborType) NOT IN (SELECT DISTINCT LOWER(name) FROM existingbillingrates)"""
        )

        has_unique_labour_type = rail.IfOperator(
            task_id="has_unique_labour_type",
            test="{{ result('query_distinct_labour_type_not_in_replicon','length') > 0 }}",
            yes_task="create_billing_rates",
            no_task="get_all_project_custom_fields"
        )

        create_billing_rates = rail.TriggerDagRunForEachItemOperator(
            task_id='create_billing_rates',
            retries=0,
            items="{{ result('query_distinct_labour_type_not_in_replicon') }}",
            trigger_dag_id=f'{config.child_dag_id_create_billing_rates}_{config.instance}',
            conf={
                'name': '{{ item.LaborType }}'
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_create_billing_rates = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_billing_rates',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("create_billing_rates") }}',
        )

        get_all_project_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_project_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={"objectUri": "urn:replicon:object-type:project"}
        )

        get_all_billing_rates_1 = rail.RepliconServiceOperator(
            task_id="get_all_billing_rates_1",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=response_filter.map_billing_rates
        )

        get_c1_leanstaffing_import_base_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_c1_leanstaffing_import_base_report_details',
            report_name=config.extract_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='c1_leanstaffing_import_base_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_c1_leanstaffing_import_base_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('c1_leanstaffing_import_base_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data',
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="Report \"**C1 Lean staffing Import base report\" execution failed",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('c1_leanstaffing_import_base_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_active_user = rail.PythonOperator(
            task_id="get_active_user",
            python_callable=python_callable_method.active_user,
            op_args=['load_report_data']
        )

        def map_items(json):
            item = json['Item']
            if not item:
                return None
            return list(map(lambda x: {
                'AssignmentStart': x['AssignmentStart'][0].get('#text'),
                'AssignmentEnd':  x['AssignmentEnd'][0].get('#text'),
                'LaborType':  x['LaborType'][0].get('#text'),
                'BillableIndicator':  x['BillableIndicator'][0].get('#text'),
                'SO_OperationandActivity':  x['SO_OperationandActivity'][0].get('#text'),
                'DateChangeFlag': x['DateChangeFlag'][0].get('#text'),
                'CreatedOn': x['CreatedOn'][0].get('#text'),
                'CreatedBy':  x['CreatedBy'][0].get('#text'),
                'ChangedOn':  x['ChangedOn'][0].get('#text'),
                'ChangedBy': x['ChangedBy'][0].get('#text'),
            }, item))

        get_item_from_xml = rail.XMLAdaptorOperator(
            task_id="get_item_from_xml",
            source='{{ result("parse_xml") }}',
            target='result',
            adaptor=[
                    'Header',
                    {
                        'PersonnelNumber': 'PersonnelNumber/text()',
                        'ObjectID': 'ObjectID/text()',
                        'WBSElement_SO': 'WBSElement_SO/text()',
                        'WBSElement_SO_Description': 'WBSElement_SO_Description/text()',
                        'Items': map_items
                    },
            ],
        )

        c1_lean_staffing_process_each_records = rail.trigger_parallel_dagrun(
            task_id='c1_lean_staffing_process_each_records',
            items=lambda: rail.result('get_item_from_xml'),
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_c1_iwo_leanstaffing_process_each_record_{config.instance}',
            conf=request_payload.get_project_dag_confg
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
                'Personnel Number',
                'WBS Element Service Order',
                'Labour Type',
                'Status',
                'Child WBS'
                'Details',
                'Job ID'],
            row=[
                '{{ item.properties.employeeid }}',
                '{{ item.properties.wbs }}',
                '{{ item.properties.billingtype }}',
                '{{ item.properties.status }}',
                '{{ item.properties.childwbs }}',
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
            subject='{{ get_company_key() + " | Replicon resource and billing assignment sync for C1 IWO Lean Staffing - " }} \
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
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        new_file_sensor >> is_xml >> rail.Label(
            "NO") >> send_bad_file_format_email
        is_xml >> rail.Label("YES") >> download_file >> rail.Label(
            "ALWAYS") >> was_new_file_found
        was_new_file_found >> rail.Label("YES") >> archive_file
        was_new_file_found >> rail.Label("NO") >> delete_this_dagrun
        download_file >> parse_xml
        parse_xml >> has_data
        has_data >> rail.Label("NO") >> send_blank_payload_email
        has_data >> rail.Label('YES') >> get_all_billing_rates
        get_all_billing_rates >> existing_billing_rates_in_replicon >> create_labour_type_item_data_collection
        create_labour_type_item_data_collection >> query_all_distinct_labour_type
        query_all_distinct_labour_type >> create_feed_labour_type_collection
        create_feed_labour_type_collection >> query_distinct_labour_type_not_in_replicon >> has_unique_labour_type
        has_unique_labour_type >> rail.Label(
            "YES") >> create_billing_rates >> wait_for_create_billing_rates >> get_all_project_custom_fields
        has_unique_labour_type >> rail.Label(
            "NO") >> get_all_project_custom_fields
        get_all_project_custom_fields >> get_all_billing_rates_1 >> get_c1_leanstaffing_import_base_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data >> get_active_user
        report_has_data >> rail.Label('NO') >> fail_no_report_data
        get_active_user >> get_item_from_xml >> c1_lean_staffing_process_each_records >> \
        generate_output_log >> [
            get_successful_logs, get_errored_logs, get_exception_logs] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
    return dag


rail.for_each_instance(create_dag)
