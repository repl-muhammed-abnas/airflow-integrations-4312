from datetime import timedelta
import rail
from dxctechnology.wf39_psa_resource_assignment.utils import request_payload
from dxctechnology.wf39_psa_resource_assignment.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment.utils import python_callable_method


# pylint: disable=too-many-statements
def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_wf39_psa_resource_assignment_import_master_{config.instance}',
        description=f'DXC_WF39 PSA Resource Assignment Master V2.0 - SFTP {config.instance}',
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

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
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

        load_labour_types_data = rail.LoadCSVFileOperator(
            task_id='load_labour_types_data',
            document="{{ result('decrypt_file') }}",
            delimiter='|'
        )

        create_labour_type_data_collection = rail.CreateCollectionOperator(
            task_id='create_labour_type_data_collection',
            source="{{ result('load_labour_types_data') }}",
            name="labourtypedata",
            columns={
                'PERN': 'employeeid',
                'WBS': 'wbs',
                'StartDate': 'startdate',
                'EndDate': 'enddate',
                'Role': 'role'
            }
        )

        has_labour_type_data = rail.IfOperator(
            task_id='has_labour_type_data',
            test="{{ result('create_labour_type_data_collection','length') > 0 }}",
            yes_task='get_billing_rates_before_create',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon WF39 Resource assignment sync - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/blank_payload.html"
        )

        get_billing_rates_before_create = rail.RepliconServiceOperator(
            task_id="get_billing_rates_before_create",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=response_filter.map_billing_rates
        )

        existing_billing_rates_in_replicon = rail.CreateCollectionOperator(
            task_id="existing_billing_rates_in_replicon",
            source="{{ result('get_billing_rates_before_create') | to_json }}",
            name="existingbillingrates"
        )

        query_hdr_labour_type_data = rail.QueryCollectionOperator(
            task_id='query_hdr_labour_type_data',
            query="""SELECT DISTINCT wbs, role, startdate, enddate, employeeid
                    FROM labourtypedata
                    WHERE NULLIF(wbs, '') IS NOT NULL AND NULLIF(employeeid, '') IS NOT NULL"""
        )

        labour_types_data_collection = rail.CreateCollectionOperator(
            task_id="labour_types_data_collection",
            source="{{ result('query_hdr_labour_type_data') }}",
            name="labourtypesdata"
        )

        all_billing_rates_for_wbs = rail.QueryCollectionOperator(
            task_id="all_billing_rates_for_wbs",
            name="billingratesforwbs",
            query="""SELECT * FROM labourtypesdata"""
        )

        has_labour_types = rail.IfOperator(
            task_id="has_labour_types",
            test="{{ result('all_billing_rates_for_wbs','length') > 0 }}",
            yes_task="input_combined_list"
        )

        input_combined_list = rail.PythonOperator(
            task_id="input_combined_list",
            python_callable=python_callable_method.get_input_combined_list,
            op_args=['all_billing_rates_for_wbs']
        )

        input_combined_data_collection = rail.CreateCollectionOperator(
            task_id="input_combined_data_collection",
            source="{{ result('input_combined_list') | to_json }}",
            name="inputcombineddata"
        )

        distinct_labour_types = rail.QueryCollectionOperator(
            task_id="distinct_labour_types",
            name="feedlabourtypes",
            query="""SELECT DISTINCT role FROM inputcombineddata WHERE NULLIF(role, '') IS NOT NULL"""
        )

        labour_types_to_be_created_in_replicon = rail.QueryCollectionOperator(
            task_id="labour_types_to_be_created_in_replicon",
            query="""SELECT * FROM feedlabourtypes WHERE LOWER(role) NOT IN (SELECT DISTINCT LOWER(name) FROM existingbillingrates)"""
        )

        has_labour_types_to_be_created = rail.IfOperator(
            task_id='has_labour_types_to_be_created',
            test='{{ result("labour_types_to_be_created_in_replicon", "length") > 0 }}',
            yes_task="create_billing_rates",
            no_task="query_distinct_projects"
        )

        create_billing_rates = rail.TriggerDagRunForEachItemOperator(
            task_id='create_billing_rates',
            retries=0,
            items="{{ result('labour_types_to_be_created_in_replicon') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_wf39_psa_resource_assignment_create_billing_rate_child_{config.instance}',
            conf=lambda item: {
                'name': item['role']
            }
        )

        wait_for_create_billing_rates = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_billing_rates',
            dag_runs='{{ result("create_billing_rates") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_billing_rates_after_create = rail.RepliconServiceOperator(
            task_id="get_billing_rates_after_create",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=response_filter.map_billing_rates
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id="query_distinct_projects",
            query="""SELECT DISTINCT wbs FROM labourtypesdata"""
        )

        get_all_filter_definitions = rail.RepliconServiceOperator(
            task_id="get_all_filter_definitions",
            endpoint="/services/ProjectListService1.svc/GetAllFilterDefinitions",
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'][0]['columns'], 'displayText', 'Parent WBS', 'uri')
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

        process_billing_rate_for_each_wbs_item = rail.TriggerDagRunForEachItemOperator(
            task_id='process_billing_rate_for_each_wbs_item',
            retries=0,
            items="{{ result('query_distinct_projects') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_wf39_psa_resource_assignment_process_distinct_wbs_item_child_{config.instance}',
            conf=request_payload.get_process_billing_rate_wbs_conf
        )

        wait_for_process_billing_rate_for_each_wbs_item = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_billing_rate_for_each_wbs_item',
            dag_runs='{{ result("process_billing_rate_for_each_wbs_item") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_successful_billing_rates = rail.FilterLogEntriesOperator(
            task_id='get_successful_billing_rates',
            properties={'status': 'Success'}
        )

        get_skipped_billing_rates = rail.FilterLogEntriesOperator(
            task_id='get_skipped_billing_rates',
            properties={'status': 'Skipped'}
        )

        get_errored_billing_rates = rail.FilterLogEntriesOperator(
            task_id='get_errored_billing_rates',
            properties={'status': 'Error'}
        )

        get_exception_billing_rates = rail.FilterLogEntriesOperator(
            task_id='get_exception_billing_rates',
            properties={'status': 'Exception'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'PERN__C',
                'WBS__C',
                'Transaction_Type__C',
                'Status__C',
                'Status_Description__C',
                'Replicon_Job__ID'],
            row=[
                '{{ item.properties.employeeid }}',
                '{{ item.properties.wbs }}',
                '{{item.properties.action}}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv')

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_billing_rates', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon PSA Resource Assignment for WF39 - " }} \
                {%- if result("get_errored_billing_rates", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_billing_rates", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> download_file

        download_file >> decrypt_file >> load_labour_types_data >> \
            create_labour_type_data_collection >> has_labour_type_data

        has_labour_type_data >> rail.Label(
            "No") >> send_blank_payload_email
        has_labour_type_data >> rail.Label(
            "Yes") >> get_billing_rates_before_create

        get_billing_rates_before_create >> existing_billing_rates_in_replicon >> query_hdr_labour_type_data >> \
            labour_types_data_collection >> all_billing_rates_for_wbs >> has_labour_types

        has_labour_types >> rail.Label(
            "Yes") >> input_combined_list >> input_combined_data_collection >> distinct_labour_types >> labour_types_to_be_created_in_replicon >> \
            has_labour_types_to_be_created

        has_labour_types_to_be_created >> rail.Label(
            "Yes") >> create_billing_rates >> wait_for_create_billing_rates >> \
            get_billing_rates_after_create >> query_distinct_projects
        has_labour_types_to_be_created >> rail.Label(
            "No") >> query_distinct_projects

        query_distinct_projects >> get_all_filter_definitions >> get_all_columns >> get_c1_leanstaffing_import_base_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data
        report_has_data >> rail.Label('NO') >> fail_no_report_data
        load_report_data >> process_billing_rate_for_each_wbs_item >> \
            wait_for_process_billing_rate_for_each_wbs_item >> \
            generate_output_log >> [get_successful_billing_rates, get_skipped_billing_rates,
                                    get_errored_billing_rates, get_exception_billing_rates] >> render_logs_csv >> \
            upload_log_to_sftp >> send_import_complete_email

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label(
                "Yes") >> archive_file
        was_new_file_found >> rail.Label(
            "No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
