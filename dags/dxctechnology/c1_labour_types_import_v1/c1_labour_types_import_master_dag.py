from datetime import timedelta
import rail
from dxctechnology.c1_labour_types_import_v1 import request_payload
from dxctechnology.c1_labour_types_import_v1 import response_filter
from dxctechnology.c1_labour_types_import_v1 import python_callable_method

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_labour_types_import_v1/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_labour_types_import_master{dag_id_postfix}_v1',
        description=f'DXC_C1_Labour Types_Automation Master V2.0 - SFTP {config.instance}',
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
            subject='{{ get_company_key() }} | Replicon billing assignment sync for C1 Labour type - Incorrect Format - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
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
            document="{{ result('download_file') }}"
        )

        create_labour_type_data_collection = rail.CreateCollectionOperator(
            task_id='create_labour_type_data_collection',
            source="{{ result('load_labour_types_data') }}",
            name="labourtypedata",
            columns={
                'WBSELEMENT': 'wbs',
                'HDR_Material': 'hdrlabortype',
                'HDR_VALID_TO': 'hdrenddate',
                'HDR_VALID_FROM': 'hdrstartdate',
                'ITM_Material': 'itmlabourtype',
                'ITM_VALID_TO': 'itmenddate',
                'ITM_VALID_FROM': 'itmstartdate',
                'LABOR_TYPE_DESCRIPTION': 'description'
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
            subject='{{ get_company_key() }} | Replicon billing assignment sync for C1 Labour type - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="email_blank_payload.html"
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
            query="""SELECT DISTINCT wbs, hdrlabortype, hdrstartdate, hdrenddate, description
                    FROM labourtypedata
                    WHERE NULLIF(wbs, '') IS NOT NULL AND NULLIF(hdrlabortype, '') IS NOT NULL"""
        )

        query_itm_labour_type_data = rail.QueryCollectionOperator(
            task_id='query_itm_labour_type_data',
            query="""SELECT DISTINCT wbs, itmlabourtype, itmstartdate, itmenddate, description
                    FROM labourtypedata
                    WHERE NULLIF(wbs, '') IS NOT NULL AND NULLIF(itmlabourtype, '') IS NOT NULL"""
        )

        final_list = rail.PythonOperator(
            task_id="final_list",
            python_callable=python_callable_method.do_final_list,
            op_args=['query_hdr_labour_type_data',
                     'query_itm_labour_type_data']
        )

        labour_types_data_collection = rail.CreateCollectionOperator(
            task_id="labour_types_data_collection",
            source="{{ result('final_list') | to_json }}",
            name="labourtypesdata"
        )

        all_billing_rates_for_wbs = rail.QueryCollectionOperator(
            task_id="all_billing_rates_for_wbs",
            name="billingratesforwbs",
            query="""SELECT * FROM labourtypesdata WHERE NULLIF(labourtypes, '') IS NOT NULL"""
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
            query="""SELECT DISTINCT labourtypes, description FROM inputcombineddata WHERE NULLIF(labourtypes, '') IS NOT NULL"""
        )

        labour_types_to_be_created_in_replicon = rail.QueryCollectionOperator(
            task_id="labour_types_to_be_created_in_replicon",
            query="""SELECT * FROM feedlabourtypes WHERE LOWER(labourtypes) NOT IN (SELECT DISTINCT LOWER(name) FROM existingbillingrates)"""
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
            trigger_dag_id=f'dxctechnology_c1_labour_types_child_create_billing_rate{dag_id_postfix}_v1',
            conf=lambda item: {
                'name': item['labourtypes'],
                'description': item['description']
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

        dummy_process_billing_rate_for_wbs = rail.EmptyOperator(
            task_id='dummy_process_billing_rate_for_wbs'
        )

        process_billing_rate_for_each_wbs_item = rail.trigger_parallel_dagrun(
            task_id='process_billing_rate_for_each_wbs_item',
            items="{{ result('query_distinct_projects') }}",
            parallel_count=config.trigger_parallel_dagrun_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_c1_labour_types_process_distinct_wbs_item{dag_id_postfix}_v1',
            conf=request_payload.get_process_billing_rate_wbs_conf
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
                'WBS Element',
                'Labor Type',
                'Status',
                'Details',
                'Job ID',
                'Job Runtime: {{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{- result("get_successful_billing_rates", key="length") + \
                    result("get_skipped_billing_rates", key="length") + \
                        result("get_errored_billing_rates", key="length") + result("get_exception_billing_rates", key="length") }}',
                'Function: C1 Labor Type'],
            row=[
                '{{ item.properties.wbs }}',
                '{{ item.properties.billingrate }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
            footer=[
                # pylint: disable=line-too-long
                'Number of Records Processed Successfully: {{- result("get_successful_billing_rates", key="length") + result("get_skipped_billing_rates", key="length") }}',
                'Number of Records with Error: {{ result("get_errored_billing_rates", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_billing_rates", key="length") }}',
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
            bcc="{%- if result('get_errored_billing_rates', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon billing assignment sync for C1 Labour type - " }} \
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
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> is_csv

        is_csv >> rail.Label(
            "No") >> send_bad_file_format_email
        is_csv >> rail.Label(
            "Yes") >> download_file

        download_file >> load_labour_types_data >> \
            create_labour_type_data_collection >> has_labour_type_data

        has_labour_type_data >> rail.Label(
            "No") >> send_blank_payload_email
        has_labour_type_data >> rail.Label(
            "Yes") >> get_billing_rates_before_create

        get_billing_rates_before_create >> existing_billing_rates_in_replicon >> [
            query_hdr_labour_type_data, query_itm_labour_type_data] >> final_list >> \
            labour_types_data_collection >> all_billing_rates_for_wbs >> has_labour_types

        has_labour_types >> rail.Label(
            "Yes") >> input_combined_list >> input_combined_data_collection >> distinct_labour_types >> labour_types_to_be_created_in_replicon >> \
            has_labour_types_to_be_created

        has_labour_types_to_be_created >> rail.Label(
            "Yes") >> create_billing_rates >> wait_for_create_billing_rates >> \
            get_billing_rates_after_create >> query_distinct_projects
        has_labour_types_to_be_created >> rail.Label(
            "No") >> query_distinct_projects

        query_distinct_projects >> get_all_filter_definitions >> get_all_columns >> dummy_process_billing_rate_for_wbs
        dummy_process_billing_rate_for_wbs >> process_billing_rate_for_each_wbs_item >> \
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
