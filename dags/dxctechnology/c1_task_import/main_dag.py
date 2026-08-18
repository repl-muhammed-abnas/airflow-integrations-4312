from datetime import timedelta
import rail
from dxctechnology.c1_task_import.send_logs import get_send_logs
from dxctechnology.c1_task_import import custom_method

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_master_{config.instance}",
        description=f"DXCTechnology C1 task import Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.c1_task_import_master_max_active_runs
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
            # We do the timeout with a soft fail here to yield to make sure this dag cycles once in a while so that transient network
            # failures have less of a chance of causing the dag to fail, and people to get notified. If this dag ran indefinitely
            # then 3 network failures several days apart would cause alerts to
            # be sent out, which really is not necessary.
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task assignment sync for C1 Task - Incorrect Format - {{ current_time() }}',
            html_content='email_bad_file_format.html',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
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

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_csv = rail.LoadCSVFileOperator(
            task_id="parse_csv",
            document='{{result("download_file")}}'
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            name="input_data",
            source="{{result('parse_csv')}}",
            columns={
                'WBSELEMENT': 'wbs',
                    'TASK': 'task',
                    'DESCRIPTION': 'description',
                    'VALID_TO': 'validto',
                    'VALID_FROM': 'validfrom',
                    'HDR_Material': 'hdrmaterial',
                    'HDR_VALID_TO': 'hdrvalidto',
                    'HDR_VALID_FROM': 'hdrvalidfrom',
                    'ITM_Material': "itmmaterial",
                    'ITM_VALID_TO': "itmvalidto",
                    'ITM_VALID_FROM': "itmvalidfrom",
            }
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("create_input_collection","length") > 0 }}',
            yes_task=['valid_input_data', 'invalid_input_data'],
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon task assignment sync for C1 Task - Blank File -  {{ current_time() }}',
            html_content="email_blank_payload.html"
        )

        valid_input_data = rail.QueryCollectionOperator(
            task_id="valid_input_data",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NOT NULL AND NULLIF(task, '') IS NOT NULL"""
        )

        invalid_input_data = rail.QueryCollectionOperator(
            task_id="invalid_input_data",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NULL OR NULLIF(task, '') IS NULL"""
        )

        has_invalid_input_data = rail.IfOperator(
            task_id="has_invalid_input_data",
            test="{{result('invalid_input_data','length') > 0}}",
            yes_task="log_invalid_input_data",
            no_task="has_any_entries_in_log"
        )

        log_invalid_input_data = rail.WriteLogOperator(
            task_id="log_invalid_input_data",
            items='{{ result("invalid_input_data") }}',
            message='WBS/Task is not present',
            severity='Exception',
            properties=lambda item: {
                'wbs': item['wbs'],
                'task': item['task'],
                'status': 'Exception',
                'details': "WBS is not available in feed file" if item['wbs'] == "" else "Task is not available in feed file"
            }
        )

        has_valid_input_data = rail.IfOperator(
            task_id="has_valid_input_data",
            test="{{result('valid_input_data','length') > 0}}",
            yes_task="input_data_to_task_data",
            no_task="has_any_entries_in_log"
        )

        input_data_to_task_data = rail.DataAdaptorOperator(
            task_id="input_data_to_task_data",
            source='{{result("valid_input_data")}}',
            columns=['wbs', 'taskname', 'taskcode', 'startdate', 'enddate'],
            data=custom_method.convert_input_data_to_task_data,
        )

        valid_input_taskdata_collection = rail.CreateCollectionOperator(
            task_id="valid_input_taskdata_collection",
            source="{{result('input_data_to_task_data')}}"
        )

        query_unique_project = rail.QueryCollectionOperator(
            task_id="query_unique_project",
            query="""SELECT DISTINCT wbs as wbs FROM valid_input_taskdata_collection"""
        )

        get_all_filter_definition = rail.RepliconServiceOperator(
            task_id="get_all_filter_definition",
            endpoint="/services/ProjectListService1.svc/GetAllFilterDefinitions",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'name', 'Parent WBS', 'uri')
        )
        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'][0]['columns'], 'displayText', 'Parent WBS', 'uri')
        )
        process_each_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_wbs",
            trigger_dag_id=f"dxctechnology_c1_task_import_child_c1_wbs_{config.instance}",
            items="{{result('query_unique_project')}}",
            execution_timeout=timedelta(days=14),
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name }}",
                "wbs": "{{item.wbs}}",
                "parent_wbs_filter_uri": "{{result('get_all_filter_definition')}}",
                "parent_wbs_column_uri": "{{result('get_all_columns')}}"
            },
            retries=0
        )

        wait_for_process_each_wbs = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_wbs',
            dag_runs='{{ result("process_each_wbs") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        send_logs, _ = get_send_logs(config)

        new_file_sensor >> is_csv >> rail.Label(
            "Yes") >> download_file >> parse_csv >> create_input_collection >> has_data

        is_csv >> rail.Label("No") >> send_bad_file_format_email

        has_data >> rail.Label("No") >> send_blank_payload_email
        has_data >> rail.Label("Yes") >> [valid_input_data, invalid_input_data]

        invalid_input_data >> has_invalid_input_data
        has_invalid_input_data >> rail.Label(
            "Yes") >> log_invalid_input_data >> send_logs
        has_invalid_input_data >> rail.Label("No") >> send_logs

        valid_input_data >> has_valid_input_data
        has_valid_input_data >> rail.Label("No") >> send_logs
        has_valid_input_data >> rail.Label("Yes") >> input_data_to_task_data \
            >> valid_input_taskdata_collection >> [query_unique_project, get_all_filter_definition, get_all_columns]\
            >> process_each_wbs >> wait_for_process_each_wbs >> send_logs

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
