from datetime import timedelta
import os
import rail
from dxctechnology.gsap_task_import.tasks.send_logs import get_send_logs

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/gsap_task_import/config.py

# pylint: disable=too-many-statements


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_gsap_task_import_master_{config.instance}",
        description=f"DXCTechnology C1 task import Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.gsap_task_import_master_max_active_runs
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=15),
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
            subject='{{ get_company_key() }} | Replicon task assignment sync for GSAP Task - Incorrect Format - {{ current_time() }}',
            html_content='/templates/emails/bad_file_format.html',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
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

        parse_xml = rail.LoadXMLFileOperator(
            task_id="parse_xml",
            document='{{result("download_file")}}',
            xsd_document='./dags/dxctechnology/gsap_task_import/xsdschema/input_schema.xsd'
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
            subject='{{ get_company_key() }} | Replicon task assignment sync for GSAP Task - Blank File -  {{ current_time() }}',
            html_content="/templates/emails/blank_payload.html"
        )

        load_input_data = rail.XMLAdaptorOperator(
            task_id="load_input_data",
            source="{{result('parse_xml')}}",
            target="result",
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

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            name="input_data",
            source="{{result('load_input_data') | to_json}}",
        )

        invalid_input_data = rail.QueryCollectionOperator(
            task_id="invalid_input_data",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NULL OR NULLIF(task_name, '') IS NULL"""
        )

        has_invalid_input_data = rail.IfOperator(
            task_id="has_invalid_input_data",
            test="{{result('invalid_input_data','length') > 0}}",
            yes_task="log_invalid_input_data",
            no_task="generate_logs"
        )

        log_invalid_input_data = rail.WriteLogOperator(
            task_id="log_invalid_input_data",
            items='{{ result("invalid_input_data") }}',
            message='WBS/Task is not present',
            severity='Exception',
            properties=lambda item: {
                'wbs': item['wbs'],
                'task': item['task_name'],
                'status': 'Exception',
                'details': "WBS is not available in feed file" if not item['wbs'] else "Task is not available in feed file"
            }
        )

        valid_input_data = rail.QueryCollectionOperator(
            task_id="valid_input_data",
            query="""SELECT * FROM input_data WHERE NULLIF(wbs, '') IS NOT NULL AND NULLIF(task_name, '') IS NOT NULL"""
        )

        has_valid_input_data = rail.IfOperator(
            task_id="has_valid_input_data",
            test="{{result('valid_input_data','length') > 0}}",
            yes_task="query_unique_project",
            no_task="generate_logs"
        )

        query_unique_project = rail.QueryCollectionOperator(
            task_id="query_unique_project",
            query="""SELECT DISTINCT wbs as wbs FROM valid_input_data"""
        )

        get_custom_field_group = rail.RepliconServiceOperator(
            task_id="get_custom_field_group",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:task"
            }
        )

        get_all_filter_definition = rail.RepliconServiceOperator(
            task_id="get_all_filter_definition",
            endpoint="/services/ProjectListService1.svc/GetAllFilterDefinitions",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'name', 'Parent WBS', 'uri')
        )

        get_task_type_udf = rail.RepliconServiceOperator(
            task_id="get_task_type_udf",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_custom_field_group').uri}}"
            },
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'Task Type', 'uri')
        )

        get_gsap_task_dropdown_value = rail.RepliconServiceOperator(
            task_id="get_gsap_task_dropdown_value",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{result('get_task_type_udf')}}"
            },
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'GSAP Task', 'uri')
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'][0]['columns'], 'displayText', 'Parent WBS', 'uri')
        )

        def get_trigger_parallel_dagrun_conf(item):
            return {
                "file_name": os.path.split(rail.result('new_file_sensor'))[1],
                "wbs": item['wbs'],
                "parent_wbs_filter_uri": rail.result('get_all_filter_definition'),
                "parent_wbs_column_uri": rail.result('get_all_columns'),
                "task_type_oef_uri": rail.result('get_task_type_udf'),
                "gsap_task_option_uri": rail.result('get_gsap_task_dropdown_value')
            }

        process_each_wbs = rail.trigger_parallel_dagrun(
            task_id = "process_each_wbs",
            items="{{ result('query_unique_project') }}",
            trigger_dag_id=config.process_each_gsap_wbs_dagid,
            conf=get_trigger_parallel_dagrun_conf,
            parallel_count=config.parallel_run_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        generate_logs = rail.EmptyOperator(
            task_id="generate_logs"
        )

        send_logs, send_logs_complete = get_send_logs(config)

        gather_details = [get_all_columns,
                          get_all_filter_definition, get_task_type_udf]

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info= {
                "file_name": "{{result('new_file_sensor') | file_name }}"
            }
        )

        new_file_sensor >> is_xml >> rail.Label(
            "No") >> send_bad_file_format_email
        is_xml >> rail.Label("Yes") >> download_file >> parse_xml >> has_data >> rail.Label(
            "No") >> send_blank_payload_email
        download_file >> rail.Label("Always") >> was_new_file_found

        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        has_data >> rail.Label("Yes") >> load_input_data >> create_input_collection >> [
            invalid_input_data, valid_input_data]

        invalid_input_data >> has_invalid_input_data >> rail.Label(
            "Yes") >> log_invalid_input_data >> generate_logs
        has_invalid_input_data >> rail.Label("No") >> generate_logs

        valid_input_data >> has_valid_input_data >> rail.Label(
            "No") >> generate_logs
        has_valid_input_data >> query_unique_project >> get_custom_field_group >> gather_details >> \
            get_gsap_task_dropdown_value >> process_each_wbs >> generate_logs >> send_logs

        send_logs_complete >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo
    return dag


rail.for_each_instance(create_master_dag)
