from datetime import timedelta
import rail
from rail.lib.ecid import get_dagrun_ecid
from lead3rllc.project_import.utils import request_payload, custom_methods


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'LEAD3R LLC Project Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_file_csv = rail.IfOperator(
            task_id='is_file_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task="download_input_csv",
            no_task="send_incorrect_file_format_mail",
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id='send_incorrect_file_format_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Project Import - Incorrect file format received - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}''',
            html_content="templates/incorrect_file_format.html"
        )

        download_input_csv = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
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

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='''{{ result("new_file_sensor") }}''',
            new_filename=config.sftp_archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_input_csv') }}"
        )

        create_csv_lines_input = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_input',
            source="{{ result('parse_csv') }}",
            header=[
                "deal_name", "deal_id", "engagement_lead", "deal_type", "netsuite_project_type", "company_name", "amount_in_company_currency",
                "contract_start_date", "contract_end_date"],
            row=custom_methods.row_data_for_input_file,
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('create_csv_lines_input') }}",
            name="inputfile",
        )

        if_csv_has_data = rail.IfOperator(
            task_id='if_csv_has_data',
            test="{{ result('create_collection_from_csv','length') > 0 }}",
            yes_task='create_project_import_log',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Project import - Blank File - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ',
            html_content="templates/email_blank_payload.html"
        )

        create_project_import_log = rail.CreateLogOperator(
            task_id='create_project_import_log'
        )

        get_all_project_codes_in_replicon = rail.RepliconServiceOperator(
            task_id='get_all_project_codes_in_replicon',
            endpoint='/services/ProjectListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: "'" + "','".join([project['cells'][1]['textValue'] for project in data['rows']
                                                       if project['cells'][1]['dataType'] == "urn:replicon:list-type:string"]) + "'"
        )

        query_list_projects_missing_required_fields = rail.QueryCollectionOperator(
            task_id='query_list_projects_missing_required_fields',
            query="""SELECT * FROM  inputfile WHERE NULLIF(deal_id,'') IS NULL OR NULLIF(deal_name,'') IS NULL""",
            name="invalid_records"
        )

        if_projects_with_missing_required_fields_list_has_data = rail.IfOperator(
            task_id='if_projects_with_missing_required_fields_list_has_data',
            test=lambda: rail.result(
                'query_list_projects_missing_required_fields', 'length') > 0,
            yes_task='log_invalid_records',
            no_task='query_list_valid_records'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{result('create_project_import_log')}}",
            items="{{result('query_list_projects_missing_required_fields')}}",
            message='One or more mandatory field is missing',
            severity='Exception',
            properties=lambda item: {
                "deal_id": item['deal_id'],
                "deal_name": item['deal_name'],
                "company_name": item['company_name'],
                "action": "Validation",
                "status": "Exception",
                "details": custom_methods.get_missing_field_message(item)
            }
        )

        query_list_valid_records = rail.QueryCollectionOperator(
            task_id='query_list_valid_records',
            query="""SELECT * FROM  inputfile WHERE (NULLIF(deal_id,'') IS NOT NULL AND NULLIF(deal_name,'') IS NOT NULL AND deal_id NOT IN ({{result('get_all_project_codes_in_replicon')}})) """,
            name="valid_records"
        )

        query_to_check_matching_existing_projects_in_replicon = rail.QueryCollectionOperator(
            task_id='query_to_check_matching_existing_projects_in_replicon',
            query="""SELECT * FROM  inputfile WHERE (NULLIF(deal_id,'') IS NOT NULL AND NULLIF(deal_name,'') IS NOT NULL AND deal_id IN ({{result('get_all_project_codes_in_replicon')}})) """,
        )

        if_records_with_existing_project_code_exist = rail.IfOperator(
            task_id='if_records_with_existing_project_code_exist',
            test=lambda: rail.result(
                'query_to_check_matching_existing_projects_in_replicon', 'length') > 0,
            yes_task='log_records_with_existing_project_code_in_replicon',
            no_task='if_valid_records_present'
        )

        log_records_with_existing_project_code_in_replicon = rail.WriteLogOperator(
            task_id='log_records_with_existing_project_code_in_replicon',
            log="{{result('create_project_import_log')}}",
            items="{{result('query_to_check_matching_existing_projects_in_replicon')}}",
            message='Project with the same Project Code already exists in replicon',
            severity='Exception',
            properties=lambda item: {
                "deal_id": item['deal_id'],
                "deal_name": item['deal_name'],
                "company_name": item['company_name'],
                "action": "Validation",
                "status": "Exception",
                "details": 'Project with the same Project Code already exists in replicon'
            }
        )

        if_valid_records_present = rail.IfOperator(
            task_id='if_valid_records_present',
            test=lambda: rail.result(
                'query_list_valid_records', 'length') > 0,
            yes_task='create_add_client_and_missing_field_values_log',
            no_task='dummy_process_log_generation'
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        create_add_client_and_missing_field_values_log = rail.CreateLogOperator(
            task_id='create_add_client_and_missing_field_values_log'
        )

        get_required_oef_uri_for_projects = rail.RepliconServiceOperator(
            task_id='get_required_oef_uri_for_projects',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: {
                'netsuite_project_type_uri': rail.find_first_by_attr_and_get_attr(response, 'name', 'Netsuite Project Type', 'uri')
            }
        )

        trigger_dag_add_missing_values_in_replicon = rail.TriggerDagRunOperator(
            task_id='trigger_dag_add_missing_values_in_replicon',
            retries=0,
            trigger_dag_id=config.child_add_missing_values_in_replicon_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.add_missing_values_in_replicon_payload(
                get_dagrun_ecid(dag_run))
        )

        wait_for_completion_trigger_dag_add_missing_values_in_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_add_missing_values_in_replicon',
            dag_runs="{{result('trigger_dag_add_missing_values_in_replicon')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_replicon_users = rail.RepliconServiceOperator(
            task_id='get_all_replicon_users',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda x: {
                'user_first_name_last_name': x['cells'][0]['textValue'],
                'user_uri': x['cells'][0]['uri'],
                'user_isenabled': x['cells'][1]['textValue']}, data['rows'])) if data['rows'] else [
                    {'user_first_name_last_name': '', 'user_uri': '', 'user_isenabled': ''}]
        )

        collection_replicon_users = rail.CreateCollectionOperator(
            task_id='collection_replicon_users',
            source=lambda: rail.result('get_all_replicon_users'),
            name='replicon_users'
        )

        get_all_updated_department_groups = rail.RepliconServiceOperator(
            task_id='get_all_updated_department_groups',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_all_department_groups_payload,
            data_handler=lambda data: list(map(lambda x: {
                'department_group_name': x['cells'][0]['textValue'],
                'department_group_uri': x['cells'][0]['uri'],
                'department_group_enabled': x['cells'][1]['textValue']}, data['rows'])) if data['rows'] else [
                    {'department_group_name': '', 'department_group_uri': '', 'department_group_enabled': ''}]
        )

        create_collection_all_updated_replicon_department_groups = rail.CreateCollectionOperator(
            task_id='create_collection_all_updated_replicon_department_groups',
            source=lambda: rail.result('get_all_updated_department_groups'),
            name='replicon_updated_department_groups'
        )

        query_valid_records_and_add_uris_for_engagement_leads_and_clients = rail.QueryCollectionOperator(
            task_id='query_valid_records_and_add_uris_for_engagement_leads_and_clients',
            query="""SELECT * FROM  valid_records 
                LEFT JOIN replicon_users ON valid_records.engagement_lead = replicon_users.user_first_name_last_name 
                LEFT JOIN replicon_updated_department_groups ON valid_records.deal_type = replicon_updated_department_groups.department_group_name""",
            name='project_records_to_process'
        )

        trigger_dag_run_add_projects = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_add_projects',
            items="{{ result('query_valid_records_and_add_uris_for_engagement_leads_and_clients')}}",
            parallel_count=config.parallel_dagrun_count,
            trigger_dag_id=config.add_project_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'netsuite_project_type_oef_uri': rail.result('get_required_oef_uri_for_projects')['netsuite_project_type_uri'],
                'project_import_log': rail.result('create_project_import_log'),
            }
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dag_id,
            conf=lambda: {
                'input_file_records': rail.result('create_collection_from_csv', 'length'),
                'project_import_log': rail.result('create_project_import_log'),
                'input_filename': rail.render_template("{{result('new_file_sensor') | file_name }}")
            }
        )

        new_file_sensor >> is_file_csv

        is_file_csv >> rail.Label(
            'No') >> send_incorrect_file_format_mail
        is_file_csv >> rail.Label('Yes') >> download_input_csv

        download_input_csv >> was_new_file_found

        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file

        download_input_csv >> parse_csv >> create_csv_lines_input >> create_collection_from_csv\
            >> if_csv_has_data

        if_csv_has_data >> rail.Label('No') >> send_blank_payload_email
        if_csv_has_data >> rail.Label('Yes') >> create_project_import_log >> get_all_project_codes_in_replicon >> query_list_projects_missing_required_fields \
            >> if_projects_with_missing_required_fields_list_has_data

        if_projects_with_missing_required_fields_list_has_data >> rail.Label(
            'No') >> query_list_valid_records
        if_projects_with_missing_required_fields_list_has_data >> rail.Label(
            'Yes') >> log_invalid_records >> query_list_valid_records

        query_list_valid_records >> query_to_check_matching_existing_projects_in_replicon >> if_records_with_existing_project_code_exist

        if_records_with_existing_project_code_exist >> rail.Label(
            'No') >> if_valid_records_present
        if_records_with_existing_project_code_exist >> rail.Label(
            'Yes') >> log_records_with_existing_project_code_in_replicon >> if_valid_records_present

        if_valid_records_present >> rail.Label(
            'No') >> dummy_process_log_generation >> process_log_generation
        if_valid_records_present >> rail.Label(
            'Yes') >> create_add_client_and_missing_field_values_log

        create_add_client_and_missing_field_values_log >> get_required_oef_uri_for_projects >> trigger_dag_add_missing_values_in_replicon \
            >> wait_for_completion_trigger_dag_add_missing_values_in_replicon >> get_all_replicon_users 

        get_all_replicon_users >> collection_replicon_users

        collection_replicon_users >> get_all_updated_department_groups >> create_collection_all_updated_replicon_department_groups \
            >> query_valid_records_and_add_uris_for_engagement_leads_and_clients >> trigger_dag_run_add_projects \
            >> process_log_generation

    return dag


rail.for_each_instance(create_dag)
