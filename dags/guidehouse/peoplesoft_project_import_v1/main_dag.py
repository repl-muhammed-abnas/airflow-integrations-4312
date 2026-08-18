from datetime import timedelta
import pendulum
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
from airflow.models import Variable
from guidehouse.peoplesoft_project_import_v1.utils import custom_method, request_payload, response_filter

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Guidehouse Project Import - File-based CSV Processing (Phase 1)',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        start_date= pendulum.datetime(2025, 9, 1, tz=config.time_zone),
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        # ========== SFTP File Processing ==========
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_pgp = rail.IfOperator(
            task_id='is_pgp',
            test="{{ result('new_file_sensor') | file_ext | lower == 'pgp' }}",
            yes_task='validate_file_name',
            no_task='send_bad_file_format_email'
        )

        validate_file_name = rail.PythonOperator(
            task_id='validate_file_name',
            python_callable=lambda: custom_method.validate_project_file_name(
                rail.result('new_file_sensor'),
                config.file_name_prefix
            )
        )

        is_valid_file_name = rail.IfOperator(
            task_id='is_valid_file_name',
            test=lambda: rail.result('validate_file_name')['is_valid'],
            yes_task='download_file',
            no_task='check_file_type'
        )

        check_file_type = rail.IfOperator(
            task_id='check_file_type',
            test=lambda: rail.result('validate_file_name')['is_resource_file'],
            yes_task='delete_this_dagrun_resource_file',
            no_task='archive_unknown_file'
        )

        delete_this_dagrun_resource_file = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun_resource_file'
        )

        archive_unknown_file = rail.SFTPMoveFileOperator(
            task_id='archive_unknown_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/invalid_{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        send_invalid_file_name_email = rail.EmailOperator(
            task_id='send_invalid_file_name_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Project Import - Invalid file name on {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/invalid_file_name.html'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Project Import - File processing is skipped on {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/bad_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda: pendulum.now(config.time_zone)
        )

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='false').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id2
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        dummy_load_data = rail.PythonOperator(
            task_id="dummy_load_data",
            python_callable=lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='false').lower() == 'true' else rail.result('download_file'),
            show_return_value_in_logs=False
        )

        load_project_data = rail.LoadCSVFileOperator(
            task_id='load_project_data',
            document="{{ result('dummy_load_data') }}",
            delimiter='|'
        )

        compose_csv =rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('load_project_data') }}",
            header=['PROJECT_ID',
                    'PROJECT_DESCR',
                    'PROJECT_STATUS',
                    'ACTIVITY',
                    'ACTIVITY_DESCR',
                    'ACTIVITY_STATUS',
                    'PROJECT_START_DATE',
                    'PROJECT_END_DATE',
                    'ACTIVITY_START_DATE',
                    'ACTIVITY_END_DATE',
                    'PROJECT_TYPE',
                    'ACTIVITY_TYPE',
                    'ENFORCE',
                    'PROJECT_MANAGER',
                    'CO-MANAGER',
                    'BILL_TO_CUST_ID',
                    'CUSTOMER_NAME',
                    'CP_PROJECT',
                    'DEPT_CODE',
                    'DEPT_NAME'
                    ],
            row=lambda item:[
                item['PROJECT_ID'],
                item['PROJECT_DESCR'],
                item['PROJECT_STATUS'],
                item['ACTIVITY'],
                item['ACTIVITY_DESCR'],
                item['ACTIVITY_STATUS'],
                item['PROJECT_START_DATE'],
                item['PROJECT_END_DATE'],
                item['ACTIVITY_START_DATE'],
                item['ACTIVITY_END_DATE'],
                item['PROJECT_TYPE'],
                item['ACTIVITY_TYPE'],
                item['ENFORCE'],
                item['PROJECT_MANAGER'],
                item['CO-MANAGER'],
                item['BILL_TO_CUST_ID'],
                item['CUSTOMER_NAME'],
                item.get('CP_PROJECT', ''),
                item.get('DEPT_CODE', ''),
                item.get('DEPT_NAME', '')
                ]
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('compose_csv') }}",
            name='inputdata',
            columns={
                "PROJECT_ID": "project_id",
                "PROJECT_DESCR": "project_descr",
                "PROJECT_STATUS": "project_status",
                "ACTIVITY": "activity",
                "ACTIVITY_DESCR": "activity_descr",
                "ACTIVITY_STATUS": "activity_status",
                "PROJECT_START_DATE": "project_start_date",
                "PROJECT_END_DATE": "project_end_date",
                "ACTIVITY_START_DATE": "activity_start_date",
                "ACTIVITY_END_DATE": "activity_end_date",
                "PROJECT_TYPE": "project_type",
                "ACTIVITY_TYPE": "activity_type",
                "ENFORCE": "enforce",
                "PROJECT_MANAGER": "project_manager",
                "CO-MANAGER": "co_manager",
                "BILL_TO_CUST_ID": "bill_to_cust_id",
                "CUSTOMER_NAME": "customer_name",
                "CP_PROJECT": "cp_project",
                "DEPT_CODE": "dept_code",
                "DEPT_NAME": "dept_name"
            }
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log'
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_from_csv', 'length') > 0 }}",
            yes_task='query_blank_mandatory_check',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon PeopleSoft Project Import - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_blank_mandatory_check = rail.QueryCollectionOperator(
            task_id='query_blank_mandatory_check',
            query="""SELECT * FROM inputdata WHERE
                NULLIF(project_id,'') IS NULL OR
                NULLIF(project_descr,'') IS NULL OR
                NULLIF(project_status,'') IS NULL OR
                NULLIF(activity_status,'') IS NULL OR
                NULLIF(project_start_date,'') IS NULL OR
                NULLIF(activity_start_date,'') IS NULL OR
                NULLIF(enforce,'') IS NULL OR
                UPPER(project_status) NOT IN ('A', 'I') OR
                UPPER(activity_status) NOT IN ('A', 'I') OR
                UPPER(enforce) NOT IN ('YES', 'NO')"""
        )

        has_blank_mandatory_fields = rail.IfOperator(
            task_id='has_blank_mandatory_fields',
            test="{{ result('query_blank_mandatory_check', 'length') > 0 }}",
            yes_task='write_blank_mandatory_log',
            no_task='query_valid_data'
        )

        write_blank_mandatory_log = rail.WriteLogOperator(
            task_id="write_blank_mandatory_log",
            items="{{ result('query_blank_mandatory_check') }}",
            log="{{ result('create_exception_log') }}",
            severity="Exception",
            message="Mandatory field(s) missing",
            properties=custom_method.get_invalid_logs_property_conf
        )

        query_valid_data = rail.QueryCollectionOperator(
            task_id='query_valid_data',
            name='validdata',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM inputdata WHERE
                NULLIF(project_id,'') IS NOT NULL AND
                NULLIF(project_descr,'') IS NOT NULL AND
                NULLIF(project_status,'') IS NOT NULL AND
                NULLIF(activity_status,'') IS NOT NULL AND
                NULLIF(project_start_date,'') IS NOT NULL AND
                NULLIF(activity_start_date,'') IS NOT NULL AND
                NULLIF(enforce,'') IS NOT NULL AND
                UPPER(project_status) IN ('A', 'I') AND
                UPPER(activity_status) IN ('A', 'I') AND
                UPPER(enforce) IN ('YES', 'NO')"""
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test="{{ result('query_valid_data', 'length') > 0 }}",
            yes_task='query_distinct_clients',
            no_task='format_logs'
        )

        query_distinct_clients = rail.QueryCollectionOperator(
            task_id='query_distinct_clients',
            name='distinctclients',
            query="""SELECT DISTINCT bill_to_cust_id as client_id, customer_name as client_name
                     FROM validdata
                     WHERE NULLIF(customer_name, '') IS NOT NULL"""
        )

        has_clients_to_process = rail.IfOperator(
            task_id='has_clients_to_process',
            test="{{ result('query_distinct_clients', 'length') > 0 }}",
            yes_task='process_clients_dummy_task',
            no_task='get_service_center_uris'
        )

        process_clients_dummy_task = rail.EmptyOperator(
            task_id='process_clients_dummy_task'
        )

        process_clients = rail.trigger_parallel_dagrun(
            task_id='process_clients',
            items='{{ result("query_distinct_clients") }}',
            trigger_dag_id=config.process_clients_dag_id,
            parallel_count=config.parallel_count_clients,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'client_id': item['client_id'],
                'client_name': item['client_name'],
                'log': rail.result("create_exception_log")
            }
        )

        get_service_center_uris = rail.RepliconServiceOperator(
            task_id='get_service_center_uris',
            endpoint='/services/ServiceCenterService1.svc/GetEnabledServiceCenters',
            data_handler=lambda response: {
                'peoplesoft_service_center_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'PeopleSoft', 'uri', None),
                'india_service_center_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'India', 'uri', None)
            }
        )

        # ========== Cost Center Processing ==========
        query_distinct_cost_centers = rail.QueryCollectionOperator(
            task_id='query_distinct_cost_centers',
            name='distinctcostcenters',
            query="""SELECT DISTINCT dept_code as cost_center_code, dept_name as cost_center_name
                     FROM validdata
                     WHERE NULLIF(dept_name, '') IS NOT NULL"""
        )

        has_costcenters_to_process = rail.IfOperator(
            task_id='has_costcenters_to_process',
            test="{{ result('query_distinct_cost_centers', 'length') > 0 }}",
            yes_task='get_all_costcenters',
            no_task='query_distinct_projects'
        )

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        get_all_costcenters = rail.RepliconServicePageOperator(
            task_id='get_all_costcenters',
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_division_payload,
            page_handler=page_handler,
            all_result_data_handler=response_filter.filter_all_divisions_data
        )

        create_costcenter_collection = rail.CreateCollectionOperator(
            task_id='create_costcenter_collection',
            source="{{ result('get_all_costcenters') | to_json }}",
            name='existingdivisions',
            columns={
                "name": "cost_center_name",
                "description": "cost_center_code",  # Note: DEPT_CODE from CSV is actually the description field
                "enabled": "enabled"
            }
        )

        query_cost_centers_with_status = rail.QueryCollectionOperator(
            task_id='query_cost_centers_with_status',
            name='costcenterswithstatus',
            query="""SELECT DISTINCT
                         dcc.cost_center_code,
                         dcc.cost_center_name,
                         ecc.cost_center_name AS existing_name,
                         ecc.cost_center_code AS existing_description,
                         ecc.enabled          AS existing_enabled
                     FROM distinctcostcenters dcc
                     LEFT JOIN existingdivisions ecc
                            ON dcc.cost_center_name = ecc.cost_center_name"""
        )

        create_cost_centers = rail.trigger_parallel_dagrun(
            task_id='create_cost_centers',
            items='{{ result("query_cost_centers_with_status") }}',
            trigger_dag_id=config.create_division_dag_id,
            parallel_count=config.parallel_count_cost_centers,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'cost_center_code': item['cost_center_code'],
                'cost_center_name': item['cost_center_name'],
                'existing_name': item['existing_name'],
                'existing_description': item['existing_description'],
                'existing_enabled': item['existing_enabled'],
                'log': rail.result("create_exception_log")
            }
        )

        # ========== Project Processing ==========
        query_distinct_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT project_id, record_id FROM validdata
                GROUP BY project_id"""
        )

        get_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:project"
            },
            data_handler=lambda response: {
                'projecttype': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Project Type', 'uri', ''),
                'enforce': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Enforce', 'uri', ''),
                'sourcesystem': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Source System', 'uri', ''),
                'owning_parent_project': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Owning (Parent) Project', 'uri', '')
            }
        )

        get_task_custom_fields = rail.RepliconServiceOperator(
            task_id='get_task_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:task'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Task Type', 'uri', '')
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler= lambda resp: {
                'project_management_permission_set_uri': rail.find_first_by_attr_and_get_attr(
                    resp, 'displayText', 'Project Manager', 'uri', '')
            }
        )

        def get_process_projects_trigger_id(item):
            try:
                modulo = int(item['record_id']) % config.PROJECT_BATCH_COUNT
            except (ValueError, KeyError, TypeError):
                # Fallback to base DAG if record_id is invalid or missing
                return config.process_project_dag_id

            if modulo == 0:
                return config.process_project_dag_id
            return f"{config.process_project_dag_id}_batch_{str(modulo)}"

        process_projects = rail.trigger_parallel_dagrun(
            task_id='process_projects',
            items='{{ result("query_distinct_projects") }}',
            parallel_count=config.parallel_count,
            trigger_dag_id=get_process_projects_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'project_id': item['project_id'],
                'projecttype_custom_field_uri': rail.result("get_project_custom_fields")["projecttype"],
                'enforce_custom_field_uri': rail.result("get_project_custom_fields")["enforce"],
                'sourcesystem_custom_field_uri': rail.result("get_project_custom_fields")["sourcesystem"],
                'owning_parent_project_custom_field_uri': rail.result("get_project_custom_fields")["owning_parent_project"],
                'task_custom_field_uri': rail.result("get_task_custom_fields"),
                'project_management_permission_set_uri': rail.result("get_all_permission_sets")['project_management_permission_set_uri'],
                'peoplesoft_service_center_uri': rail.result("get_service_center_uris")["peoplesoft_service_center_uri"],
                'india_service_center_uri': rail.result("get_service_center_uris")["india_service_center_uri"]
            }
        )

        # ========== Collate per-project child logs ==========
        # Each project child writes to its own log (create_project_log) to avoid
        # lock contention on the shared master log across parallel batches.
        # Gather every child dagrun's log so the main DAG can merge them with the master log.
        def get_all_child_project_dagrun_ids():
            dagrun_ids = list(filter(None, map(
                lambda x: rail.result(f'process_projects_{x + 1}'), range(config.parallel_count))))
            if not dagrun_ids:
                return []
            flattened = []
            for ids in dagrun_ids:
                flattened.extend(ids)
            return flattened

        get_all_child_project_dagrun_ids_task = rail.PythonOperator(
            task_id="get_all_child_project_dagrun_ids",
            python_callable=get_all_child_project_dagrun_ids,
            show_return_value_in_logs=False
        )

        gather_child_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_child_project_logs",
            dag_runs="{{ result('get_all_child_project_dagrun_ids') }}",
            dagrun_task_id='create_project_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        # ========== Log Formatting and Output ==========
        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_method.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{ result('format_logs') }}",
            header=[
                'client_id',
                'client_name',
                'project_id',
                'project_name',
                'task_code',
                'task_name',
                'action',
                'status',
                'details',
                'enforce_value',
                'jobid'
            ],
            row=[
                "{{ item.properties.client_id | default('') }}",
                "{{ item.properties.client_name | default('') }}",
                "{{ item.properties.project_id | default('') }}",
                "{{ item.properties.project_name | default('') }}",
                "{{ item.properties.task_code | default('') }}",
                "{{ item.properties.task_name | default('') }}",
                "{{ item.properties.action | default('') }}",
                "{{ item.properties.status | default('') }}",
                "{{ item.properties.details | default('') }}",
                "{{ item.properties.enforce_value | default('') }}",
                "{{ item.ecid | default('') }}"
            ],
        )

        def get_log_file_details(dag_run):
            log_filename = f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0]}.csv'
            start_time = rail.result("log_start_time")
            end_time = pendulum.now(config.time_zone)

            return {
                'log_file': log_filename,
                'job_start_time': start_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'job_end_time': end_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            }

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=get_log_file_details
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_log')}}",
            output_file_name="{{ result('get_log_file_name').log_file }}",
            expires_in_seconds=config.log_file_download_link_expiry_in_sec,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.sftp_log_path + "/{{ result('get_log_file_name').log_file }}",
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon PeopleSoft Project Sync " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete.html",
            params={
                'log_filepath': config.sftp_log_path
            }
        )

        new_file_sensor >> is_pgp
        is_pgp >> rail.Label("Yes") >> validate_file_name >> is_valid_file_name
        is_pgp >> rail.Label("No") >> send_bad_file_format_email

        is_valid_file_name >> rail.Label("Yes") >> download_file >> archive_file >> log_start_time >> can_decrypt_file
        is_valid_file_name >> rail.Label("No") >> check_file_type

        check_file_type >> rail.Label("Yes") >> delete_this_dagrun_resource_file
        check_file_type >> rail.Label("No") >> archive_unknown_file >> send_invalid_file_name_email

        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> dummy_load_data >> was_new_file_found
        can_decrypt_file >> rail.Label("No") >> dummy_load_data

        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        dummy_load_data >> load_project_data  >> compose_csv >> create_collection_from_csv
        create_collection_from_csv >> create_exception_log >> has_collection_data

        has_collection_data >> rail.Label("Yes") >> query_blank_mandatory_check
        has_collection_data >> rail.Label("No") >> send_blank_payload_email

        query_blank_mandatory_check >> has_blank_mandatory_fields
        has_blank_mandatory_fields >> rail.Label("Yes") >> write_blank_mandatory_log >> query_valid_data
        has_blank_mandatory_fields >> rail.Label("No") >> query_valid_data

        query_valid_data >> has_valid_data
        has_valid_data >> rail.Label("Yes") >> query_distinct_clients
        has_valid_data >> rail.Label("No") >> format_logs

        query_distinct_clients >> has_clients_to_process
        has_clients_to_process >> rail.Label("Yes") >> process_clients_dummy_task >> process_clients >> get_service_center_uris >> query_distinct_cost_centers >> has_costcenters_to_process
        has_clients_to_process >> rail.Label("No") >> get_service_center_uris >> query_distinct_cost_centers >> has_costcenters_to_process

        has_costcenters_to_process >> rail.Label("Yes") >> get_all_costcenters >> create_costcenter_collection >> query_cost_centers_with_status >> create_cost_centers >> query_distinct_projects
        has_costcenters_to_process >> rail.Label("No") >> query_distinct_projects

        query_distinct_projects >> get_project_custom_fields >> get_task_custom_fields >> get_all_permission_sets >> process_projects >> get_all_child_project_dagrun_ids_task >> gather_child_project_logs >> format_logs
        format_logs >> create_csv_log >> get_log_file_name >> generate_download_link >> upload_log_to_sftp >> send_completion_email

    return dag

rail.for_each_instance(create_main_dag)
