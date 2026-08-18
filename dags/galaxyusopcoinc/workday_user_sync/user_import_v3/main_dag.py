from datetime import timedelta
import itertools
import os
import rail
from airflow.models import Variable
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import response_filter
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import custom_methods
from galaxyusopcoinc.workday_user_sync.user_import_v3.tasks.gather_fields import gather_required_details
# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'VialtoPartners_User Import Master V1.0 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
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
            subject='{{ get_company_key() }} | Replicon User import Sync - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='false').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file_var_name,
                            default_var='false').lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        def do_has_file_content():
            with rail.existing_artifact(rail.result('dummy_load_data')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=do_has_file_content,
            yes_task='load_user_import_data',
            no_task='dummy_send_blank_file_email'
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            # yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            # trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_user_import_data = rail.LoadCSVFileOperator(
            task_id='load_user_import_data',
            document="{{ result('dummy_load_data') }}",
            delimiter=config.delimiter,
            encoding="utf-8-sig"
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User import Sync - Blank File - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_payload.html"
        )

        create_user_import_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_import_data_collection',
            source="{{ result('load_user_import_data') }}",
            name="userimportdata",
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{result('create_user_import_data_collection', 'length') > 0}}",
            yes_task='process_records',
            no_task='dummy_send_blank_file_email'
        )

        dummy_send_blank_file_email = rail.EmptyOperator(
            task_id="dummy_send_blank_file_email"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        query_not_allowed_records = rail.QueryCollectionOperator(
            task_id='query_not_allowed_records',
            query="""SELECT * FROM userimportdata WHERE LOWER(WorkerType) NOT IN ("employee", "contingent worker")"""
        )

        query_not_allowed_records_for_log = rail.QueryCollectionOperator(
            task_id = "query_not_allowed_records_for_log",
            query=f"SELECT * FROM {query_not_allowed_records.task_id} WHERE NULLIF(TerminationDate, '') IS NULL"
        )

        log_not_allowed_records = rail.WriteLogOperator(
            task_id='log_not_allowed_records',
            message="Worker Type is not in 'Employee', 'Contingent Worker'",
            items="{{result('query_not_allowed_records_for_log')}}",
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['EmployeeID'],
                'username': item['LegalFirstName'] + item['LegalLastName'],
                'loginname': item['WorkEmail'],
                'status': 'Exception',
                'action': 'Pre-Check',
                'message': "Worker Type is not in 'Employee', 'Contingent Worker'",
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        query_allowed_records = rail.QueryCollectionOperator(
            task_id='query_allowed_records',
            query="""SELECT * FROM userimportdata WHERE LOWER(WorkerType) IN ("employee", "contingent worker")"""
        )

        query_records_with_mandatory_values_present = rail.QueryCollectionOperator(
            task_id="query_records_with_mandatory_values_present",
            query="""SELECT * from query_allowed_records u WHERE
                        NULLIF(EmployeeID, '') IS NOT NULL AND NULLIF(HireDate, '') IS NOT NULL AND
                        NULLIF(WorkEmail, '') IS NOT NULL AND NULLIF(Company, '') IS NOT NULL AND
                        NULLIF(CompanyCode, '') IS NOT NULL AND NULLIF(Country, '') IS NOT NULL AND
                        NULLIF(JobCategory, '') IS NOT NULL AND NULLIF(CostCenterName, '') IS NOT NULL AND
                        NULLIF(CostCenterID, '') IS NOT NULL AND
                        NULLIF(WorkerType, '') IS NOT NULL AND NULLIF(EmployeeType, '') IS NOT NULL AND
                        NULLIF(PositionID, '') IS NOT NULL AND NULLIF(ManagementLevel, '') IS NOT NULL AND
                        NULLIF(LegalFirstName, '') IS NOT NULL AND NULLIF(LegalLastName, '') IS NOT NULL AND
                        ((WorkerType == "Employee" AND NULLIF(CompensationGrade, '') IS NOT NULL) OR WorkerType == "Contingent Worker")""",
            name="queryuserimportdata"
        )

        has_valid_users = rail.IfOperator(
            task_id='has_valid_users',
            test='{{ result("query_records_with_mandatory_values_present", "length") > 0 }}',
            yes_task='gather_required_details_start',
            no_task='empty_has_valid_users_no_task'
        )

        empty_has_valid_users_no_task = rail.EmptyOperator(
            task_id = "empty_has_valid_users_no_task"
        )

        query_records_with_mandatory_values_not_present = rail.QueryCollectionOperator(
            task_id="query_records_with_mandatory_values_not_present",
            query="""SELECT * from userimportdata u WHERE
                        NULLIF(EmployeeID, '') IS NULL OR NULLIF(HireDate, '') IS NULL OR
                        NULLIF(WorkEmail, '') IS NULL OR NULLIF(Company, '') IS NULL OR
                        NULLIF(CompanyCode, '') IS NULL OR NULLIF(Country, '') IS NULL OR
                        NULLIF(JobCategory, '') IS NULL OR NULLIF(CostCenterName, '') IS NULL OR
                        NULLIF(CostCenterID, '') IS NULL OR
                        NULLIF(WorkerType, '') IS NULL OR NULLIF(EmployeeType, '') IS NULL OR
                        NULLIF(PositionID, '') IS NULL OR NULLIF(ManagementLevel, '') IS NULL OR
                        NULLIF(LegalFirstName, '') IS NULL OR NULLIF(LegalLastName, '') IS NULL OR
                        (WorkerType == "Employee" AND NULLIF(CompensationGrade, '') IS NULL)"""
        )

        query_records_with_mandatory_values_not_present_2 = rail.QueryCollectionOperator(
            task_id="query_records_with_mandatory_values_not_present_2",
            query="""SELECT * from userimportdata u WHERE
                        (NULLIF(EmployeeID, '') IS NULL OR NULLIF(HireDate, '') IS NULL OR
                        NULLIF(WorkEmail, '') IS NULL OR NULLIF(Company, '') IS NULL OR
                        NULLIF(CompanyCode, '') IS NULL OR NULLIF(Country, '') IS NULL OR
                        NULLIF(JobCategory, '') IS NULL OR NULLIF(CostCenterName, '') IS NULL OR
                        NULLIF(CostCenterID, '') IS NULL OR
                        NULLIF(WorkerType, '') IS NULL OR NULLIF(EmployeeType, '') IS NULL OR
                        NULLIF(PositionID, '') IS NULL OR NULLIF(ManagementLevel, '') IS NULL OR
                        NULLIF(LegalFirstName, '') IS NULL OR NULLIF(LegalLastName, '') IS NULL) AND
                        (WorkerType == "Contingent Worker")"""
        )

        merge_mandatory_missing_data = rail.QueryCollectionOperator(
            task_id = "merge_mandatory_missing_data",
            query=f"""SELECT * FROM {query_records_with_mandatory_values_not_present.task_id} UNION
                        SELECT * FROM {query_records_with_mandatory_values_not_present_2.task_id} UNION
                        SELECT * FROM {query_not_allowed_records.task_id}"""
        )

        query_user_records_with_end_date = rail.QueryCollectionOperator(
            task_id = "query_user_records_with_end_date",
            query = f"""SELECT * FROM {merge_mandatory_missing_data.task_id} data 
                        WHERE NULLIF(data.TerminationDate, '') IS NOT NULL AND
                        NULLIF(EmployeeID, '') IS NOT NULL"""
        )

        has_any_missing_mandatory_records_with_end_date = rail.IfOperator(
            task_id = "has_any_missing_mandatory_records_with_end_date",
            test="{{ result('query_user_records_with_end_date', 'length') > 0 }}",
            yes_task="trigger_update_user_end_date",
            no_task="query_missing_data_to_log"
        )

        trigger_update_user_end_date = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_update_user_end_date",
            items="{{result('query_user_records_with_end_date')}}",
            trigger_dag_id=config.update_user_enddate_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'file_name': rail.render_template("{{result('new_file_sensor') | file_name }}"),
                'EmployeeID': item['EmployeeID'],
                'TerminationDate': item['TerminationDate'],
                'username': item['LegalFirstName'] + item['LegalLastName'],
                'loginname': item['WorkEmail'],
                'action': 'Update',
            }
        )

        wait_for_trigger_update_user_end_date = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_update_user_end_date',
            dag_runs='{{ result("trigger_update_user_end_date") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_missing_data_to_log = rail.QueryCollectionOperator(
            task_id = "query_missing_data_to_log",
            query=f"""SELECT * FROM {merge_mandatory_missing_data.task_id} data WHERE NULLIF(data.TerminationDate, '') IS NULL
                    OR NULLIF(EmployeeID, '') IS NULL"""
        )

        has_invalid_users = rail.IfOperator(
            task_id='has_invalid_users',
            test=lambda: rail.result("query_missing_data_to_log", "length") > 0,
            yes_task='log_invalid_users',
            no_task='empty_has_invalid_users_no_task'
        )

        empty_has_invalid_users_no_task = rail.EmptyOperator(
            task_id = "empty_has_invalid_users_no_task"
        )

        log_invalid_users = rail.WriteLogOperator(
            task_id='log_invalid_users',
            message=custom_methods.get_invalid_log_message,
            items="{{result('query_missing_data_to_log')}}",
            severity='Exception',
            properties=lambda item: {
                'employeeid': item['EmployeeID'],
                'username': item['LegalFirstName'] + item['LegalLastName'],
                'loginname': item['WorkEmail'],
                'status': 'Exception',
                'action': 'Pre-Check',
                'message': custom_methods.get_invalid_log_message(item),
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": ""
            }
        )

        gather_required_details_start, gather_required_details_done = gather_required_details()

        process_groups = rail.TriggerDagRunForEachItemOperator(
            task_id="process_groups",
            items=[1],
            trigger_dag_id=config.process_groups_dag_id,
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name}}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_process_groups",
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_updated_deparments = rail.RepliconServiceOperator(
            task_id='get_updated_deparments',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload,
            response_filter=response_filter.map_list_data
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            response_filter=response_filter.map_list_data
        )

        get_updated_costcenter = rail.RepliconServiceOperator(
            task_id='get_updated_costcenter',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            response_filter=response_filter.map_list_data
        )

        get_timeoff_balance_event_script = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_event_script',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
        )
        get_timeoff_balance_validation_script = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
        )

        get_updated_service_centers = rail.RepliconServiceOperator(
            task_id="get_updated_service_centers",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:code",
                    "urn:replicon:service-center-list-column:description",
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=response_filter.get_service_centers_date_handler
        )

        get_updated_division_from_replicon = rail.RepliconServiceOperator(
            task_id="get_updated_division_from_replicon",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division",
                        "urn:replicon:division-list-column:full-path"
                    ]
            },
            data_handler=response_filter.get_all_division_from_replicon_filter
        )

        get_updated_employee_types_from_replicon = rail.RepliconServiceOperator(
            task_id="get_updated_employee_types_from_replicon",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:employee-type-group-list-column:name",
                        "urn:replicon:employee-type-group-list-column:employee-type-group",
                        "urn:replicon:employee-type-group-list-column:full-path"
                    ]
            },
            data_handler=response_filter.get_all_employee_type_from_replicon_filter
        )

        start_processing = rail.EmptyOperator(
            task_id='start_processing',
        )

        query_unique_country_from_feed = rail.QueryCollectionOperator(
            task_id="query_unique_country_from_feed",
            query="""SELECT DISTINCT Country FROM queryuserimportdata WHERE NULLIF(Country, '') IS NOT NULL"""
        )

        load_mapper_per_country = rail.PythonOperator(
            task_id="load_mapper_per_country",
            python_callable=custom_methods.set_mappers_as_xcom,
            op_args=[
                config.instance
            ],
            execution_timeout=timedelta(hours=1),
            show_return_value_in_logs=False
        )

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            parallel_count=config.parallel_trigger_run_count,
            items=lambda: rail.result(
                'query_records_with_mandatory_values_present'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.user_dag_id,
            conf=request_payload.get_process_user_conf
        )

        get_all_process_user_dag_runs = rail.PythonOperator(
            task_id="get_all_process_user_dag_runs",
            python_callable=lambda: list(itertools.chain(
                *list(filter(None, map(lambda x: rail.result(
                    f'process_users_{x+1}'), range(config.parallel_trigger_run_count)))))),
            show_return_value_in_logs=False
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('get_all_process_user_dag_runs') }}",
            dagrun_task_id='create_user_log',
            flatten=True,
        )

        def _get_users_records_for_supervisor_process():
            user_logs = rail.result('gather_logs')
            if not user_logs:
                return []
            user_records_for_supervisor_process = []
            for log in user_logs:
                log_data = rail.load_all_records(log)
                for log in log_data:
                    if log['properties']['allowed_for_supervisor_dag'] in ["true", True, "True"]:
                        if log['properties'].get('create_user_log', False):
                            user_records_for_supervisor_process.append(log)
            return user_records_for_supervisor_process

        get_users_records_for_supervisor_process = rail.PythonOperator(
            task_id="get_users_records_for_supervisor_process",
            python_callable=_get_users_records_for_supervisor_process
        )

        process_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id="process_supervisor",
            items="{{result('get_users_records_for_supervisor_process') | to_json }}",
            trigger_dag_id=config.update_supervisor_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'employeeid': item['properties']['employeeid'],
                'username': item['properties']['username'],
                'loginname': item['properties']['loginname'],
                'action': item['properties']['action'],
                "useruri": item['properties']['user_uri'],
                "managerid": item['properties']['managerid'],
                "create_user_log": item['properties']['create_user_log'],
                # For newly added users the supervisor will be assigned as initial
                # there wont be any effective date present
                "user_effective_date": item['properties']['user_effective_date'] if item['properties']['action'].lower()!="add" else None,
                "permissionsets":rail.result('get_all_permissionset')
            }
        )

        wait_for_process_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor',
            dag_runs='{{ result("process_supervisor") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ get_master_log() | load_all_records | to_json }}"
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=response_filter.do_format_logs
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id='write_csv_file',
            source="{{ result('format_logs').final_logs }}",
            header=[
                'Employee ID',
                'User Name',
                'Login Name',
                'Status',
                'Action',
                'Details',
                'Jobid'],
            row=[
                '{{ item.employeeid }}',
                '{{ item.username }}',
                '{{ item.loginname }}',
                '{{ item.status}}',
                '{{ item.action }}',
                '{{ item.details }}',
                '{{ item.jobid }}'],
            footer=[
                'Number of records found: {{result("create_user_import_data_collection", "length")}}',
                'Number of records processed:{{result("query_records_with_mandatory_values_present", "length")}}',
                'Number of Successes: {{result("format_logs", "success")}}',
                'Number of failures: {{result("format_logs", "error")}}',
                'Number of new users added: {{result("format_logs").get_record_summary.new_users_added}}',
                'Number of user profiles updated: {{result("format_logs").get_record_summary.users_updated}}',
                ''
            ]
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('write_csv_file') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_csv_file')}}",
            output_file_name='{{ ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() + " | Replicon User Import Sync - " }} \
                {%- if result("format_logs", key="error") > 0 -%} \
                    Completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception") > 0 -%} \
                        Completed with exceptions  \
                    {%- else -%} \
                        Completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}''',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> is_csv >> rail.Label(
            "Yes") >> download_file
        download_file >> archive_file >> can_decrypt_file
        can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> dummy_load_data >> has_file_content
        can_decrypt_file >> rail.Label("No") >> dummy_load_data >> has_file_content
        has_file_content >> rail.Label(
            "Yes") >> load_user_import_data >> create_user_import_data_collection
        dummy_send_blank_file_email >> send_blank_payload_email

        create_user_import_data_collection >> has_any_records >> rail.Label(
            "No") >> dummy_send_blank_file_email

        has_any_records >> rail.Label("Yes") >> process_records >> [
            query_not_allowed_records, query_allowed_records]
        query_not_allowed_records >> query_not_allowed_records_for_log >> log_not_allowed_records \
            >> query_allowed_records >> query_records_with_mandatory_values_not_present
        query_records_with_mandatory_values_present >> has_valid_users >> rail.Label(
            "Yes") >> gather_required_details_start

        gather_required_details_done >> process_groups >> wait_process_groups

        wait_process_groups >> [get_updated_locations, get_updated_costcenter, get_updated_deparments,
                                get_updated_service_centers, get_timeoff_balance_event_script, get_timeoff_balance_validation_script,
                                get_updated_division_from_replicon, get_updated_division_from_replicon,
                                get_updated_employee_types_from_replicon] >> start_processing

        start_processing >> query_unique_country_from_feed >> load_mapper_per_country >> process_users

        process_users >> get_all_process_user_dag_runs >> gather_logs >> get_users_records_for_supervisor_process

        query_records_with_mandatory_values_not_present >> query_records_with_mandatory_values_not_present_2\
          >> merge_mandatory_missing_data >> query_user_records_with_end_date
        
        query_user_records_with_end_date >> has_any_missing_mandatory_records_with_end_date >> rail.Label("Yes") \
            >> trigger_update_user_end_date >> wait_for_trigger_update_user_end_date >> query_missing_data_to_log
        has_any_missing_mandatory_records_with_end_date >> rail.Label("No") >> query_missing_data_to_log >> has_invalid_users

        has_valid_users >> rail.Label(
            'No') >> empty_has_valid_users_no_task >> get_users_records_for_supervisor_process

        has_invalid_users >> rail.Label(
            'Yes') >> log_invalid_users >> empty_has_invalid_users_no_task >> query_records_with_mandatory_values_present

        has_invalid_users >> rail.Label(
            'No') >> empty_has_invalid_users_no_task

        get_users_records_for_supervisor_process >> process_supervisor >> wait_for_process_supervisor >> \
            load_master_log >> format_logs >> write_csv_file >> upload_csv_to_sftp >> \
            generate_download_link >> send_import_complete_email

        is_csv >> rail.Label(
            "No") >> send_bad_file_format_email

        has_file_content >> rail.Label(
            "No") >> dummy_send_blank_file_email

        download_file >> was_new_file_found

        was_new_file_found >> rail.Label(
            "No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_dag)
