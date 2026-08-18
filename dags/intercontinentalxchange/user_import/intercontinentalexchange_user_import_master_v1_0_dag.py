
from datetime import timedelta
import hashlib
import chardet
import pendulum
from airflow.models import Variable
# pylint: disable=no-name-in-module
from intercontinentalxchange.user_import.intercontinentalexchange_timezone_mapper import intercontinentalexchange_timezone_mapper
from intercontinentalxchange.user_import.intercontinentalexchange_holiday_calendar_mapper import intercontinentalexchange_holiday_calendar_mapper
import rail
from rail.lib.log import get_master_log_artifact_name
from rail.lib.ecid import get_dagrun_ecid
from rail.lib.artifact import existing_artifact

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_userimportmasterv10_{config.instance}',
        description=f'IntercontinentalExchange_User import - Master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_today_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_today_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_today_4 = rail.PythonOperator(
            task_id='log_today_4',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        has_input_filename_ends_with_csv = rail.IfOperator(
            task_id="has_input_filename_ends_with_csv",
            test='{{ result("new_file_sensor").split(".")[-1] | lower == "csv" | lower if result("new_file_sensor") else False }}',
            yes_task="download_file",
            no_task="archive_incorrect_file",
        )

        archive_incorrect_file = rail.SFTPMoveFileOperator(
            task_id='archive_incorrect_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}"
        )

        send_mail_for_incorrect_file = rail.EmailOperator(
            task_id='send_mail_for_incorrect_file',
            to=config.tenant_email_for_user_import,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon user import - skipped {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> Replicon user import for {{get_company_key()}}, created on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} skipped, since the file format is incorrect </p>
            <p> File name : {{ result('new_file_sensor') | file_name }} </p>
            <p>Please send the correct input file in csv file format.<br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''',
            params=None,
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding = rail.PythonOperator(
            task_id = "find_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_file.task_id]
        )

        parse_input_csv = rail.LoadCSVFileOperator(
            task_id="parse_input_csv",
            document="{{result('download_file')}}",
            encoding="{{ result('find_file_encoding')[0].encoding}}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}_{{ result('get_time_for_file') }}"
        )

        list_import_files = rail.SFTPListFilesOperator(
            task_id="list_import_files",
            paths=[config.referance_filepath],
        )

        def has_any_file(result_task_id, input_file_path):
            if not result_task_id or not input_file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)
            if not data:
                return False
            return len(data[input_file_path]) > 0

        has_any_referance_files = rail.IfOperator(
            task_id="has_any_referance_files",
            test=lambda: has_any_file(
                "list_import_files", config.referance_filepath),
            yes_task="download_referance_file",
            no_task="log_to_sumo"
        )

        download_referance_file = rail.SFTPDownloadFileOperator(
            task_id='download_referance_file',
            remote_filepath=config.referance_filepath +
            "/IntercontinentalExchange_reference.csv"
        )

        parse_referance_csv = rail.LoadCSVFileOperator(
            task_id="parse_referance_csv",
            document="{{result('download_referance_file')}}"
        )

        def get_formated_user_row(item):
            user_md5 = hashlib.md5((
                item["PERSON_NUMBER"]+"_" +
                item["FIRST_NAME"]+"_" +
                item['MIDDLE_NAME']+"_" +
                item['LAST_NAME']+"_" +
                item['EMPLOYEE_STATUS']+"_" +
                item['EMAIL']+"_" +
                item['LOCATION']+"_" +
                item['DEPARTMENT_ID']+"_" +
                item['DEPARTMENT']+"_" +
                item['LEGAL_ENTITY_ID']+"_" +
                item['LEGAL_ENTITY_NAME']+"_" +
                item['REPORTING_ENTITY_ID']+"_" +
                item['REPORTING_ENTITY_NAME']+"_" +
                item['WORKER_TYPE']+"_" +
                item['LINE_MANAGER']+"_" +
                item['ACTUAL_TERMINATION_DATE']+"_" +
                item['WORK_SCHEDULE']+"_" +
                item['WEEK_HOURS']+"_" +
                item['LOCATION_NODE']+"_" +
                item['USER_TYPE']).encode()).hexdigest()

            return {
                "PERSON_NUMBER": item["PERSON_NUMBER"].strip() if item["PERSON_NUMBER"] else "",
                "FIRST_NAME": item["FIRST_NAME"].strip() if item["FIRST_NAME"] else "",
                "MIDDLE_NAME": item["MIDDLE_NAME"].strip() if item["MIDDLE_NAME"] else "",
                "LAST_NAME": item["LAST_NAME"].strip() if item["LAST_NAME"] else "",
                "EMPLOYEE_STATUS": item["EMPLOYEE_STATUS"].strip() if item["EMPLOYEE_STATUS"] else "",
                "EMAIL": item["EMAIL"].strip() if item["EMAIL"] else "",
                "EFFECTIVE_DATE": item["EFFECTIVE_DATE"].strip() if item["EFFECTIVE_DATE"] else "",
                "LOCATION": item["LOCATION"].strip() if item["LOCATION"] else "",
                "DEPARTMENT_ID": item["DEPARTMENT_ID"].strip() if item["DEPARTMENT_ID"] else "",
                "DEPARTMENT": item["DEPARTMENT"].strip() if item["DEPARTMENT"] else "",
                "LEGAL_ENTITY_ID": item["LEGAL_ENTITY_ID"].strip() if item["LEGAL_ENTITY_ID"] else "",
                "LEGAL_ENTITY_NAME": item["LEGAL_ENTITY_NAME"].strip() if item["LEGAL_ENTITY_NAME"] else "",
                "REPORTING_ENTITY_ID": item["REPORTING_ENTITY_ID"].strip() if item["REPORTING_ENTITY_ID"] else "",
                "REPORTING_ENTITY_NAME": item["REPORTING_ENTITY_NAME"].strip() if item["REPORTING_ENTITY_NAME"] else "",
                "WORKER_TYPE": item["WORKER_TYPE"].strip() if item["WORKER_TYPE"] else "",
                "LINE_MANAGER": item["LINE_MANAGER"].strip() if item["LINE_MANAGER"] else "",
                "ACTUAL_TERMINATION_DATE": item["ACTUAL_TERMINATION_DATE"].strip() if item["ACTUAL_TERMINATION_DATE"] else "",
                "WORK_SCHEDULE": item["WORK_SCHEDULE"].strip() if item["WORK_SCHEDULE"] else "",
                "WEEK_HOURS": item["WEEK_HOURS"].strip() if item["WEEK_HOURS"] else "",
                "LOCATION_NODE": item["LOCATION_NODE"].strip() if item["LOCATION_NODE"] else "",
                "USER_TYPE": item["USER_TYPE"].strip() if item["USER_TYPE"] else "",
                "md5": user_md5
            }.values()

        create_csv_lines_create_inputfilewith_m_d5_flatfiletesting_5 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_create_inputfilewith_m_d5_flatfiletesting_5',
            source="{{ result('parse_input_csv') }}",
            header=['PERSON_NUMBER',
                    'FIRST_NAME',
                    'MIDDLE_NAME',
                    'LAST_NAME',
                    'EMPLOYEE_STATUS',
                    'EMAIL',
                    'EFFECTIVE_DATE',
                    'LOCATION',
                    'DEPARTMENT_ID',
                    'DEPARTMENT',
                    'LEGAL_ENTITY_ID',
                    'LEGAL_ENTITY_NAME',
                    'REPORTING_ENTITY_ID',
                    'REPORTING_ENTITY_NAME',
                    'WORKER_TYPE',
                    'LINE_MANAGER',
                    'ACTUAL_TERMINATION_DATE',
                    'WORK_SCHEDULE',
                    'WEEK_HOURS',
                    'LOCATION_NODE',
                    'USER_TYPE',
                    'md5'],
            row=get_formated_user_row
        )

        create_collection_create_list_from_csv_8 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_8',
            source="{{ result('create_csv_lines_create_inputfilewith_m_d5_flatfiletesting_5') }}",
            name="inputfilewithmd5",
            columns={
                'PERSON_NUMBER': 'person_number',
                'FIRST_NAME': 'first_name',
                'MIDDLE_NAME': 'middle_name',
                'LAST_NAME': 'last_name',
                'EMPLOYEE_STATUS': 'employee_status',
                'EMAIL': 'email',
                'EFFECTIVE_DATE': 'effective_date',
                'LOCATION': 'location',
                'DEPARTMENT_ID': 'department_id',
                'DEPARTMENT': 'department',
                'LEGAL_ENTITY_ID': 'legal_entity_id',
                'LEGAL_ENTITY_NAME': 'legal_entity_name',
                'REPORTING_ENTITY_ID': 'reporting_entity_id',
                'REPORTING_ENTITY_NAME': 'reporting_entity_name',
                'WORKER_TYPE': 'worker_type',
                'LINE_MANAGER': 'line_manager',
                'ACTUAL_TERMINATION_DATE': 'actual_termination_date',
                'WORK_SCHEDULE': 'work_schedule',
                'WEEK_HOURS': 'week_hours',
                'LOCATION_NODE': 'location_node',
                'USER_TYPE': 'user_type',
                'md5': 'md5'
            }
        )

        query_list_inputfilerecords_9 = rail.QueryCollectionOperator(
            task_id='query_list_inputfilerecords_9',
            query="""SELECT * FROM  inputfilewithmd5""",
        )

        if_query_list_inputfilerecords_greater_than_9 = rail.IfOperator(
            task_id='if_query_list_inputfilerecords_greater_than_9',
            test='{{ result("query_list_inputfilerecords_9", "length") > 0 }}',
            yes_task="create_collection_create_list_from_csv_10",
            no_task="log_to_sumo",
        )

        create_collection_create_list_from_csv_10 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_10',
            source="{{ result('parse_referance_csv') }}",
            name="referencefilewithmd5",
            columns={
                'person_number': 'person_number',
                'first_name': 'first_name',
                'middle_name': 'middle_name',
                'last_name': 'last_name',
                'employee_status': 'employee_status',
                'email': 'email',
                'effective_date': 'effective_date',
                'location': 'location',
                'department_id': 'department_id',
                'department': 'department',
                'legal_entity_id': 'legal_entity_id',
                'legal_entity_name': 'legal_entity_name',
                'reporting_entity_id': 'reporting_entity_id',
                'reporting_entity_name': 'reporting_entity_name',
                'worker_type': 'worker_type',
                'line_manager': 'line_manager',
                'actual_termination_date': 'actual_termination_date',
                'work_schedule': 'work_schedule',
                'week_hours': 'week_hours',
                'location_node': 'location_node',
                'user_type': 'user_type',
                'md5': 'md5'
            }
        )

        query_list_referencefilerecords_11 = rail.QueryCollectionOperator(
            task_id='query_list_referencefilerecords_11',
            query="""SELECT * FROM  referencefilewithmd5""",
        )

        declare_list_12 = rail.SetVariableOperator(
            task_id='declare_list_12',
            append=False,
            name='importlogger',
            value=[]
        )

        query_list_identify_unchangedrecords_13 = rail.QueryCollectionOperator(
            task_id='query_list_identify_unchangedrecords_13',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        if_query_list_identify_unchangedrecords_13_rows_greater_than_0_14 = rail.IfOperator(
            task_id='if_query_list_identify_unchangedrecords_13_rows_greater_than_0_14',
            test='{{ result("query_list_identify_unchangedrecords_13", "length") > 0 }}',
            yes_task="add_import_log_add_entry_15",
            no_task="query_list_identify_changedrecords_16",
        )

        add_import_log_add_entry_15 = rail.WriteLogOperator(
            task_id='add_import_log_add_entry_15',
            message="Un Processed/Ignored Records",
            items="{{ result('query_list_identify_unchangedrecords_13') }}",
            severity="Ignored",
            properties={
                "Empid": "{{ item.person_number }}",
                "Username": "{{ item.first_name }} {{ item.middle_name }}",
                "Action": "pre-check",
                "Status": "Ignored",
                "Details": "No changes in user record",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        query_list_identify_changedrecords_16 = rail.QueryCollectionOperator(
            task_id='query_list_identify_changedrecords_16',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 NOT IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        if_query_list_identify_changedrecords_16_rows_greater_than_400_17 = rail.IfOperator(
            task_id='if_query_list_identify_changedrecords_16_rows_greater_than_400_17',
            test=lambda: rail.result("query_list_identify_changedrecords_16", "length") > config.threshold,
            yes_task="send_mail_18",
            no_task="create_list_21",
        )

        send_mail_18 = rail.EmailOperator(
            task_id='send_mail_18',
            to=config.tenant_email_for_user_import,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " |  Replicon user import not processed -" }} \
                {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon user import is not processed as a delta record count is more than the provided threshold of'''+str(config.threshold)+'''records.
            </p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        create_list_21 = rail.CreateCollectionOperator(
            task_id='create_list_21',
            source="{{result('query_list_identify_changedrecords_16') }}",
            name="changedrecordslist",
        )

        query_list_changedrecordswithout_mandatoryfields_22 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswithout_mandatoryfields_22',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.person_number= "" OR  changedrecordslist.first_name= "" OR  changedrecordslist.last_name= "" OR  changedrecordslist.email= "" OR  changedrecordslist.employee_status= "" OR  changedrecordslist.location= "" OR  changedrecordslist.department_id= "" OR  changedrecordslist.legal_entity_id= "" OR  changedrecordslist.reporting_entity_id= "" OR  changedrecordslist.worker_type= "" OR  changedrecordslist.work_schedule= "" OR  changedrecordslist.person_number IS NULL OR  changedrecordslist.first_name IS NULL OR  changedrecordslist.last_name IS NULL OR  changedrecordslist.email IS NULL OR  changedrecordslist.employee_status IS NULL OR  changedrecordslist.department_id IS NULL OR  changedrecordslist.legal_entity_id IS NULL OR  changedrecordslist.reporting_entity_id IS NULL OR  changedrecordslist.worker_type IS NULL OR  changedrecordslist.work_schedule IS NULL)""",
        )

        if_query_list_changedrecordswithout_mandatoryfields_22_rows_greater_than_0_23 = rail.IfOperator(
            task_id='if_query_list_changedrecordswithout_mandatoryfields_22_rows_greater_than_0_23',
            test='{{ result("query_list_changedrecordswithout_mandatoryfields_22", "length") > 0 }}',
            yes_task="foreach_document_24",
            no_task="log_formatteddateandtime_25",
        )

        foreach_document_24 = rail.ForEachOperator(
            task_id='foreach_document_24',
            items="{{ result('query_list_changedrecordswithout_mandatoryfields_22') }}",
            start_task='add_import_log_add_entry_24',
            end_task='foreach_document_24_end'
        )

        add_import_log_add_entry_24 = rail.WriteLogOperator(
            task_id='add_import_log_add_entry_24',
            message="Un Processed/Ignored Records",
            severity="Ignored",
            properties={
                "Empid": "{{ result('foreach_document_24').person_number }}",
                "Username": "{{ result('foreach_document_24').first_name }} {{ result('foreach_document_24').middle_name }}",
                "Action": "pre-check",
                "Status": "Ignored",
                "Details": "One or more mandatory fields are missing",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_document_24_end = rail.EmptyOperator(
            task_id='foreach_document_24_end',
        )

        log_formatteddateandtime_25 = rail.PythonOperator(
            task_id='log_formatteddateandtime_25',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%Y_%m_%d_T%H_%M_%S')
        )

        query_list_changedrecordswith_mandatoryfields_31 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswith_mandatoryfields_31',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.person_number!= "" AND  changedrecordslist.first_name!= "" AND  changedrecordslist.last_name!= "" AND  changedrecordslist.email!= "" AND  changedrecordslist.employee_status!= "" AND  changedrecordslist.location!= "" AND  changedrecordslist.department_id!= "" AND  changedrecordslist.legal_entity_id!= "" AND  changedrecordslist.reporting_entity_id!= "" AND  changedrecordslist.worker_type!= "" AND  changedrecordslist.work_schedule!= "" AND  changedrecordslist.person_number IS NOT NULL AND  changedrecordslist.first_name IS NOT NULL AND  changedrecordslist.last_name IS NOT NULL AND  changedrecordslist.email IS NOT NULL AND  changedrecordslist.employee_status IS NOT NULL AND  changedrecordslist.department_id IS NOT NULL AND  changedrecordslist.legal_entity_id IS NOT NULL AND  changedrecordslist.reporting_entity_id IS NOT NULL AND  changedrecordslist.worker_type IS NOT NULL AND  changedrecordslist.work_schedule IS NOT NULL)""",
        )

        create_list_changedrecordswith_mandatoryfields_32 = rail.CreateCollectionOperator(
            task_id='create_list_changedrecordswith_mandatoryfields_32',
            source="{{ result('query_list_changedrecordswith_mandatoryfields_31') }}",
            name="changedrecords",
        )

        if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_33 = rail.IfOperator(
            task_id='if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_33',
            test='{{ result("query_list_changedrecordswith_mandatoryfields_31", "length") > 0 }}',
            yes_task="get_report_details_34",
            no_task="rename_archivethereferncefile_92",
        )

        get_report_details_34 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_34',
            report_name=config.user_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details_34').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='if_generate_report_34_payload_starts_with_nodata_35',
            no_task='log_to_sumo'
        )

        if_generate_report_34_payload_starts_with_nodata_35 = rail.IfOperator(
            task_id='if_generate_report_34_payload_starts_with_nodata_35',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="stop_36",
            no_task="if_generate_report_34_payload_not_starts_with_column_37",
        )

        stop_36 = rail.FailOperator(
            task_id='stop_36',
            message='''No Data in the base report'''
        )

        if_generate_report_34_payload_not_starts_with_column_37 = rail.IfOperator(
            task_id='if_generate_report_34_payload_not_starts_with_column_37',
            # pylint: disable=line-too-long
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('User Name,Login Name,Employee ID,UserUri,User Status,User End Date,EmployeeType')}}",
            yes_task="load_report_data_39",
            no_task="stop_38",
        )

        stop_38 = rail.FailOperator(
            task_id='stop_38',
            message='''Base report column order doesn't match'''
        )

        load_report_data_39 = rail.LoadCSVFileOperator(
            task_id='load_report_data_39',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection_40 = rail.CreateCollectionOperator(
            task_id='create_user_collection_40',
            name='enabledusers',
            source="{{ result('load_report_data_39') }}",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'UserUri': 'useruri',
                'User Status': 'status',
                'User End Date': 'enddate',
                'EmployeeType': 'employeetype'}
        )

        get_all_custom_fields_41 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_41',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        get_all_time_zones_getalltimezones_42 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_getalltimezones_42',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data=None
        )

        def get_customoef_uri(custom_field_name):
            existing_customoefs = rail.result('get_all_custom_fields_41')
            matching_custom_field = list(filter(
                lambda item: item['displayText'] == custom_field_name, existing_customoefs))
            return matching_custom_field[0]['uri'] if matching_custom_field else None

        invoke_custom_ruby_code_customfieldsuri_43 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_customfieldsuri_43',
            python_callable=lambda: {
                "weekhours": get_customoef_uri('Week Hours'),
                "node": get_customoef_uri('Node'),
                "adminmodified": get_customoef_uri('Admin Modified')
            }
        )

        get_all_permission_sets_get_all_permission_sets_44 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_get_all_permission_sets_44',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        get_all_time_zones_get_all_time_zones_45 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_get_all_time_zones_45',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data=None
        )

        get_all_office_schedules_get_all_office_schedules_46 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_get_all_office_schedules_46',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data=None
        )

        get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_47 = rail.RepliconServiceOperator(
            task_id='get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_47',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data=None
        )

        get_all_policy_sets_get_all_policy_sets_48 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_get_all_policy_sets_48',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data=None
        )

        get_all_holiday_calendars_get_all_holiday_calendars_49 = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars_get_all_holiday_calendars_49',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        def get_filtered_data(response):
            data = response.json()['d']['rows']
            timesheet_periods = list(map(lambda item: {
                "period_name": item['cells'][0]['textValue'],
                "uri": item['cells'][0].get('uri'),
            }, data))
            return timesheet_periods if timesheet_periods else []

        get_data_timesheet_period_list_service1_timesheet_period_list_service1_50 = rail.RepliconServiceOperator(
            task_id='get_data_timesheet_period_list_service1_timesheet_period_list_service1_50',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_data
        )

        query_list_getlocationsvalue_52 = rail.QueryCollectionOperator(
            task_id='query_list_getlocationsvalue_52',
            query="""SELECT DISTINCT  changedrecords.location FROM  changedrecords WHERE ( changedrecords.location!= "" AND  changedrecords.location!= "")""",
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_groups(location_task):
            groups = []
            location_collections = get_data_from_document(
                rail.result(location_task))
            for grp in location_collections:
                grp_arr = grp['location'].split('|')
                if len(grp_arr) >= 3:
                    groups.append({
                        "display_text": grp_arr[1],
                        "code": grp_arr[-1],
                        "parent": grp_arr[0]
                    })
            return groups

        trigger_dag_run_live_intercontinentalexchange_groups_check_location53 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_groups_check_location53',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_groups_check_location_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": get_groups('query_list_getlocationsvalue_52'),
                "grouptype": "Location"
            }
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_groups_check_location53 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_groups_check_location53',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_intercontinentalexchange_groups_check_location53") }}'
        )

        query_list_get_departmentgroupsvalue_54 = rail.QueryCollectionOperator(
            task_id='query_list_get_departmentgroupsvalue_54',
            query="""SELECT DISTINCT  changedrecords.department as name,  changedrecords.department_id as id FROM  changedrecords WHERE ( changedrecords.department_id!= "" AND  changedrecords.department_id!= "")""",
        )

        def get_groups_info(group_task_name):
            groups = []
            grp_collections = get_data_from_document(
                rail.result(group_task_name))
            for dept in grp_collections:
                groups.append({
                    "display_text": dept['name'],
                    "code": dept.get('id')
                })
            return groups

        trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_process_groups_child_v10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": get_groups_info('query_list_get_departmentgroupsvalue_54'),
                "grouptype": "Department"
            }
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055") }}'
        )

        query_list_get_divisionvalue_56 = rail.QueryCollectionOperator(
            task_id='query_list_get_divisionvalue_56',
            query="""SELECT DISTINCT  changedrecords.legal_entity_name as name,  changedrecords.legal_entity_id as id FROM  changedrecords WHERE ( changedrecords.legal_entity_id!= "" AND  changedrecords.legal_entity_id!= "")""",
        )

        trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_process_groups_child_v10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": get_groups_info('query_list_get_divisionvalue_56'),
                "grouptype": "Division"
            }
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57") }}'
        )

        query_list_getemployeetypevalue_58 = rail.QueryCollectionOperator(
            task_id='query_list_getemployeetypevalue_58',
            query="""SELECT DISTINCT  changedrecords.worker_type as name FROM  changedrecords WHERE ( changedrecords.worker_type != "")""",
        )

        trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_process_groups_child_v10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "group": get_groups_info('query_list_getemployeetypevalue_58'),
                "grouptype": "Employee Type"
            }
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59") }}'
        )

        query_list_getcostcentergroupsvalue_60 = rail.QueryCollectionOperator(
            task_id='query_list_getcostcentergroupsvalue_60',
            query="""SELECT DISTINCT  changedrecords.reporting_entity_name as name, changedrecords.reporting_entity_id as id FROM  changedrecords WHERE ( changedrecords.reporting_entity_id!= "" AND  changedrecords.reporting_entity_id IS NOT NULL)""",
        )

        trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_process_groups_child_v10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "group": get_groups_info('query_list_getcostcentergroupsvalue_60'),
                "grouptype": "CostCenter"
            }
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61") }}'
        )

        def get_filtered_groups_data(response):
            data = response.json()['d']['rows']
            groups_info = list(map(lambda item: {
                "code": item['cells'][0].get('textValue'),
                "textvalue": item['cells'][1].get('textValue'),
                "uri": item['cells'][1].get('uri'),
            }, data))
            return groups_info if groups_info else []

        get_data_department_group_list_service1_62 = rail.RepliconServiceOperator(
            task_id='get_data_department_group_list_service1_62',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:code",
                    "urn:replicon:department-group-list-column:department-group"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        get_enabled_employee_type_groups_employeetypes_64 = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups_employeetypes_64',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            data=None
        )

        get_enabled_locations_locations_65 = rail.RepliconServiceOperator(
            task_id='get_enabled_locations_locations_65',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data=None
        )

        get_data_division_list_service1_66 = rail.RepliconServiceOperator(
            task_id='get_data_division_list_service1_66',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:division-list-column:code",
                    "urn:replicon:division-list-column:division"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        get_data_cost_center_list_service1_68 = rail.RepliconServiceOperator(
            task_id='get_data_cost_center_list_service1_68',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:code",
                    "urn:replicon:cost-center-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        intercontinentalexchange_holiday_calendar_mapper_search_entries_70 = rail.PythonOperator(
            task_id='intercontinentalexchange_holiday_calendar_mapper_search_entries_70',
            python_callable=lambda:  list(
                filter(lambda x: x['check'] == 'yes', intercontinentalexchange_holiday_calendar_mapper))
        )

        intercontinentalexchange_timezone_mapper_search_entries_71 = rail.PythonOperator(
            task_id='intercontinentalexchange_timezone_mapper_search_entries_71',
            python_callable=lambda:  list(
                filter(lambda x: x['check'] == 'yes', intercontinentalexchange_timezone_mapper))
        )

        supervisor_processing_log = rail.CreateLogOperator(
            task_id='supervisor_processing_log',
        )

        declare_list_dag_runs_72 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_72',
            name='user_process_dag_runs',
            value=[]
        )

        def get_existing_user_info(foreach_task):
            person_no = rail.result(foreach_task)['person_number']
            user_collections = get_data_from_document(
                rail.result('create_user_collection_40'))

            return {
                "useruri": rail.find_first_by_attr_and_get_attr(user_collections, 'employeeid', person_no, 'useruri'),
                "loginname": rail.result(foreach_task)['person_number']
            }

        foreach_query_list_changedrecordswith_mandatoryfields_31_72 = rail.ForEachOperator(
            task_id='foreach_query_list_changedrecordswith_mandatoryfields_31_72',
            items="{{ result('query_list_changedrecordswith_mandatoryfields_31') }}",
            start_task='invoke_custom_ruby_code_73',
            end_task='foreach_query_list_changedrecordswith_mandatoryfields_31_72_end'
        )

        invoke_custom_ruby_code_73 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_73',
            python_callable=lambda: get_existing_user_info(
                'foreach_query_list_changedrecordswith_mandatoryfields_31_72')
        )

        if_output_useruri_blank_74 = rail.IfOperator(
            task_id='if_output_useruri_blank_74',
            test='''{{ result('invoke_custom_ruby_code_73').useruri | is_falsy }}''',
            yes_task="if_foreach_query_list_changedrecordswith_mandatoryfields_31_72_employee_status_equals_to_active_75",
            no_task="trigger_dag_run_live_intercontinentalexchange_user_update_v1_0async_80",
        )

        if_foreach_query_list_changedrecordswith_mandatoryfields_31_72_employee_status_equals_to_active_75 = rail.IfOperator(
            task_id='if_foreach_query_list_changedrecordswith_mandatoryfields_31_72_employee_status_equals_to_active_75',
            test='''{{ result('foreach_query_list_changedrecordswith_mandatoryfields_31_72').employee_status == 'Active' }}''',
            yes_task="trigger_dag_run_live_intercontinentalexchange_child_add_user_v1_0async_76",
            no_task="intercontinentalexchange_user_import_logs_add_entry_78",
        )

        def get_location_uri():
            if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72').get('location'):
                existing_location = rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location'].split(
                    '|')[1]
                location_uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_enabled_locations_locations_65'),
                    'displayText', existing_location, 'uri') \
                    if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72').get('location') else None
            return location_uri

        def get_timezone():
            timezone_info = None
            existing_location = rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_31_72').get('location')
            if existing_location:
                timezone_info = rail.find_first_by_attr_and_get_attr(intercontinentalexchange_timezone_mapper,
                                                                     'location_code', existing_location.split('|')[1], 'time_zone')
            return timezone_info

        def get_timezone_uri():
            timezone_info = get_timezone()
            return rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_zones_get_all_time_zones_45'),
                                                        'displayText', timezone_info, 'uri')

        def get_holiday_calendar():
            work_schedule = rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['work_schedule'] if rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_31_72')['work_schedule'] else ""
            location_node = rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location_node'] if rail.result(
                'foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location_node'] else ""
            holidays_info = list(filter(lambda data: data['work_schedule'].lower() == work_schedule.lower(
            ) and data['node'].lower() == location_node.lower(), intercontinentalexchange_holiday_calendar_mapper))
            return holidays_info[0]['holiday_calendar'] if holidays_info else None

        def get_holiday_calendar_uri():
            holiday_calendar = get_holiday_calendar()
            return rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars_get_all_holiday_calendars_49'),
                                                        'displayText', holiday_calendar, 'uri')

        trigger_dag_run_live_intercontinentalexchange_child_add_user_v1_0async_76 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_child_add_user_v1_0async_76',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_child_adduser_v10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "employeeid": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['person_number'],
                "firstname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['first_name']
                + "" +
                rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')[
                    'middle_name'],
                "lastname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['last_name'],
                "work_email": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['email'],
                "effective_date": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['effective_date'],
                "location": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location'],
                "locationuri": get_location_uri(),
                "department_id": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['department_id'],
                "department": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_data_department_group_list_service1_62'),
                    'code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['department_id'], 'uri')
                if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['department_id'] else None,
                "legal_entity_id": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['legal_entity_id'],
                "legal_entity_name": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_data_division_list_service1_66'),
                    'code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['legal_entity_id'], 'uri')
                if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['legal_entity_id'] else None,
                "reporting_entity_id": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['reporting_entity_id'],
                "reporting_entity_name": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_data_cost_center_list_service1_68'),
                    'code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['reporting_entity_id'], 'uri')
                if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['reporting_entity_id'] else None,
                "worker_type": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['worker_type'],
                "line_manager": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['line_manager'],
                "actual_termination_date": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['actual_termination_date'],
                "work_schedule": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['work_schedule'],
                "week_hours": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['week_hours'],
                "location_node": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location_node'],
                "user_type": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'],
                "timesheettemplate": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets_get_all_policy_sets_48'),
                    'name', "Time distribution grid", 'uri') if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'] == "EMP" else None,
                "timezone": get_timezone(),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_all_permission_sets_get_all_permission_sets_44'),
                    'name', "Supervisor", 'uri'),
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_enabled_employee_type_groups_employeetypes_64'),
                    'displayText', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['worker_type'], 'uri'),
                "timezoneuri": get_timezone_uri(),
                "timesheetperioduri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_data_timesheet_period_list_service1_timesheet_period_list_service1_50'),
                    'period_name', "Weekly starting on Sunday", 'uri') if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'] == "EMP" else None,
                "timesheetapprovalpath": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_47'),
                    'displayText', "Supervisor", 'uri') if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'] == "EMP" else None,
                "holidaycalendar": rail.find_first_by_attr_and_get_attr(intercontinentalexchange_holiday_calendar_mapper, 'work_schedule', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['work_schedule'], 'holiday_calendar'),
                "holidaycalendaruri": get_holiday_calendar_uri(),
                "nodeudfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_43')['node'],
                "weeklyhoursudfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_43')['weekhours'],
                "useruri": rail.result('invoke_custom_ruby_code_73')['useruri'],
                "employeestatus": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['employee_status'],
                "supervisor_processing_log": rail.result('supervisor_processing_log')
            }
        )

        intercontinentalexchange_user_import_logs_add_entry_78 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_user_import_logs_add_entry_78',
            message="User not added as the Employee status is not active",
            severity="Ignored",
            properties={
                "Empid": "{{ result('foreach_query_list_changedrecordswith_mandatoryfields_31_72').person_number }}",
                "Username": "{{ result('foreach_query_list_changedrecordswith_mandatoryfields_31_72').first_name }} {{ result('foreach_query_list_changedrecordswith_mandatoryfields_31_72').last_name }}",
                "Action": "Add",
                "Status": "Ignored",
                "Details": "User not added as the Employee status is not active",
                "Jobid": "{{ dag_run_ecid() }}"
            }
        )

        trigger_dag_run_live_intercontinentalexchange_user_update_v1_0async_80 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_user_update_v1_0async_80',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchange_userupdatev10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "employeeid": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['person_number'],
                "firstname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['first_name']
                + "" +
                rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')[
                    'middle_name'],
                "lastname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['last_name'],
                "work_email": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['email'],
                "effective_date": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['effective_date'],
                "location": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location'],
                "locationuri": get_location_uri(),
                "department_id": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['department_id'],
                "department": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_data_department_group_list_service1_62'),
                    'code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['department_id'], 'uri')
                if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['department_id'] else None,
                "legal_entity_id": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['legal_entity_id'],
                "legal_entity_name": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_data_division_list_service1_66'),
                    'code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['legal_entity_id'], 'uri')
                if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['legal_entity_id'] else None,
                "reporting_entity_id": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['reporting_entity_id'],
                "reporting_entity_name": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_data_cost_center_list_service1_68'),
                    'code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['reporting_entity_id'], 'uri')
                if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['reporting_entity_id'] else None,
                "worker_type": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['worker_type'],
                "line_manager": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['line_manager'],
                "actual_termination_date": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['actual_termination_date'],
                "work_schedule": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['work_schedule'],
                "week_hours": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['week_hours'],
                "location_node": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['location_node'],
                "user_type": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'],
                "timesheettemplate": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_policy_sets_get_all_policy_sets_48'),
                    'name', "Time distribution grid", 'uri') if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'] == "EMP" else None,
                "timezone": get_timezone(),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_all_permission_sets_get_all_permission_sets_44'),
                    'name', "Supervisor", 'uri'),
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_enabled_employee_type_groups_employeetypes_64'),
                    'displayText', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['worker_type'], 'uri'),
                "timezoneuri": get_timezone_uri(),
                "timesheetperioduri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_data_timesheet_period_list_service1_timesheet_period_list_service1_50'),
                    'period_name', "Weekly starting on Sunday", 'uri') if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'] == "EMP" else None,
                "timesheetapprovalpath": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_47'),
                    'displayText', "Supervisor", 'uri') if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['user_type'] == "EMP" else None,
                "holidaycalendar": rail.find_first_by_attr_and_get_attr(intercontinentalexchange_holiday_calendar_mapper, 'work_schedule', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['work_schedule'], 'holiday_calendar'),
                "holidaycalendaruri": get_holiday_calendar_uri(),
                "nodeudfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_43')['node'],
                "weeklyhoursudfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_43')['weekhours'],
                "useruri": rail.result('invoke_custom_ruby_code_73')['useruri'],
                "employeestatus": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_31_72')['employee_status'],
                "supervisor_processing_log": rail.result('supervisor_processing_log')
            }
        )

        insert_to_user_dag_run_list_80 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_80',
            append=True,
            name='{{ result("declare_list_dag_runs_72").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_intercontinentalexchange_user_update_v1_0async_80") or result("trigger_dag_run_live_intercontinentalexchange_child_add_user_v1_0async_76"))[0]}}'
        )

        foreach_query_list_changedrecordswith_mandatoryfields_31_72_end = rail.EmptyOperator(
            task_id='foreach_query_list_changedrecordswith_mandatoryfields_31_72_end',
        )

        is_adduser_trigger_runs_avaialbale = rail.IfOperator(
            task_id='is_adduser_trigger_runs_avaialbale',
            test='''{{ result('insert_to_user_dag_run_list_80') | is_truthy }}''',
            yes_task="wait_for_completion_trigger_dag_run_live_intercontinentalexchange_80",
            no_task="ice_supervisor_check_search_entries_84",
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_80 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_80',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_80").value | to_json }}'
        )

        def get_supervisor_entries():
            supervisor_details = []
            supervisor_log_informations = get_data_from_document(
                rail.result('supervisor_processing_log'))
            for supervisor_info in supervisor_log_informations:
                if supervisor_info['properties']:
                    supervisor_details.append({
                        "employeeid": supervisor_info['properties'].get('employeeid'),
                        "userid": supervisor_info['properties'].get('userid'),
                        "username": supervisor_info['properties'].get('username'),
                        "supervisorempid": supervisor_info['properties'].get('supervisorempid'),
                        "useruri": supervisor_info['properties'].get('useruri'),
                        "action": supervisor_info['properties'].get('action'),
                        "effectivedate": supervisor_info['properties'].get('effectivedate')
                    })
            return supervisor_details

        ice_supervisor_check_search_entries_84 = rail.PythonOperator(
            task_id='ice_supervisor_check_search_entries_84',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: get_supervisor_entries()
        )

        if_entry_col1_present_85 = rail.IfOperator(
            task_id='if_entry_col1_present_85',
            test='''{{ result('ice_supervisor_check_search_entries_84') | is_truthy }}''',
            yes_task="declare_list_dag_runs_86",
            no_task="rename_archivethereferncefile_92",
        )

        declare_list_dag_runs_86 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_86',
            name='supervisor_process_dag_runs',
            value=[]
        )

        foreach_ice_supervisor_check_search_entries_84_86 = rail.ForEachOperator(
            task_id='foreach_ice_supervisor_check_search_entries_84_86',
            items="{{ result('ice_supervisor_check_search_entries_84') | to_json }}",
            start_task='trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87',
            end_task='foreach_ice_supervisor_check_search_entries_84_86_end'
        )

        trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalexchangechild_supervisorassignmentv10_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "employeeid": rail.result('foreach_ice_supervisor_check_search_entries_84_86')['employeeid'],
                "username": rail.result('foreach_ice_supervisor_check_search_entries_84_86')['username'],
                "supervisorempid": rail.result('foreach_ice_supervisor_check_search_entries_84_86')['supervisorempid'],
                "useruri": rail.result('foreach_ice_supervisor_check_search_entries_84_86')['useruri'],
                "action": rail.result('foreach_ice_supervisor_check_search_entries_84_86')['action'],
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_all_permission_sets_get_all_permission_sets_44'),
                    'name', "Supervisor", 'uri'),
                "supeffectivedate": rail.result('foreach_ice_supervisor_check_search_entries_84_86')['effectivedate'],
                "teammanagerpermission": null,
                "userid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_87 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_87',
            append=True,
            name='{{ result("declare_list_dag_runs_86").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87"))[0]}}'
        )

        foreach_ice_supervisor_check_search_entries_84_86_end = rail.EmptyOperator(
            task_id='foreach_ice_supervisor_check_search_entries_84_86_end',
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_87").value | to_json }}'
        )

        rename_archivethereferncefile_92 = rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferncefile_92',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_Old_IntercontinentalExchange_reference.csv",
            existing_filename=config.referance_filepath +
            "/IntercontinentalExchange_reference.csv",
        )

        upload_uploadnewreference_93 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreference_93',
            content='''{{ result('create_csv_lines_create_inputfilewith_m_d5_flatfiletesting_5') }}''',
            remote_filepath=config.referance_filepath +
            "/IntercontinentalExchange_reference.csv",
        )

        def do_format_logs():
            context = get_master_log_artifact_name(rail.get_current_context())
            user_import_log = rail.load_all_records(context)
            unique_users = list(
                set(map(lambda item: item['properties'].get(
                    "Empid", ''), user_import_log))
            )

            def get_log_details(user_logs):
                return "|".join(list(filter(bool, (set(map(lambda x: x['properties']['Details'], user_logs))))))

            def get_status_details(user_logs):
                return ";".join(list(filter(bool, (set(map(lambda x: x['properties']['Status'], user_logs))))))

            logs = []
            # pylint: disable= cell-var-from-loop
            for employee_id in unique_users:
                if employee_id:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'Empid', '') == employee_id, user_import_log)
                    )

                    if len(user_logs) > 0:
                        first = user_logs[0]
                        logs.append(
                            {
                                "Empid": employee_id,
                                "Username": first['properties']['Username'],
                                "Action": first['properties']['Action'],
                                "Status": get_status_details(user_logs),
                                "Details": get_log_details(user_logs),
                                "Jobid": first['ecid']
                            }
                        )
                else:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'Empid', '') == '' or x['properties'].get(
                            'Empid', '') is None, user_import_log)
                    )
                    for user in user_logs:
                        logs.append(
                            {
                                "Empid": user['properties']['Empid'],
                                "Username": user['properties']['Username'],
                                "Action": user['properties']['Action'],
                                "Status": user['properties']['Status'],
                                "Details": user['properties']['Details'],
                                "Jobid": user['properties']['Jobid']
                            }
                        )

            return logs

        log_merge_94 = rail.PythonOperator(
            task_id='log_merge_94',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: do_format_logs()
        )

        create_csv_lines_94 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_94',
            source="{{ result('log_merge_94') | to_json }}",
            header=['Empid',
                    'Username',
                    'Action',
                    'Status',
                    'Details',
                    'Jobid'],
            row=[
                '{{ item | attr_or_default("Empid", "") }}',
                '{{ item | attr_or_default("Username", "") }}',
                '{{ item | attr_or_default("Action", "")}}',
                '{{ item | attr_or_default("Status", "") }}',
                '{{ item | attr_or_default("Details", "") }}',
                '{{ item | attr_or_default("Jobid", "") }}']
        )

        def file_upload_failed(context):
            subject = '''{{ get_company_key() }} | Replicon user import - Uploading Logs to SFTP failed  - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} '''
            body = '''<p>Hi Team,<br /> <br /> The 'Replicon user import job for {{get_company_key()}}, created on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} has been completed.however,
            the log upload to sftp has failed. Attached is the log file for reference.</p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> '''
            email = rail.EmailOperator(
                task_id='send_user_import_data_to_sftp_failure_email',
                to=config.tenant_email_for_user_import,
                subject=subject,
                html_content=body,
                files=[
                    ("{{ result('create_csv_lines_94') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_uploadlogs_94 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadlogs_94',
            content="{{ result('create_csv_lines_94') }}",
            remote_filepath=config.log_filepath +
            "/{{ dag_run_ecid() }}_UserImportLogs_{{ result('log_formatteddateandtime_25') }}.csv",
            on_failure_callback=file_upload_failed
        )

        if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_94 = rail.IfOperator(
            task_id='if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_94',
            test='{{ result("query_list_changedrecordswith_mandatoryfields_31", "length") > 0 }}',
            yes_task="get_logged_errors_94",
            no_task="send_mail_95",
        )

        get_logged_errors_94 = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors_94',
            severity='Error',
        )

        get_logged_exception_94 = rail.FilterLogEntriesOperator(
            task_id='get_logged_exception_94',
            severity='Exception',
        )

        def get_subject_line():
            import_completion_message = "completed succesfully"
            has_error_message = rail.render_template(
                '{{result("get_logged_errors_94", key="length") > 0}}')
            has_exception_message = rail.render_template(
                '{{result("get_logged_exception_94", key="length") > 0}}')
            if has_error_message == 'True':
                import_completion_message = "completed with errors"
            elif has_exception_message == 'True':
                import_completion_message = "completed with exceptions"
            return import_completion_message

        email_subject_line_94 = rail.PythonOperator(
            task_id='email_subject_line_94',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: get_subject_line()
        )

        send_log_mail_94 = rail.EmailOperator(
            task_id='send_log_mail_94',
            to=config.tenant_email_for_user_import,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon user import is {{ result("email_subject_line_94") }} - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
            The Replicon user import is {{result("email_subject_line_94")}} on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}. Please find the  log file details below for reference: <br /> <br />
            File path: {{params.log_file_path}} <br />
            File name: {{ dag_run_ecid() }}_UserImportLogs_{{ result('log_formatteddateandtime_25') }}.csv<br />
            </p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''',
            params={'log_file_path': config.log_filepath},
        )

        # catch_94 = rail.EmptyOperator(
        #     task_id='catch_94',
        #     trigger_rule='one_failed',
        # )

        # generate_download_link_94 = rail.GeneratePresignedDownloadUrlOperator(
        #     task_id='generate_download_link_94',
        #     artifact_name="{{ result('create_csv_lines_94') }}",
        #     output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
        #     expires_in_seconds=7*24*60*60,
        # )

        # send_mail_94 = rail.EmailOperator(
        #     task_id='send_mail_94',
        #     to=config.tenant_email,
        #     bcc=config.internal_logs_email,
        #     subject='''{{ get_company_key() }} | Replicon user import - Uploading Logs to SFTP failed  - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
        #     html_content='''<p>Hi Team,<br /> <br /> The 'Replicon user import job for {{get_company_key()}}, created on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} has been completed.however,
        #     the log upload to sftp has failed. Attached is the log file for reference.</p>
        #     <p>Log has been attached for reference&nbsp;</p><a href="{{ result('generate_download_link_94') }}">Download log file</a>
        #     <p>For any queries, Please contact our support team at https://support.deltek.com</p>
        #     <p>Thanks, <br /> Deltek Inc.</p> ''',
        #     params=None,
        # )

        send_mail_95 = rail.EmailOperator(
            task_id='send_mail_95',
            to=config.tenant_email_for_user_import,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon user import completed with exceptions - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon user import is completed with exceptions on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}. Please find the  log file details below for reference: <br /> <br />
                File path: {{params.log_file_path}} <br />
                File name: {{ dag_run_ecid() }}_UserImportLogs_{{ result('log_formatteddateandtime_25') }}.csv<br />
                </p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'log_file_path': config.log_filepath},
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> log_today_4 >> get_time_for_file >> has_input_filename_ends_with_csv
        has_input_filename_ends_with_csv >> rail.Label(
            'No') >> archive_incorrect_file >> send_mail_for_incorrect_file >> log_to_sumo
        has_input_filename_ends_with_csv >> rail.Label(
            'Yes') >> download_file >> find_file_encoding >> parse_input_csv >> archive_file >> list_import_files >> has_any_referance_files
        has_any_referance_files >> rail.Label('No') >> log_to_sumo
        has_any_referance_files >> rail.Label('Yes') >> download_referance_file >> parse_referance_csv >> \
            create_csv_lines_create_inputfilewith_m_d5_flatfiletesting_5 >> create_collection_create_list_from_csv_8 >> \
            query_list_inputfilerecords_9 >> if_query_list_inputfilerecords_greater_than_9
        if_query_list_inputfilerecords_greater_than_9 >> rail.Label('Yes') >> \
            create_collection_create_list_from_csv_10 >> query_list_referencefilerecords_11 >> \
            declare_list_12 >> query_list_identify_unchangedrecords_13 >> if_query_list_identify_unchangedrecords_13_rows_greater_than_0_14
        if_query_list_identify_unchangedrecords_13_rows_greater_than_0_14 >> rail.Label(
            'Yes') >> add_import_log_add_entry_15 >> query_list_identify_changedrecords_16
        if_query_list_identify_unchangedrecords_13_rows_greater_than_0_14 >> rail.Label(
            'No') >> query_list_identify_changedrecords_16 >> if_query_list_identify_changedrecords_16_rows_greater_than_400_17
        if_query_list_identify_changedrecords_16_rows_greater_than_400_17 >> rail.Label(
            'Yes') >> send_mail_18 >> log_to_sumo
        if_query_list_identify_changedrecords_16_rows_greater_than_400_17 >> rail.Label(
            'No') >> create_list_21 >> query_list_changedrecordswithout_mandatoryfields_22 >> \
            if_query_list_changedrecordswithout_mandatoryfields_22_rows_greater_than_0_23
        if_query_list_changedrecordswithout_mandatoryfields_22_rows_greater_than_0_23 >> rail.Label(
            'Yes') >> foreach_document_24 >> add_import_log_add_entry_24 >> foreach_document_24_end
        foreach_document_24 >> foreach_document_24_end >> log_formatteddateandtime_25
        if_query_list_changedrecordswithout_mandatoryfields_22_rows_greater_than_0_23 >> rail.Label(
            'No') >> log_formatteddateandtime_25 >> query_list_changedrecordswith_mandatoryfields_31 >> create_list_changedrecordswith_mandatoryfields_32 >> \
            if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_33
        if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_33 >> rail.Label(
            'Yes') >> get_report_details_34 >> run_report_group_entry
        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label('No') >> log_to_sumo
        report_has_data >> rail.Label(
            'Yes') >> if_generate_report_34_payload_starts_with_nodata_35
        if_generate_report_34_payload_starts_with_nodata_35
        if_generate_report_34_payload_starts_with_nodata_35 >> rail.Label(
            'Yes') >> stop_36 >> log_to_sumo
        if_generate_report_34_payload_starts_with_nodata_35 >> rail.Label(
            'No') >> if_generate_report_34_payload_not_starts_with_column_37
        if_generate_report_34_payload_not_starts_with_column_37 >> rail.Label(
            'No') >> stop_38 >> log_to_sumo
        if_generate_report_34_payload_not_starts_with_column_37 >> rail.Label('Yes') >> load_report_data_39 >> create_user_collection_40 >> \
            get_all_custom_fields_41 >> get_all_time_zones_getalltimezones_42 >> invoke_custom_ruby_code_customfieldsuri_43 >> \
            get_all_permission_sets_get_all_permission_sets_44 >> get_all_time_zones_get_all_time_zones_45 >> \
            get_all_office_schedules_get_all_office_schedules_46 >> get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_47 >> \
            get_all_policy_sets_get_all_policy_sets_48 >> get_all_holiday_calendars_get_all_holiday_calendars_49 >> \
            get_data_timesheet_period_list_service1_timesheet_period_list_service1_50 >> query_list_getlocationsvalue_52 >> \
            trigger_dag_run_live_intercontinentalexchange_groups_check_location53 >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_groups_check_location53 >> \
            query_list_get_departmentgroupsvalue_54 >> trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055 >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_055 >> query_list_get_divisionvalue_56 >> \
            trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57 >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0division_57 >> query_list_getemployeetypevalue_58 >> \
            trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59 >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0employeetype_59 >> query_list_getcostcentergroupsvalue_60 >> \
            trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61 >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_process_groups_child_v1_0costcentersgroups_61 >> \
            get_data_department_group_list_service1_62 >> get_enabled_employee_type_groups_employeetypes_64 >> \
            get_enabled_locations_locations_65 >> get_data_division_list_service1_66 >> get_data_cost_center_list_service1_68 >> \
            intercontinentalexchange_holiday_calendar_mapper_search_entries_70 >> intercontinentalexchange_timezone_mapper_search_entries_71 >> \
            supervisor_processing_log >> declare_list_dag_runs_72 >> foreach_query_list_changedrecordswith_mandatoryfields_31_72 >> \
            invoke_custom_ruby_code_73 >> if_output_useruri_blank_74
        if_output_useruri_blank_74 >> rail.Label(
            'No') >> trigger_dag_run_live_intercontinentalexchange_user_update_v1_0async_80 >> insert_to_user_dag_run_list_80 >> \
            foreach_query_list_changedrecordswith_mandatoryfields_31_72_end
        if_output_useruri_blank_74 >> rail.Label(
            'Yes') >> if_foreach_query_list_changedrecordswith_mandatoryfields_31_72_employee_status_equals_to_active_75
        if_foreach_query_list_changedrecordswith_mandatoryfields_31_72_employee_status_equals_to_active_75 >> rail.Label(
            'Yes') >> trigger_dag_run_live_intercontinentalexchange_child_add_user_v1_0async_76 >> \
            insert_to_user_dag_run_list_80 >> \
            foreach_query_list_changedrecordswith_mandatoryfields_31_72_end
        if_foreach_query_list_changedrecordswith_mandatoryfields_31_72_employee_status_equals_to_active_75 >> rail.Label(
            'No') >> intercontinentalexchange_user_import_logs_add_entry_78 >> \
            foreach_query_list_changedrecordswith_mandatoryfields_31_72_end
        foreach_query_list_changedrecordswith_mandatoryfields_31_72 >> foreach_query_list_changedrecordswith_mandatoryfields_31_72_end >> \
            is_adduser_trigger_runs_avaialbale
        is_adduser_trigger_runs_avaialbale >> rail.Label('Yes') >> wait_for_completion_trigger_dag_run_live_intercontinentalexchange_80 >> \
            ice_supervisor_check_search_entries_84
        is_adduser_trigger_runs_avaialbale >> rail.Label('No') >> ice_supervisor_check_search_entries_84 >>\
            if_entry_col1_present_85
        if_entry_col1_present_85 >> rail.Label(
            'Yes') >> declare_list_dag_runs_86 >> foreach_ice_supervisor_check_search_entries_84_86 >> \
            trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87 >> \
            insert_to_user_dag_run_list_87 >> foreach_ice_supervisor_check_search_entries_84_86_end
        foreach_ice_supervisor_check_search_entries_84_86 >> foreach_ice_supervisor_check_search_entries_84_86_end >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_child_supervisor_assignment_v1_0async_87 >>\
            rename_archivethereferncefile_92
        if_entry_col1_present_85 >> rail.Label(
            'No') >> rename_archivethereferncefile_92 >> upload_uploadnewreference_93 >> log_merge_94 >> create_csv_lines_94 >> \
            upload_uploadlogs_94 >> if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_94
        if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_94 >> rail.Label('Yes') >> \
            get_logged_errors_94 >> get_logged_exception_94 >> email_subject_line_94 >> \
            send_log_mail_94 >> log_to_sumo
        if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_33 >> rail.Label(
            'No') >> rename_archivethereferncefile_92
        if_query_list_changedrecordswith_mandatoryfields_31_rows_greater_than_0_94 >> rail.Label('No') >>\
            send_mail_95 >> log_to_sumo
        if_query_list_inputfilerecords_greater_than_9 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
