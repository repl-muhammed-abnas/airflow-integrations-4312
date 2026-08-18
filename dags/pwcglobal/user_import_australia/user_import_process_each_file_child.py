import hashlib
from datetime import timedelta
import rail
from airflow.models import Variable
from pwcglobal.user_import_australia import custom_methods
from pwcglobal.user_import_australia.send_logs_user_import import get_send_logs

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_process_each_file_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - User import process each file {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")
        create_user_import_log = rail.CreateLogOperator(
            task_id="create_user_import_log"
        )

        is_file_csv = rail.IfOperator(
            task_id="is_file_csv",
            test="{{dag_run.conf.file_name | file_ext | lower == 'csv' }}",
            yes_task="download_file",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath="{{dag_run.conf.file_path}}" +
            "/"+"{{dag_run.conf.file_name}}"
        )
        parse_input_file = rail.LoadCSVFileOperator(
            task_id="parse_input_file",
            document="{{result('download_file')}}",
            encoding="latin_1"
        )
        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            source="{{result('parse_input_file')}}"
        )

        def get_create_md5_data(item):
            if not item:
                return []
            res = {
                'employee_id': item["Employee_ID"],
                'party_id': item["Party_ID"],
                'guid': item["GUID"],
                'staff_code': item["Staff_Code"],
                'active_status': item["Active_Status"],
                'id': item["ID"],
                'work_email': item["Work_Email"],
                'first_name': item["First_Name"],
                'last_name': item["Last_Name"],
                'hire_date': item["Hire_Date"],
                'employee_type': item["Employee_Type"],
                'time_type': item["Time_Type"],
                'management_level': item["Management_Level"],
                'line_of_service': item["Line_of_Service"],
                'manager_id': item["Manager_ID"],
                'cost_center_id': item["Cost_Centre_ID"],
                'cost_center_level_1': item["Cost_Center_Level_1"],
                'cost_center_level_2': item["Cost_Center_Level_2"],
                'cost_center_level_3': item["Cost_Center_Level_3"],
                'cost_center_level_4': item["Cost_Center_Level_4"],
                'location_level_1': item["Location_Level_1"],
                'location_level_2': item["Location_Level_2"],
                'location_level_3': item["Location_Level_3"],
                'location_level_4': item["Location_Level_4"],
                'classification': item["Classification"],
                'md5': hashlib.md5((item["Employee_ID"]+","+item["Party_ID"]+","+item["GUID"]+","+item["Staff_Code"]+","
                                    + item["Active_Status"]+"," +
                                    item["ID"]+"," + item["Work_Email"]+","
                                    + item["First_Name"]+"," +
                                    item["Last_Name"]+","+item["Hire_Date"]+","
                                    + item["Employee_Type"]+"," + item["Time_Type"]+"," +
                                    item["Management_Level"]+"," +
                                    item["Line_of_Service"] + ","
                                    + item["Manager_ID"]+"," + item["Cost_Centre_ID"] +
                                    ","+item["Cost_Center_Level_1"] + ","
                                    + item["Cost_Center_Level_2"]+"," + item["Cost_Center_Level_3"] +
                                    "," + item["Cost_Center_Level_4"]+","
                                    + item["Location_Level_1"]+"," +
                                    item["Location_Level_2"]+"," +
                                    item["Location_Level_3"]+","
                                    + item["Location_Level_4"] +
                                    ","+item["Classification"]
                                    ).encode('utf-8')).hexdigest()

            }

            return {k: v if v is not None else '' for k, v in res.items()}

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('create_input_collection')}}",
            columns=['employee_id', 'party_id', 'guid', 'staff_code', 'active_status', 'id', 'work_email',
                     'first_name', 'last_name', 'hire_date', 'employee_type', 'time_type', 'management_level', 'line_of_service',
                     'manager_id', 'cost_center_id', 'cost_center_level_1', 'cost_center_level_2', 'cost_center_level_3', 'cost_center_level_4',
                     'location_level_1', 'location_level_2', 'location_level_3', 'location_level_4', 'classification', 'md5'],
            data=get_create_md5_data
        )

        input_data_with_md5 = rail.CreateCollectionOperator(
            task_id="input_data_with_md5",
            name="input_data",
            source="{{result('create_md5')}}"
        )
        get_all_input_data = rail.QueryCollectionOperator(
            task_id="get_all_input_data",
            query="SELECT * FROM input_data"
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{result('get_all_input_data','length') > 0}}",
            no_task="send_blank_input_file_email",
            yes_task="process_location_cost_center"
        )

        send_blank_input_file_email = rail.EmailOperator(
            task_id="send_blank_input_file_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{get_company_key()}}' + \
            ' | Australia - User import has been skipped - {{current_time_in_specified_tz("Australia/Sydney","%Y-%m-%dT%H%M%S")}}',
            html_content="templates/email/email_blank_file_user_import.html",
        )

        archive_input_file_start = rail.EmptyOperator(
            task_id="archive_input_file_start"
        )

        upload_to_archive_folder = rail.SFTPUploadFileOperator(
            task_id = "upload_to_archive_folder",
            remote_filepath=config.user_import_archive_path + "{{dag_run.conf.file_name}}",
            content="{{result('download_file')}}",
        )

        delete_the_input_file = rail.SFTPDeleteFileOperator(
            task_id = "delete_the_input_file",
            existing_filename="{{dag_run.conf.file_path}}/{{dag_run.conf.file_name}}"
        )

        process_location_cost_center = rail.TriggerDagRunOperator(
            task_id="process_location_cost_center",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_process_location_cost_center_child_{config.instance}",
            conf={
                "file_name": "{{dag_run.conf.file_name}}"
            }
        )
        wait_for_location_cost_center = rail.WaitForDagRunsSensor(
            task_id="wait_for_location_cost_center",
            dag_runs="{{result('process_location_cost_center')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        get_blank_guid_user = rail.QueryCollectionOperator(
            task_id="get_blank_guid_user",
            query="""SELECT * FROM input_data WHERE NULLIF(guid,'') IS NULL"""
        )
        has_any_blank_users_data = rail.IfOperator(
            task_id="has_any_blank_users_data",
            test="{{result('get_blank_guid_user', 'length') > 0}}",
            yes_task="log_skipped_users",
            no_task="get_reference_variable"
        )

        log_skipped_users = rail.WriteLogOperator(
            task_id="log_skipped_users",
            log="{{result('create_user_import_log')}}",
            items="{{result('get_blank_guid_user')}}",
            severity="skipped",
            message="Guid id is missing",
            properties={
                "guid": "{{item.guid}}",
                "action": "validation",
                "status": "skipped",
                "details": "Guid id is missing",
                "manager_id": "{{item.manager_id}}"
            }
        )

        get_valid_input_data = rail.QueryCollectionOperator(
            task_id="get_valid_input_data",
            query="""SELECT * FROM input_data WHERE NULLIF(guid,'') IS NOT NULL"""
        )

        get_custom_field_groups = rail.RepliconServiceOperator(
            task_id="get_custom_field_groups",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroups",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'User', 'uri')
        )

        def get_required_user_custom_fields(response):
            response = response.json()['d']
            if not response:
                return []

            return {
                "party_id_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Party ID', 'uri'),
                "management_level_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Management Level', 'uri'),
                "line_of_services_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Line of Services', 'uri'),
                "local_staff_code_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Local Staff ID', 'uri')
            }

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_custom_field_groups')}}"
            },
            response_filter=get_required_user_custom_fields
        )

        def ref_variable():
            return Variable.get(f"pwc_user_import_can_user_reference_{config.instance}").lower() == 'true'
        get_reference_variable = rail.PythonOperator(
            task_id="get_reference_variable",
            python_callable=ref_variable
        )
        use_reference_logic = rail.IfOperator(
            task_id="use_reference_logic",
            test="{{result('get_reference_variable')}}",
            yes_task="get_reference_file",
            no_task="process_each_records"
        )

        get_reference_file = rail.SFTPDownloadFileOperator(
            task_id="get_reference_file",
            sftp_conn_id=config.reference_sftp_conn_id,
            remote_filepath=config.reference_file,
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('get_reference_file')}}",
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id="create_reference_data_collection",
            name="reference_data",
            source="{{result('parse_reference_file')}}"
        )

        get_delta_records = rail.QueryCollectionOperator(
            task_id="get_delta_records",
            query="""SELECT * FROM get_valid_input_data WHERE md5 NOT IN (SELECT DISTINCT md5 FROM reference_data)"""
        )

        has_any_changed_records = rail.IfOperator(
            task_id="has_any_changed_records",
            test="{{result('get_delta_records', 'length') > 0}}",
            yes_task="process_each_records",
            no_task="format_logs"
        )

        get_unchanged_records = rail.QueryCollectionOperator(
            task_id="get_unchanged_records",
            query="""SELECT * FROM get_valid_input_data WHERE md5 IN (SELECT DISTINCT md5 FROM reference_data)"""
        )

        has_any_unchanged_records = rail.IfOperator(
            task_id="has_any_unchanged_records",
            test="{{result('get_unchanged_records', 'length') > 0}}",
            yes_task="log_unchanged_records",
            no_task="format_logs"
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id="log_unchanged_records",
            log="{{result('create_user_import_log')}}",
            items="{{result('get_unchanged_records')}}",
            message="No change to user record",
            severity="skipped",
            properties={
                "guid": "{{item.guid}}",
                "action": "validation",
                "status": "skipped",
                "details": "No change to user record",
                "manager_id": "{{item.manager_id}}",
                "processed": "no"
            }
        )

        create_supervisor_processing_log = rail.CreateLogOperator(
            task_id="create_supervisor_processing_log"
        )

        process_each_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_records",
            items="{{result('get_delta_records') if result('get_reference_variable') else result('get_valid_input_data')}}",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_process_each_record_child_{config.instance}",
            conf=custom_methods.get_user_import_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_process_each_records = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_each_records",
            dag_runs="{{result('process_each_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_supervisor_records_to_process = rail.PythonOperator(
            task_id="get_supervisor_records_to_process",
            python_callable=lambda: rail.load_all_records(
                rail.result("create_supervisor_processing_log"))
        )

        process_add_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id="process_add_supervisor",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_process_supervisor_child_{config.instance}",
            items="{{result('get_supervisor_records_to_process') | to_json}}",
            conf={
                'employee_id': "{{item.properties.employee_id}}",
                'severity': "{{item.severity}}",
                'guid': "{{item.properties.guid}}",
                'manager_id': "{{item.properties.manager_id}}",
                'firstname': "{{ item.properties.firstname }}",
                'lastname': "{{ item.properties.lastname }}",
                'log': "{{result('create_user_import_log')}}",
                "action": "{{item.properties.action}}",
                "details": "{{item.properties.details}}",
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_for_add_supervisors = rail.WaitForDagRunsSensor(
            task_id="wait_for_add_supervisors",
            dag_runs="{{result('process_add_supervisor')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_methods.do_format_logs
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('get_all_input_data'),
            header=['employee_id', 'party_id', 'guid', 'staff_code', 'active_status', 'id', 'work_email',
                    'first_name', 'last_name', 'hire_date', 'employee_type', 'time_type', 'management_level', 'line_of_service',
                    'manager_id', 'cost_center_id', 'cost_center_level_1', 'cost_center_level_2', 'cost_center_level_3', 'cost_center_level_4',
                    'location_level_1', 'location_level_2', 'location_level_3', 'location_level_4', 'classification', 'md5'],
            row=[
                '{{item.employee_id}}',
                '{{item.party_id}}',
                '{{item.guid}}',
                '{{item.staff_code}}',
                '{{item.active_status}}',
                '{{item.id}}',
                '{{item.work_email}}',
                '{{item.first_name}}',
                '{{item.last_name}}',
                '{{item.hire_date}}',
                '{{item.employee_type}}',
                '{{item.time_type}}',
                '{{item.management_level}}',
                '{{item.line_of_service}}',
                '{{item.manager_id}}',
                '{{item.cost_center_id}}',
                '{{item.cost_center_level_1}}',
                '{{item.cost_center_level_2}}',
                '{{item.cost_center_level_3}}',
                '{{item.cost_center_level_4}}',
                '{{item.location_level_1}}',
                '{{item.location_level_2}}',
                '{{item.location_level_3}}',
                '{{item.location_level_4}}',
                '{{item.classification}}',
                '{{item.md5}}'
            ]
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            sftp_conn_id=config.reference_sftp_conn_id,
            new_filename=config.reference_archive_file_path +
            "pwcglobal_aus_userImport_reference_file_{{current_time_in_specified_tz('Australia/Sydney','%Y-%m-%dT%H%M%S%z')}}.csv",
            existing_filename=config.reference_file
        )

        update_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="update_new_reference_file",
            sftp_conn_id=config.reference_sftp_conn_id,
            content="{{result('create_reference_file')}}",
            remote_filepath=config.reference_file
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        has_any_entries_in_log, send_complete_mail = get_send_logs(config)

        create_supervisor_processing_log >> create_user_import_log >> is_file_csv >> rail.Label(
            "Yes") >> download_file >> parse_input_file >> create_input_collection >> create_md5 >> input_data_with_md5 >> get_all_input_data
        get_all_input_data >> has_any_data >> rail.Label(
            "No") >> send_blank_input_file_email
        has_any_data >> rail.Label("Yes") >> process_location_cost_center >> wait_for_location_cost_center >> [
            get_valid_input_data, get_blank_guid_user]

        get_blank_guid_user >> has_any_blank_users_data >> rail.Label(
            "Yes") >> log_skipped_users >> get_reference_variable >> use_reference_logic
        has_any_blank_users_data >> rail.Label(
            "No") >> get_reference_variable >> use_reference_logic
        get_valid_input_data >> get_custom_field_groups >> get_all_user_custom_fields >> get_reference_variable >> use_reference_logic \
            >> rail.Label("No") >> process_each_records >> wait_for_process_each_records >> get_supervisor_records_to_process >> process_add_supervisor\
            >> wait_for_add_supervisors >> create_reference_file >> archive_reference_file >> update_new_reference_file >> format_logs >> has_any_entries_in_log
        use_reference_logic >> rail.Label("Yes") >> get_reference_file >> parse_reference_file >> create_reference_data_collection >> [
            get_unchanged_records, get_delta_records]
        get_delta_records >> has_any_changed_records >> rail.Label(
            "Yes") >> process_each_records
        get_unchanged_records >> has_any_unchanged_records >> rail.Label(
            "Yes") >> log_unchanged_records >> format_logs
        send_complete_mail >> log_to_sumo
        [has_any_changed_records, has_any_unchanged_records] >> rail.Label(
            "No") >> format_logs

        download_file >> archive_input_file_start >> upload_to_archive_folder >> delete_the_input_file
    return dag


rail.for_each_instance(create_child_dag)
