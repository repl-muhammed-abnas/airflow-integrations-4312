from datetime import timedelta, datetime
import rail
from lendingclub.user_import.utils import request_payload
from lendingclub.user_import.utils import python_callable

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_userimport_master_{config.instance}',
        description=f'lendingclub_userimport_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_master,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor_to_process") == "success" }}',
            yes_task="get_current_time",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        get_current_time = rail.PythonOperator(
            task_id = "get_current_time",
            python_callable=lambda: datetime.now().strftime("%Y-%m-%dT%H%M%S%z")
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        parse_user_import_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_import_csv",
            document='{{result("download_sftp_file")}}',
            delimiter=","
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath + "/{{ result('get_current_time') }}_{{ result('new_file_sensor_to_process') | file_name }}"
        )

        check_csv_has_data = rail.IfOperator(
            task_id = "check_csv_has_data",
            test = "{{result('parse_user_import_csv') | length > 0}}",
            yes_task = "create_collection_from_csv",
            no_task = "send_no_data_to_import_mail"
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('parse_user_import_csv') }}",
            name="inputfile",
            columns={
                "Login ID" : "loginname",
                "Employee ID" :"empid",
                "Name" : "firstname",
                "Last Name" : "lastname",
                "Hire Date" : "hiredate",
                "Employee Type Code" : "employeetypecode",
                "Employee Type" : "employeetype",
                "Vendor" : "vendor",
                "Employee Status" : "employeestatus",
                "Department Code" : "departmentcode",
                "Department" : "department",
                "Manager ID" : "managerid",
                "Email" : "email",
                "Location Code" : "locationcode",
                "Location" : "location",
                "Permission": "permission",
                "JobLevel" : "joblevel",
                "ScrumTeam" : "Scrum"
            }
        )

        get_collection_info = rail.PythonOperator(
            task_id = "get_collection_info",
            python_callable=lambda: rail.load_all_records(rail.result('create_collection_from_csv'))
        )

        check_import_csv_has_data = rail.IfOperator(
            task_id = "check_import_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('create_collection_from_csv'))) > 0,
            yes_task = "get_all_custom_fields_for_required_group",
            no_task = "send_no_data_to_import_mail"
        )

        send_no_data_to_import_mail = rail.EmailOperator(
            task_id='send_no_data_to_import_mail',
            to=config.to_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} |User import has been skipped - {datetime.now().strftime("%d-%m-%Y")}',
            html_content="templates/emails/no_records_to_process_email.html",
        )

        get_all_custom_fields_for_required_group = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_for_required_group',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler= lambda response:{
                "vendors_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Vendors', 'uri', ''),
                "joblevel_uri" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Level', 'uri', '')
            }
        )

        query_distinct_departments = rail.QueryCollectionOperator(
            task_id="query_distinct_departments",
            query="""SELECT DISTINCT departmentcode, department FROM inputfile""",
            name="distinct_department_data"
        )

        process_each_department = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_department",
            items = "{{ result('query_distinct_departments')}}",
            trigger_dag_id = f'lendingclub_user_import_division_add_update_child_{config.instance}',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = lambda item:{
                "departmentcode": item['departmentcode'],
                "departmentname": item['department'],
            }
        )

        wait_for_process_each_department = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_department',
            dag_runs='{{ result("process_each_department") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_distinct_locations = rail.QueryCollectionOperator(
            task_id="query_distinct_locations",
            query="""SELECT DISTINCT locationcode, location FROM inputfile""",
            name="distinct_location_data"
        )

        process_each_location = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_location",
            items = "{{ result('query_distinct_locations')}}",
            trigger_dag_id = f'lendingclub_user_import_location_add_update_child_{config.instance}',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = lambda item:{
                "locationcode": item['locationcode'],
                "locationname": item['location'],
            }
        )

        wait_for_process_each_location = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_location',
            dag_runs='{{ result("process_each_location") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_distinct_vendors = rail.QueryCollectionOperator(
            task_id="query_distinct_vendors",
            query="""SELECT DISTINCT vendor FROM inputfile WHERE (NULLIF(vendor, '') IS NOT NULL)""",
            name="distinct_vendor"
        )

        if_distinct_vendor_present = rail.IfOperator(
            task_id = "if_distinct_vendor_present",
            test = "{{result('query_distinct_vendors') | length > 0}}",
            yes_task = "compose_vendor_data_csv",
            no_task = "query_data_with_blank_loginname"
        )

        compose_vendor_data_csv = rail.WriteCSVFileOperator(
            task_id='compose_vendor_data_csv',
            source="{{ result('query_distinct_vendors') }}",
            header=[
                'vendor'
            ],
            row=lambda item:[
                item['vendor']
            ]
        )

        process_each_vendor = rail.TriggerDagRunOperator(
            task_id='process_each_vendor',
            trigger_dag_id=f'lendingclub_user_import_update_vendor_dropdown_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:{
                'vendor': rail.result('compose_vendor_data_csv'),
                'vendor_uri': rail.result('get_all_custom_fields_for_required_group')['vendors_uri']
            }
        )

        query_data_with_blank_loginname = rail.QueryCollectionOperator(
            task_id="query_data_with_blank_loginname",
            query="""SELECT * FROM inputfile WHERE (NULLIF(loginname, '') IS NULL)""",
            name="blank_loginname_record"
        )

        user_import_log = rail.CreateLogOperator(
            task_id = "user_import_log"
        )

        supervisor_assignment_log = rail.CreateLogOperator(
            task_id = "supervisor_assignment_log"
        )

        log_blank_loginname_records = rail.WriteLogOperator(
            task_id="log_blank_loginname_records",
            log = '{{result("user_import_log")}}',
            items="{{result('query_data_with_blank_loginname')}}",
            message="Skipped",
            severity="Skipped",
            properties=lambda item, dag_run: {
                "UserID": item['loginname'] + "|" + item['empid'],
                "Action": "Validation",
                "Status": "Skipped",
                'Details': "Login  ID must be present"
            }
        )

        query_data_with_loginname = rail.QueryCollectionOperator(
            task_id="query_data_with_loginname",
            query="""SELECT * FROM inputfile WHERE (NULLIF(loginname, '') IS NOT NULL)""",
            name="data_with_loginname_present"
        )

        process_each_user = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_user",
            items = "{{ result('query_data_with_loginname')}}",
            trigger_dag_id = f'lendingclub_user_import_process_user_child_{config.instance}',
            execution_timeout = timedelta(config.execution_timeout_days),
            conf = request_payload.user_process_conf
        )

        wait_for_process_each_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_user',
            dag_runs='{{ result("process_each_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        write_supervisor_assignment_log_file = rail.WriteCSVFileOperator(
            task_id="write_supervisor_assignment_log_file",
            source=lambda: rail.result('supervisor_assignment_log'),
            header=['loginid', 'managerid', 'empid', 'useruri', 'type'],
            row=lambda item: [
                item['properties']['loginid'],
                item['properties']['managerid'],
                item['properties']['empid'],
                item['properties']['useruri'],
                item['properties']['type']
            ]
        )

        load_csv_supervisor_mapper = rail.LoadCSVFileOperator(
            task_id='load_csv_supervisor_mapper',
            document="{{ result('write_supervisor_assignment_log_file') }}",
        )

        check_supervisor_mapper_csv_has_data = rail.IfOperator(
            task_id = "check_supervisor_mapper_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('load_csv_supervisor_mapper'))) > 0 ,
            yes_task = "process_each_mapper_data",
            no_task = "write_userimportlog_file"
        )

        process_each_mapper_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_mapper_data',
            items = "{{ result('load_csv_supervisor_mapper')}}",
            trigger_dag_id=f'lendingclub_user_import_update_supervisor_child_{config.instance}',
            conf=request_payload.process_supervisor_mapper_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_mapper_data_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_mapper_data_process',
            dag_runs='{{ result("process_each_mapper_data") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        write_userimportlog_file = rail.WriteCSVFileOperator(
            task_id="write_userimportlog_file",
            source=lambda: rail.result('user_import_log'),
            header=['userid', 'action', 'status', 'details'],
            row=lambda item: [
                item['properties']['UserID'],
                item['properties']['Action'],
                item['properties']['Status'],
                item['properties']['Details']
            ]
        )

        check_userimportlog_csv_has_data = rail.IfOperator(
            task_id = "check_userimportlog_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_userimportlog_file'))) > 0 ,
            yes_task = "get_log_file_name",
            no_task = "log_to_sumo"
        )

        get_log_file_name = rail.PythonOperator(
            task_id = "get_log_file_name",
            python_callable=lambda: "/log_" + rail.result('get_current_time') + "_" + rail.result('new_file_sensor_to_process').split('/')[-1]
        )

        upload_logs_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('write_userimportlog_file') }}",
            remote_filepath=config.log_filepath + "/log_{{ result('get_current_time') }}_{{ result('new_file_sensor_to_process') | file_name }}"
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('write_userimportlog_file')}}",
            output_file_name="log_{{ result('get_current_time') }}_{{ result('new_file_sensor_to_process') | file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        check_if_upload_success = rail.IfOperator(
            task_id='check_if_upload_success',
            test="{{ get_task_state('upload_logs_to_sftp') == 'success' }}",
            yes_task='filter_master_log',
            no_task='send_error_in_upload_mail'
        )

        send_error_in_upload_mail = rail.EmailOperator(
            task_id='send_error_in_upload_mail',
            to=config.to_email,
            bcc=config.alert_email,
            subject=f'{config.company_key} |Failed while uploading Logs to SFTP  -' + datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%z"),
            html_content="templates/emails/send_error_in_upload.html",
            params={
                'company_key': config.company_key
            },
            files=[
                ("{{ result('get_log_file_name') }}" , "{{ result('write_userimportlog_file') }}")]
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            log = '{{result("user_import_log")}}',
            severity='Error',
        )


        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ result('user_import_log') | load_all_records | to_json }}"
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.do_format_logs
        )

        write_csv_file1 = rail.WriteCSVFileOperator(
            task_id='write_csv_file1',
            source="{{ result('format_logs').final_logs }}",
            header=['userid', 'action', 'status', 'details', 'ecid'],
            row=[
                '{{ item.UserID }}',
                '{{ item.Action }}',
                '{{ item.Status}}',
                '{{ item.Details }}',
                '{{ item.ecid }}'
            ]
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.to_email,
            bcc="{%- if result('format_logs').get_record_summary.failed == 0 -%}\
                    "+config.bcc_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User import - " }} \
                {%- if result("format_logs").get_record_summary.failed > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs").get_record_summary.exception > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%}' \
                + " " + datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%z"),
            html_content="templates/emails/import_complete_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor_to_process >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label('Yes') >> get_current_time >> download_sftp_file >> parse_user_import_csv >> archive_input_file >> check_csv_has_data
        check_csv_has_data >> rail.Label('Yes') >> create_collection_from_csv >> get_collection_info >> check_import_csv_has_data
        check_csv_has_data >> rail.Label('No') >> send_no_data_to_import_mail >> log_to_sumo

        check_import_csv_has_data >> rail.Label('Yes') >> get_all_custom_fields_for_required_group
        check_import_csv_has_data >> rail.Label('No') >> send_no_data_to_import_mail >> log_to_sumo

        get_all_custom_fields_for_required_group >> query_distinct_departments >> process_each_department >> \
            wait_for_process_each_department >> query_distinct_locations >> process_each_location >> wait_for_process_each_location >> \
            query_distinct_vendors >> if_distinct_vendor_present

        if_distinct_vendor_present >> rail.Label('Yes') >> compose_vendor_data_csv >> process_each_vendor >> \
        query_data_with_blank_loginname
        if_distinct_vendor_present >> rail.Label('No') >> query_data_with_blank_loginname

        query_data_with_blank_loginname >> user_import_log >> supervisor_assignment_log >> log_blank_loginname_records >> \
            query_data_with_loginname >> process_each_user >> wait_for_process_each_user >> write_supervisor_assignment_log_file >> \
            load_csv_supervisor_mapper >> check_supervisor_mapper_csv_has_data

        check_supervisor_mapper_csv_has_data >> rail.Label('Yes') >> process_each_mapper_data >> wait_for_mapper_data_process >> write_userimportlog_file
        check_supervisor_mapper_csv_has_data >> rail.Label('No') >> write_userimportlog_file

        write_userimportlog_file >> check_userimportlog_csv_has_data

        check_userimportlog_csv_has_data >> rail.Label('Yes') >> get_log_file_name >> upload_logs_to_sftp >> generate_downloadlink >> check_if_upload_success
        check_userimportlog_csv_has_data >> rail.Label('No') >> log_to_sumo

        check_if_upload_success >> rail.Label('Yes') >> filter_master_log >> load_master_log >> format_logs >> write_csv_file1 >> send_import_complete_email >> log_to_sumo
        check_if_upload_success >> rail.Label('No') >> send_error_in_upload_mail >> log_to_sumo

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
