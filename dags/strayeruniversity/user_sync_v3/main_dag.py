from datetime import timedelta
import itertools
from pendulum import now, datetime as dt
import json
import rail
from strayeruniversity.user_sync_v3.utils import request_payload
from strayeruniversity.user_sync_v3.utils import python_callable
from strayeruniversity.user_sync_v3.utils.python_callable import get_ref_file_name, get_inp_file_name, get_inp_file_name_no_changed_records
from airflow.models import Variable


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'strayeruniversity_usersync_master_v3_{config.instance}',
        description=f'strayeruniversity_usersync_master_v3_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        if_instance_trial = rail.IfOperator(
            task_id='if_instance_trial',
            test=lambda: 'trial' in config.instance,
            yes_task='new_file_sensor_to_process',
            no_task='get_workdayreport_http_payload'
        )

        get_workdayreport_http_payload = rail.SimpleHttpOperator(
            task_id='get_workdayreport_http_payload',
            method='GET',
            # endpoint="https://wd2-impl-services1.workday.com/ccx/service/customreport2/strayer/ISU+RPT/RPT_Replicon_Outbound?format=json",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        workdayreport_json_load = rail.PythonOperator(
            task_id='workdayreport_json_load',
            python_callable=lambda: json.loads(
                rail.result('get_workdayreport_http_payload'))
        )

        if_first_employee_id_blank_1_5 = rail.IfOperator(
            task_id='if_first_employee_id_blank_1_5',
            test='''{{ result('workdayreport_json_load') | is_falsy or result('workdayreport_json_load')['Report_Entry'] | length == 0  or result('workdayreport_json_load')['Report_Entry'][0].EmplID | is_falsy }}''',
            yes_task="send_mail_no_record_in_report",
            no_task="get_write_csv_task_source",
        )

        send_mail_no_record_in_report = rail.EmailOperator(
            task_id='send_mail_no_record_in_report',
            to=config.tenant_email,
            cc=config.bcc_email,
            subject='''{{ get_company_key() }} | Replicon user import skipped - Blank File - {{ current_time_in_specified_tz("''' + config.time_zone + '''")}}''',
            html_content='''<p>Hello, <br /> <br /> The Replicon user import is skipped is skipped on {{ current_time_in_specified_tz("''' + config.time_zone + '''")}} as the RAAS file is blank.<br /><br />Please check the input file and make the required correction.&nbsp;</p>
            <p>URL used: https://services1.myworkday.com/ccx/service/customreport2/strayer/ISU+RPT/RPT_Replicon_Outbound?format=json</p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_master,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule='all_done',
            test='{{get_task_state("new_file_sensor_to_process") == "success" }}',
            yes_task="download_sftp_file",
            no_task="if_instance_is_trial"
        )

        if_instance_is_trial = rail.IfOperator(
            task_id="if_instance_is_trial",
            test=lambda: 'trial' in config.instance,
            yes_task="delete_dagrun",
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        archive_input_processing_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_processing_file',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath +
            "/{{ result('new_file_sensor_to_process') | file_name }}"
        )

        parse_user_sync_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_sync_csv",
            document='{{result("download_sftp_file")}}',
            delimiter=","
        )

        get_write_csv_task_source = rail.PythonOperator(
            task_id='get_write_csv_task_source',
            trigger_rule='one_success',
            python_callable=lambda: json.dumps(rail.result('workdayreport_json_load')['Report_Entry']) if rail.result(
                'workdayreport_json_load') else rail.result('parse_user_sync_csv')
        )

        write_user_sync_csv = rail.WriteCSVFileOperator(
            task_id="write_user_sync_csv",
            source="{{result('get_write_csv_task_source')}}",
            header=['FirstName', 'LastName', 'UserName', 'WorkEmail', 'EmplID', 'TimeType', 'HireDate', 'TermDate',
                    'ManagerName', 'Department', 'Location', 'SubstituteName', 'Timezone', 'ScheduledHours', 'ManagementLevel',
                    'Division', 'Position', 'homeworkstate', 'employeestatus', 'Approver', 'md5'],
            row=request_payload.user_import_csv_data
        )

        get_current_time = rail.PythonOperator(
            task_id="get_current_time",
            python_callable=lambda: now(
                config.time_zone).strftime("%m_%d_%YT%H_%M")
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id='get_all_reports',
            endpoint="/services/reportservice1.svc/GetAllReports",
            data_handler=lambda response: {
                "user_ref_data_uri": rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'User reference data', 'uri', '')
            }
        )

        user_import_log = rail.CreateLogOperator(
            task_id="user_import_log"
        )

        supervisor_assignment_log = rail.CreateLogOperator(
            task_id="supervisor_assignment_log"
        )

        upload_inputfile_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_inputfile_to_sftp',
            content="{{ result('write_user_sync_csv') }}",
            remote_filepath=config.input_filepath +
            "/userdata{{ result('get_current_time') }}.csv"
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('write_user_sync_csv') }}",
            name="sourceuserdata"
        )

        query_usersync_csv_records_line13 = rail.QueryCollectionOperator(
            task_id="query_usersync_csv_records_line13",
            query="""SELECT * FROM sourceuserdata""",
            name="usersync_records"
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'management_level_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Management Level', 'uri', ''),
                'employee_status_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'EmployeeStatus', 'uri', '')
            }
        )

        query_distinct_managementlevel = rail.QueryCollectionOperator(
            task_id="query_distinct_managementlevel",
            query="""SELECT DISTINCT ManagementLevel FROM sourceuserdata WHERE (NULLIF(ManagementLevel, '') IS NOT NULL)""",
            name="distinct_managementlevel"
        )

        if_distinct_managementlevel_present = rail.IfOperator(
            task_id="if_distinct_managementlevel_present",
            test="{{result('query_distinct_managementlevel', 'length') > 0}}",
            yes_task="compose_managementlevel_data_csv",
            no_task="get_user_ref_data_from_replicon"
        )

        compose_managementlevel_data_csv = rail.WriteCSVFileOperator(
            task_id='compose_managementlevel_data_csv',
            source="{{ result('query_distinct_managementlevel') }}",
            header=[
                'managementlevel'
            ],
            row=lambda item: [
                item['ManagementLevel']
            ]
        )

        process_each_managementlevel = rail.TriggerDagRunOperator(
            task_id='process_each_managementlevel',
            trigger_dag_id=f'strayeruniversity_managementlevel_customfield_check_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'managementlevel': rail.result('compose_managementlevel_data_csv'),
                'managementlevel_customfield_uri': rail.result('get_required_user_customfields')['management_level_uri']
            }
        )

        get_user_ref_data_from_replicon = rail.RepliconServiceOperator(
            task_id='get_user_ref_data_from_replicon',
            endpoint="/services/reportservice1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('get_all_reports').user_ref_data_uri }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        parse_referencecsv_data_from_replicon = rail.LoadCSVFileOperator(
            task_id="parse_referencecsv_data_from_replicon",
            document='{{result("get_user_ref_data_from_replicon").payload }}',
            delimiter=","
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.reference_filepath]
        )

        get_ref_filepath_name = rail.PythonOperator(
            task_id="get_ref_filepath_name",
            python_callable=lambda: get_ref_file_name(
                config.reference_filepath)
        )

        is_use_reference_file_allowed = rail.IfOperator(
            task_id="is_use_reference_file_allowed",
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='Y').lower() == 'y',
            yes_task="is_csv",
            no_task="is_use_reference_file_not_allowed"
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("get_ref_filepath_name") | file_ext | lower == "csv" }}',
            yes_task='download_reference_file',
            no_task="is_use_reference_file_not_allowed",
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath="{{ result('get_ref_filepath_name')}}"
        )

        load_reference_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_csv",
            delimiter=",",
            document="{{ result('download_reference_file') }}",
            headers=['FirstName', 'LastName', 'UserName', 'WorkEmail', 'EmplID', 'TimeType', 'HireDate', 'TermDate',
                     'ManagerName', 'Department', 'Location', 'SubstituteName', 'Timezone', 'ScheduledHours', 'ManagementLevel',
                     'Division', 'Position', 'homeworkstate', 'employeestatus', 'Approver', 'md5']
        )

        create_ref_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_ref_collection_from_csv',
            source="{{ result('load_reference_csv') }}",
            name="referenceuserdata"
        )

        query_for_changed_records = rail.QueryCollectionOperator(
            task_id="query_for_changed_records",
            query="""SELECT * FROM sourceuserdata WHERE md5 NOT IN (SELECT md5 FROM referenceuserdata)""",
            name="changed_records"
        )

        query_for_unchanged_records = rail.QueryCollectionOperator(
            task_id="query_for_unchanged_records",
            query="""SELECT * FROM sourceuserdata WHERE md5 IN (SELECT md5 FROM referenceuserdata)""",
            name="unchanged_records"
        )

        if_changedrecords_present = rail.IfOperator(
            task_id="if_changedrecords_present",
            test="{{result('query_for_changed_records', 'length')> 0}}",
            yes_task="process_each_user_dummy",
            no_task="send_userimport_completed_success_email"
        )

        is_use_reference_file_not_allowed = rail.IfOperator(
            task_id="is_use_reference_file_not_allowed",
            test=lambda: Variable.get(
                config.can_use_reference_file, default_var='Y').lower() == 'n',
            yes_task="if_first_firstname_present_in_records",
            no_task="write_supervisor_assignment_log_file"
        )

        if_first_firstname_present_in_records = rail.IfOperator(
            task_id='if_first_firstname_present_in_records',
            test='''{{ result('query_usersync_csv_records_line13', 'length') > 0 }}''',
            yes_task="if_records_present",
            no_task="send_userimport_completed_success_email",
        )

        if_records_present = rail.IfOperator(
            task_id="if_records_present",
            test="{{result('query_usersync_csv_records_line13', 'length') > 0}}",
            yes_task="process_each_user_dummy",
            no_task="write_supervisor_assignment_log_file"
        )

        process_each_user_dummy = rail.EmptyOperator(
            task_id='process_each_user_dummy',
        )

        query_distinct_namagername_from_input_records = rail.QueryCollectionOperator(
            task_id="query_distinct_namagername_from_input_records",
            query="""SELECT DISTINCT ManagerName FROM sourceuserdata """,
            name="managers_in_input_records"
        )

        def get_distinct_managername_list():
            records = rail.load_all_records(
                rail.result('query_distinct_namagername_from_input_records'))
            return {item['ManagerName']: "Yes" for item in records} if records else None

        load_distinct_managers_list = rail.PythonOperator(
            task_id='load_distinct_managers_list',
            python_callable=get_distinct_managername_list,
            show_return_value_in_logs=False
        )

        create_substitute_user_log = rail.CreateLogOperator(
            task_id="create_substitute_user_log"
        )

        process_each_user = rail.trigger_parallel_dagrun(
            task_id='process_each_user',
            items=lambda: rail.result('query_usersync_csv_records_line13') if (
                config.can_use_reference_file).lower() == 'n' else rail.result('query_for_changed_records'),
            trigger_dag_id=f'strayeruniversity_usersync_proecss_each_user_child_v3_{config.instance}',
            conf=request_payload.process_each_user_payload,
            parallel_count=config.process_each_user_parallel_dagruns_count,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_process_each_user_dag_ids = rail.PythonOperator(
            task_id='get_process_each_user_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_each_user_{x+1}') if rail.result(
                    f'process_each_user_{x+1}') else []), range(config.process_each_user_parallel_dagruns_count))))),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_each_user_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        write_supervisor_assignment_log_file = rail.WriteCSVFileOperator(
            task_id="write_supervisor_assignment_log_file",
            source=lambda: rail.result('supervisor_assignment_log'),
            header=['username', 'managername', 'useruri', 'empid'],
            row=lambda item: [
                item['properties']['employee_maanger_id'].split('|')[0],
                item['properties']['employee_maanger_id'].split('-')[-1],
                item['properties']['useruri'],
                item['properties']['employee_maanger_id'].split(
                    '-')[0].split('|')[-1]
            ]
        )

        check_supervisor_mapper_csv_has_data = rail.IfOperator(
            task_id="check_supervisor_mapper_csv_has_data",
            test=lambda: len(rail.load_all_records(rail.result(
                'write_supervisor_assignment_log_file'))) > 0,
            yes_task="process_each_mapper_data",
            no_task="archive_reference_file"
        )

        process_each_mapper_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_mapper_data',
            items="{{ result('write_supervisor_assignment_log_file')}}",
            trigger_dag_id=f'strayeruniversity_usersync_update_supervisor_child_v3_{config.instance}',
            conf=request_payload.process_supervisor_mapper_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_mapper_data_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_mapper_data_process',
            dag_runs='{{ result("process_each_mapper_data") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        send_userimport_completed_success_email = rail.EmailOperator(
            task_id='send_userimport_completed_success_email',
            to=config.tenant_email,
            bcc=config.bcc_email,
            subject=f'{config.company_key} | User import completed successfully' +
            now(config.time_zone).strftime("%m/%d/%YT%H:%M:%S"),
            html_content="templates/emails/send_no_data_to_import.html"
        )

        list_input_files_for_no_changed_records = rail.SFTPListFilesOperator(
            task_id='list_input_files_for_no_changed_records',
            paths=[config.input_filepath]
        )

        get_inp_filepath_name_for_no_changed_records = rail.PythonOperator(
            task_id="get_inp_filepath_name_for_no_changed_records",
            python_callable=lambda: get_inp_file_name_no_changed_records(config.input_filepath)
        )

        archive_input_file_no_changed_records = rail.SFTPMoveFileOperator(
            task_id='archive_input_file_no_changed_records',
            existing_filename='{{ result("get_inp_filepath_name_for_no_changed_records") }}',
            new_filename=config.archive_filepath +
            "/{{ result('get_inp_filepath_name_for_no_changed_records') | file_name }}"
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            existing_filename='{{ result("get_ref_filepath_name") }}',
            new_filename=config.archive_filepath +
            "/{{ result('get_ref_filepath_name') | file_name }}"
        )

        list_input_files = rail.SFTPListFilesOperator(
            task_id='list_input_files',
            paths=[config.input_filepath]
        )

        get_inp_filepath_name = rail.PythonOperator(
            task_id="get_inp_filepath_name",
            python_callable=lambda: get_inp_file_name(config.input_filepath)
        )

        is_inp_csv = rail.IfOperator(
            task_id='is_inp_csv',
            test='{{ result("get_inp_filepath_name") | file_ext | lower == "csv" }}',
            yes_task='set_inputfile_as_new_reference',
            no_task="load_master_log",
        )

        set_inputfile_as_new_reference = rail.SFTPMoveFileOperator(
            task_id='set_inputfile_as_new_reference',
            existing_filename='{{ result("get_inp_filepath_name") }}',
            new_filename=config.reference_filepath +
            "/{{ result('get_inp_filepath_name') | file_name }}"
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

        write_userimportlog_file = rail.WriteCSVFileOperator(
            task_id="write_userimportlog_file",
            source=lambda: rail.result('format_logs')['final_logs'],
            header=['username', 'action', 'status', 'details', 'ecid'],
            row=[
                '{{ item.username }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details}}',
                '{{ item.ecid }}']
        )

        check_userimportlog_csv_has_data = rail.IfOperator(
            task_id="check_userimportlog_csv_has_data",
            test=lambda: len(rail.load_all_records(
                rail.result('write_userimportlog_file'))) > 0,
            yes_task="get_log_file_name",
            no_task="finish"
        )

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=lambda: rail.render_template(
                "/logs_userdata{{ result('get_current_time') }}.csv")
        )

        upload_logs_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_logs_to_sftp',
            sftp_conn_id= config.sftp_conn_id_internal,
            content="{{ result('write_userimportlog_file') }}",
            remote_filepath=config.log_filepath +
            "{{ result('get_log_file_name') }}"
        )

        check_if_upload_success = rail.IfOperator(
            task_id='check_if_upload_success',
            test="{{ get_task_state('upload_logs_to_sftp') == 'success' }}",
            yes_task='generate_downloadlink',
            no_task='send_error_in_upload_mail'
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('write_userimportlog_file')}}",
            output_file_name="{{ result('get_log_file_name') }}",
            expires_in_seconds=7*24*60*60,
        )

        send_error_in_upload_mail = rail.EmailOperator(
            task_id='send_error_in_upload_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject=f'{config.company_key} |User Import - Failed while uploading logs to SFTP' +
            now(config.time_zone).strftime("%m/%d/%YT%H:%M:%S"),
            html_content="templates/emails/send_error_in_upload.html",
            params={
                'username': config.user_name,
                'company_key': config.company_key,
                'today': now(config.time_zone).strftime("%m/%d/%YT%H:%M:%S")
            },
            files=[
                ("{{ result('get_log_file_name') }}", "{{ result('write_userimportlog_file') }}")]
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
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
                {%- endif -%}'
                + now(config.time_zone).strftime("%m/%d/%YT%H:%M:%S"),
            html_content="templates/emails/import_complete_mail.html",
            params={
                'today': now(config.time_zone).strftime("%m/%d/%YT%H:%M:%S")
                    }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule='all_done',
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        if_instance_trial >> rail.Label('Yes') >> new_file_sensor_to_process
        if_instance_trial >> rail.Label('No') >> get_workdayreport_http_payload

        get_workdayreport_http_payload >> workdayreport_json_load >> if_first_employee_id_blank_1_5

        if_first_employee_id_blank_1_5 >> rail.Label(
            'Yes') >> send_mail_no_record_in_report
        if_first_employee_id_blank_1_5 >> rail.Label(
            'No') >> get_write_csv_task_source

        new_file_sensor_to_process >> was_new_file_found

        was_new_file_found >> rail.Label(
            'Yes') >> download_sftp_file
        was_new_file_found >> rail.Label(
            'No') >> if_instance_is_trial >> rail.Label('Yes') >> delete_dagrun

        download_sftp_file >> archive_input_processing_file
        download_sftp_file >> parse_user_sync_csv >> get_write_csv_task_source

        get_write_csv_task_source >> write_user_sync_csv >> get_current_time >> get_all_reports >> user_import_log >> supervisor_assignment_log >> upload_inputfile_to_sftp >> \
            create_collection_from_csv >> query_usersync_csv_records_line13 >> get_required_user_customfields >> query_distinct_managementlevel >> \
            if_distinct_managementlevel_present

        if_distinct_managementlevel_present >> rail.Label(
            'Yes') >> compose_managementlevel_data_csv >> process_each_managementlevel >> get_user_ref_data_from_replicon
        if_distinct_managementlevel_present >> rail.Label(
            'No') >> get_user_ref_data_from_replicon

        get_user_ref_data_from_replicon >> parse_referencecsv_data_from_replicon >> list_reference_files >> get_ref_filepath_name >> is_use_reference_file_allowed

        is_use_reference_file_allowed >> rail.Label('Yes') >> is_csv
        is_use_reference_file_allowed >> rail.Label(
            'No') >> is_use_reference_file_not_allowed

        is_csv >> rail.Label('Yes') >> download_reference_file >> load_reference_csv >> create_ref_collection_from_csv >> \
            query_for_changed_records >> query_for_unchanged_records >> if_changedrecords_present
        is_csv >> rail.Label('No') >> is_use_reference_file_not_allowed

        if_changedrecords_present >> rail.Label(
            'Yes') >> process_each_user_dummy
        if_changedrecords_present >> rail.Label(
            'No') >> send_userimport_completed_success_email

        is_use_reference_file_not_allowed >> rail.Label(
            'Yes') >> if_first_firstname_present_in_records
        is_use_reference_file_not_allowed >> rail.Label(
            'No') >> write_supervisor_assignment_log_file

        if_first_firstname_present_in_records >> rail.Label(
            'Yes') >> if_records_present
        if_first_firstname_present_in_records >> rail.Label(
            'No') >> send_userimport_completed_success_email

        if_records_present >> rail.Label(
            'Yes') >> process_each_user_dummy
        if_records_present >> rail.Label(
            'No') >> write_supervisor_assignment_log_file

        process_each_user_dummy >> query_distinct_namagername_from_input_records >> load_distinct_managers_list >> create_substitute_user_log \
            >> process_each_user >> get_process_each_user_dag_ids >> gather_user_logs >> write_supervisor_assignment_log_file

        write_supervisor_assignment_log_file >> check_supervisor_mapper_csv_has_data

        check_supervisor_mapper_csv_has_data >> rail.Label(
            'Yes') >> process_each_mapper_data >> wait_for_mapper_data_process >> archive_reference_file
        check_supervisor_mapper_csv_has_data >> rail.Label(
            'No') >> archive_reference_file

        archive_reference_file >> list_input_files >> get_inp_filepath_name >> is_inp_csv

        is_inp_csv >> rail.Label(
            'Yes') >> set_inputfile_as_new_reference >> load_master_log
        is_inp_csv >> rail.Label('No') >> load_master_log

        load_master_log >> format_logs >> write_userimportlog_file

        write_userimportlog_file >> check_userimportlog_csv_has_data

        check_userimportlog_csv_has_data >> rail.Label(
            'Yes') >> get_log_file_name >> upload_logs_to_sftp >> check_if_upload_success
        check_userimportlog_csv_has_data >> rail.Label('No') >> finish

        check_if_upload_success >> rail.Label(
            'Yes') >> generate_downloadlink >> send_import_complete_email >> finish
        check_if_upload_success >> rail.Label(
            'No') >> send_error_in_upload_mail >> finish

        send_userimport_completed_success_email >> list_input_files_for_no_changed_records >> get_inp_filepath_name_for_no_changed_records \
            >> archive_input_file_no_changed_records >> finish

        finish >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_dag)
