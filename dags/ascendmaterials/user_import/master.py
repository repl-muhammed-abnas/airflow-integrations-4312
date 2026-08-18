import hashlib
from datetime import timedelta, datetime
from airflow.models import Variable
from os import path
import rail
from ascendmaterials.user_import.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Ascend User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        # After sensor (all_done covers both success and soft-fail)
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='can_run_batch_task',
            no_task='delete_dagrun',
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_dagrun',
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_timestamp'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_timestamp',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # ── Timestamps ──────────────────────────────────────────────────
        log_timestamp = rail.PythonOperator(
            task_id='log_timestamp',
            python_callable=lambda: datetime.now().strftime('%Y%m%dT%H%M%S')
        )

        log_today_yyyymmdd = rail.PythonOperator(
            task_id='log_today_yyyymmdd',
            python_callable=lambda: datetime.now().strftime('%Y%m%d')
        )

        log_today_mmddyyyy = rail.PythonOperator(
            task_id='log_today_mmddyyyy',
            python_callable=lambda: datetime.now().strftime('%m/%d/%Y')
        )

        # ── Status variable & logs ───────────────────────────────────────
        declare_variable = rail.SetVariableOperator(
            task_id='declare_variable',
            append=False,
            name='file_processing_status',
            value=None
        )

        create_import_log = rail.CreateLogOperator(
            task_id='create_import_log'
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id='create_supervisor_log',
        )

        # ── File format validation ───────────────────────────────────────
        if_filename_not_ends_with_csv = rail.IfOperator(
            task_id='if_filename_not_ends_with_csv',
            test="{{ result('new_file_sensor') | file_name | ends_with('csv') | is_falsy }}",
            yes_task='email_incorrect_fileformat',
            no_task='if_filename_not_in_valid_name_format',
        )

        email_incorrect_fileformat = rail.EmailOperator(
            task_id='email_incorrect_fileformat',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject=f'''{config.company_key} | User import - file processing is skipped - {{{{ current_time("%Y-%m-%dT%H:%M:%S") }}}} ''',
            html_content='''<p><strong><em>This is an automated mail, please don't reply</em></strong></p>
                            <p>Hello,</p>
                            <p>The file "{{ result('new_file_sensor') | file_name }}" is not processed since the file name does not have the allowed file format (.csv).</p>
                            <p>If you have questions, please write to https://support.deltek.com</p>
                            <p>Thanks, <br />Deltek Inc.</p>''',
            params=None,
        )

        archive_skipped_format = rail.SFTPMoveFileOperator(
            task_id='archive_skipped_format',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Skipped_{{ result('log_timestamp') }}_{{ result('new_file_sensor') | file_name }}",
        )

        set_status_bad_format = rail.SetVariableOperator(
            task_id='set_status_bad_format',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='File processing skipped due to incorrect file format'
        )

        if_filename_not_in_valid_name_format = rail.IfOperator(
            task_id='if_filename_not_in_valid_name_format',
            test="{{ result('new_file_sensor') | file_name != 'ASCEND_User_' + result('log_today_yyyymmdd') + '.csv' }}",
            yes_task='email_incorrect_filenameformat',
            no_task='get_department_report_details',
        )

        email_incorrect_filenameformat = rail.EmailOperator(
            task_id='email_incorrect_filenameformat',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject=f'''{config.company_key} | User import - file processing is skipped - {{{{ current_time("%Y-%m-%dT%H:%M:%S") }}}} ''',
            html_content='''<p><strong><em>This is an automated mail, please don't reply</em></strong></p>
                            <p>Hello,</p>
                            <p>The file "{{ result('new_file_sensor') | file_name }}" is not processed since the file name does not have the allowed name format (ASCEND_User_{{ result('log_today_yyyymmdd') }}.csv).</p>
                            <p>If you have questions, please write to https://support.deltek.com</p>
                            <p>Thanks, <br />Deltek Inc.</p>''',
            params=None,
        )

        archive_skipped_name = rail.SFTPMoveFileOperator(
            task_id='archive_skipped_name',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Skipped_{{ result('log_timestamp') }}_{{ result('new_file_sensor') | file_name }}",
        )

        set_status_bad_name = rail.SetVariableOperator(
            task_id='set_status_bad_name',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='File processing skipped due to incorrect file name format'
        )

        # ── Department report ────────────────────────────────────────────
        get_department_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_department_report_details',
            report_name=config.department_list_report,
        )

        if_department_reporturi_blank = rail.IfOperator(
            task_id='if_department_reporturi_blank',
            test='''{{ result('get_department_report_details').uri | is_falsy }}''',
            yes_task='stop_dept_report',
            no_task='trigger_dept_costcenter',
        )

        stop_dept_report = rail.FailOperator(
            task_id='stop_dept_report',
            message='Department list report is not available in Replicon'
        )

        trigger_dept_costcenter = rail.TriggerDagRunOperator(
            task_id='trigger_dept_costcenter',
            retries=0,
            trigger_dag_id=config.dept_costcenter_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "filepath": "{{ result('new_file_sensor') }}",
                "departmentreporturi": "{{ result('get_department_report_details').uri }}",
                "ascend_user_import_logs_lookuptable": "{{ result('create_import_log') }}"
            }
        )

        wait_dept_costcenter = rail.WaitForDagRunsSensor(
            task_id='wait_dept_costcenter',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dept_costcenter") }}'
        )

        generate_report = rail.RepliconServiceOperator(
            task_id='generate_report',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('get_department_report_details').uri }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_report_error_present = rail.IfOperator(
            task_id='if_report_error_present',
            test='''{{ result('generate_report').error | is_truthy }}''',
            yes_task='stop_report_error',
            no_task='parse_csv_report',
        )

        stop_report_error = rail.FailOperator(
            task_id='stop_report_error',
            message="{{ result('generate_report').error }}"
        )

        parse_csv_report = rail.LoadCSVFileOperator(
            task_id='parse_csv_report',
            document="{{ result('generate_report').payload }}",
            delimiter=',',
            headers=['Department Name', 'Parent Department Name',
                     'Department Full Name', 'Department URI']
        )

        # Replicon report uses ' / ' (space-slash-space); feed is normalized to '/'.
        # Strip the spaces so Department Full Name matches the normalized department path.
        normalize_dept_report = rail.PythonOperator(
            task_id='normalize_dept_report',
            python_callable=lambda: [
                {**row, 'Department Full Name': row['Department Full Name'].replace(' / ', '/')}
                for row in rail.load_all_records(rail.result('parse_csv_report'))
            ]
        )

        # ── Input file ───────────────────────────────────────────────────
        download_input_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        parse_input_csv_file = rail.LoadCSVFileOperator(
            task_id='parse_input_csv_file',
            document="{{ result('download_input_csv_file') }}",
            delimiter=',',
            headers=['loginname', 'employeefirstname', 'employeelastname', 'employeetype', 'timetype',
                     'department', 'authenticationtype', 'enabled', 'employeeid', 'startdate', 'lastdayofwork',
                     'continuousservicedate', 'emailaddress', 'manager', 'location', 'homecountry', 'homestateprovince',
                     'homecity', 'hourlypayrollrate', 'hourlypayrollcurrency', 'costcenter', 'udf']
        )

        if_no_records = rail.IfOperator(
            task_id='if_no_records',
            test="{{ result('parse_input_csv_file') | length < 1 }}",
            yes_task='email_no_records',
            no_task='write_csv_with_encoded',
        )

        email_no_records = rail.EmailOperator(
            task_id='email_no_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject=f'''{config.company_key} | User import - no records in file {{{{ result('log_today_mmddyyyy') }}}} ''',
            html_content='''<p><strong><em>This is an automated mail, please don't reply</em></strong></p>
                            <p>Hello,</p>
                            <p>The User import is completed on {{ result('log_today_mmddyyyy') }}. There were no records in the file - {{ result('new_file_sensor') | file_name }} to be processed.</p>
                            <p>For any queries, please contact our support team at https://support.deltek.com</p>
                            <p>Thanks, <br />Deltek Inc.</p>''',
        )

        set_status_no_records = rail.SetVariableOperator(
            task_id='set_status_no_records',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='File processing skipped due to no records to process'
        )

        archive_input_file_no_records = rail.SFTPMoveFileOperator(
            task_id='archive_input_file_no_records',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Processed_{{ result('log_timestamp') }}_{{ result('new_file_sensor') | file_name }}",
        )

        # ── Build encoded CSV for change detection ───────────────────────
        write_csv_with_encoded = rail.WriteCSVFileOperator(
            task_id='write_csv_with_encoded',
            source="{{ result('parse_input_csv_file') }}",
            header=['loginname', 'employeefirstname', 'employeelastname', 'employeetype', 'timetype', 'department',
                    'authenticationtype', 'enabled', 'employeeid', 'startdate', 'lastdayofwork', 'continuousservicedate',
                    'emailaddress', 'manager', 'location', 'homecountry', 'homestateprovince', 'homecity', 'hourlypayrollrate',
                    'hourlypayrollcurrency', 'costcenter', 'udf', 'encoded'],
            row=lambda item: [
                item['loginname'].strip() if item['loginname'] else '',
                item['employeefirstname'].strip() if item['employeefirstname'] else '',
                item['employeelastname'].strip() if item['employeelastname'] else '',
                item['employeetype'].strip() if item['employeetype'] else '',
                item['timetype'].strip() if item['timetype'] else '',
                item['department'].strip() if item['department'] else '',
                item['authenticationtype'].strip() if item['authenticationtype'] else '',
                item['enabled'].strip() if item['enabled'] else '',
                item['employeeid'].strip() if item['employeeid'] else '',
                item['startdate'],
                item['lastdayofwork'],
                item['continuousservicedate'],
                item['emailaddress'].strip() if item['emailaddress'] else '',
                item['manager'].strip() if item['manager'] else '',
                item['location'].strip() if item['location'] else '',
                item['homecountry'].strip() if item['homecountry'] else '',
                item['homestateprovince'].strip() if item['homestateprovince'] else '',
                item['homecity'].strip() if item['homecity'] else '',
                item['hourlypayrollrate'].strip() if item['hourlypayrollrate'] else '',
                item['hourlypayrollcurrency'].strip() if item['hourlypayrollcurrency'] else '',
                item['costcenter'].strip() if item['costcenter'] else '',
                item['udf'].strip() if item['udf'] else '',
                hashlib.md5((str(
                    str(item['loginname']) + str(item['employeefirstname']) + str(item['employeelastname']) +
                    str(item['employeetype']) + str(item['timetype']) + str(item['department']) +
                    str(item['authenticationtype']) + str(item['enabled']) + str(item['employeeid']) +
                    str(item['startdate']) + str(item['lastdayofwork']) + str(item['continuousservicedate']) +
                    str(item['emailaddress']) + str(item['manager']) + str(item['location']) +
                    str(item['homecountry']) + str(item['homestateprovince']) + str(item['homecity']) +
                    str(item['hourlypayrollrate']) + str(item['hourlypayrollcurrency']) +
                    str(item['costcenter']) + str(item['udf'])
                )).encode('utf-8')).hexdigest()
            ]
        )

        create_collection_from_csv_with_encoded = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv_with_encoded',
            source="{{ result('write_csv_with_encoded') }}",
            name='inputfilerawdata',
        )

        # ── Reference file ───────────────────────────────────────────────
        dir_list_reference_files = rail.SFTPListFilesOperator(
            task_id='dir_list_reference_files',
            paths=[config.reference_filepath]
        )

        if_no_reference_file = rail.IfOperator(
            task_id='if_no_reference_file',
            test=lambda: not bool(
                rail.result('dir_list_reference_files')[config.reference_filepath][0]['name']
                if rail.result('dir_list_reference_files') and
                rail.result('dir_list_reference_files')[config.reference_filepath][0] else null
            ),
            yes_task='stop_no_reference',
            no_task='get_reference_filename',
        )

        stop_no_reference = rail.FailOperator(
            task_id='stop_no_reference',
            message='Reference File not present'
        )

        get_reference_filename = rail.PythonOperator(
            task_id='get_reference_filename',
            python_callable=lambda: rail.result('dir_list_reference_files')[config.reference_filepath][0]['name']
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_filepath + "/{{ result('get_reference_filename') }}",
        )

        load_csv_reference_file = rail.LoadCSVFileOperator(
            task_id='load_csv_reference_file',
            document="{{ result('download_reference_file') }}",
            headers=['loginname', 'employeefirstname', 'employeelastname', 'employeetype', 'timetype',
                     'department', 'authenticationtype', 'enabled', 'employeeid', 'startdate', 'lastdayofwork',
                     'continuousservicedate', 'emailaddress', 'manager', 'location', 'homecountry', 'homestateprovince',
                     'homecity', 'hourlypayrollrate', 'hourlypayrollcurrency', 'costcenter', 'udf', 'encoded']
        )

        create_collection_data_from_reference_file = rail.CreateCollectionOperator(
            task_id='create_collection_data_from_reference_file',
            source="{{ result('load_csv_reference_file') }}",
            name='referencefiledata',
        )

        # ── Compare changed/unchanged profiles ───────────────────────────
        query_un_changed_profiles = rail.QueryCollectionOperator(
            task_id='query_un_changed_profiles',
            query="SELECT * FROM inputfilerawdata WHERE inputfilerawdata.encoded IN (SELECT DISTINCT referencefiledata.encoded FROM referencefiledata)",
        )

        log_unchanged_batch = rail.WriteLogOperator(
            task_id='log_unchanged_batch',
            log="{{ result('create_import_log') }}",
            items="{{ result('query_un_changed_profiles') }}",
            message="na",
            properties={
                "jobid": "{{ dag_run_ecid() }}",
                "userloginname": "{{item.loginname}}",
                "action": "NA",
                "status": "Skipped",
                "details": "No change in user records",
                "username": "{{item.employeefirstname}} {{item.employeelastname}}",
            }
        )

        query_new_changed_profiles = rail.QueryCollectionOperator(
            task_id='query_new_changed_profiles',
            query="SELECT * FROM inputfilerawdata WHERE inputfilerawdata.encoded NOT IN (SELECT DISTINCT referencefiledata.encoded FROM referencefiledata)",
        )

        # ── Trigger one child DAG per changed profile (parallel) ─────────
        trigger_user_processor = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_processor',
            retries=0,
            items="{{ result('query_new_changed_profiles') }}",
            trigger_dag_id=config.user_processor_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "loginname": item.get('loginname'),
                "employeefirstname": item.get('employeefirstname'),
                "employeelastname": item.get('employeelastname'),
                "employeetype": item.get('employeetype'),
                "timetype": item.get('timetype'),
                "department": item.get('department'),
                "authenticationtype": item.get('authenticationtype'),
                "enabled": item.get('enabled'),
                "employeeid": item.get('employeeid'),
                "startdate": item.get('startdate'),
                "terminationdate": item.get('lastdayofwork'),
                "continuousservicedate": item.get('continuousservicedate'),
                "emailaddress": item.get('emailaddress'),
                "manager": item.get('manager'),
                "location": item.get('location'),
                "homecountry": item.get('homecountry'),
                "homestateprovince": item.get('homestateprovince'),
                "homecity": item.get('homecity'),
                "hourlypayrollrate": item.get('hourlypayrollrate'),
                "hourlypayrollcurrency": item.get('hourlypayrollcurrency'),
                "costcenter": item.get('costcenter'),
                "udf": item.get('udf'),
                "departmenturi": rail.find_first_by_attr_and_get_attr(
                    rail.result('normalize_dept_report'), 'Department Full Name',
                    item.get('department', '').replace(' | ', '/'),
                    'Department URI', None),
                "ascend_user_import_logs_lookuptable": rail.result('create_import_log'),
                "ascend_supervisor_assignments_logs_lookuptable": rail.result('create_supervisor_log'),
                "parentjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_user_processor = rail.WaitForDagRunsSensor(
            task_id='wait_user_processor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_user_processor") }}'
        )

        # ── Supervisor assignment ────────────────────────────────────────
        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_supervisor_entries():
            return [
                {
                    "userloginname": info['properties'].get('userloginname'),
                    "useruri": info['properties'].get('useruri'),
                    "supervisorloginname": info['properties'].get('supervisorloginname'),
                    "action": info['properties'].get('action'),
                }
                for info in get_data_from_document(rail.result('create_supervisor_log'))
                if info['properties']
            ]

        get_supervisor_entries_task = rail.PythonOperator(
            task_id='get_supervisor_entries',
            python_callable=get_supervisor_entries
        )

        if_supervisors_present = rail.IfOperator(
            task_id='if_supervisors_present',
            test="{{ result('get_supervisor_entries') | length > 0 }}",
            yes_task='trigger_supervisor',
            no_task='format_logs',
        )

        trigger_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_supervisor',
            retries=0,
            items="{{ result('get_supervisor_entries') | to_json }}",
            trigger_dag_id=config.supervisor_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "loginname": item.get('userloginname'),
                "supervisorloginname": item.get('supervisorloginname'),
                "useruri": item.get('useruri'),
                "action": item.get('action'),
                "ascend_user_import_logs_lookuptable": rail.result('create_import_log')
            }
        )

        wait_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_supervisor") }}'
        )

        # ── Log generation & customer email ─────────────────────────────
        # Converges from both supervisor and no-supervisor paths
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=python_callable.format_logs_callable
        )

        create_log_csv = rail.WriteCSVFileOperator(
            task_id='create_log_csv',
            source="{{ result('format_logs') }}",
            header=['User Name', 'Login Name', 'Action', 'Status', 'Details', 'JobID'],
            row=lambda item: [
                item['properties'].get('username', ''),
                item['properties'].get('userloginname', ''),
                item['properties'].get('action', ''),
                item['properties'].get('status', ''),
                item['properties'].get('details', ''),
                item.get('ecid', ''),
            ]
        )

        get_logfile_name = rail.PythonOperator(
            task_id='get_logfile_name',
            python_callable=lambda: f"Logs_{datetime.now().strftime('%H%M%S')}_{path.split(rail.result('new_file_sensor'))[1]}"
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('create_log_csv') }}",
            remote_filepath=config.log_filepath + "/{{ result('get_logfile_name') }}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " | User import - " }}\
                {%- if result("format_logs", key="error_record_count") > 0 -%}\
                    completed with errors\
                {%- elif result("format_logs", key="exception_record_count") > 0 -%}\
                    completed with exceptions\
                {%- else -%}\
                    {%- if result("format_logs", key="skipped_record_count") > 0 and result("format_logs", key="success_record_count") == 0 -%}\
                        completed with skipped records\
                    {%- else -%}\
                        completed successfully\
                    {%- endif -%}\
                {%- endif -%}\
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S") }}',
            html_content="templates/email_import_complete.html",
            params={"log_filepath": config.log_filepath}
        )

        # ── Cleanup ──────────────────────────────────────────────────────
        rename_move_old_reference = rail.SFTPMoveFileOperator(
            task_id='rename_move_old_reference',
            existing_filename=config.reference_filepath + "/{{ result('get_reference_filename') }}",
            new_filename=config.archive_filepath +
            "/Old_reference_{{ result('log_timestamp') }}_{{ result('get_reference_filename') }}",
        )

        upload_new_reference = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference',
            content="{{ result('write_csv_with_encoded') }}",
            remote_filepath=config.reference_filepath +
            "/{{ result('log_timestamp') }}_{{ result('new_file_sensor') | file_name }}",
        )

        set_status_success = rail.SetVariableOperator(
            task_id='set_status_success',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='File successfully processed'
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Processed_{{ result('log_timestamp') }}_{{ result('new_file_sensor') | file_name }}",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_timestamp >> log_today_yyyymmdd >> log_today_mmddyyyy \
            >> declare_variable >> create_import_log >> create_supervisor_log >> if_filename_not_ends_with_csv

        if_filename_not_ends_with_csv >> rail.Label('Yes') >> email_incorrect_fileformat \
            >> archive_skipped_format >> set_status_bad_format >> finish
        if_filename_not_ends_with_csv >> rail.Label('No') >> if_filename_not_in_valid_name_format
        if_filename_not_in_valid_name_format >> rail.Label('Yes') >> email_incorrect_filenameformat \
            >> archive_skipped_name >> set_status_bad_name >> finish
        if_filename_not_in_valid_name_format >> rail.Label('No') >> get_department_report_details \
            >> if_department_reporturi_blank
        if_department_reporturi_blank >> rail.Label('Yes') >> stop_dept_report >> finish
        if_department_reporturi_blank >> rail.Label('No') >> trigger_dept_costcenter \
            >> wait_dept_costcenter >> generate_report >> if_report_error_present
        if_report_error_present >> rail.Label('Yes') >> stop_report_error >> finish
        if_report_error_present >> rail.Label('No') >> parse_csv_report \
            >> normalize_dept_report >> download_input_csv_file >> parse_input_csv_file >> if_no_records
        if_no_records >> rail.Label('Yes') >> email_no_records \
            >> set_status_no_records >> archive_input_file_no_records >> finish
        if_no_records >> rail.Label('No') >> write_csv_with_encoded \
            >> create_collection_from_csv_with_encoded >> dir_list_reference_files >> if_no_reference_file
        if_no_reference_file >> rail.Label('Yes') >> stop_no_reference >> finish
        if_no_reference_file >> rail.Label('No') >> get_reference_filename >> download_reference_file \
            >> load_csv_reference_file >> create_collection_data_from_reference_file \
            >> query_un_changed_profiles >> log_unchanged_batch >> query_new_changed_profiles \
            >> trigger_user_processor >> wait_user_processor \
            >> get_supervisor_entries_task >> if_supervisors_present
        if_supervisors_present >> rail.Label('Yes') >> trigger_supervisor >> wait_supervisor >> format_logs
        if_supervisors_present >> rail.Label('No') >> format_logs
        format_logs >> create_log_csv >> get_logfile_name >> upload_log_to_sftp >> send_import_complete_email \
            >> rename_move_old_reference >> upload_new_reference >> set_status_success \
            >> archive_input_file >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
