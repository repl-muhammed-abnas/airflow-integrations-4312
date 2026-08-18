from datetime import timedelta, datetime
import json
import rail
from ttecholdingsinc.schedule_creation.utils import custom_methods,request_payload

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'ttec_schedule_creation_master_{config.instance}',
        description=f'TTEC Schedule Creation Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon Shift Schedule Import - File processing is skipped on {{ current_time_in_specified_tz() }}",
            html_content='templates/bad_file_format.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task= 'archive_file',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath + '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        load_user_data = rail.LoadCSVFileOperator(
            task_id='load_user_data',
            document="{{ result('download_file') }}",
            delimiter=','
        )

        create_rawdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawdata_collection',
            source=lambda: rail.result('load_user_data'),
            name='rawdata',
            columns={
                'seg_code': 'schedulename',
                'Paycode': 'schedulecode',
                'seg_desc': 'description',
                'emp_id': 'empid',
                'std_date': 'startdate',
                'start_time': 'starttime',
                'end_time': 'endtime',
                'break1': 'break1',
                'break1_start_time': 'break1starttime',
                'break1_duration': 'break1duration',
                'break2': 'break2',
                'break2_start_time': 'break2starttime',
                'break2_duration': 'break2duration',
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_rawdata_collection', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Shift Schedule Import  - No records to process - {{ current_time_in_specified_tz() }}',
            html_content='templates/blank_payload.html'
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM rawdata WHERE NULLIF(schedulename,'') IS NULL OR NULLIF(empid,'') IS NULL OR
                NULLIF(startdate,'') IS NULL OR NULLIF(starttime,'') IS NULL OR NULLIF(endtime,'') IS NULL"""
        )

        has_any_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_blank_mandatory_field',
            test="{{ result('query_any_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_blankmandatory_field_log',
            no_task='query_valid_data_from_rawdata'
        )

        write_blankmandatory_field_log = rail.WriteLogOperator(
            task_id="write_blankmandatory_field_log",
            items="{{result('query_any_blankmandatory_check')}}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=custom_methods.get_invalid_logs_property_conf
        )

        query_valid_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_data_from_rawdata',
            name='inputdatacollection',
            query="""SELECT * FROM rawdata
                    WHERE NULLIF(schedulename,'') IS NOT NULL AND NULLIF(empid,'') IS NOT NULL AND NULLIF(startdate,'') IS NOT NULL AND
                    NULLIF(starttime,'') IS NOT NULL AND NULLIF(endtime,'') IS NOT NULL"""
        )

        query_distinct_shifts = rail.QueryCollectionOperator(
            task_id='query_distinct_shifts',
            name='schedulecreation',
            query="""SELECT DISTINCT schedulename FROM inputdatacollection WHERE NULLIF(schedulename,'') IS NOT NULL AND schedulename!= 'PTO' """
        )

        has_schedules_to_create = rail.IfOperator(
            task_id='has_schedules_to_create',
            test="{{ result('query_distinct_shifts', 'length') > 0 }}",
            yes_task='process_create_schedules',
            no_task='get_all_shift_schedules'
        )

        process_create_schedules = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_create_schedules',
            items= '{{ result("query_distinct_shifts") }}',
            retries = 0,
            trigger_dag_id= config.create_schedule_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'schedulename': '{{ item.schedulename }}'
            }
        )

        wait_for_process_create_schedules = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_create_schedules',
            dag_runs= '{{ result("process_create_schedules") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_shift_schedules = rail.RepliconServiceOperator(
            task_id = 'get_all_shift_schedules',
            endpoint= '/services/ShiftListService1.svc/GetData',
            data= request_payload.get_all_shifts_payload,
            data_handler= custom_methods.get_replicon_shift_data
        )

        get_report_result = rail.RepliconReportDetailsOperator(
            task_id='get_report_result',
            report_name=config.user_report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{ result('get_report_result').uri }}"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_has_data',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data') }}",
            yes_task='load_users_report_data',
            no_task= 'send_no_active_users_email'
        )

        load_users_report_data = rail.LoadCSVFileOperator(
            task_id='load_users_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id='users_report_data_collection',
            source="{{ result('load_users_report_data') }}",
            name='usersdetails'
        )

        create_csv_lines_for_raw_data = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_raw_data',
            source="{{ result('query_valid_data_from_rawdata') }}",
            header=['schedulename','schedulecode','description','empid','startdate','starttime','endtime','useruri',
                'userstatus','schedule','user_startdate','user_enddate'],
            row=custom_methods.get_required_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        final_data_collection = rail.CreateCollectionOperator(
            task_id='final_data_collection',
            source="{{ result('create_csv_lines_for_raw_data') }}",
            name='finaldata'
        )

        query_invalid_users = rail.QueryCollectionOperator(
            task_id = 'query_invalid_users',
            query= '''SELECT * FROM finaldata WHERE NULLIF(useruri,'') IS NULL or useruri == '' '''
        )

        has_query_invalid_users = rail.IfOperator(
            task_id='has_query_invalid_users',
            test="{{ result('query_invalid_users', 'length') > 0 }}",
            yes_task='write_blank_user_field_log',
            no_task='query_non_shift_users'
        )

        write_blank_user_field_log = rail.WriteLogOperator(
            task_id="write_blank_user_field_log",
            items="{{result('query_invalid_users')}}",
            severity="Skipped",
            message="User uri not present, user not found/disabled in replicon",
            properties=custom_methods.get_invalid_users_conf
        )

        query_non_shift_users = rail.QueryCollectionOperator(
            task_id = 'query_non_shift_users',
            query= '''SELECT DISTINCT empid,useruri FROM finaldata WHERE NULLIF(useruri,'') IS NOT NULL AND schedule != "Shift Schedule" ''',
            name= 'nonshiftusers'
        )

        has_query_non_shift_users = rail.IfOperator(
            task_id='has_query_non_shift_users',
            test="{{ result('query_non_shift_users', 'length') > 0 }}",
            yes_task='assign_default_shift',
            no_task='query_unique_users_for_shift'
        )

        assign_default_shift = rail.RepliconServiceCallForEachItemOperator(
            task_id ='assign_default_shift',
            items=lambda: rail.load_all_records(rail.result("query_non_shift_users")),
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=request_payload.get_default_shift_payload
        )

        query_unique_users_for_shift = rail.QueryCollectionOperator(
            task_id = 'query_unique_users_for_shift',
            query= '''SELECT DISTINCT empid FROM finaldata WHERE NULLIF(useruri,'') IS NOT NULL AND description == "SHIFT" ''',
            name= 'unique_users'
        )

        process_shift_schedules_to_assign = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_shift_schedules_to_assign',
            items= '{{ result("query_unique_users_for_shift") }}',
            retries = 0,
            trigger_dag_id= config.shift_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'empid': '{{ item.empid }}'
            }
        )

        wait_for_process_shift_schedules_to_assign = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_shift_schedules_to_assign',
            dag_runs= '{{ result("process_shift_schedules_to_assign") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_unique_users_for_pto = rail.QueryCollectionOperator(
            task_id = 'query_unique_users_for_pto',
            query= '''SELECT DISTINCT empid FROM finaldata WHERE NULLIF(useruri,'') IS NOT NULL AND description == "PTO" ''',
            name= 'unique_users_for_pto'
        )

        process_pto_schedules_to_assign = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_pto_schedules_to_assign',
            items= '{{ result("query_unique_users_for_pto") }}',
            retries = 0,
            trigger_dag_id= config.pto_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'empid': item['empid'],
                'shift_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_all_shift_schedules"),'name',config.default_shift_name,'uri')
            }
        )

        wait_for_process_pto_schedules_to_assign = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_pto_schedules_to_assign',
            dag_runs= '{{ result("process_pto_schedules_to_assign") }}',
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
            python_callable=custom_methods.do_format_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('format_logs') }}",
            header=[
                'employeeid',
                'schedulename',
                'startdate ',
                'status',
                'action',
                'details',
                'ecid'
            ],
            row=[
                '{{ item.employeeid }}',
                '{{ item.schedulename }}',
                '{{ item.startdate }}',
                '{{ item.status }}',
                '{{ item.action }}',
                '{{ item.details }}',
                '{{ item.ecid }}'
            ],
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == "Error", json.loads(rail.result('format_logs'))))), 'length')
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: 'logs_' + datetime.now().strftime('%H%M%S') + "_" + rail.render_template("{{result('new_file_sensor') | file_name}}")
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("get_log_file_name") }}',
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Shift Schedule Import is - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content='templates/import_complete.html',
            params={
                'log_filepath': config.log_filepath,
            }
        )

        send_no_active_users_email = rail.EmailOperator(
            task_id='send_no_active_users_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | Replicon Shift Schedule Import - File processing is skipped on {{ current_time_in_specified_tz() }}",
            html_content='templates/empty_users.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
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

        new_file_sensor >> is_csv >> rail.Label(
            "Yes") >> download_file >> was_new_file_found >> rail.Label(
                "No") >> delete_this_dagrun

        was_new_file_found >> rail.Label(
            "Yes") >> archive_file

        is_csv >> rail.Label(
            "No") >> send_bad_file_format_email

        download_file >> load_user_data >> create_rawdata_collection >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_any_blankmandatory_check >> has_any_blank_mandatory_field

        has_collection_data >> rail.Label(
            "No") >> send_blank_payload_email

        has_any_blank_mandatory_field >> rail.Label(
            "Yes") >> write_blankmandatory_field_log >> query_valid_data_from_rawdata

        has_any_blank_mandatory_field >> rail.Label(
            "No") >> query_valid_data_from_rawdata >> query_distinct_shifts >> has_schedules_to_create

        has_schedules_to_create >> rail.Label(
            "Yes") >> process_create_schedules >> wait_for_process_create_schedules >> get_all_shift_schedules

        has_schedules_to_create >> rail.Label(
            "No") >> get_all_shift_schedules >> get_report_result >> run_report_entry,run_report_exit >> is_report_failed

        is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation

        is_report_failed >> rail.Label(
            "No") >> report_has_expected_columns

        report_has_expected_columns >> rail.Label(
            "Yes") >> report_has_data

        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_colums

        report_has_data >> rail.Label(
            "No") >> send_no_active_users_email >> log_to_sumo

        report_has_data >> rail.Label(
            "Yes") >> load_users_report_data >> users_report_data_collection >> create_csv_lines_for_raw_data >> final_data_collection >>\
                query_invalid_users >> has_query_invalid_users

        has_query_invalid_users >> rail.Label(
            "Yes") >> write_blank_user_field_log >> query_non_shift_users

        has_query_invalid_users >> rail.Label(
            "No") >> query_non_shift_users >> has_query_non_shift_users

        has_query_non_shift_users >> rail.Label(
            "No") >> query_unique_users_for_shift

        has_query_non_shift_users >> rail.Label(
            "Yes") >> assign_default_shift >> query_unique_users_for_shift >> process_shift_schedules_to_assign

        process_shift_schedules_to_assign >> wait_for_process_shift_schedules_to_assign >> query_unique_users_for_pto

        query_unique_users_for_pto >> process_pto_schedules_to_assign >> wait_for_process_pto_schedules_to_assign >> load_master_log

        load_master_log >> format_logs >> render_logs_csv >> get_errored_logs >> get_log_file_name >> upload_logs_to_sftp >>\
            send_import_complete_email >> log_to_sumo

        log_to_sumo >> can_fail_dag >> rail.Label(
            "Yes") >> fail_dagrun

        return dag

rail.for_each_instance(create_dag)
