# pylint: disable=too-many-statements
from datetime import timedelta, datetime
import rail
from momentive.user_import_south_korea.utils import request_payload
from momentive.user_import_south_korea.utils import python_callable
from momentive.user_import_south_korea.utils.request_payload import get_invalid_record

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_southkorea_master_{config.instance}',
        description=f'momentive_userimport_southkorea_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        schedule_interval=None,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        logger_list = rail.CreateLogOperator(
            task_id = "logger_list"
        )

        supervisor_logger_list = rail.CreateLogOperator(
            task_id = "supervisor_logger_list"
        )

        get_current_datetime = rail.PythonOperator(
            task_id="get_current_datetime",
            python_callable=python_callable.get_current_date_time
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id='get_all_reports',
            endpoint="/services/reportservice1.svc/GetAllReports"
        )

        get_dag_run_val = rail.PythonOperator(
            task_id="get_dag_run_val",
            python_callable=lambda dag_run: dag_run.conf
        )

        write_user_sync_csv = rail.WriteCSVFileOperator(
            task_id="write_user_sync_csv",
            source=lambda dag_run: dag_run.conf['data'],
            header=['userid','workerreferenceemployeeid','emailaddress','firstname','lastname','workertype',
                    'effective_date_of_worker_type','exemptionstatus','cf_lrv_job_exempt_eff_date','gender','hiredate',
                    'terminationdate','active','function','function_change_effective_date','businesstitle',
                    'cf_lrv_business_title_change','fieldhr','managerid','effective_date_of_manager_change','work_shift',
                    'work_shift_change_effective_date','location','location_change_eff_date','country','date_of_birth',
                    'cf_lrv_manager_email','cf_lrv_manager_first_name','cf_lrv_manager_last_name','legalentity','worker_subType',
                    'cost_center','worker_cc_change_date','year_of_service','paygroup','japan_special_schedule_flag',
                    'continous_service_date','timeoff_service_date'],
            row=request_payload.user_import_data
        )

        upload_input_to_sftp = rail.SFTPAppendCSVFileOperator(
            task_id='upload_input_to_sftp',
            content="{{ result('write_user_sync_csv') }}",
            remote_filepath=config.archive_filepath + "/input_data_{{ result('get_current_datetime') }}.csv"
        )

        if_no_data_present = rail.IfOperator(
            task_id='if_no_data_present',
            test=lambda dag_run: bool(len(dag_run.conf['data']) < 1),
            yes_task="send_mail_no_changerecords",
            no_task="create_workdayuserdata_collection_17"
        )

        create_workdayuserdata_collection_17 = rail.CreateCollectionOperator(
            task_id='create_workdayuserdata_collection_17',
            source="{{ result('write_user_sync_csv') }}",
            name="workdayuserdata"
        )

        query_blank_loginname_records = rail.QueryCollectionOperator(
            task_id="query_blank_loginname_records",
            query="""SELECT * FROM workdayuserdata WHERE (NULLIF(userid, '') IS NULL )""",
            name="blank_records"
        )

        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_invalid_records",
            test="{{ result('query_blank_loginname_records', 'length') > 0 }}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('logger_list') }}",
            items='{{result("query_blank_loginname_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item : get_invalid_record(item)
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM workdayuserdata WHERE (NULLIF(userid, '') IS NOT NULL AND \
                legalentity == 'MOMENTIVE PERFORMANCE MATERIALS KOREA CO., LTD.' )""",
            name="valid_records"
        )

        is_validated_records_present = rail.IfOperator(
            task_id="is_validated_records_present",
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task="get_all_enabled_divisions",
            no_task="load_master_log"
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_enabled_service_centers = rail.RepliconServiceOperator(
            task_id='get_enabled_service_centers',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_enabled_cost_centers = rail.RepliconServiceOperator(
            task_id='get_enabled_cost_centers',
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",
        )

        get_department_list = rail.RepliconServiceOperator(
            task_id="get_department_list",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_enabled_dept,
            data_handler=python_callable.get_department_group_list
        )

        process_each_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_user',
            items = lambda: rail.result('query_valid_records'),
            trigger_dag_id=f'momentive_userimport_proecss_each_user_child_{config.instance}',
            conf=request_payload.process_each_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_each_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_user',
            dag_runs='{{ result("process_each_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("process_each_user") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        write_supervisor_assignment_log_file = rail.WriteCSVFileOperator(
            task_id="write_supervisor_assignment_log_file",
            source=lambda: rail.result('supervisor_logger_list'),
            header=["loginid", "supervisorempid", "useruri",'type',"sup_email","sup_firstname","sup_lastname","sup_change_effective_date"],
            row=lambda item: [
                item['properties']['loginid'],
                item['properties']['supervisorempid'],
                item['properties']['useruri'],
                item['properties']['type'],
                item['properties']['sup_email'],
                item['properties']['sup_firstname'],
                item['properties']['sup_lastname'],
                item['properties']['sup_change_effective_date']
            ]
        )

        check_supervisor_mapper_csv_has_data = rail.IfOperator(
            task_id = "check_supervisor_mapper_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_supervisor_assignment_log_file'))) > 0 ,
            yes_task = "process_each_supervisor_mapper_data",
            no_task = "load_master_log"
        )

        process_each_supervisor_mapper_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_supervisor_mapper_data',
            items = "{{ result('write_supervisor_assignment_log_file')}}",
            trigger_dag_id=f'momentive_userimport_supervisor_assignment_child_{config.instance}',
            conf=request_payload.process_supervisor_mapper_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_mapper_data_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_mapper_data_process',
            dag_runs='{{ result("process_each_supervisor_mapper_data") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        load_master_log = rail.RenderTemplateOperator(
            task_id='load_master_log',
            target='result',
            template="{{ result('logger_list') | load_all_records | to_json }}"
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.do_format_logs
        )

        write_log_user_import = rail.WriteCSVFileOperator(
            task_id='write_log_user_import',
            source="{{ result('format_logs').final_logs }}",
            header=['userid', 'username', 'action','status', 'details', 'country', 'childjobid'],
            row=lambda item: [
                item['userid'],
                item['username'],
                item['action'].split('|')[0] if '|' in item['action'] else item['action'],
                item['status'],
                item['details'],
                item['country'],
                item['ecid']
            ]
        )

        check_csv_has_data = rail.IfOperator(
            task_id = "check_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_log_user_import'))) > 0,
            yes_task = "generate_downloadlink",
            no_task = "fail_the_dag"
        )

        fail_the_dag = rail.FailOperator(
            task_id="fail_the_dag",
            message='No log found'
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('write_log_user_import')}}",
            output_file_name="userimport_log_{{ result('get_current_datetime') }}.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.to_email,
            bcc="{%- if result('format_logs').get_record_summary.failed == 0 -%}\
                    "+config.bcc_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject=f'{ config.company_key } - {config.country}' + ' | User import -  \
                {%- if result("format_logs").get_record_summary.failed > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if (result("format_logs").get_record_summary.exception > 0) or (result("format_logs").get_record_summary.skipped > 0) -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%}' \
                + datetime.now().strftime("%m/%d/%YT%H:%M:%S"),
            html_content="templates/emails/import_complete_mail.html",
            params={
                'today': datetime.now().strftime("%m/%d/%YT%H:%M:%S")
            }
        )

        send_mail_no_changerecords = rail.EmailOperator(
            task_id='send_mail_no_changerecords',
            to=config.to_email,
            bcc=config.alert_email,
            subject=f'{config.company_key} - {config.country} | User import completed- No change records found-' + datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            html_content="templates/emails/no_change_records.html"
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

        logger_list >> supervisor_logger_list >> get_current_datetime >> get_all_reports >> get_dag_run_val >> write_user_sync_csv >> \
            upload_input_to_sftp >> if_no_data_present

        if_no_data_present >> rail.Label('Yes') >> send_mail_no_changerecords >> log_to_sumo
        if_no_data_present >> rail.Label('No') >> create_workdayuserdata_collection_17 >> query_blank_loginname_records >> has_any_invalid_records

        has_any_invalid_records >> rail.Label('Yes') >> log_invalid_records >> query_valid_records >> is_validated_records_present
        has_any_invalid_records >> rail.Label('No') >> query_valid_records >> is_validated_records_present

        is_validated_records_present >> rail.Label('Yes') >> get_all_enabled_divisions
        is_validated_records_present >> rail.Label('No') >> load_master_log

        get_all_enabled_divisions >> get_enabled_service_centers >> get_enabled_cost_centers >> get_department_list >> \
            process_each_user >> wait_for_process_each_user >> gather_user_logs >> write_supervisor_assignment_log_file >> check_supervisor_mapper_csv_has_data

        check_supervisor_mapper_csv_has_data >> rail.Label('Yes') >> process_each_supervisor_mapper_data >> wait_for_mapper_data_process >> \
            load_master_log
        check_supervisor_mapper_csv_has_data >> rail.Label('No') >> load_master_log

        load_master_log >> format_logs >> write_log_user_import >> check_csv_has_data

        check_csv_has_data >> rail.Label('Yes') >> generate_downloadlink >> send_import_complete_email >> log_to_sumo
        check_csv_has_data >> rail.Label('No') >> fail_the_dag

        write_user_sync_csv >> log_to_sumo

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
