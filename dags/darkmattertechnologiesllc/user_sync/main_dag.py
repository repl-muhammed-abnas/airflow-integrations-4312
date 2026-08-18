from datetime import timedelta, datetime
import itertools
import pendulum
from os import path
import rail
from rail.filters import split
from darkmattertechnologiesllc.user_sync.utils import request_payload
from darkmattertechnologiesllc.user_sync.utils.request_payload import get_invalid_record
from darkmattertechnologiesllc.user_sync.utils import python_callable

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'darkmattertechnologiesllc_usersync_master_{config.instance}',
        description=f'darkmattertechnologiesllc_usersync_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_active_runs,
        start_date=pendulum.datetime(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor_to_process = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor_to_process',
            path=config.input_filepath_master,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_current_datetime = rail.PythonOperator(
            task_id="get_current_datetime",
            python_callable=lambda : datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor_to_process") | file_ext | lower == "csv" }}',
            yes_task='download_sftp_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.to_email,
            bcc=config.bcc_email,
            subject='{{ get_company_key() }} | User Sync - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_sftp_file = rail.SFTPDownloadFileOperator(
            task_id='download_sftp_file',
            remote_filepath="{{ result('new_file_sensor_to_process') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor_to_process") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor_to_process") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor_to_process') | file_name }}"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_user_sync_csv = rail.LoadCSVFileOperator(
            task_id="parse_user_sync_csv",
            document='{{result("download_sftp_file")}}',
            delimiter=",",
            encoding = 'utf-8-sig'
        )

        user_import_log = rail.CreateLogOperator(
            task_id = "user_import_log"
        )

        supervisor_assignment_log = rail.CreateLogOperator(
            task_id = "supervisor_assignment_log"
        )

        write_user_import_csv = rail.WriteCSVFileOperator(
            task_id="write_user_import_csv",
            source='{{result("parse_user_sync_csv")}}',
            header=['Employee ID', 'Black Knight ID', 'Perferred Name - First Name', 'Perferred Name - Last Name', \
                    'Worker Type', 'Employee Type', 'Business Title', 'Cost Center - ID', 'Cost Center - Name', \
                        'Department Name', "Worker's Manager", 'Location Hierarchy', 'Location - Name', 'Work State', \
                            'Work City', 'Scheduled Weekly Hours', 'FTE %', 'Continuous Service Date', 'Termination Date', \
                                'Email - Primary Work', 'Manager - Level 02', 'Manager - Level 03', 'Manager - Level 04', \
                                    'Manager - Level 05', 'Manager - Level 06', 'Manager - Level 07', 'Manager - Level 08', \
                                        'Manager - Level 09', 'Manager - Level 10', 'Employee Status', 'First Day of Leave', 'Return Date from Leave'],
            row=request_payload.user_import_csv_data
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('write_user_import_csv') }}",
            name="sourceuserdata",
            columns={
                'Employee ID': 'employeeid',
                'Black Knight ID': 'blackknightid',
                'Perferred Name - First Name': 'firstname',
                'Perferred Name - Last Name': 'lastname',
                'Worker Type': 'workertype',
                'Employee Type': 'employeetype',
                'Business Title': 'businesstitle',
                'Cost Center - ID': 'costcenterid',
                'Cost Center - Name': 'costcentername',
                'Department Name': 'departmentname',
                "Worker's Manager": 'workermanager',
                'Location Hierarchy': 'locationhierarchy',
                'Location - Name': 'locationname',
                'Work State': 'workstate',
                'Work City': 'workcity',
                'Scheduled Weekly Hours': 'scheduledweeklyhours',
                'FTE %': 'fte',
                'Continuous Service Date': 'startdate',
                'Termination Date': 'enddate',
                'Email - Primary Work': 'loginname',
                'Manager - Level 02': 'manager2',
                'Manager - Level 03': 'manager3',
                'Manager - Level 04': 'manager4',
                'Manager - Level 05': 'manager5',
                'Manager - Level 06': 'manager6',
                'Manager - Level 07': 'manager7',
                'Manager - Level 08': 'manager8',
                'Manager - Level 09': 'manager9',
                'Manager - Level 10': 'manager10',
                'Employee Status': 'employeestatus',
                'First Day of Leave': 'firstdayofleave',
                'Return Date from Leave': 'returndatefromleave'
            }
        )

        if_records_present = rail.IfOperator(
            task_id = "if_records_present",
            test = "{{result('create_collection_from_csv', 'length') > 0}}",
            yes_task = "query_invalid_records",
            no_task = "send_no_data_to_import_mail"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM sourceuserdata WHERE NULLIF(employeeid, '') IS NULL or
                    NULLIF(firstname, '') IS NULL or NULLIF(lastname, '') IS NULL or NULLIF(workertype, '') IS NULL or
                    NULLIF(employeetype, '') IS NULL or NULLIF(businesstitle, '') IS NULL or NULLIF(costcenterid, '') IS NULL or
                    NULLIF(costcentername, '') IS NULL or NULLIF(departmentname, '') IS NULL or NULLIF(workermanager, '') IS NULL or 
                    NULLIF(locationhierarchy, '') IS NULL or NULLIF(locationname, '') IS NULL or NULLIF(workstate, '') IS NULL or 
                    NULLIF(workcity, '') IS NULL or NULLIF(scheduledweeklyhours, '') IS NULL or NULLIF(fte, '') IS NULL or 
                    NULLIF(startdate, '') IS NULL or NULLIF(loginname, '') IS NULL or NULLIF(employeestatus, '') IS NULL """
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('user_import_log') }}",
            items='{{result("query_invalid_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item : get_invalid_record(item)
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM sourceuserdata WHERE NULLIF(employeeid, '') IS NOT NULL and
                    NULLIF(firstname, '') IS NOT NULL and NULLIF(lastname, '') IS NOT NULL and NULLIF(workertype, '') IS NOT NULL and
                    NULLIF(employeetype, '') IS NOT NULL and NULLIF(businesstitle, '') IS NOT NULL and NULLIF(costcenterid, '') IS NOT NULL and
                    NULLIF(costcentername, '') IS NOT NULL and NULLIF(departmentname, '') IS NOT NULL and NULLIF(workermanager, '') IS NOT NULL and 
                    NULLIF(locationhierarchy, '') IS NOT NULL and NULLIF(locationname, '') IS NOT NULL and NULLIF(workstate, '') IS NOT NULL and 
                    NULLIF(workcity, '') IS NOT NULL and NULLIF(scheduledweeklyhours, '') IS NOT NULL and NULLIF(fte, '') IS NOT NULL and 
                    NULLIF(startdate, '') IS NOT NULL and NULLIF(loginname, '') IS NOT NULL and NULLIF(employeestatus, '') IS NOT NULL """
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='process_groups',
            no_task="load_master_log"
        )

        send_no_data_to_import_mail = rail.EmailOperator(
            task_id='send_no_data_to_import_mail',
            to=config.to_email,
            bcc=config.bcc_email,
            subject='{{ get_company_key() }} | User Sync - No Records to Import - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/send_no_data_to_import.html"
        )

        process_groups = rail.TriggerDagRunForEachItemOperator(
            task_id="process_groups",
            items=[1],
            trigger_dag_id=f'darkmattertechnologiesllc_usersync_process_groups_child_{config.instance}',
            conf={
                "file_name": "{{ result('new_file_sensor_to_process') | file_name}}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_groups = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_groups',
            dag_runs='{{ result("process_groups") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_updated_departments = rail.RepliconServiceOperator(
            task_id='get_updated_departments',
            endpoint='/services/DepartmentGroupService1.svc/GetAllDepartmentGroups',
            data_handler=python_callable.get_data_from_replicon
        )

        get_updated_employee_types_from_replicon = rail.RepliconServiceOperator(
            task_id="get_updated_employee_types_from_replicon",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_employeetype_details_from_replicon,
            data_handler=python_callable.get_all_group_data_from_replicon_filter
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            data_handler=python_callable.get_all_group_data_from_replicon_filter
        )

        get_updated_costcenters = rail.RepliconServiceOperator(
            task_id='get_updated_costcenters',
            endpoint='/services/CostCenterService1.svc/GetEnabledCostCenters',
            data_handler=python_callable.get_data_from_replicon
        )

        get_required_permission_set = rail.RepliconServiceOperator(
            task_id="get_required_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        process_each_user = rail.trigger_parallel_dagrun(
            task_id='process_each_user',
            items = lambda: rail.result('query_valid_records'),
            trigger_dag_id=f'darkmattertechnologiesllc_usersync_process_each_user_child_{config.instance}',
            parallel_count=config.max_active_process_run_count,
            conf=request_payload.process_each_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_process_each_user_dag_ids =rail.PythonOperator(
            task_id= 'get_process_each_user_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_each_user_{x+1}') if rail.result(
                    f'process_each_user_{x+1}') else []), range(config.max_active_process_run_count))))),
            show_return_value_in_logs= False
        )

        wait_for_process_each_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_user',
            dag_runs='{{ result("get_process_each_user_dag_ids") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_each_user_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        check_supervisor_csv_has_data = rail.IfOperator(
            task_id = "check_supervisor_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('supervisor_assignment_log'))) > 0 ,
            yes_task = "process_each_supervisor_data",
            no_task = "gather_user_logs"
        )

        process_each_supervisor_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_supervisor_data',
            items = "{{ result('supervisor_assignment_log')}}",
            trigger_dag_id=f'darkmattertechnologiesllc_usersync_update_supervisor_child_{config.instance}',
            conf=request_payload.process_supervisor_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_supervisor_data_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_data_process',
            dag_runs='{{ result("process_each_supervisor_data") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
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
            task_id='write_userimportlog_file',
            source="{{ result('format_logs').final_logs }}",
            header=[ 'Employee Id','Action','Status','Details','ecid'],
            row=[
                '{{ item.employeeid }}',
                '{{ item.action }}',
                '{{ item.status }}',
                '{{ item.details}}',
                '{{ item.ecid }}']
        )

        get_logfile_name = rail.PythonOperator(
            task_id = "get_logfile_name",
            python_callable=lambda: f'''log_{split(string=path.split(rail.result(
                "new_file_sensor_to_process"))[1], separator=".")[0] }_{datetime.now().strftime("%Y%m%dT%H%M%S")}.csv'''
        )

        check_csv_has_data = rail.IfOperator(
            task_id = "check_csv_has_data",
            test = lambda: len(rail.load_all_records(rail.result('write_userimportlog_file'))) > 0,
            yes_task = "upload_csv_to_sftp",
            no_task = "fail_the_dag"
        )

        upload_csv_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_csv_to_sftp',
            content="{{ result('write_userimportlog_file') }}",
            remote_filepath=config.log_filepath + "{{ result('get_logfile_name') }}"
        )

        fail_the_dag = rail.FailOperator(
            task_id="fail_the_dag",
            message='No log found'
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
                    {%- if (result("format_logs").get_record_summary.exception > 0) or (result("format_logs").get_record_summary.skipped > 0) -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%}' \
                + ' - ' + datetime.now().strftime("%m/%d/%YT%H:%M:%S"),
            html_content="templates/emails/import_complete_mail.html",
            params={
                'today': datetime.now().strftime("%m/%d/%YT%H:%M:%S"),
                'log_path': config.log_filepath
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda: {
                'records': rail.result('create_collection_from_csv', 'length'),
                'invalid_record_count': rail.result('query_invalid_records', 'length'),
                'valid_record_count': rail.result('query_valid_records', 'length'),
                'filename': rail.result("new_file_sensor_to_process").split('/')[-1] if rail.result("new_file_sensor_to_process") else ''
            }
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

        new_file_sensor_to_process >> get_current_datetime >> is_csv
        
        is_csv >> rail.Label('Yes') >> download_sftp_file >> was_new_file_found
        is_csv >> rail.Label('No') >> send_bad_file_format_email

        was_new_file_found >> rail.Label('No') >> delete_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file

        download_sftp_file >> parse_user_sync_csv

        parse_user_sync_csv >> user_import_log >> supervisor_assignment_log >> write_user_import_csv >> create_collection_from_csv >> if_records_present

        if_records_present >> rail.Label('Yes') >> query_invalid_records >> has_invalid_records
        if_records_present >> rail.Label('No') >> send_no_data_to_import_mail

        has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> query_valid_records
        has_invalid_records >> rail.Label('No') >> query_valid_records

        query_valid_records >> has_valid_records

        has_valid_records >> rail.Label('Yes') >> process_groups
        has_valid_records >> rail.Label('No') >> load_master_log

        process_groups >> wait_for_process_groups >> get_updated_departments >> get_updated_employee_types_from_replicon >> \
            get_updated_locations >> get_updated_costcenters >> get_required_permission_set >> process_each_user >> get_process_each_user_dag_ids >> \
                wait_for_process_each_user >> check_supervisor_csv_has_data

        check_supervisor_csv_has_data >> rail.Label('Yes') >> process_each_supervisor_data >> wait_for_supervisor_data_process >> gather_user_logs
        check_supervisor_csv_has_data >> rail.Label('No') >> gather_user_logs

        gather_user_logs >> load_master_log

        load_master_log >> format_logs >> write_userimportlog_file >> get_logfile_name >> check_csv_has_data
        
        check_csv_has_data >> rail.Label('Yes') >> upload_csv_to_sftp >> send_import_complete_email >> log_to_sumo
        check_csv_has_data >> rail.Label('No') >> fail_the_dag

        log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
