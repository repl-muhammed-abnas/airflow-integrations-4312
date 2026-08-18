from datetime import datetime, timedelta, timezone
from os import path
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
from adtalem.user_import.utils import request_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_master_{config.instance}',
        description=f'Adtalem User Import Master_CR2021_V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_base }}_{{ result('get_time_for_file') }}.csv"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_userimport_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userimport_report_details',
            report_name='***User Import Reference'
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='userimport_report_generation',
            report_params=request_payload.get_report_params
        )

        # pylint: disable=line-too-long
        expected_report_columns = 'User First Name,User Last Name,User Email,User Status,User Start Date,User End Date,User Supervisor Name (Current),User Department Name,Employee ID,Login Name,Employee Type,Punch Entry Policy Name,Service Date,Student Worker,Job Code,Job_Title,Paygroup (Current),Division,Salary/Hourly,Regular/Temp,Full/Part Time,Active/Leave Status,Home State,FLSA Status,File Number,Rehire Date,Colleague D Number,CoCode,Holiday Calendar,Time Zone,Authentication Type,Timesheet Approval Path,Time Off Approval Path,Timesheet Period Type,Timesheet Template,Time Off Template,Schedule Name (Current),Batch ID,Work Week,supervisor uri,Pay Rule Name,Standard Hours,Department Number,Work Location,Salary Grade Code'
        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('userimport_report_generation.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % expected_report_columns,
            yes_task="process_file_content",
            no_task="send_report_modified_mail",
        )

        send_report_modified_mail = rail.EmailOperator(
            task_id='send_report_modified_mail',
            to=config.alert_email,
            subject='Issue with Adtalem Base Report - ***User Import Reference!!',
            html_content='templates/email/report_modified.html',
        )

        process_file_content = rail.EmptyOperator(
            task_id='process_file_content',
        )

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=request_payload.do_has_file_content,
            yes_task='load_inputfile_csv',
            no_task='process_complete_maindag'
        )

        load_inputfile_csv = rail.LoadCSVFileOperator(
            task_id='load_inputfile_csv',
            document="{{ result('download_file') }}"
        )

        create_raw_inputdata_collection = rail.CreateCollectionOperator(
            task_id='create_raw_inputdata_collection',
            source="{{ result('load_inputfile_csv') }}",
            name='rawinputfile',
            columns={
                'Last Name': 'lastname',
                'First Name': 'firstname',
                'Middle Name': 'middlename',
                'Employee Number': 'employeenumber',
                'D Number': 'dnumber',
                'Position Number': 'positionnumber',
                'Job Code': 'jobcode',
                'Job Title': 'jobtitle',
                'Job Function Code': 'jobfunctioncode',
                'Job Function Name': 'jobfunctionname',
                'Manager Indicator': 'managerindicator',
                'Hire Date': 'hiredate',
                'Rehire Date': 'rehiredate',
                'Service Date': 'servicedate',
                'Company': 'company',
                'Payrgroup': 'paygroup',
                'Division': 'division',
                'Work Location Code': 'worklocationcode',
                'Work Location Name': 'worklocationname',
                'Work Address': 'workaddress',
                'Reporting Location Code': 'reportinglocationcode',
                'Reporting Location Name': 'reportinglocationname',
                'Salary/Hourly': 'salaryhourly',
                'Regular/Temp': 'regulartemp',
                'Full/Part Time': 'fullparttime',
                'Employee Status': 'employeestatus',
                'Distibution Code': 'distibutioncode',
                'Department Number': 'departmentnumber',
                'Department Name': 'departmentname',
                'GL Company': 'glcompany',
                'GL Account Number': 'glaccountnumber',
                'File Number': 'filenumber',
                'Manager Position Number': 'managerpositionnumber',
                'Manager Name': 'managername',
                'Manager D Number': 'managerdnumber',
                'Manager Email Address': 'manageremailaddress',
                'Business Email Address': 'businessemailaddress',
                'State/Province Code': 'stateprovincecode',
                'ZIP/Postal Code': 'zippostalcode',
                'Country': 'country',
                'Work Phone': 'workphone',
                'SPM Name': 'spmname',
                'Street 1': 'street1',
                'Street 2': 'street2',
                'City': 'city',
                'State': 'state',
                'Zip': 'zip',
                'Date of Birth': 'dateofbirth',
                'Standard Hours': 'standardhours',
                'FLSA status': 'flsastatus',
                'Manager emplid': 'manageremplid',
                'Leave effective date': 'leaveeffectivedate',
                'Leave reason code': 'leavereasoncode',
                'Termination Date': 'terminationdate',
                'Salary Grade': 'salarycode',
                'Effective Date': 'effectivedate',
                'Encoded': 'encoded'
            }
        )

        query_raw_data = rail.QueryCollectionOperator(
            task_id='query_raw_data',
            query="""SELECT lastname, firstname, employeenumber, dnumber,
                    jobcode, jobtitle, jobfunctionname, managerindicator,
                    hiredate, rehiredate, servicedate, paygroup, division,
                    worklocationname, salaryhourly, regulartemp, fullparttime,
                    employeestatus, departmentnumber, filenumber, managerdnumber,
                    businessemailaddress, state, standardhours, flsastatus, terminationdate,
                    salarycode, effectivedate, encoded FROM rawinputfile""",
            name='rawdatacollection'
        )

        create_rawdata_csv = rail.WriteCSVFileOperator(
            task_id='create_rawdata_csv',
            source="{{ result('query_raw_data') }}",
            header=['lastname', 'firstname', 'employeenumber', 'dnumber', 'jobcode', 'jobtitle', 'jobfunctionname',
                    'managerindicator', 'hiredate', 'rehiredate', 'servicedate', 'paygroup', 'division', 'worklocationname',
                    'salaryhourly', 'regulartemp', 'fullparttime', 'employeestatus', 'departmentnumber', 'filenumber',
                    'managerdnumber', 'businessemailaddress', 'state', 'standardhours', 'flsastatus', 'terminationdate',
                    'salarycode', 'effectivedate', 'encoded'],
            row=request_payload.get_row_data
        )

        create_inputdatafilerefreshed_collection = rail.CreateCollectionOperator(
            task_id='create_inputdatafilerefreshed_collection',
            source="{{ result('create_rawdata_csv') }}",
            name='inputdatafilerefreshed'
        )

        create_supervisorlog = rail.CreateLogOperator(
            task_id='create_supervisorlog'
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        trigger_update_jobcode_dropdowns = rail.TriggerDagRunOperator(
            task_id='trigger_update_jobcode_dropdowns',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_update_jobcodedropdowns_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        trigger_adtalem_caribbean_user_import = rail.TriggerDagRunOperator(
            task_id='trigger_adtalem_caribbean_user_import',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_caribbean_master_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'filepath': "{{ result('new_file_sensor') }}",
                'filename': "{{ result('new_file_sensor') | file_base }}",
                'supervisorpermissionuri': "{{ result('get_all_permissionsets') | \
                    find_first_by_attr_and_get_attr('displayText', 'Supervisor', 'uri', '') }}",
                'enduserpermissionuri': "{{ result('get_all_permissionsets') | \
                    find_first_by_attr_and_get_attr('displayText', 'End User', 'uri', '') }}"
            }
        )

        can_process_us_canada_import = rail.IfOperator(
            task_id='can_process_us_canada_import',
            test=lambda: Variable.get(
                config.can_process_us_canada_import, default_var='false').lower() == 'true',
            yes_task='list_reference_files',
            no_task='process_complete_maindag'
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            paths=[config.reference_filepath]
        )

        should_use_referencefile = rail.IfOperator(
            task_id='should_use_referencefile',
            test=lambda: bool(rail.result('list_reference_files').get(
                config.reference_filepath)),
            yes_task='trigger_referencefile_download_child',
            no_task='trigger_user_child_dag'
        )

        trigger_referencefile_download_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_referencefile_download_child',
            retries=0,
            items=lambda: rail.result('list_reference_files')[
                config.reference_filepath],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_child_referencefile_{config.instance}',
            conf=lambda item: {
                'reference_file': f"{config.reference_filepath}/{item['name']}",
                'action': 'download'
            }
        )

        wait_for_referencefile_download_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_referencefile_download_child',
            dag_runs="{{ result('trigger_referencefile_download_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_userreference_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_userreference_data',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_referencefile_download_child') }}",
            dagrun_task_id='create_userreference_data',
            flatten=True
        )

        create_userreference_data_collection = rail.CreateCollectionOperator(
            task_id='create_userreference_data_collection',
            name='referencefile',
            source=lambda: rail.result('gather_userreference_data')
        )

        query_changed_users = rail.QueryCollectionOperator(
            task_id='query_changed_users',
            query="""SELECT * FROM inputdatafilerefreshed WHERE
                    encoded NOT IN (SELECT DISTINCT encoded FROM referencefile)""",
        )

        query_unchanged_users = rail.QueryCollectionOperator(
            task_id='query_unchanged_users',
            query="""SELECT * FROM inputdatafilerefreshed WHERE
                    encoded IN (SELECT DISTINCT encoded FROM referencefile)""",
        )

        is_changed_users_present = rail.IfOperator(
            task_id='is_changed_users_present',
            test="{{ result('query_changed_users', 'length') > 0 }}",
            yes_task='trigger_user_child_dag',
            no_task='process_complete_maindag'
        )

        trigger_user_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_child_dag',
            retries=0,
            items=lambda: rail.result('query_changed_users') or rail.result(
                'create_inputdatafilerefreshed_collection'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_process_user_{config.instance}',
            conf=lambda item: {
                **dict(item.items()),
                'supervisor_log': rail.result('create_supervisorlog'),
                'supervisorpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permissionsets'), 'displayText', 'Supervisor', 'uri', ''),
                'enduserpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permissionsets'), 'displayText', 'End User', 'uri', '')
            }
        )

        wait_for_user_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_child_dag',
            dag_runs="{{ result('trigger_user_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('create_supervisorlog') }}",
            severity='Queued',
            remove_filtered_entries=True
        )

        is_supervisorcheck_queued_logs = rail.IfOperator(
            task_id='is_supervisorcheck_queued_logs',
            test="{{ result('get_supervisorcheck_queued_logs', 'length') > 0 }}",
            yes_task='trigger_supervisor_child_dag',
            no_task='gather_user_child_logs'
        )

        trigger_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_queued_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_user_import_supervisor_child_{config.instance}',
            conf=lambda item: {
                **dict(item['properties'].items()),
                'supervisor_log': rail.result('create_supervisorlog'),
                'supervisorpermissionuri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permissionsets'), 'displayText', 'Supervisor', 'uri', '')
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('trigger_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_user_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_child_logs',
            dag_runs="{{ result('trigger_user_child_dag') }}",
            dagrun_task_id='create_userlog',
            flatten=True
        )

        process_logs = rail.TriggerDagRunOperator(
            task_id='process_logs',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_child_log_{config.instance}',
            conf=lambda: {
                'import_type': 'User import',
                # pylint: disable=line-too-long
                'log_filename': f"Logs_{rail.result('get_time_for_file')}_{split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]}",
                'user_logs': rail.result('gather_user_child_logs'),
                'time': datetime.now(timezone.utc).strftime('%m%d%Y'),
                'filename': split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]
            }
        )

        has_reference_files_archive = rail.IfOperator(
            task_id='has_reference_files_archive',
            test=lambda: bool(rail.result('list_reference_files').get(
                config.reference_filepath)),
            yes_task='trigger_referencefile_archive_child',
            no_task='upload_referencefile_to_sftp'
        )

        trigger_referencefile_archive_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_referencefile_archive_child',
            retries=0,
            items=lambda: rail.result('list_reference_files')[
                config.reference_filepath],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'adtalem_userimport_child_referencefile_{config.instance}',
            conf=lambda item, dag_run: {
                'reference_file': f"{config.reference_filepath}/{item['name']}",
                'time': rail.result('get_time_for_file'),
                'filename': f"{get_dagrun_ecid(dag_run).replace(':', '-')}_{split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]}",
                'archive_filepath': config.archive_filepath,
                'action': 'archive'
            }
        )

        wait_for_referencefile_archive_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_referencefile_archive_child',
            dag_runs="{{ result('trigger_referencefile_archive_child') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        upload_referencefile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_referencefile_to_sftp',
            content="{{ result('create_rawdata_csv') }}",
            remote_filepath=config.reference_filepath +
            "/New_Reference_{{ result('get_time_for_file') }}.csv"
        )

        process_complete_maindag = rail.EmptyOperator(
            task_id='process_complete_maindag',
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='dagrun_log_to_sumo'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id
        )

        new_file_sensor >> download_file >> get_time_for_file

        get_time_for_file >> rail.Label(
            'Always') >> was_new_file_found

        was_new_file_found >> rail.Label(
            'Yes') >> archive_file

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        get_time_for_file >> get_userimport_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_has_expected_columns

        is_report_has_expected_columns >> rail.Label(
            'No') >> send_report_modified_mail
        is_report_has_expected_columns >> rail.Label(
            'Yes') >> process_file_content >> has_file_content

        has_file_content >> rail.Label(
            'Yes') >> load_inputfile_csv >> create_raw_inputdata_collection >> \
            query_raw_data >> create_rawdata_csv >> create_inputdatafilerefreshed_collection >> get_all_permissionsets >> \
            create_supervisorlog >> trigger_update_jobcode_dropdowns >> trigger_adtalem_caribbean_user_import >> \
            can_process_us_canada_import

        can_process_us_canada_import >> rail.Label(
            'Yes') >> list_reference_files >> should_use_referencefile

        should_use_referencefile >> rail.Label(
            'Yes') >> trigger_referencefile_download_child >> wait_for_referencefile_download_child >> gather_userreference_data >> \
            create_userreference_data_collection >> query_changed_users >> query_unchanged_users >> is_changed_users_present

        is_changed_users_present >> rail.Label(
            'Yes') >> trigger_user_child_dag

        is_changed_users_present >> rail.Label(
            'No') >> process_complete_maindag

        should_use_referencefile >> rail.Label(
            'No') >> trigger_user_child_dag

        trigger_user_child_dag >> wait_for_user_child_dag >> \
            get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs

        is_supervisorcheck_queued_logs >> rail.Label(
            'Yes') >> trigger_supervisor_child_dag >> wait_for_supervisor_child_dag >> gather_user_child_logs

        is_supervisorcheck_queued_logs >> rail.Label(
            'No') >> gather_user_child_logs

        gather_user_child_logs >> process_logs >> has_reference_files_archive

        has_reference_files_archive >> rail.Label(
            'Yes') >> trigger_referencefile_archive_child >> \
            wait_for_referencefile_archive_child >> upload_referencefile_to_sftp

        has_reference_files_archive >> rail.Label(
            'No') >> upload_referencefile_to_sftp

        upload_referencefile_to_sftp >> process_complete_maindag

        has_file_content >> rail.Label(
            'No') >> process_complete_maindag

        can_process_us_canada_import >> rail.Label(
            'No') >> process_complete_maindag

        process_complete_maindag >> rail.Label(
            'Always') >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found >> rail.Label(
                'Yes') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_main_dag)
