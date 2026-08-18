
from datetime import timedelta
import pendulum
from airflow.models import Variable
from rail.lib.log import get_master_log_artifact_name
from ge.user_sync_netherlands.netherlands_master_mapper import netherlands_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_netherlands_user_import_master_{config.instance}',
        description=f'GE netherlands User Import Master {config.instance}',
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
                config.pacific_timezone).strftime('%Y%m%dT%H%M%S')
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
            no_task='if_name_downcase_not_ends_with_pgp_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_name_downcase_not_ends_with_pgp_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_name_downcase_not_ends_with_pgp_3 = rail.IfOperator(
            task_id='if_name_downcase_not_ends_with_pgp_3',
            test="{{result('new_file_sensor') | file_name | lower | ends_with('.pgp') }}",
            yes_task="download_6",
            no_task="is_healthcare_invalid_file",
        )

        is_healthcare_invalid_file = rail.IfOperator(
            task_id='is_healthcare_invalid_file',
            test="{{ get_company_key().lower() == 'gehealthcare' }}",
            yes_task="delete_file",
            no_task="archive_invalid_file_4",
        )

        delete_file = rail.SFTPDeleteFileOperator(
            task_id='delete_file',
            existing_filename="{{ result('new_file_sensor') }}",
        )

        archive_invalid_file_4 = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file_4',
            new_filename=config.archive_filepath +
            '''/Processed_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename="{{ result('new_file_sensor') }}",
        )

        send_mail_send_emailfor_incorrect_fileformat_4 = rail.EmailOperator(
            task_id='send_mail_send_emailfor_incorrect_fileformat_4',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | netherlands Republic User import - file processing is skipped - {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }} ''',
            html_content='''<p><strong><em>This is an automated mail, please don't reply</em></strong></p>
            <p>Hello,</p>
            <p>The file "{{ result('new_file_sensor') | file_name }}" is not processed since the file name is not in the allowed encrypted file format (.pgp).</p>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p> ''',
            params=None,
        )

        download_6 = rail.SFTPDownloadFileOperator(
            task_id='download_6',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_7 = rail.PGPDecryptionOperator(
            task_id='decrypt_7',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('download_6') }}",
        )

        log_filenametouse_8 = rail.PythonOperator(
            task_id='log_filenametouse_8',
            python_callable=lambda:  rail.render_template(
                '''{{ result('new_file_sensor') | file_name | replace('.pgp', '') }}''')
        )

        parse_csv_8 = rail.LoadCSVFileOperator(
            task_id="parse_csv_8",
            document="{{ result('decrypt_7') }}",
            delimiter='|'
        )

        is_healthcare_file = rail.IfOperator(
            task_id='is_healthcare_file',
            test="{{ get_company_key().lower() == 'gehealthcare' }}",
            yes_task="delete_healthcare_file",
            no_task="rename_move_input_file_to_archive_10",
        )

        delete_healthcare_file = rail.SFTPDeleteFileOperator(
            task_id='delete_healthcare_file',
            existing_filename="{{ result('new_file_sensor') }}",
        )

        rename_move_input_file_to_archive_10 = rail.SFTPMoveFileOperator(
            task_id='rename_move_input_file_to_archive_10',
            new_filename=config.archive_filepath +
            '''/Processed_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename="{{ result('new_file_sensor') }}",
        )

        def get_formated_user_row(item):
            return {
                "EmployeeFirstName": item["Employee First Name"].strip() if item["Employee First Name"] else "",
                "EmployeeLastName": item["Employee Last Name"].strip() if item["Employee Last Name"] else "",
                "EmployeeEmailAddress": item["Employee Email Address"].strip() if item["Employee Email Address"] else "",
                "OHRID": item["OHR ID"].strip() if item["OHR ID"] else "",
                "LegalEntityHireDate": item["Legal Entity Hire Date"].strip() if item["Legal Entity Hire Date"] else "",
                "LegacyPayrollID": item["Legacy Payroll ID"].strip() if item["Legacy Payroll ID"] else "",
                "Job/PositionTitle": item["Job/Position Title"].strip() if item["Job/Position Title"] else "",
                "SupervisorSSOID": item["Supervisor SSO ID"].strip() if item["Supervisor SSO ID"] else "",
                "SupervisorName": item["Supervisor Name"].strip() if item["Supervisor Name"] else "",
                "DW StartDate": item["DWS Start Date"].strip() if item["DWS Start Date"] else "",
                "DWSMonday": item["DWS - Monday"].strip() if item["DWS - Monday"] else "",
                "DWSTuesday": item["DWS - Tuesday"].strip() if item["DWS - Tuesday"] else "",
                "DWSWednesday": item["DWS - Wednesday"].strip() if item["DWS - Wednesday"] else "",
                "DWSThursday": item["DWS - Thursday"].strip() if item["DWS - Thursday"] else "",
                "DWSFriday": item["DWS - Friday"].strip() if item["DWS - Friday"] else "",
                "DWSSaturday": item["DWS - Saturday"].strip() if item["DWS - Saturday"] else "",
                "DWSSunday": item["DWS - Sunday"].strip() if item["DWS - Sunday"] else "",
                "TerminationEffectiveDate": item["Termination Effective Date"].strip() if item["Termination Effective Date"] else "",
                "IndustryFocusGroup": item["Industry Focus Group"].strip() if item["Industry Focus Group"] else "",
                "LegalEntity": item["Legal Entity"].strip() if item["Legal Entity"] else "",
                "ContractID": item["Contract ID"].strip() if item["Contract ID"] else "",
                "ContractType": item["Contract Type"].strip() if item["Contract Type"] else "",
                "RadiationFlag": item["Radiation Flag"].strip() if item["Radiation Flag"] else "",
                "PositionCapacity": item["Position Capacity"].strip() if item["Position Capacity"] else "",
                "PreviousExperience": item["Previous Experience"].strip() if item["Previous Experience"] else "",
                "OvertimeEligibility": item["Overtime Eligibility"].strip() if item["Overtime Eligibility"] else "",
                "SuspendAssignmentCategory": item["Suspend Assignment Category"].strip() if item["Suspend Assignment Category"] else "",
                "Payroll": item["Payroll"].strip() if item["Payroll"] else "",
                "Healthcare Product Line EIT": item["Healthcare Product Line EIT"].strip() if item["Healthcare Product Line EIT"] else "",
                "JobType": item["Job Type"].strip() if item["Job Type"] else "",
                "CareerBand": item["Career Band"].strip() if item["Career Band"] else "",
                "AdjustedServiceDate": item["Adjusted Service Date"].strip() if item["Adjusted Service Date"] else "",
                "Work": item["Work"].strip() if item["Work"] else "",
                "HRMSSOID": item["HRM SSO ID"].strip() if item["HRM SSO ID"] else "",
                "HRMName": item["HRM Name"].strip() if item["HRM Name"] else "",
                "SpecialWorkSchedule": item["Special Work Schedule"].strip() if item["Special Work Schedule"] else "",
                "EducationLevel": item["Education Level"].strip() if item["Education Level"] else "",
                "WorkLocation": item["Work Location"].strip() if item["Work Location"] else "",
                "AssignmentEffectiveDate": item["Assignment Effective Date"].strip() if item["Assignment Effective Date"] else "",
                "HireEffectiveDate": item["Hire Effective Date"].strip() if item["Hire Effective Date"] else "",
                "RevTermEffectiveDate": item["Rev Term Effective Date"].strip() if item["Rev Term Effective Date"] else ""
            }.values()

        load_csv_create_list_from_csv_10 = rail.WriteCSVFileOperator(
            task_id='load_csv_create_list_from_csv_10',
            source="{{ result('parse_csv_8') }}",
            header=['EmployeeFirstName',
                    'EmployeeLastName',
                    'EmployeeEmailAddress',
                    'OHRID',
                    'LegalEntityHireDate',
                    'LegacyPayrollID',
                    'Job/PositionTitle',
                    'SupervisorSSOID',
                    'SupervisorName',
                    'DWSStartDate',
                    'DWSMonday',
                    'DWSTuesday',
                    'DWSWednesday',
                    'DWSThursday',
                    'DWSFriday',
                    'DWSSaturday',
                    'DWSSunday',
                    'TerminationEffectiveDate',
                    'IndustryFocusGroup',
                    'LegalEntity',
                    'ContractID',
                    'ContractType',
                    'RadiationFlag',
                    'PositionCapacity',
                    'PreviousExperience',
                    'OvertimeEligibility',
                    'SuspendAssignmentCategory',
                    'Payroll',
                    'HealthcareProductLineEIT',
                    'JobType',
                    'CareerBand',
                    'AdjustedServiceDate',
                    'Work',
                    'HRMSSOID',
                    'HRMName',
                    'SpecialWorkSchedule',
                    'EducationLevel',
                    'WorkLocation',
                    'AssignmentEffectiveDate',
                    'HireEffectiveDate',
                    'RevTermEffectiveDate'],
            row=get_formated_user_row,
            delimiter='|'
        )

        create_collection_create_list_from_csv_10 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_10',
            source="{{ result('load_csv_create_list_from_csv_10') }}",
            name="inputfilerawdata",
            columns={
                "EmployeeFirstName": "EmployeeFirstName",
                "EmployeeLastName": "EmployeeLastName",
                "EmployeeEmailAddress": "EmployeeEmailAddress",
                "OHRID": "OHRID",
                "LegalEntityHireDate": "LegalEntityHireDate",
                "LegacyPayrollID": "LegacyPayrollID",
                "Job/PositionTitle": "Job_PositionTitle",
                "SupervisorSSOID": "SupervisorSSOID",
                "SupervisorName": "SupervisorName",
                "DWSStartDate": "DWSStartDate",
                "DWSMonday": "DWSMonday",
                "DWSTuesday": "DWSTuesday",
                "DWSWednesday": "DWSWednesday",
                "DWSThursday": "DWSThursday",
                "DWSFriday": "DWSFriday",
                "DWSSaturday": "DWSSaturday",
                "DWSSunday": "DWSSunday",
                "TerminationEffectiveDate": "TerminationEffectiveDate",
                "IndustryFocusGroup": "IndustryFocusGroup",
                "LegalEntity": "LegalEntity",
                "ContractID": "ContractID",
                "ContractType": "ContractType",
                "RadiationFlag": "RadiationFlag",
                "PositionCapacity": "PositionCapacity",
                "PreviousExperience": "PreviousExperience",
                "OvertimeEligibility": "OvertimeEligibility",
                "SuspendAssignmentCategory": "SuspendAssignmentCategory",
                "Payroll": "Payroll",
                "HealthcareProductLineEIT": "HealthcareProductLineEIT",
                "JobType": "JobType",
                "CareerBand": "CareerBand",
                "AdjustedServiceDate": "AdjustedServiceDate",
                "Work": "Work",
                "HRMSSOID": "HRMSSOID",
                "HRMName": "HRMName",
                "SpecialWorkSchedule": "SpecialWorkSchedule",
                "EducationLevel": "EducationLevel",
                "WorkLocation": "WorkLocation",
                "AssignmentEffectiveDate": "AssignmentEffectiveDate",
                "HireEffectiveDate": "HireEffectiveDate",
                "RevTermEffectiveDate": "RevTermEffectiveDate"
            }
        )

        if_parse_csv_10_10_lines_less_than_1_11 = rail.IfOperator(
            task_id='if_parse_csv_10_10_lines_less_than_1_11',
            test='{{ result("create_collection_create_list_from_csv_10", "length") == 0 }}',
            yes_task="send_mail_send_emailfor_blankfile_filewithnorecords_12",
            no_task="trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019",
        )

        send_mail_send_emailfor_blankfile_filewithnorecords_12 = rail.EmailOperator(
            task_id='send_mail_send_emailfor_blankfile_filewithnorecords_12',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | netherlands Republic User import - file processing is skipped - {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }} ''',
            html_content='''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
            <p>Hi ,</p>
            <p>The User import is completed on {{ current_time_in_specified_tz("US/Pacific", "%m/%d/%YT%H:%M:%S") }}. There were no records in the file - {{ result('new_file_sensor') | file_name }} to be processed.</p>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p> ''',
            params=None,
        )

        trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_user_sync_ge_netherlands_schedule_add_master_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "inputdata": '''{{ result('load_csv_create_list_from_csv_10') }}'''
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019") }}'
        )

        trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_user_sync_netherlands_suspend_assignment_category_custom_fieldv1_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "inputdata": '''{{ result('load_csv_create_list_from_csv_10') }}'''
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021") }}'
        )

        _adhoc_http_action_16 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_16',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments"
        )

        netherlands_master_mapper_search_entries_19 = rail.PythonOperator(
            task_id='netherlands_master_mapper_search_entries_19',
            python_callable=lambda:  list(
                filter(lambda x: x['type'] == "Department", netherlands_master_mapper))
        )

        log_required_department_name_20 = rail.PythonOperator(
            task_id='log_required_department_name_20',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'netherlands_master_mapper_search_entries_19'), 'type', 'Department', 'value', "")
        )

        log_required_department_uri_21 = rail.PythonOperator(
            task_id='log_required_department_uri_21',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_16'), 'name', rail.result('log_required_department_name_20'), 'uri', "")
            if rail.result('_adhoc_http_action_16') else None
        )

        supervisor_processing_log = rail.CreateLogOperator(
            task_id='supervisor_processing_log',
        )

        process_user_22 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_user_22',
            retries=0,
            items="{{ result('create_collection_create_list_from_csv_10') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'ge_netherlands_process_each_user_dag_{config.instance}',
            conf=lambda item: {
                "EmployeeFirstName": item['EmployeeFirstName'] if item['EmployeeFirstName'] else None,
                "EmployeeLastName": item['EmployeeLastName'] if item['EmployeeLastName'] else None,
                "EmployeeEmailAddress": item['EmployeeEmailAddress'] if item['EmployeeEmailAddress'] else None,
                "Job_PositionTitle": item['Job_PositionTitle'] if item['Job_PositionTitle'] else None,
                "SupervisorSSOID": item['SupervisorSSOID'] if item['SupervisorSSOID'] else None,
                "SupervisorName": item['SupervisorName'] if item['SupervisorName'] else None,
                "DWSMonday": item['DWSMonday'] if item['DWSMonday'] else None,
                "DWSTuesday": item['DWSTuesday'] if item['DWSTuesday'] else None,
                "DWSWednesday": item['DWSWednesday'] if item['DWSWednesday'] else None,
                "DWSThursday": item['DWSThursday'] if item['DWSThursday'] else None,
                "DWSFriday": item['DWSFriday'] if item['DWSFriday'] else None,
                "DWSSaturday": item['DWSSaturday'] if item['DWSSaturday'] else None,
                "DWSSunday": item['DWSSunday'] if item['DWSSunday'] else None,
                "DWSStartDate": item['DWSStartDate'] if item['DWSStartDate'] else None,
                "OHRID": item['OHRID'] if item['OHRID'] else None,
                "LegalEntity": item['LegalEntity'] if item['LegalEntity'] else None,
                "RevTermEffectiveDate": item['RevTermEffectiveDate'] if item['RevTermEffectiveDate'] else None,
                "TerminationEffectiveDate": item['TerminationEffectiveDate'] if item['TerminationEffectiveDate'] else None,
                "HRMSSOID": item['HRMSSOID'] if item['HRMSSOID'] else None,
                "HRMName": item['HRMName'] if item['HRMName'] else None,
                "SuspendAssignmentCategory": item['SuspendAssignmentCategory'] if item['SuspendAssignmentCategory'] else None,
                "AssignmentEffectiveDate": item['AssignmentEffectiveDate'] if item['AssignmentEffectiveDate'] else None,
                "LegalEntityHireDate": item['LegalEntityHireDate'] if item['LegalEntityHireDate'] else None,
                "HireEffectiveDate": item['HireEffectiveDate'] if item['HireEffectiveDate'] else None,
                "LegacyPayrollID": item['LegacyPayrollID'] if item['LegacyPayrollID'] else None,
                "IndustryFocusGroup": item['IndustryFocusGroup'] if item['IndustryFocusGroup'] else None,
                "ContractID": item['ContractID'] if item['ContractID'] else None,
                "RadiationFlag": item['RadiationFlag'] if item['RadiationFlag'] else None,
                "PositionCapacity": item['PositionCapacity'] if item['PositionCapacity'] else None,
                "Payroll": item['Payroll'] if item['Payroll'] else None,
                "HealthcareProductLineEIT": item['HealthcareProductLineEIT'] if item['HealthcareProductLineEIT'] else None,
                "JobType": item['JobType'] if item['JobType'] else None,
                "CareerBand": item['CareerBand'] if item['CareerBand'] else None,
                "AdjustedServiceDate": item['AdjustedServiceDate'] if item['AdjustedServiceDate'] else None,
                "Work": item['Work'] if item['Work'] else None,
                "LocationName": item['WorkLocation'] if item['WorkLocation'] else None,
                "SpecialWorkSchedule": item['SpecialWorkSchedule'] if item['SpecialWorkSchedule'] else None,
                "EducationLevel": item['EducationLevel'] if item['EducationLevel'] else None,
                "OvertimeEligibility": item['OvertimeEligibility'] if item['OvertimeEligibility'] else None,
                "Departmenturi": rail.result('log_required_department_uri_21'),
                "supervisor_processing_log": rail.result('supervisor_processing_log'),
            }
        )

        wait_for_completion_process_user_22 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_process_user_22',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_user_22") }}'
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_supervisor_entries():
            supervisor_details = []
            supervisor_log_informations = get_data_from_document(
                rail.result('supervisor_processing_log'))
            for supervisor_info in supervisor_log_informations:
                if supervisor_info['properties']:
                    supervisor_details.append({
                        "username": supervisor_info['properties'].get('username'),
                        "useruri": supervisor_info['properties'].get('useruri'),
                        "supervisorloginname": supervisor_info['properties'].get('supervisorloginname'),
                        "action": supervisor_info['properties'].get('action'),
                        "supervisoreffectivedate": supervisor_info['properties'].get('supervisoreffectivedate'),
                        "supervisorusername": supervisor_info['properties'].get('supervisorusername'),
                    })
            return supervisor_details

        ge_supervisor_assignment_table_search_entries_41 = rail.PythonOperator(
            task_id='ge_supervisor_assignment_table_search_entries_41',
            python_callable=get_supervisor_entries
        )

        if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42 = rail.IfOperator(
            task_id='if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42',
            test='''{{ result('ge_supervisor_assignment_table_search_entries_41') | length > 0 }}''',
            yes_task="declare_list_dag_runs_43",
            no_task="trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53",
        )

        declare_list_dag_runs_43 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_43',
            name='super_process_dag_runs',
            value=[]
        )

        log_getalltheuniqsupervisor_43 = rail.PythonOperator(
            task_id='log_getalltheuniqsupervisor_43',
            python_callable=lambda:  list(set(map(lambda record: record['supervisorloginname'], rail.result(
                'ge_supervisor_assignment_table_search_entries_41'))))
        )

        def get_superuer_info(user_name):
            all_super_user = rail.result(
                'ge_supervisor_assignment_table_search_entries_41')
            super_user_info = list(
                filter(lambda x: x['supervisorloginname'] == user_name, all_super_user))
            return super_user_info[0] if super_user_info else {}

        def get_foreign_super_info():
            foreign_super_user = []
            unique_super_user = rail.result('log_getalltheuniqsupervisor_43')
            for user_name in unique_super_user:
                super_user_info = get_superuer_info(user_name)
                foreign_super_user.append({
                    "OHRID": super_user_info['username'],
                    "supervisorloginname": user_name,
                    "supervisorname": super_user_info['supervisorusername']
                })
            return foreign_super_user

        trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049',
            retries=0,
            items=get_foreign_super_info,
            trigger_dag_id=f'ge_netherlands_child_add_foreign_supervisor_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "OHRID": "{{ item.OHRID }}",
                "supervisorloginname": "{{ item.supervisorloginname }}",
                "supervisorname": "{{ item.supervisorname }}",
                "foreignsupervisordepartmenturi": "{{ result('log_required_department_uri_21') }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049") }}'
        )

        trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53',
            retries=0,
            items="{{ result('ge_supervisor_assignment_table_search_entries_41') | to_json }}",
            trigger_dag_id=f'ge_netherlands_child_add_supervisor_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "loginname": "{{ item.username }}",
                "supervisorloginname": "{{ item.supervisorloginname }}",
                "useruri": "{{ item.useruri }}",
                "action": "{{ item.action }}",
                "supervisoreffectivedate": "{{ item.supervisoreffectivedate }}",
                "supervisorusername": "{{ item.supervisorusername }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53") }}'
        )

        def do_format_logs():
            context = get_master_log_artifact_name(rail.get_current_context())
            user_import_log = rail.load_all_records(context)
            unique_users = list(
                set(map(lambda item: item['properties'].get(
                    "OHRID", ''), user_import_log))
            )

            def get_log_details(user_logs):
                return "|".join(list(filter(bool, (set(map(lambda x: x['properties']['details'], user_logs))))))

            def get_status_details(user_logs):
                return ";".join(list(filter(bool, (set(map(lambda x: x['properties']['status'], user_logs))))))

            logs = []
            # pylint: disable= cell-var-from-loop
            for employee_id in unique_users:
                if employee_id:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'OHRID', '') == employee_id, user_import_log)
                    )

                    if len(user_logs) > 0:
                        first = user_logs[0]
                        logs.append(
                            {
                                "OHRID": employee_id,
                                "UserName": first['properties'].get('username', ''),
                                "Action": first['properties'].get('action', ''),
                                "Status": get_status_details(user_logs),
                                "Details": get_log_details(user_logs),
                                "Jobid": first['properties'].get('child_job_id', '')
                            }
                        )
                else:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'OHRID', '') == '' or x['properties'].get(
                            'OHRID', '') is None, user_import_log)
                    )
                    for user in user_logs:
                        logs.append(
                            {
                                "OHRID": user['properties'].get('OHRID', ''),
                                "UserName": user['properties'].get('username', ''),
                                "Action": user['properties'].get('action', ''),
                                "Status": user['properties'].get('status', ''),
                                "Details": user['properties'].get('details', ''),
                                "Jobid": user['properties'].get('child_job_id', '')
                            }
                        )

            return logs

        log_merge_54 = rail.PythonOperator(
            task_id='log_merge_54',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: do_format_logs()
        )

        create_csv_lines_55 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_55',
            source="{{ result('log_merge_54') | to_json }}",
            header=['OHRID',
                    'Username',
                    'Action',
                    'Status',
                    'Details',
                    'JobID'],
            row=[
                '{{ item | attr_or_default("OHRID", "") }}',
                '{{ item | attr_or_default("UserName", "") }}',
                '{{ item | attr_or_default("Action", "") }}',
                '{{ item | attr_or_default("Status", "")}}',
                '{{ item | attr_or_default("Details", "") }}',
                '{{ item | attr_or_default("Jobid", "") }}'],
        )

        def file_upload_failed(context):
            subject = '''{{ get_company_key() }} | Replicon user import - Uploading Logs to SFTP failed  - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} '''
            body = '''<p>Hi Team,<br /> <br /> The 'Replicon user import job for {{get_company_key()}}, created on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} has been completed.however,
            the log upload to sftp has failed. Attached is the log file for reference.</p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> '''
            email = rail.EmailOperator(
                task_id='send_user_import_data_to_sftp_failure_email',
                to=config.tenant_email,
                subject=subject,
                html_content=body,
                files=[
                    ("{{ result('create_csv_lines_55') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_lines_55')}}",
            output_file_name="{{ dag_run_ecid() }}_UserImportLogs_{{ result('get_time_for_file') }}.csv",
            expires_in_seconds=7*24*60*60
        )

        upload_logs_57 = rail.SFTPUploadFileOperator(
            task_id='upload_logs_57',
            content="{{ result('create_csv_lines_55') }}",
            sftp_conn_id=config.internal_sftp_conn_id,
            remote_filepath=config.log_filepath +
            "/{{ dag_run_ecid() }}_UserImportLogs_{{ result('get_time_for_file') }}.csv",
            on_failure_callback=file_upload_failed
        )

        get_logged_errors_58 = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors_58',
            severity='Error',
        )

        get_logged_exception_59 = rail.FilterLogEntriesOperator(
            task_id='get_logged_exception_59',
            severity='Exception',
        )

        def get_subject_line():
            import_completion_message = "completed succesfully"
            has_error_message = rail.render_template(
                '{{result("get_logged_errors_58", key="length") > 0}}')
            has_exception_message = rail.render_template(
                '{{result("get_logged_exception_59", key="length") > 0}}')
            if has_error_message == 'True':
                import_completion_message = "completed with errors"
            elif has_exception_message == 'True':
                import_completion_message = "completed with exceptions"
            return import_completion_message

        email_subject_line_60 = rail.PythonOperator(
            task_id='email_subject_line_60',
            python_callable=get_subject_line
        )

        send_log_mail_61 = rail.EmailOperator(
            task_id='send_log_mail_61',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors_58', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Replicon user import is {{ result("email_subject_line_60") }} - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
            The Replicon user import is {{result("email_subject_line_60")}} on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}. <br /> <br />
            Please click on the below link to download the logs and review.<br /> <br /><a href="{{result('generate_downloadable_link')}}">Download log</a></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''',
            params={'log_file_path': config.log_filepath},
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_name_downcase_not_ends_with_pgp_3
        if_name_downcase_not_ends_with_pgp_3 >> rail.Label(
            'No') >> is_healthcare_invalid_file
        is_healthcare_invalid_file >> rail.Label(
            "No") >> archive_invalid_file_4 >> send_mail_send_emailfor_incorrect_fileformat_4
        is_healthcare_invalid_file >> rail.Label(
            "Yes") >> delete_file >> send_mail_send_emailfor_incorrect_fileformat_4 >> log_to_sumo
        if_name_downcase_not_ends_with_pgp_3 >> rail.Label('Yes') >> download_6 >> \
            decrypt_7 >> log_filenametouse_8 >> parse_csv_8 >> is_healthcare_file
        is_healthcare_file >> rail.Label(
            "Yes") >> delete_healthcare_file >> load_csv_create_list_from_csv_10
        is_healthcare_file >> rail.Label(
            "No") >> rename_move_input_file_to_archive_10 >> load_csv_create_list_from_csv_10 >> \
            create_collection_create_list_from_csv_10 >> if_parse_csv_10_10_lines_less_than_1_11
        if_parse_csv_10_10_lines_less_than_1_11 >> rail.Label(
            'Yes') >> send_mail_send_emailfor_blankfile_filewithnorecords_12 >> log_to_sumo
        if_parse_csv_10_10_lines_less_than_1_11 >> rail.Label('No') >> \
            trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019 >> wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_schedule_add_master_v1_019 >> \
            trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021 >> wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_suspend_assignment_category_custom_field_021 >> \
            _adhoc_http_action_16 >> netherlands_master_mapper_search_entries_19 >> \
            log_required_department_name_20 >> log_required_department_uri_21 >> \
            supervisor_processing_log >> process_user_22 >> wait_for_completion_process_user_22 >> \
            ge_supervisor_assignment_table_search_entries_41 >> \
            if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42
        if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42 >> rail.Label('Yes') >> declare_list_dag_runs_43 >> \
            log_getalltheuniqsupervisor_43 >> trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_foreign_supervisor_v1_049 >> \
            trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53
        if_ge_supervisor_assignment_table_search_entries_41_entries_greater_than_0_42 >> rail.Label('No') >> \
            trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_child_add_supervisor_v1_0async_53 >> \
            log_merge_54 >> create_csv_lines_55 >>\
            generate_downloadable_link >> upload_logs_57 >> get_logged_errors_58 >> get_logged_exception_59 >> email_subject_line_60 >> \
            send_log_mail_61 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
