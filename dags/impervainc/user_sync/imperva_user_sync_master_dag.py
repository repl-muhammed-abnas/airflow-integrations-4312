import json
from datetime import timedelta
from airflow.models import Variable
from impervainc.user_sync.utils import python_callable, request_payload
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_usersync_master,
        description=f'Imperva user sync - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_use_conf_payload = rail.IfOperator(
            task_id='can_use_conf_payload',
            test=lambda: Variable.get(
                config.can_use_conf_payload_var_name, default_var='false').lower() == 'true',
            yes_task='get_conf_payload',
            no_task='get_workdayreport_http_payload'
        )

        get_conf_payload = rail.PythonOperator(
            task_id='get_conf_payload',
            python_callable=lambda: json.dumps(rail.get_dag_run_conf())
        )

        get_workdayreport_http_payload = rail.SimpleHttpOperator(
            task_id='get_workdayreport_http_payload',
            method='GET',
            endpoint=config.workday_report_endpoint,
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        workdays_report_data = rail.PythonOperator(
            task_id='workdays_report_data',
            python_callable=lambda: json.loads(rail.result(
                'get_conf_payload') or rail.result('get_workdayreport_http_payload'))
        )

        dag_trigger_time = rail.PythonOperator(
            task_id='dag_trigger_time',
            python_callable=python_callable.dag_trigger_time
        )

        imperva_user_sync_logs = rail.CreateLogOperator(
            task_id="imperva_user_sync_logs"
        )

        imperva_supervisor_sync_logs = rail.CreateLogOperator(
            task_id="imperva_supervisor_sync_logs"
        )

        if_get_report_2_report_less_than_1_3=rail.IfOperator(
            task_id='if_get_report_2_report_less_than_1_3',
            test='''{{result('workdays_report_data') | length < 1}}''',
            yes_task="send_mail_4",
            no_task="if_get_report_2_report_greater_than_0_6",
        )

        send_mail_4=rail.EmailOperator(
            task_id='send_mail_4',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import has been skipped - {{ result('dag_trigger_time').y_m_d_h_m_s }} ''',
            html_content= "templates/emails/send_skipped_email.html",
        )

        if_get_report_2_report_greater_than_0_6=rail.IfOperator(
            task_id='if_get_report_2_report_greater_than_0_6',
            test='''{{result('workdays_report_data') | length > 0}}''',
            yes_task="variable_trigger_custom_field_dag_ids",
            no_task="create_csv_lines_composewithmd5_value",
        )

        variable_trigger_custom_field_dag_ids = rail.SetVariableOperator(
            task_id='variable_trigger_custom_field_dag_ids',
            append=False,
            name='trigger_custom_field_dag_ids',
            value=[]
        )

        trigger_imperva_department_and_cost_center_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_department_and_cost_center_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_department_and_cost_center_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_7 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_7',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_department_and_cost_center_check_child')}}"
        )

        get_rit_user_reference_details = rail.RepliconReportDetailsOperator(
            task_id='get_rit_user_reference_details',
            report_name=config.rit_user_reference_report,
        )

        run_rit_user_reference_report_entry, run_rit_user_reference_report_exit = rail.run_report(
            group_id='run_rit_user_reference_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_rit_user_reference_details').uri}}"
                    }
                ]
            }
        )

        get_rit_dept_lookup_details = rail.RepliconReportDetailsOperator(
            task_id='get_rit_dept_lookup_details',
            report_name=config.rit_dept_lookup_report,
        )

        run_rit_dept_lookup_report_entry, run_rit_dept_lookup_report_exit = rail.run_report(
            group_id='run_rit_dept_lookup_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_rit_dept_lookup_details').uri}}"
                    }
                ]
            }
        )

        trigger_imperva_organization_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_organization_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_organization_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_13 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_13',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_organization_custom_field_check_child')}}"
        )

        trigger_imperva_state_iso_code_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_state_iso_code_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_state_iso_code_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_14 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_14',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_state_iso_code_custom_field_check_child')}}"
        )

        trigger_imperva_work_state_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_work_state_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_work_state_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_15 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_15',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_work_state_custom_field_check_child')}}"
        )

        trigger_imperva_country_iso_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_country_iso_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_country_iso_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_16 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_16',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_country_iso_custom_field_check_child')}}"
        )

        trigger_imperva_work_country_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_work_country_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_work_country_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_17 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_17',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_work_country_custom_field_check_child')}}"
        )

        trigger_imperva_time_type_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_time_type_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_time_type_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_18 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_18',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_time_type_custom_field_check_child')}}"
        )

        trigger_imperva_employee_type_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_employee_type_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_employee_type_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_19 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_19',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_employee_type_custom_field_check_child')}}"
        )

        trigger_imperva_worker_type_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_worker_type_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_worker_type_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_20 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_20',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_worker_type_custom_field_check_child')}}"
        )

        trigger_imperva_payrate_type_custom_field_check_child = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_payrate_type_custom_field_check_child',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_payrate_type_custom_field_check_child,
            conf=lambda dag_run: {
                "workdayreportlink": config.workday_report_endpoint,
                "user_sync_log": rail.result('imperva_user_sync_logs'),
                "supervisor_sync_log": rail.result('imperva_supervisor_sync_logs'),
                "conf":dag_run.conf
            }
        )

        insert_to_trigger_dag_ids_21 = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_21',
            append=True,
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}',
            value="{{result('trigger_imperva_payrate_type_custom_field_check_child')}}"
        )

        get_variable_trigger_custom_field_dag_ids = rail.GetVariableOperator(
            task_id='get_variable_trigger_custom_field_dag_ids',
            name='{{ result("variable_trigger_custom_field_dag_ids").name }}'
        )

        wait_for_variable_trigger_custom_field_dag_ids = rail.WaitForDagRunsSensor(
            task_id='wait_for_variable_trigger_custom_field_dag_ids',
            dag_runs='{{ result("get_variable_trigger_custom_field_dag_ids").value | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_all_cost_center = rail.RepliconServiceOperator(
            task_id="get_all_cost_center",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        create_csv_lines_composewithmd5_value=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_composewithmd5_value',
            source="{{result('workdays_report_data')['report'] | to_json }}",
            header=['Status',
                'Employee ID',
                'Legal first name',
                'Legal last name',
                'Primary work email',
                'Username',
                'Authentication ID',
                'Hire date',
                'Original hire date',
                'Termination date',
                'Manager',
                'Imperva worker type',
                'Imperva employee type',
                'Time type',
                'Pay rate type',
                'Hourly pay',
                'Currency',
                'Job code',
                'Cost Center ID',
                'Cost Center Name',
                'Imperva Organization',
                'Time zone of location',
                'Work address country',
                'Country ISO Code',
                'Work Address State Province',
                'State ISO Code',
                'Exempt Status',
                'Is manager',
                'md5'],
            row= lambda item: [
                item['Status'],
                item['Employee_ID'],
                item['Legal_First_Name'],
                item['Legal_Last_Name'],
                item['primaryWorkEmail'],
                item['Username'],
                item['Authentication_ID'],
                item['Hire_Date'],
                item['Original_Hire_Date'],
                item['termination_date'],
                item['Manager'],
                item['Imperva_Worker_Type'],
                item['Imperva_Employee_Type'],
                item['Time_Type'],
                item['Pay_Rate_Type'],
                item['Hourly_Pay'],
                item['Currency'],
                item['Job_Code'],
                item['Cost_Center_ID'],
                item['Cost_Center_Name'],
                item['Imperva_Organization'],
                item['Time_Zone_of_Location_of_Worker_s_Primary_Position'],
                item['Work_Address_Country'],
                item['Country_ISO_Code'],
                item['Work_Address_State_Province'],
                item['State_ISO_Code'],
                item['Exempt_Status'],
                item['isManager'],
                python_callable.get_md5(item)
            ],
        )

        load_user_reference_report_data = rail.LoadCSVFileOperator(
            task_id='load_user_reference_report_data',
            document="{{ result('run_rit_user_reference_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        load_rit_dept_lookup_report_data = rail.LoadCSVFileOperator(
            task_id='load_rit_dept_lookup_report_data',
            document="{{ result('run_rit_dept_lookup_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_collection_sourceuserdata = rail.CreateCollectionOperator(
            task_id='create_collection_sourceuserdata',
            source = "{{ result('create_csv_lines_composewithmd5_value') }}",
            name = "sourceuserdata",
            columns = {
                'Status':'status', 
                'Employee ID':'Employee_ID', 
                'Legal first name':'Legal_First_Name', 
                'Legal last name':'Legal_Last_Name', 
                'Primary work email':'primaryWorkEmail', 
                'Username':'Username', 
                'Authentication ID':'Authentication_ID', 
                'Hire date':'Hire_Date', 
                'Original hire date':'Original_Hire_Date', 
                'Termination date':'termination_date', 
                'Manager':'Manager', 
                'Imperva worker type':'Imperva_Worker_Type', 
                'Imperva employee type':'Imperva_Employee_Type', 
                'Time type':'Time_Type', 
                'Pay rate type':'Pay_Rate_Type', 
                'Hourly pay':'Hourly_Pay', 
                'Currency':'Currency', 
                'Job code':'Job_Code', 
                'Cost Center ID':'Cost_Center_ID', 
                'Cost Center Name':'Cost_Center_Name', 
                'Imperva Organization':'Imperva_Organization', 
                'Time zone of location':'timezone', 
                'Work address country':'Work_Address_Country', 
                'Country ISO Code':'Country_ISO_Code', 
                'Work Address State Province':'Work_Address_State_Province', 
                'State ISO Code':'State_ISO_Code', 
                'Exempt Status':'Exempt_Status', 
                'Is manager':'isManager', 
                'md5':'md_5'
            }
        )

        if_parameters_usereferencefile_contains_y=rail.IfOperator(
            task_id='if_parameters_usereferencefile_contains_y',
            test=lambda: config.usereferencefile == 'Yes',
            yes_task="dir_user_reference",
            no_task="finish",
        )

        dir_user_reference=rail.SFTPListFilesOperator(
            task_id='dir_user_reference',
            paths=[config.reference_filepath],
        )

        if_user_referencefile_present = rail.IfOperator(
            task_id='if_user_referencefile_present',
            test=lambda: bool(rail.result('dir_user_reference')[config.reference_filepath][0]['name'] if rail.result(
                'dir_user_reference') and rail.result(
                'dir_user_reference')[config.reference_filepath][0] else null),
            yes_task="user_reference_filename",
            no_task="finish",
        )

        user_reference_filename = rail.PythonOperator(
            task_id='user_reference_filename',
            python_callable=lambda: rail.result('dir_user_reference')[config.reference_filepath][0]['name']
        )

        is_file_csv = rail.IfOperator(
            task_id="is_file_csv",
            test="{{result('user_reference_filename') | file_ext | lower == 'csv'}}",
            yes_task="download_file",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath +"/{{result('user_reference_filename')}}"
        )

        parse_csv_data = rail.LoadCSVFileOperator(
            task_id='parse_csv_data',
            document="{{result('download_file')}}",
        )

        create_collection_referenceuserdata = rail.CreateCollectionOperator(
            task_id='create_collection_referenceuserdata',
            source = "{{ result('parse_csv_data') }}",
            name = "referenceuserdata",
            columns = {
                'Status':'status', 
                'Employee ID':'Employee_ID', 
                'Legal first name':'Legal_First_Name', 
                'Legal last name':'Legal_Last_Name', 
                'Primary work email':'primaryWorkEmail', 
                'Username':'Username', 
                'Authentication ID':'Authentication_ID', 
                'Hire date':'Hire_Date', 
                'Original hire date':'Original_Hire_Date', 
                'Termination date':'termination_date', 
                'Manager':'Manager', 
                'Imperva worker type':'Imperva_Worker_Type', 
                'Imperva employee type':'Imperva_Employee_Type', 
                'Time type':'Time_Type', 
                'Pay rate type':'Pay_Rate_Type', 
                'Hourly pay':'Hourly_Pay', 
                'Currency':'Currency', 
                'Job code':'Job_Code', 
                'Cost Center ID':'Cost_Center_ID', 
                'Cost Center Name':'Cost_Center_Name', 
                'Imperva Organization':'Imperva_Organization', 
                'Time zone of location':'timezone', 
                'Work address country':'Work_Address_Country', 
                'Country ISO Code':'Country_ISO_Code', 
                'Work Address State Province':'Work_Address_State_Province', 
                'State ISO Code':'State_ISO_Code', 
                'Exempt Status':'Exempt_Status', 
                'Is manager':'isManager', 
                'md5':'md_5'
            }
        )

        query_list_getdeltavalues=rail.QueryCollectionOperator(
            task_id='query_list_getdeltavalues',
            query="""SELECT * FROM  sourceuserdata WHERE  sourceuserdata.md_5 NOT IN (SELECT DISTINCT  referenceuserdata.md_5 FROM  referenceuserdata)""",
        )

        if_first_legal_first_name_present=rail.IfOperator(
            task_id='if_first_legal_first_name_present',
            test=lambda: bool(rail.load_all_records(rail.result('query_list_getdeltavalues'))[0]['Legal_First_Name'] if rail.load_all_records(
                rail.result('query_list_getdeltavalues')) and rail.load_all_records(
                rail.result('query_list_getdeltavalues'))[0] else null),
            yes_task="variable_trigger_dag_ids",
            no_task="finish",
        )

        variable_trigger_dag_ids = rail.SetVariableOperator(
            task_id='variable_trigger_dag_ids',
            append=False,
            name='trigger_dag_ids',
            value=[]
        )

        foreach_query_list_getdeltavalues=rail.ForEachOperator(
            task_id='foreach_query_list_getdeltavalues',
            items="{{ result('query_list_getdeltavalues') | load_all_records() | to_json }}",
            start_task = 'if_username_presence_present',
            end_task = 'foreach_query_list_getdeltavalues_end'
        )

        if_username_presence_present=rail.IfOperator(
            task_id='if_username_presence_present',
            test=lambda: python_callable.get_if_username_present(rail.result('foreach_query_list_getdeltavalues')['Username']),
            yes_task="trigger_impervainc_user_sync_updateasync",
            no_task="trigger_impervainc_user_sync_addsync",
        )

        trigger_impervainc_user_sync_updateasync=rail.TriggerDagRunOperator(
            task_id='trigger_impervainc_user_sync_updateasync',
            trigger_dag_id=config.imperva_usersync_update,
            execution_timeout=timedelta(days=14),
            conf=lambda: request_payload.update_usersync_payload(rail.result('foreach_query_list_getdeltavalues'))
        )

        insert_to_trigger_dag_ids_update = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_update',
            append=True,
            name='{{ result("variable_trigger_dag_ids").name }}',
            value="{{result('trigger_impervainc_user_sync_updateasync')}}"
        )

        trigger_impervainc_user_sync_addsync=rail.TriggerDagRunOperator(
            task_id='trigger_impervainc_user_sync_addsync',
            trigger_dag_id=config.imperva_usersync_add,
            execution_timeout=timedelta(days=14),
            conf=lambda: request_payload.add_usersync_payload(rail.result('foreach_query_list_getdeltavalues'))
        )

        insert_to_trigger_dag_ids_create = rail.SetVariableOperator(
            task_id='insert_to_trigger_dag_ids_create',
            append=True,
            name='{{ result("variable_trigger_dag_ids").name }}',
            value="{{result('trigger_impervainc_user_sync_addsync')}}"
        )

        foreach_query_list_getdeltavalues_end = rail.EmptyOperator(
            task_id='foreach_query_list_getdeltavalues_end'
        )

        get_variable_trigger_dag_ids = rail.GetVariableOperator(
            task_id='get_variable_trigger_dag_ids',
            name='{{ result("variable_trigger_dag_ids").name }}'
        )

        wait_for_variable_trigger_dag_ids = rail.WaitForDagRunsSensor(
            task_id='wait_for_variable_trigger_dag_ids',
            dag_runs='{{ result("get_variable_trigger_dag_ids").value | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        entries_present_in_supervisor_sync_log=rail.FilterLogEntriesOperator(
            task_id='entries_present_in_supervisor_sync_log',
            log="{{result('imperva_supervisor_sync_logs')}}"
        )

        if_supervisor_sync_log_present = rail.IfOperator(
            task_id='if_supervisor_sync_log_present',
            test="{{ result('entries_present_in_supervisor_sync_log', 'length') > 0 }}",
            yes_task='trigger_supervisor_assignment_child',
            no_task='entries_present_in_user_sync_log'
        )

        trigger_supervisor_assignment_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_supervisor_assignment_child',
            items='{{ result("entries_present_in_supervisor_sync_log") }}',
            trigger_dag_id=config.imperva_supervisor_assignment_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_supervisor_assignment_payload
        )

        wait_for_supervisor_assignment_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_assignment_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_supervisor_assignment_child") }}'
        )

        entries_present_in_user_sync_log=rail.FilterLogEntriesOperator(
            task_id='entries_present_in_user_sync_log',
            log="{{result('imperva_user_sync_logs')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        compose_user_sync_logs_csv = rail.WriteCSVFileOperator(
            task_id="compose_user_sync_logs_csv",
            source='{{result("imperva_user_sync_logs")}}',
            header=["Job ID", "Child Job ID", "Country", "User Name",
                    "Employee ID", "Status", "Action", "Reason"],
            row=[
                '{{ item.properties | attr_or_default("parentjobid", "") }}',
                '{{ item.properties | attr_or_default("childjobid", "") }}',
                '{{ item.properties | attr_or_default("country", "") }}',
                '{{ item.properties | attr_or_default("loginname", "") }}',
                '{{ item.properties | attr_or_default("employeeid", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.properties | attr_or_default("action", "") }}',
                '{{ item.properties | attr_or_default("reason", "") }}',
            ]
        )

        upload_user_sync_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_user_sync_logs_to_sftp',
            content="{{ result('compose_user_sync_logs_csv') }}",
            remote_filepath=config.usersynclogs_filepath + "/userimport_log_{{ result('dag_trigger_time').y_m_d_h_m_s }}.csv",
        )

        catch_error=rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        send_mail_uploading_fail=rail.EmailOperator(
            task_id='send_mail_uploading_fail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} |  Failed while uploading Logs to SFTP  - {{ result('dag_trigger_time').y_m_d_h_m_s }}''',
            html_content= "templates/emails/send_logs_upload_fail_email.html",
            params={
                'dag_id': config.imperva_usersync_master
            },
            files=[
                ("userimport_log_{{ result('dag_trigger_time').y_m_d_h_m_s }}.csv", "{{ result('compose_user_sync_logs_csv') }}")]
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath +
            '/{{ result("user_reference_filename") }}',
            existing_filename=config.reference_filepath +
            '/{{ result("user_reference_filename") }}'
        )

        upload_new_reference_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file_to_sftp',
            content="{{ result('create_csv_lines_composewithmd5_value') }}",
            remote_filepath=config.reference_filepath + "/newreference{{ result('dag_trigger_time').y_m_d_h_m_s }}.csv",
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('compose_user_sync_logs_csv')}}",
            output_file_name="userimport_log_{{result('dag_trigger_time').y_m_d_h_m_s}}.csv",
            expires_in_seconds=7*24*60*60
        )

        error_present_in_user_sync_log=rail.FilterLogEntriesOperator(
            task_id='error_present_in_user_sync_log',
            log="{{result('imperva_user_sync_logs')}}",
            properties={
                'status': "Error"
            }
        )

        if_any_errors_present_in_user_sync_log = rail.IfOperator(
            task_id='if_any_errors_present_in_user_sync_log',
            test="{{ result('error_present_in_user_sync_log', 'length') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='warning_present_in_user_sync_log'
        )

        warning_present_in_user_sync_log=rail.FilterLogEntriesOperator(
            task_id='warning_present_in_user_sync_log',
            log="{{result('imperva_user_sync_logs')}}",
            properties={
                'status': "Warning"
            }
        )

        if_any_warnings_present_in_user_sync_log = rail.IfOperator(
            task_id='if_any_warnings_present_in_user_sync_log',
            test="{{ result('warning_present_in_user_sync_log', 'length') > 0 }}",
            yes_task='send_completion_warning_mail',
            no_task='send_completion_sucussful_mail'
        )

        send_completion_error_mail=rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import completed with errors - {{ result('dag_trigger_time').y_m_d_h_m_s }} ''',
            html_content= "templates/emails/send_with_error_email.html",
        )

        send_completion_warning_mail=rail.EmailOperator(
            task_id='send_completion_warning_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import completed with warning - {{ result('dag_trigger_time').y_m_d_h_m_s }} ''',
            html_content= "templates/emails/send_with_warning_email.html",
        )

        send_completion_sucussful_mail=rail.EmailOperator(
            task_id='send_completion_sucussful_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import completed successfully  - {{ result('dag_trigger_time').y_m_d_h_m_s }} ''',
            html_content= "templates/emails/send_completion_successful_email.html",
        )

        archive_file_after_completion = rail.SFTPMoveFileOperator(
            task_id='archive_file_after_completion',
            new_filename=config.archive_filepath +
            '/{{ result("user_reference_filename") }}',
            existing_filename=config.reference_filepath +
            '/{{ result("user_reference_filename") }}'
        )

        upload_new_reference_file_to_sftp_70 = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file_to_sftp_70',
            content="{{ result('create_csv_lines_composewithmd5_value') }}",
            remote_filepath=config.reference_filepath + "/newreference{{ result('dag_trigger_time').y_m_d_h_m_s }}.csv",
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_use_conf_payload >> rail.Label("Yes") >> get_conf_payload >> workdays_report_data
        can_use_conf_payload >> rail.Label("No") >> get_workdayreport_http_payload >> workdays_report_data >> dag_trigger_time >> \
        imperva_user_sync_logs >> imperva_supervisor_sync_logs >> if_get_report_2_report_less_than_1_3 >> rail.Label(
            "Yes") >> send_mail_4 >> finish
        if_get_report_2_report_less_than_1_3 >> rail.Label(
            "No") >> if_get_report_2_report_greater_than_0_6 >> rail.Label("Yes") >> variable_trigger_custom_field_dag_ids >> \
        trigger_imperva_department_and_cost_center_check_child >> \
        insert_to_trigger_dag_ids_7 >> get_rit_user_reference_details >> run_rit_user_reference_report_entry
        run_rit_user_reference_report_exit >> get_rit_dept_lookup_details >> run_rit_dept_lookup_report_entry
        run_rit_dept_lookup_report_exit >> \
        trigger_imperva_organization_custom_field_check_child >> \
        insert_to_trigger_dag_ids_13 >> trigger_imperva_state_iso_code_custom_field_check_child >> \
        insert_to_trigger_dag_ids_14 >> trigger_imperva_work_state_custom_field_check_child >> \
        insert_to_trigger_dag_ids_15 >> trigger_imperva_country_iso_custom_field_check_child >> \
        insert_to_trigger_dag_ids_16 >> trigger_imperva_work_country_custom_field_check_child >> \
        insert_to_trigger_dag_ids_17 >> trigger_imperva_time_type_custom_field_check_child >> \
        insert_to_trigger_dag_ids_18 >> trigger_imperva_employee_type_custom_field_check_child >> \
        insert_to_trigger_dag_ids_19 >> trigger_imperva_worker_type_custom_field_check_child >> \
        insert_to_trigger_dag_ids_20 >> trigger_imperva_payrate_type_custom_field_check_child >> \
        insert_to_trigger_dag_ids_21 >> get_variable_trigger_custom_field_dag_ids >> wait_for_variable_trigger_custom_field_dag_ids >> \
        get_all_cost_center >> create_csv_lines_composewithmd5_value
        if_get_report_2_report_greater_than_0_6 >> rail.Label("No") >> create_csv_lines_composewithmd5_value >> load_user_reference_report_data >> \
        load_rit_dept_lookup_report_data >> create_collection_sourceuserdata >> if_parameters_usereferencefile_contains_y >> rail.Label(
            "Yes") >> dir_user_reference >> if_user_referencefile_present >> rail.Label("Yes") >> user_reference_filename >> is_file_csv >> rail.Label(
            "Yes") >> download_file >> parse_csv_data >> create_collection_referenceuserdata >> query_list_getdeltavalues >> \
        if_first_legal_first_name_present >> rail.Label("Yes") >> variable_trigger_dag_ids >> foreach_query_list_getdeltavalues >> \
        if_username_presence_present >> rail.Label(
            "Yes") >> trigger_impervainc_user_sync_updateasync >> insert_to_trigger_dag_ids_update >> foreach_query_list_getdeltavalues_end
        if_username_presence_present >> rail.Label(
            "No") >> trigger_impervainc_user_sync_addsync >> insert_to_trigger_dag_ids_create >> foreach_query_list_getdeltavalues_end
        foreach_query_list_getdeltavalues >> foreach_query_list_getdeltavalues_end >> get_variable_trigger_dag_ids >> wait_for_variable_trigger_dag_ids >> \
        entries_present_in_supervisor_sync_log >> if_supervisor_sync_log_present >> rail.Label(
            "Yes") >> trigger_supervisor_assignment_child >> wait_for_supervisor_assignment_child >> entries_present_in_user_sync_log
        if_supervisor_sync_log_present >> rail.Label(
            "No") >> entries_present_in_user_sync_log
        entries_present_in_user_sync_log >> compose_user_sync_logs_csv >> \
        upload_user_sync_logs_to_sftp >> catch_error >> send_mail_uploading_fail >> \
        archive_file >> upload_new_reference_file_to_sftp >> finish
        upload_user_sync_logs_to_sftp >> generate_downloadable_link >> error_present_in_user_sync_log >> \
        if_any_errors_present_in_user_sync_log >> rail.Label("Yes") >> \
        send_completion_error_mail >> archive_file_after_completion
        if_any_errors_present_in_user_sync_log >> rail.Label("No") >> warning_present_in_user_sync_log >> \
        if_any_warnings_present_in_user_sync_log >> rail.Label("Yes") >> send_completion_warning_mail >> archive_file_after_completion
        if_any_warnings_present_in_user_sync_log >> rail.Label("No") >> send_completion_sucussful_mail >> archive_file_after_completion
        archive_file_after_completion >> upload_new_reference_file_to_sftp_70 >> finish
        if_first_legal_first_name_present >> rail.Label("Yes") >> finish
        if_user_referencefile_present >> rail.Label("No") >> finish
        if_parameters_usereferencefile_contains_y >> rail.Label(
            "No") >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
