
from datetime import datetime, timedelta
import itertools
import json
from pendulum import datetime as pendulum_datetime
from airflow.models import Variable
import rail

from horizonmedia.user_import_v1.horizonmedia_activity_mapper_v3_0_mapper import horizonmedia_activity_mapper_v3_0
from horizonmedia.user_import_v1.horizonmedia_user_import_master_mapper import horizonmedia_user_import_master_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.horizonmedia_user_import_master,
        description=f'HorizonMedia_User import - Master V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum_datetime(
            2022, 10, 10, tz=config.schedule_time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='can_use_conf_payload'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='can_use_conf_payload',
            end_task='upload_logs_sftp',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

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
            # endpoint="https://services1.myworkday.com/ccx/service/customreport2/horizonmedia/ISU_Replicon_Demographic/Replicon_Demographic_File?format=json",
            http_conn_id=config.http_conn_id,
            headers={
                "Content-Type": 'application/json; charset=utf-8'
            },
            extra_options={
                'verify': False
            }
        )

        workdayreport_4 = rail.PythonOperator(
            task_id='workdayreport_4',
            python_callable=lambda: json.loads(rail.result(
                'get_conf_payload') or rail.result('get_workdayreport_http_payload'))
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        if_first_employee_id_blank_1_5 = rail.IfOperator(
            task_id='if_first_employee_id_blank_1_5',
            test='''{{ result('workdayreport_4') | is_falsy or result('workdayreport_4')['Report_Entry'] | length == 0  or result('workdayreport_4')['Report_Entry'][0].Employee_ID | is_falsy }}''',
            yes_task="send_mail_6",
            no_task="create_csv_lines_create_inputfilewith_m_d5_r_a_a_sdata_10",
        )

        send_mail_6 = rail.EmailOperator(
            task_id='send_mail_6',
            to=config.tenant_email,
            subject='''{{ get_company_key() }} | Replicon user import skipped - Blank File - {{ current_time() }}''',
            html_content='''<p>Hello, <br /> <br /> The Replicon user import is skipped is skipped on {{ current_time() }} as the RAAS file is blank.<br /><br />Please check the input file and make the required correction.&nbsp;</p>
            <p>URL used: https://services1.myworkday.com/ccx/service/customreport2/horizonmedia/ISU_Replicon_Demographic/Replicon_Demographic_File?format=json</p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        stop_7 = rail.EmptyOperator(
            task_id='stop_7',

        )

        create_csv_lines_create_inputfilewith_m_d5_r_a_a_sdata_10 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_create_inputfilewith_m_d5_r_a_a_sdata_10',
            source="{{ result('workdayreport_4')['Report_Entry'] | to_json }}",
            header=['Pref_First_Name',
                    'Pref_LastName',
                    'Work_Email',
                    'Employee_ID',
                    'TS_Approval_Path',
                    'Position_ID',
                    'Start_Date',
                    'BusinessTitle',
                    'Location',
                    'Location_Code',
                    'Location_Eff_Date',
                    'Work_Space',
                    'Cost_Center_Code',
                    'Cost_Center',
                    'Department',
                    'Department_Code',
                    'Profit_Center',
                    'Profit_Center_Code',
                    'Company',
                    'Company_Code',
                    'Pref_Name',
                    'Legal_Name',
                    'Sup_Org',
                    'Sup_Org_Code',
                    'Mgmt_Level',
                    'Mgmt_Code',
                    'JobPositionTag',
                    'JobPositionTagCode',
                    'JobPositionTagEffDate',
                    'Job_Profile',
                    'Job_Profile_Code',
                    'Job_Profile_Eff_Date',
                    'Home_State',
                    'CEO',
                    'CEO_1',
                    'CEO_2',
                    'CEO_3',
                    'CEO_4',
                    'CEO_5',
                    'CEO_6',
                    'Group_Head',
                    'Business_Leader',
                    'User_Name',
                    'Supervisor',
                    'Employee_Type',
                    'Employee_Type_Eff_Date',
                    'Time_Type',
                    'FLSA',
                    'FLSA_Eff_Date',
                    'Worker_Status',
                    'FirstDayofLeave',
                    'ActualLastDayofLeave',
                    'Substitute_User',
                    'Subs_User_StartDate',
                    'Sub_User_EndDate',
                    'Country',
                    'Scheduled_Weekly_Hours',
                    'Is_Manager',
                    'Payroll_ID',
                    'MD5'],
            row=[
                "{{ item | attr_or_default('Pref_First_Name', '') }}",
                "{{ item | attr_or_default('Pref_LastName', '') }}",
                "{{ item | attr_or_default('Work_Email', '') }}",
                "{{ item | attr_or_default('Employee_ID', '') }}",
                "{{ item | attr_or_default('TS_Approval_Path', '') }}",
                "{{ item | attr_or_default('Position_ID', '') }}",
                "{{ item | attr_or_default('Start_Date', '') }}",
                "{{ item | attr_or_default('BusinessTitle', '') }}",
                "{{ item | attr_or_default('Location','') }}",
                "{{ item | attr_or_default('Location_Code', '') }}",
                "{{ item | attr_or_default('Location_Eff_Date','')  }}",
                "{{ item | attr_or_default('Work_Space', '') }}",
                "{{ item | attr_or_default('Cost_Center_Code', '') }}",
                "{{ item | attr_or_default('Cost_Center', '') }}",
                "{{ item | attr_or_default('Department','') }}",
                "{{ item | attr_or_default('Department_Code', '') }}",
                "{{ item | attr_or_default('Profit_Center', '') }}",
                "{{ item | attr_or_default('Profit_Center_Code', '') }}",
                "{{ item | attr_or_default('Company','') }}",
                "{{ item | attr_or_default('Company_Code', '') }}",
                "{{ item | attr_or_default('Pref_Name', '') }}",
                "{{ item | attr_or_default('Legal_Name', '') }}",
                "{{ item | attr_or_default('Sup_Org','') }}",
                "{{ item | attr_or_default('Sup_Org_Code', '') }}",
                "{{ item | attr_or_default('Mgmt_Level', '') }}",
                "{{ item | attr_or_default('Mgmt_Code', '') }}",
                "{{ item | attr_or_default('JobPositionTag', '') }}",
                "{{ item | attr_or_default('JobPositionTagCode', '') }}",
                "{{ item | attr_or_default('JobPositionTagEffDate', '') }}",
                "{{ item | attr_or_default('Job_Profile', '') }}",
                "{{ item | attr_or_default('Job_Profile_Code', '') }}",
                "{{ item | attr_or_default('Job_Profile_Eff_Date', '') }}",
                "{{ item | attr_or_default('Home_State', '') }}",
                "{{ item | attr_or_default('CEO','') }}",
                "{{ item | attr_or_default('CEO_1','') }}",
                "{{ item | attr_or_default('CEO_2','') }}",
                "{{ item | attr_or_default('CEO_3','') }}",
                "{{ item | attr_or_default('CEO_4','') }}",
                "{{ item | attr_or_default('CEO_5','') }}",
                "{{ item | attr_or_default('CEO_6','') }}",
                "{{ item | attr_or_default('Group_Head', '') }}",
                "{{ item | attr_or_default('Business_Leader', '') }}",
                "{{ item | attr_or_default('User_Name', '') }}",
                "{{ item | attr_or_default('Supervisor','') }}",
                "{{ item | attr_or_default('Employee_Type', '') }}",
                "{{ item | attr_or_default('Employee_Type_Eff_Date', '') }}",
                "{{ item | attr_or_default('Time_Type', '') }}",
                "{{ item | attr_or_default('FLSA','') }}",
                "{{ item | attr_or_default('FLSA_Eff_Date', '') }}",
                "{{ item | attr_or_default('Worker_Status', '') }}",
                "{{ item | attr_or_default('FirstDayofLeave', '') }}",
                "{{ item | attr_or_default('ActualLastDayofLeave', '') }}",
                "{{ item['Current_Delegations'][0]['Substitute_User'] if item | attr_or_default('Current_Delegations') | is_truthy else '' }}",
                "{{ item['Current_Delegations'][0]['Subs_User_StartDate'] if item | attr_or_default('Current_Delegations') | is_truthy else ''}}",
                "{{ item['Current_Delegations'][0]['Sub_User_EndDate'] if item | attr_or_default('Current_Delegations') | is_truthy else ''}}",
                "{{ item | attr_or_default('Country','') }}",
                "{{ item | attr_or_default('Scheduled_Weekly_Hours','') }}",
                "{{ item | attr_or_default('Is_Manager', '') }}",
                "{{ item | attr_or_default('Payroll_ID','') }}",
                '''{{ (item | attr_or_default('Pref_First_Name','') + "_" + item | attr_or_default('Pref_LastName','') + "_" + item | attr_or_default('Work_Email','') + "_" + item | attr_or_default('Employee_ID','') + "_" + item | attr_or_default('TS_Approval_Path','') + "_" + item | attr_or_default('Position_ID','') + "_" + item | attr_or_default('Start_Date','') + "_" + item | attr_or_default('BusinessTitle','') + "_" + item | attr_or_default('Location','') + "_" + item | attr_or_default('Location_Code','') + "_" + item | attr_or_default('Location_Eff_Date','')  + "_" + item | attr_or_default('Work_Space','') + "_" + item | attr_or_default('Cost_Center_Code','') + "_" + item | attr_or_default('Cost_Center','') + "_" + item | attr_or_default('Department','') + "_" + item | attr_or_default('Department_Code','') + "_" + item | attr_or_default('Profit_Center','') + "_" + item | attr_or_default('Profit_Center_Code','') + "_" + item | attr_or_default('Company','') + "_" + item | attr_or_default('Company_Code','') + "_" + item | attr_or_default('Pref_Name','') + "_" + item | attr_or_default('Legal_Name','') + "_" + item | attr_or_default('Sup_Org','')+ "_" + item | attr_or_default('Sup_Org_Code','') + "_" + item | attr_or_default('Mgmt_Level','') + "_" + item | attr_or_default('Mgmt_Code','') + "_" + item | attr_or_default('JobPositionTag','') + "_" + item | attr_or_default('JobPositionTagCode','') + "_" + item | attr_or_default('JobPositionTagEffDate','') + "_" + item | attr_or_default('Job_Profile','') + "_" + item | attr_or_default('Job_Profile_Code','') + "_" + item | attr_or_default('Job_Profile_Eff_Date','') + "_" + item | attr_or_default('Home_State','') + "_" + item | attr_or_default('CEO','') + "_" + item | attr_or_default('CEO_1','') + "_" + item | attr_or_default('CEO_2','') + "_" + item | attr_or_default('CEO_3','') + "_" + item | attr_or_default('CEO_4','') + "_" + item | attr_or_default('CEO_5','') + "_" + item | attr_or_default('CEO_6','') + "_" + item | attr_or_default('Group_Head','') + "_" + item | attr_or_default('Business_Leader','') + "_" + item | attr_or_default('User_Name','') + "_" + item | attr_or_default('Supervisor','') + "_" + item | attr_or_default('Employee_Type','') + "_" + item | attr_or_default('Employee_Type_Eff_Date','') + "_" + item | attr_or_default('Time_Type','') + "_" + item | attr_or_default('FLSA','') + "_" + item | attr_or_default('FLSA_Eff_Date','') + "_" + item | attr_or_default('Worker_Status','') + "_" + item | attr_or_default('FirstDayofLeave','') + "_" + item | attr_or_default('ActualLastDayofLeave','') + "_" + item | attr_or_default('Country','') + "-" + (item['Current_Delegations'][0]['Substitute_User'] if item | attr_or_default('Current_Delegations') | is_truthy else '') + "-" + (item['Current_Delegations'][0]['Subs_User_StartDate'] if item | attr_or_default('Current_Delegations') | is_truthy else '') + "-" + (item['Current_Delegations'][0]['Sub_User_EndDate'] if item | attr_or_default('Current_Delegations') | is_truthy else '') + "-" + item | attr_or_default('Scheduled_Weekly_Hours','') + "-" + item | attr_or_default('Is_Manager','') + "-" + item | attr_or_default('Payroll_ID','')) | md5 }}'''
            ]
        )

        download_13 = rail.SFTPDownloadFileOperator(
            task_id='download_13',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.sftp_ref_file_path
        )

        load_csv_create_list_from_csv_14 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_14",
            document="{{ result('create_csv_lines_create_inputfilewith_m_d5_r_a_a_sdata_10') }}",
        )

        create_collection_create_list_from_csv_14 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_14',
            source="{{ result('load_csv_create_list_from_csv_14') }}",
            name="inputfilewithmd5",
        )

        query_list_inputrecords_15 = rail.QueryCollectionOperator(
            task_id='query_list_inputrecords_15',
            query="""SELECT * FROM  inputfilewithmd5""",
        )

        load_csv_create_list_from_csv_16 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_16",
            document="{{result('download_13') }}",
        )

        create_collection_create_list_from_csv_16 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_16',
            source="{{ result('load_csv_create_list_from_csv_16') }}",
            name="referencefilewithmd5",
        )

        query_list_referencerecords_17 = rail.QueryCollectionOperator(
            task_id='query_list_referencerecords_17',
            query="""SELECT * FROM  referencefilewithmd5""",
        )

        declare_list_18 = rail.SetVariableOperator(
            task_id='declare_list_18',
            append=False,
            name='importlogger',
            value=[]
        )

        query_list_identify_unchangedrecords_19 = rail.QueryCollectionOperator(
            task_id='query_list_identify_unchangedrecords_19',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)
""",
        )

        if_query_list_identify_unchangedrecords_19_rows_greater_than_0_20 = rail.IfOperator(
            task_id='if_query_list_identify_unchangedrecords_19_rows_greater_than_0_20',
            test='''{{ result('query_list_identify_unchangedrecords_19','length') > 0 }}''',
            yes_task="add_logs_ignored_records",
            no_task="query_list_identify_changedrecords_22",
        )

        add_logs_ignored_records = rail.WriteLogOperator(
            task_id='add_logs_ignored_records',
            log="{{ result('create_log') }}",
            message='No changes in user record',
            items="{{ result('query_list_identify_unchangedrecords_19') }}",
            properties={
                    "employeeid": "{{ item.Employee_ID }}",
                    "username": "{{ item.Pref_First_Name }} {{ item.Pref_LastName }}",
                    "action": "pre-check",
                    "status": "Ignored",
                    "details": "No changes in user record",
            }
        )

        query_list_identify_changedrecords_22 = rail.QueryCollectionOperator(
            task_id='query_list_identify_changedrecords_22',
            name='query_list_identify_changedrecords_22',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 NOT IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)
""",
        )

        create_list_23 = rail.QueryCollectionOperator(
            task_id='create_list_23',
            name='create_list_23',
            query="""SELECT * FROM  query_list_identify_changedrecords_22""",
        )

        query_list_changedrecordswithout_mandatoryfields_24 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswithout_mandatoryfields_24',
            query="""SELECT * FROM  create_list_23 WHERE ( create_list_23.Employee_ID= "" OR  create_list_23.Pref_First_Name= "" OR  create_list_23.Pref_LastName= "" OR  create_list_23.Location= "" OR  create_list_23.Sup_Org= "" OR  create_list_23.JobPositionTag= "" OR  create_list_23.Job_Profile= "" OR  create_list_23.Employee_Type= "" OR  create_list_23.FLSA= "" OR  create_list_23.Employee_ID IS NULL OR  create_list_23.Pref_First_Name IS NULL OR  create_list_23.Pref_LastName IS NULL OR  create_list_23.Location IS NULL OR  create_list_23.Sup_Org IS NULL OR  create_list_23.JobPositionTag IS NULL OR  create_list_23.Job_Profile IS NULL OR  create_list_23.Employee_Type IS NULL OR  create_list_23.FLSA IS NULL)""",
        )

        if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_25 = rail.IfOperator(
            task_id='if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_25',
            test='''{{ result('query_list_changedrecordswithout_mandatoryfields_24','length') > 0 }}''',
            yes_task="log_entry_missing_values",
            no_task="log_formatteddateandtime_27",
        )

        log_entry_missing_values = rail.WriteLogOperator(
            task_id='log_entry_missing_values',
            log="{{ result('create_log') }}",
            message="One or more mandatory fields are missing",
            items="{{ result('query_list_changedrecordswithout_mandatoryfields_24') }}",
            properties={
                    "employeeid": "{{ item.Employee_ID }}",
                    "username": "{{ item.Pref_First_Name }} {{ item.Pref_LastName }}",
                    "action": "pre-check",
                    "status": "Ignored",
                    "details": "One or more mandatory fields are missing",
            }
        )

        log_formatteddateandtime_27 = rail.PythonOperator(
            task_id='log_formatteddateandtime_27',
            python_callable=lambda:  datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        )

        if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_28 = rail.IfOperator(
            task_id='if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_28',
            test='''{{ result('query_list_changedrecordswithout_mandatoryfields_24','length') > 0  or result('query_list_identify_unchangedrecords_19','length') > 0 }}''',
            yes_task="create_csv_lines_31",
            no_task="query_list_changedrecordswith_mandatoryfields_33",
        )

        create_csv_lines_31 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_31',
            source="{{ result('create_log') }}",
            header=['employeeid',
                    'username',
                    'action',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.properties.employeeid }}",
                "{{ item.properties.username }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}"
            ]
        )

        upload_32 = rail.SFTPUploadFileOperator(
            task_id='upload_32',
            content="{{ result('create_csv_lines_31') }}",
            remote_filepath=config.logpath +
            '/{{ dag_run_ecid() | replace(":", "-") }}_UserImportLogs_{{result("log_formatteddateandtime_27")}}.csv',
        )

        query_list_changedrecordswith_mandatoryfields_33 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswith_mandatoryfields_33',
            name='query_list_changedrecordswith_mandatoryfields_33',
            query="""SELECT * FROM  create_list_23 WHERE ( create_list_23.Employee_ID!= "" OR  create_list_23.Pref_First_Name!= "" AND  create_list_23.Pref_LastName!= "" AND  create_list_23.Location!= "" AND  create_list_23.Sup_Org!= "" AND  create_list_23.JobPositionTag!= "" AND  create_list_23.Job_Profile!= "" AND  create_list_23.Employee_Type!= "" AND  create_list_23.FLSA!= "" AND  create_list_23.Employee_ID IS NOT NULL AND  create_list_23.Pref_First_Name IS NOT NULL AND  create_list_23.Pref_LastName IS NOT NULL AND  create_list_23.Location IS NOT NULL AND  create_list_23.Sup_Org IS NOT NULL AND  create_list_23.JobPositionTag IS NOT NULL AND  create_list_23.Job_Profile IS NOT NULL AND  create_list_23.Employee_Type IS NOT NULL AND  create_list_23.FLSA IS NOT NULL)""",
        )

        create_list_changedrecordswith_mandatoryfields_34 = rail.QueryCollectionOperator(
            task_id='create_list_changedrecordswith_mandatoryfields_34',
            query="""SELECT * FROM  query_list_changedrecordswith_mandatoryfields_33""",
            name="changedrecords"
        )

        if_query_list_changedrecordswith_mandatoryfields_33_rows_greater_than_0_39 = rail.IfOperator(
            task_id='if_query_list_changedrecordswith_mandatoryfields_33_rows_greater_than_0_39',
            test='''{{ result('query_list_changedrecordswith_mandatoryfields_33','length') > 0 }}''',
            yes_task="get_report_details",
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_list_report_name,
        )

        generate_report_group = rail.run_report2(
            group_id='run_user_list_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        if_generate_report_40_payload_starts_with_nodata_41 = rail.IfOperator(
            task_id='if_generate_report_40_payload_starts_with_nodata_41',
            test='{{ result("run_user_list_report.get_report_result", "has_data") }}',
            yes_task='if_generate_report_40_payload_not_starts_with_usernameloginnameemployeeiduseruriuserstatususerenddateemployeetypecurrent_43',
            no_task='stop_42'
        )

        stop_42 = rail.FailOperator(
            task_id='stop_42',
            message='''No Data in the base report'''
        )

        if_generate_report_40_payload_not_starts_with_usernameloginnameemployeeiduseruriuserstatususerenddateemployeetypecurrent_43 = rail.IfOperator(
            task_id='if_generate_report_40_payload_not_starts_with_usernameloginnameemployeeiduseruriuserstatususerenddateemployeetypecurrent_43',
            test='''{{ not result("run_user_list_report.get_report_result").reportGenerationResults[0].payload | starts_with('User Name,Login Name,Employee ID,UserUri,User Status,User End Date,Employee Type (Current)') }}''',
            yes_task="stop_44",
            no_task="load_user_report_csv_data",
        )

        stop_44 = rail.FailOperator(
            task_id='stop_44',
            message='''Base report column order doesn't match'''
        )

        load_user_report_csv_data = rail.LoadCSVFileOperator(
            task_id='load_user_report_csv_data',
            document='{{ result("run_user_list_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        create_list_46 = rail.CreateCollectionOperator(
            task_id='create_list_46',
            source="{{ result('load_user_report_csv_data') }}",
            name='userlist',
        )

        get_all_custom_fields_47 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_47',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        get_all_time_zones_getalltimezones_48 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_getalltimezones_48',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        invoke_custom_ruby_code_customfieldsuri_49 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_customfieldsuri_49',
            python_callable=lambda: {
                "positionid": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Position ID", 'uri'),
                "businesstitle": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Business Title", 'uri'),
                "workspace": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Work Space", 'uri'),
                "costcenter": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Cost Center", 'uri'),
                "costcentercode": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Cost Center Code", 'uri'),
                "department": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Department", 'uri'),
                "departmentcode": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Department Code", 'uri'),
                "profitcenter": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Profit Center", 'uri'),
                "profitcentercode": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Profit Center Code", 'uri'),
                "company": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Company", 'uri'),
                "companycode": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Company Code", 'uri'),
                "prefferedfullname": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Preferred Full Name", 'uri'),
                "fulllegalname": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Full Legal Name", 'uri'),
                "managementlevel": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Management Level", 'uri'),
                "managementlevelcode": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Management Level Code", 'uri'),
                "employeeresidence": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Employee Residence - State", 'uri'),
                "ceo": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO", 'uri'),
                "ceo1": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO -1", 'uri'),
                "ceo2": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO -2", 'uri'),
                "ceo3": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO -3", 'uri'),
                "ceo4": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO -4", 'uri'),
                "ceo5": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO -5", 'uri'),
                "ceo6": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "CEO -6", 'uri'),
                "groupleader": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Group Leader", 'uri'),
                "businesleader": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Business Leader", 'uri'),
                "contingentworkertype": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Time_Type", 'uri'),
                "workerstatus": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Worker Status", 'uri'),
                "firstdayofleave": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "First Day of Leave", 'uri'),
                "lastdayofleave": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Actual Last Day of Leave", 'uri'),
                "country": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Country", 'uri'),
                "scheduledweeklyhours": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Scheduled Weekly Hours", 'uri'),
                "payrollid": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Payroll ID", 'uri'),
                "ismanager": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields_47'), 'displayText', "Manager", 'uri')
            }
        )

        get_all_permission_sets_get_all_permission_sets_51 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_get_all_permission_sets_51',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",

        )

        get_enabled_activities_get_enabled_activities_52 = rail.RepliconServiceOperator(
            task_id='get_enabled_activities_get_enabled_activities_52',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",

        )

        get_all_scripts_timeoffvalidationscripts_timeoffvalidationscripts_53 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeoffvalidationscripts_timeoffvalidationscripts_53',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",

        )

        get_all_scripts_time_off_balance_event_scripts_time_off_balance_event_scripts_54 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_scripts_time_off_balance_event_scripts_54',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",

        )

        get_all_time_zones_get_all_time_zones_55 = rail.RepliconServiceOperator(
            task_id='get_all_time_zones_get_all_time_zones_55',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",

        )

        get_all_office_schedules_get_all_office_schedules_56 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_get_all_office_schedules_56',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",

        )

        get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_57 = rail.RepliconServiceOperator(
            task_id='get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_57',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",

        )

        get_all_approval_paths_timeoff_get_all_approval_paths_timeoff_58 = rail.RepliconServiceOperator(
            task_id='get_all_approval_paths_timeoff_get_all_approval_paths_timeoff_58',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",

        )

        get_all_policy_sets_get_all_policy_sets_59 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_get_all_policy_sets_59',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",

        )

        get_all_holiday_calendars_get_all_holiday_calendars_60 = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars_get_all_holiday_calendars_60',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",

        )

        get_data_timesheet_period_list_service1_timesheet_period_list_service1_61 = rail.RepliconServiceOperator(
            task_id='get_data_timesheet_period_list_service1_timesheet_period_list_service1_61',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        invoke_custom_ruby_code_timesheet_period_list_62 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_timesheet_period_list_62',
            python_callable=lambda: list(map(lambda x: {
                "textValue": x['cells'][0]['textValue'],
                "uri": x['cells'][0]['uri']
            },
                rail.result('get_data_timesheet_period_list_service1_timesheet_period_list_service1_61')['rows']))
        )

        query_list_getlocationsvalue_63 = rail.QueryCollectionOperator(
            task_id='query_list_getlocationsvalue_63',
            query="""SELECT DISTINCT  changedrecords.Location,  changedrecords.Location_Code FROM  changedrecords WHERE ( changedrecords.Location!= "" AND  changedrecords.Location_Code!= "")""",
        )

        trigger_dag_run_live_horizonmedia_groups_check_child64 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_groups_check_child64',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_groups_check_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": list(map(lambda x: {
                    "displayText": x['Location'],
                    "code": x['Location_Code']
                }, rail.load_all_records(rail.result('query_list_getlocationsvalue_63')))),
                "grouptype": "Location"
            }

        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_child64 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_child64',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_groups_check_child64") }}'
        )

        query_list_get_departmentgroupsvalue_65 = rail.QueryCollectionOperator(
            task_id='query_list_get_departmentgroupsvalue_65',
            query="""SELECT DISTINCT  changedrecords.Sup_Org as department,  changedrecords.Sup_Org_Code as departmentcode FROM  changedrecords WHERE ( changedrecords.Sup_Org!= "" AND  changedrecords.Sup_Org_Code!= "")""",
        )

        trigger_dag_run_live_horizonmedia_groups_check_child66 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_groups_check_child66',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_groups_check_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": list(map(lambda x: {
                    "displayText": x['departmentcode'],
                    "code": x['department']
                }, rail.load_all_records(rail.result('query_list_get_departmentgroupsvalue_65')))),
                "grouptype": "Department"
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_child66 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_child66',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_groups_check_child66") }}'
        )

        query_list_get_divisionvalue_67 = rail.QueryCollectionOperator(
            task_id='query_list_get_divisionvalue_67',
            query="""SELECT DISTINCT  changedrecords.JobPositionTag as division,  changedrecords.JobPositionTagCode as divisioncode FROM  changedrecords WHERE ( changedrecords.JobPositionTag!= "" AND  changedrecords.JobPositionTagCode!= "")""",
        )

        trigger_dag_run_live_horizonmedia_groups_check_childdivision_68 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_groups_check_childdivision_68',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_groups_check_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": list(map(lambda x: {
                    "displayText": x['division'],
                    "code": x['divisioncode']
                }, rail.load_all_records(rail.result('query_list_get_divisionvalue_67')))),
                "grouptype": "Division"
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childdivision_68 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childdivision_68',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_groups_check_childdivision_68") }}'
        )

        query_list_get_servicecentervalue_69 = rail.QueryCollectionOperator(
            task_id='query_list_get_servicecentervalue_69',
            query="""SELECT DISTINCT  changedrecords.Job_Profile as servicecenter,  changedrecords.Job_Profile_Code as servicecentercode FROM  changedrecords WHERE ( changedrecords.Job_Profile!= "" AND  changedrecords.Job_Profile_Code!= "")""",
        )

        trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_groups_check_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": list(map(lambda x: {
                    "displayText": x['servicecenter'],
                    "code": x['servicecentercode']
                }, rail.load_all_records(rail.result('query_list_get_servicecentervalue_69')))),
                "grouptype": "Service Center"
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70") }}'
        )

        query_list_getemployeetypevalue_71 = rail.QueryCollectionOperator(
            task_id='query_list_getemployeetypevalue_71',
            query="""SELECT DISTINCT  changedrecords.Employee_Type as employeetype FROM  changedrecords WHERE ( changedrecords.Employee_Type!= "")""",
        )

        trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_groups_check_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": list(map(lambda x: {
                    "displayText": x['employeetype'],
                    "code": null
                }, rail.load_all_records(rail.result('query_list_getemployeetypevalue_71')))),
                "grouptype": "Employee Type"
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72") }}'
        )

        query_list_getcostcentergroupsvalue_73 = rail.QueryCollectionOperator(
            task_id='query_list_getcostcentergroupsvalue_73',
            query="""SELECT DISTINCT  changedrecords.FLSA as costcenter FROM  changedrecords WHERE ( changedrecords.FLSA!= "")""",
        )

        trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_groups_check_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "group": list(map(lambda x: {
                    "displayText": x['costcenter'],
                    "code": null
                }, rail.load_all_records(rail.result('query_list_getcostcentergroupsvalue_73')))),
                "grouptype": "Cost Center"
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74") }}'
        )

        query_list_getunique_c_e_ovalues_75 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_ovalues_75',
            query="""SELECT DISTINCT  changedrecords.CEO as ceo FROM  changedrecords WHERE ( changedrecords.CEO!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child76 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child76',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['ceo']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_ovalues_75')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child76 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child76',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child76") }}'
        )

        query_list_getunique_c_e_o1values_77 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_o1values_77',
            query="""SELECT DISTINCT  changedrecords.CEO_1 FROM  changedrecords WHERE ( changedrecords.CEO_1!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child78 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child78',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['CEO_1']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_o1values_77')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo1']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child78 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child78',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child78") }}'
        )

        query_list_getunique_c_e_o2values_79 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_o2values_79',
            query="""SELECT DISTINCT  changedrecords.CEO_2 FROM  changedrecords WHERE ( changedrecords.CEO_2!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child80 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child80',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['CEO_2']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_o2values_79')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo2']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child80 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child80',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child80") }}'
        )

        query_list_getunique_c_e_o3values_81 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_o3values_81',
            query="""SELECT DISTINCT  changedrecords.CEO_3 FROM  changedrecords WHERE ( changedrecords.CEO_3!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child82 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child82',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['CEO_3']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_o3values_81')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo3']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child82 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child82',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child82") }}'
        )

        query_list_getunique_c_e_o4values_83 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_o4values_83',
            query="""SELECT DISTINCT  changedrecords.CEO_4 FROM  changedrecords WHERE ( changedrecords.CEO_4!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child84 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child84',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['CEO_4']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_o4values_83')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo4']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child84 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child84',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child84") }}'
        )

        query_list_getunique_c_e_o5values_85 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_o5values_85',
            query="""SELECT DISTINCT  changedrecords.CEO_5 FROM  changedrecords WHERE ( changedrecords.CEO_5!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child86 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child86',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['CEO_5']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_o5values_85')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo5']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child86 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child86',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child86") }}'
        )

        query_list_getunique_c_e_o6values_87 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_c_e_o6values_87',
            query="""SELECT DISTINCT  changedrecords.CEO_6 FROM  changedrecords WHERE ( changedrecords.CEO_6!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child88 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child88',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['CEO_6']
                }, rail.load_all_records(rail.result('query_list_getunique_c_e_o6values_87')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo6']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child88 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child88',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child88") }}'
        )

        query_list_getuniqueworkspacevalues_89 = rail.QueryCollectionOperator(
            task_id='query_list_getuniqueworkspacevalues_89',
            query="""SELECT DISTINCT  changedrecords.Work_Space FROM  changedrecords WHERE ( changedrecords.Work_Space!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child90 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child90',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Work_Space']
                }, rail.load_all_records(rail.result('query_list_getuniqueworkspacevalues_89')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['workspace']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child90 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child90',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child90") }}'
        )

        query_list_getuniquecostcentervalues_91 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquecostcentervalues_91',
            query="""SELECT DISTINCT  changedrecords.Cost_Center FROM  changedrecords WHERE ( changedrecords.Cost_Center!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child92 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child92',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Cost_Center']
                }, rail.load_all_records(rail.result('query_list_getuniquecostcentervalues_91')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['costcenter']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child92 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child92',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child92") }}'
        )

        query_list_getuniquedepartmentcustomfields_93 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquedepartmentcustomfields_93',
            query="""SELECT DISTINCT  changedrecords.Department FROM  changedrecords WHERE ( changedrecords.Department!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child94 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child94',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Department']
                }, rail.load_all_records(rail.result('query_list_getuniquedepartmentcustomfields_93')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['department']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child94 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child94',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child94") }}'
        )

        query_list_getuniqueprofitcentercustomfields_95 = rail.QueryCollectionOperator(
            task_id='query_list_getuniqueprofitcentercustomfields_95',
            query="""SELECT DISTINCT  changedrecords.Profit_Center FROM  changedrecords WHERE ( changedrecords.Profit_Center!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child96 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child96',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Profit_Center']
                }, rail.load_all_records(rail.result('query_list_getuniqueprofitcentercustomfields_95')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['profitcenter']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child96 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child96',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child96") }}'
        )

        query_list_getuniqueprofitcentercustomfields_97 = rail.QueryCollectionOperator(
            task_id='query_list_getuniqueprofitcentercustomfields_97',
            query="""SELECT DISTINCT  changedrecords.Profit_Center FROM  changedrecords WHERE ( changedrecords.Profit_Center!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child98 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child98',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Profit_Center']
                }, rail.load_all_records(rail.result('query_list_getuniqueprofitcentercustomfields_97')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['profitcenter']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child98 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child98',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child98") }}'
        )

        query_list_getuniquecompanycustomfields_99 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquecompanycustomfields_99',
            query="""SELECT DISTINCT  changedrecords.Company FROM  changedrecords WHERE ( changedrecords.Company!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child100 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child100',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Company']
                }, rail.load_all_records(rail.result('query_list_getuniquecompanycustomfields_99')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['company']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child100 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child100',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child100") }}'
        )

        query_list_getuniquemgmtlevelcustomfields_101 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquemgmtlevelcustomfields_101',
            query="""SELECT DISTINCT  changedrecords.Mgmt_Level FROM  changedrecords WHERE ( changedrecords.Mgmt_Level!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child102 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child102',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Mgmt_Level']
                }, rail.load_all_records(rail.result('query_list_getuniquemgmtlevelcustomfields_101')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['managementlevel']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child102 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child102',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child102") }}'
        )

        query_list_getuniquehomestatecustomfields_103 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquehomestatecustomfields_103',
            query="""SELECT DISTINCT  changedrecords.Home_State FROM  changedrecords WHERE ( changedrecords.Home_State!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child104 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child104',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Home_State']
                }, rail.load_all_records(rail.result('query_list_getuniquehomestatecustomfields_103')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['employeeresidence']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child104 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child104',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child104") }}'
        )

        query_list_getuniquegroupheadcustomfields_105 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquegroupheadcustomfields_105',
            query="""SELECT DISTINCT  changedrecords.Group_Head FROM  changedrecords WHERE ( changedrecords.Group_Head!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child106 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child106',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Group_Head']
                }, rail.load_all_records(rail.result('query_list_getuniquegroupheadcustomfields_105')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['groupleader']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child106 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child106',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child106") }}'
        )

        query_list_getuniquebusinessleadercustomfields_107 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquebusinessleadercustomfields_107',
            query="""SELECT DISTINCT  changedrecords.Business_Leader FROM  changedrecords WHERE ( changedrecords.Business_Leader!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child108 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child108',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Business_Leader']
                }, rail.load_all_records(rail.result('query_list_getuniquebusinessleadercustomfields_107')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['businesleader']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child108 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child108',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child108") }}'
        )

        query_list_getuniquecompanycustomfields_109 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquecompanycustomfields_109',
            query="""SELECT DISTINCT  changedrecords.Time_Type FROM  changedrecords WHERE ( changedrecords.Time_Type!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child110 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child110',
            retries=0,
                    items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Time_Type']
                }, rail.load_all_records(rail.result('query_list_getuniquecompanycustomfields_109')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['contingentworkertype']
                    }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child110 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child110',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child110") }}'
        )

        query_list_getuniqueworkerstatuscustomfields_111 = rail.QueryCollectionOperator(
            task_id='query_list_getuniqueworkerstatuscustomfields_111',
            query="""SELECT DISTINCT  changedrecords.Worker_Status FROM  changedrecords WHERE ( changedrecords.Worker_Status!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child112 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child112',
            retries=0,
                    items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Worker_Status']
                }, rail.load_all_records(rail.result('query_list_getuniqueworkerstatuscustomfields_111')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['workerstatus']
                    }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child112 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child112',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child112") }}'
        )

        query_list_getuniquecountrycustomfields_113 = rail.QueryCollectionOperator(
            task_id='query_list_getuniquecountrycustomfields_113',
            query="""SELECT DISTINCT  changedrecords.Country FROM  changedrecords WHERE ( changedrecords.Country!= "")""",
        )

        trigger_dag_run_live_horizonmedia_process_custom_fields_child114 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_process_custom_fields_child114',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_process_custom_fields_child,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "customFieldvalues": list(map(lambda x: {
                    "displayText": x['Country']
                }, rail.load_all_records(rail.result('query_list_getuniquecountrycustomfields_113')))),
                "customFieldUri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['country']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child114 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child114',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_process_custom_fields_child114") }}'
        )

        get_enabled_employee_type_groups_employeetypes_115 = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups_employeetypes_115',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",

        )

        get_enabled_department_groups_departments_116 = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups_departments_116',
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",

        )

        get_enabled_service_centers_servicecenters_117 = rail.RepliconServiceOperator(
            task_id='get_enabled_service_centers_servicecenters_117',
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",

        )

        get_enabled_locations_locations_118 = rail.RepliconServiceOperator(
            task_id='get_enabled_locations_locations_118',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",

        )

        get_enabled_divisions_divisions_119 = rail.RepliconServiceOperator(
            task_id='get_enabled_divisions_divisions_119',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",

        )

        get_enabled_cost_centers_costcenters_120 = rail.RepliconServiceOperator(
            task_id='get_enabled_cost_centers_costcenters_120',
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",

        )

        get_all_custom_field_drop_down_optionsworkspace_dropdownoptionworkspace_121 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsworkspace_dropdownoptionworkspace_121',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').workspace }}"
            }
        )

        get_all_custom_field_drop_down_optionscostcenter_dropdownoptioncostcenter_122 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionscostcenter_dropdownoptioncostcenter_122',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').costcenter }}"
            }
        )

        get_all_custom_field_drop_down_options_department_dropdownoptiondepartment_123 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_department_dropdownoptiondepartment_123',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').department }}"
            }
        )

        get_all_custom_field_drop_down_optionsprofitcenter_dropdownoptionprofitcenter_124 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsprofitcenter_dropdownoptionprofitcenter_124',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').profitcenter }}"
            }
        )

        get_all_custom_field_drop_down_optionscompany_dropdownoptioncompany_125 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionscompany_dropdownoptioncompany_125',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').company }}"
            }
        )

        get_all_custom_field_drop_down_options_mgmtlevel_dropdownoption_mgmtlevel_126 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_mgmtlevel_dropdownoption_mgmtlevel_126',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').managementlevel }}"
            }
        )

        get_all_custom_field_drop_down_options_residencestate_dropdownoption_residencestate_127 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_residencestate_dropdownoption_residencestate_127',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').employeeresidence }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o_dropdownoption_c_e_o_128 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o_dropdownoption_c_e_o_128',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o1_dropdownoption_c_e_o1_129 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o1_dropdownoption_c_e_o1_129',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo1 }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o2_dropdownoption_c_e_o2_130 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o2_dropdownoption_c_e_o2_130',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo2 }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o3_dropdownoption_c_e_o3_131 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o3_dropdownoption_c_e_o3_131',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo3 }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o4_dropdownoption_c_e_o4_132 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o4_dropdownoption_c_e_o4_132',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo4 }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o5_dropdownoption_c_e_o5_133 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o5_dropdownoption_c_e_o5_133',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo5 }}"
            }
        )

        get_all_custom_field_drop_down_options_c_e_o6_dropdownoption_c_e_o6_134 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_c_e_o6_dropdownoption_c_e_o6_134',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ceo6 }}"
            }
        )

        get_all_custom_field_drop_down_optionsgroupleader_dropdownoptiongroupleader_135 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsgroupleader_dropdownoptiongroupleader_135',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').groupleader }}"
            }
        )

        get_all_custom_field_drop_down_options_businessleader_dropdownoption_businessleader_136 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_businessleader_dropdownoption_businessleader_136',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').businesleader }}"
            }
        )

        get_all_custom_field_drop_down_optionscontigentworkertype_dropdownoptioncontigentworkertype_137 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionscontigentworkertype_dropdownoptioncontigentworkertype_137',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').contingentworkertype }}"
            }
        )

        get_all_custom_field_drop_down_optionsworkerstatus_dropdownoptionworkerstatus_138 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsworkerstatus_dropdownoptionworkerstatus_138',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').workerstatus }}"
            }
        )

        get_all_custom_field_drop_down_optionscountry_dropdownoptionworkspace_139 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionscountry_dropdownoptionworkspace_139',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').country }}"
            }
        )

        get_all_custom_field_drop_down_options_manager_dropdownoptionworkspace_140 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_manager_dropdownoptionworkspace_140',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('invoke_custom_ruby_code_customfieldsuri_49').ismanager }}"
            }
        )

        horizonmedia_activity_mapper_v3_0_search_entries_141 = rail.PythonOperator(
            task_id='horizonmedia_activity_mapper_v3_0_search_entries_141',
            python_callable=lambda:  list(
                filter(lambda x: x["check"] == "yes", horizonmedia_activity_mapper_v3_0))
        )

        horizonmedia_user_import_master_mapper_search_entries_142 = rail.PythonOperator(
            task_id='horizonmedia_user_import_master_mapper_search_entries_142',
            python_callable=lambda:  list(
                filter(lambda x: x["check"] == "yes", horizonmedia_user_import_master_mapper))
        )

        declare_list_dag_runs = rail.SetVariableOperator(
            task_id='declare_list_dag_runs',
            name='user_process_dag_runs',
            value=[]
        )

        query_report_records_matching_with_delta_records = rail.QueryCollectionOperator(
            task_id = "query_report_records_matching_with_delta_records",
            query= """SELECT * FROM userlist WHERE userlist.Employee_ID IN (SELECT DISTINCT changedrecords.Employee_ID FROM changedrecords)"""
        )

        load_filtered_report_records_as_per_changed_records = rail.PythonOperator(
            task_id = "load_filtered_report_records_as_per_changed_records",
            python_callable=lambda: rail.load_all_records(rail.result("query_report_records_matching_with_delta_records"))
        )

        foreach_user_to_process = rail.ForEachOperator(
            task_id='foreach_user_to_process',
            items="{{ result('query_list_changedrecordswith_mandatoryfields_33') }}",
            start_task='search_user_uri',
            end_task='foreach_user_to_process_end'
        )

        search_user_uri = rail.PythonOperator(
            task_id = 'search_user_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result("load_filtered_report_records_as_per_changed_records"), 'Employee_ID', rail.result(
                'foreach_user_to_process')['Employee_ID'], 'UserUri', '')
        )

        invoke_custom_ruby_code_145 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_145',
            python_callable=lambda: {
                "useruri": rail.result('search_user_uri'),
                "loginname": rail.result('foreach_user_to_process')['User_Name'],
                "timesheettemplate": next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location'] == rail.result('foreach_user_to_process')['Location'] and x['jobexempt'] == rail.result('foreach_user_to_process')['FLSA'] and x['country'] == rail.result('foreach_user_to_process')['Country'] and x['weekly_hours'] == rail.result('foreach_user_to_process')['Scheduled_Weekly_Hours'], rail.result('horizonmedia_user_import_master_mapper_search_entries_142')
                                                                                ))), None)
                if rail.result('foreach_user_to_process')['Location'] and rail.result('foreach_user_to_process')['Country'] == "USA" and rail.result('foreach_user_to_process')['Scheduled_Weekly_Hours'] == "40" and rail.result('foreach_user_to_process')['Location'] == "Los Angeles" else
                (
                    (next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location'] == "Other than Los Angeles" and x['jobexempt'] == "All" and x['country'] == rail.result(
                        'foreach_user_to_process')['Country'] and x['weekly_hours'] == rail.result('foreach_user_to_process')['Scheduled_Weekly_Hours'], rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null))
                    or
                    (next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location'] == rail.result('foreach_user_to_process')['Location'] and x['jobexempt'] == rail.result('foreach_user_to_process')[
                        'FLSA'] and x['country'] == rail.result('foreach_user_to_process')['Country'] and x['weekly_hours'] == "< 40", rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null) if rail.result('foreach_user_to_process')['Location'] == "Los Angeles" else null)
                    or
                    (next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location'] == 'Other than Los Angeles' and x['jobexempt'] == "All" and x['country'] == rail.result(
                        'foreach_user_to_process')['Country'] and x['weekly_hours'] == "< 40", rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null))
                    or
                    (next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location'] == 'All Locations' and x['jobexempt'] == "All" and x['country'] == 'CAN' and x['weekly_hours'] == rail.result(
                        'foreach_user_to_process')['Scheduled_Weekly_Hours'], rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null) if rail.result('foreach_user_to_process')['Scheduled_Weekly_Hours'] == "37.5" else null)
                    or
                    (next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location'] == 'All Locations' and x['jobexempt']
                                                                == "All" and x['country'] == 'CAN' and x['weekly_hours'] == rail.result('foreach_user_to_process')['Scheduled_Weekly_Hours'], rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null))
                    or
                    (next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timesheet Template" and x['location']
                                                                == 'All Locations' and x['jobexempt'] == "All" and x['country'] == 'CAN' and x['weekly_hours'] == "< 37.5", rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null))
                ),
                "payrule": next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Payrule" and x['location'] == rail.result('foreach_user_to_process')['Location'] and x['jobexempt'] == rail.result('foreach_user_to_process')['FLSA'], rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null) if rail.result('foreach_user_to_process')['Location'] and rail.result('foreach_user_to_process')['FLSA'] == "Non-Exempt" else null,
                "timezone": next(iter(map(lambda x: x['value'], filter(lambda x: x['field'] == "Timezone" and x['location'] == rail.result('foreach_user_to_process')['Location'], rail.result('horizonmedia_user_import_master_mapper_search_entries_142')))), null) if rail.result('foreach_user_to_process')['Location'] else null,
            }
        )

        if_output_useruri_blank_150 = rail.IfOperator(
            task_id='if_output_useruri_blank_150',
            test='''{{ result('invoke_custom_ruby_code_145').useruri | is_falsy }}''',
            yes_task="trigger_dag_run_live_horizonmedia_child_add_user_v2_0async_151",
            no_task="trigger_dag_run_live_horizonmedia_user_update_v2_0async_153",
        )

        def get_holiday_calendar_uri(country):
            response = rail.result('get_all_holiday_calendars_get_all_holiday_calendars_60')
            holiday_calendar = list(filter(lambda x: x['country'].lower() == country.lower(),
                config.HORIZONMEDIA_HOLIDAY_CALENDAR_MAPPER))
            uri = ""
            if holiday_calendar:
                uri = rail.find_first_by_attr_and_get_attr(
                response, 'name', holiday_calendar[0]['holiday_calendar'], 'uri', '')
            return uri

        def get_user_dag_run_conf():
            return {
                "useruri": rail.result('search_user_uri'),
                "employeeid": rail.result('foreach_user_to_process')['Employee_ID'],
                "firstname": rail.result('foreach_user_to_process')['Pref_First_Name'],
                "lastname": rail.result('foreach_user_to_process')['Pref_LastName'],
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_employee_type_groups_employeetypes_115'), 'displayText', rail.result('foreach_user_to_process')['Employee_Type'], 'uri'),
                "timesheettemplate": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets_get_all_policy_sets_59'), 'displayText', rail.result('invoke_custom_ruby_code_145')['timesheettemplate'], 'uri'),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_get_all_permission_sets_51'), 'displayText', 'SUPERVISOR', 'uri'),
                "activities": list(map(lambda x: x['uri'], rail.result('get_enabled_activities_get_enabled_activities_52'))),
                "Work_Email": rail.result('foreach_user_to_process')['Work_Email'],
                "TS_Approval_Path": rail.result('foreach_user_to_process')['TS_Approval_Path'],
                "Position_ID": rail.result('foreach_user_to_process')['Position_ID'],
                "Start_Date": rail.result('foreach_user_to_process')['Start_Date'],
                "BusinessTitle": rail.result('foreach_user_to_process')['BusinessTitle'],
                "Location": rail.result('foreach_user_to_process')['Location'],
                "Location_Code": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_locations_locations_118'), 'displayText', rail.result('foreach_user_to_process')['Location'], 'uri'),
                "Location_Eff_Date": rail.result('foreach_user_to_process')['Location_Eff_Date'],
                "Work_Space": rail.result('foreach_user_to_process')['Work_Space'],
                "Cost_Center_Code": rail.result('foreach_user_to_process')['Cost_Center_Code'],
                "Cost_Center": rail.result('foreach_user_to_process')['Cost_Center'],
                "Department": rail.result('foreach_user_to_process')['Department'],
                "Department_Code": rail.result('foreach_user_to_process')['Department_Code'],
                "Profit_Center": rail.result('foreach_user_to_process')['Profit_Center'],
                "Profit_Center_Code": rail.result('foreach_user_to_process')['Profit_Center_Code'],
                "Company": rail.result('foreach_user_to_process')['Company'],
                "Company_Code": rail.result('foreach_user_to_process')['Company_Code'],
                "Pref_Name": rail.result('foreach_user_to_process')['Pref_Name'],
                "Legal_Name": rail.result('foreach_user_to_process')['Legal_Name'],
                "Sup_Org": rail.result('foreach_user_to_process')['Sup_Org_Code'],
                "Sup_Org_Code": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_department_groups_departments_116'), 'displayText', rail.result('foreach_user_to_process')['Sup_Org_Code'], 'uri'),
                "Mgmt_Level": rail.result('foreach_user_to_process')['Mgmt_Level'],
                "Mgmt_Code": rail.result('foreach_user_to_process')['Mgmt_Code'],
                "JobPositionTag": rail.result('foreach_user_to_process')['JobPositionTag'],
                "JobPositionTagCode": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions_divisions_119'), 'displayText', rail.result('foreach_user_to_process')['JobPositionTag'], 'uri'),
                "JobPositionTagEffDate": rail.result('foreach_user_to_process')['JobPositionTagEffDate'],
                "Job_Profile": rail.result('foreach_user_to_process')['Job_Profile'],
                "Job_Profile_Code": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_service_centers_servicecenters_117'), 'displayText', rail.result('foreach_user_to_process')['Job_Profile'], 'uri'),
                "Job_Profile_Eff_Date": rail.result('foreach_user_to_process')['Job_Profile_Eff_Date'],
                "Home_State": rail.result('foreach_user_to_process')['Home_State'],
                "CEO": rail.result('foreach_user_to_process')['CEO'],
                "CEO_1": rail.result('foreach_user_to_process')['CEO_1'],
                "CEO_2": rail.result('foreach_user_to_process')['CEO_2'],
                "CEO_3": rail.result('foreach_user_to_process')['CEO_3'],
                "CEO_4": rail.result('foreach_user_to_process')['CEO_4'],
                "CEO_5": rail.result('foreach_user_to_process')['CEO_5'],
                "CEO_6": rail.result('foreach_user_to_process')['CEO_6'],
                "Group_Head": rail.result('foreach_user_to_process')['Group_Head'],
                "Business_Leader": rail.result('foreach_user_to_process')['Business_Leader'],
                "User_Name": rail.result('foreach_user_to_process')['User_Name'],
                "Supervisor": rail.result('foreach_user_to_process')['Supervisor'],
                "Employee_Type": rail.result('foreach_user_to_process')['Employee_Type'],
                "Employee_Type_Eff_Date": rail.result('foreach_user_to_process')['Employee_Type_Eff_Date'],
                "Contingent_Worker_Type": rail.result('foreach_user_to_process')['Time_Type'],
                "FLSA": rail.result('foreach_user_to_process')['FLSA'],
                "FLSA_Eff_Date": rail.result('foreach_user_to_process')['FLSA_Eff_Date'],
                "Worker_Status": rail.result('foreach_user_to_process')['Worker_Status'],
                "FirstDayofLeave": rail.result('foreach_user_to_process')['FirstDayofLeave'],
                "ActualLastDayofLeave": rail.result('foreach_user_to_process')['ActualLastDayofLeave'],
                "Substitute_User": rail.result('foreach_user_to_process')['Substitute_User'],
                "Subs_User_StartDate": rail.result('foreach_user_to_process')['Subs_User_StartDate'],
                "Sub_User_EndDate": rail.result('foreach_user_to_process')['Sub_User_EndDate'],
                "Country": rail.result('foreach_user_to_process')['Country'],
                "positionid_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['positionid'],
                "businesstitle_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['businesstitle'],
                "workspace_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['workspace'],
                "workspace_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionsworkspace_dropdownoptionworkspace_121'), 'displayText', rail.result('foreach_user_to_process')['Work_Space'], 'uri'),
                "costcenter_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['costcenter'],
                "costcenter_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionscostcenter_dropdownoptioncostcenter_122'), 'displayText', rail.result('foreach_user_to_process')['Cost_Center'], 'uri'),
                "costcentercode_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['costcentercode'],
                "department_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['department'],
                "departmentcode_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['departmentcode'],
                "department_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_department_dropdownoptiondepartment_123'), 'displayText', rail.result('foreach_user_to_process')['Department'], 'uri'),
                "profitcenter_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['profitcenter'],
                "profitcentercode_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['profitcentercode'],
                "profitcenter_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionsprofitcenter_dropdownoptionprofitcenter_124'), 'displayText', rail.result('foreach_user_to_process')['Profit_Center'], 'uri'),
                "company_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['company'],
                "company_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionscompany_dropdownoptioncompany_125'), 'displayText', rail.result('foreach_user_to_process')['Company'], 'uri'),
                "companycode_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['companycode'],
                "prefferedfullname_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['prefferedfullname'],
                "fulllegalname_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['fulllegalname'],
                "managementlevel_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['managementlevel'],
                "managementlevel_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_mgmtlevel_dropdownoption_mgmtlevel_126'), 'displayText', rail.result('foreach_user_to_process')['Mgmt_Level'], 'uri'),
                "managementlevelcode_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['managementlevelcode'],
                "employeeresidence_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['employeeresidence'],
                "employeeresidence_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_residencestate_dropdownoption_residencestate_127'), 'displayText', rail.result('foreach_user_to_process')['Home_State'], 'uri'),
                "ceo_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo'],
                "ceo1_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo1'],
                "ceo2_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo2'],
                "ceo3_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo3'],
                "ceo4_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo4'],
                "ceo5_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo5'],
                "ceo6_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ceo6'],
                "groupleader_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['groupleader'],
                "businesleader_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['businesleader'],
                "contingentworkertype_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['contingentworkertype'],
                "workerstatus_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['workerstatus'],
                "firstdayofleave_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['firstdayofleave'],
                "country_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['country'],
                "ceo_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o_dropdownoption_c_e_o_128'), 'displayText', rail.result('foreach_user_to_process')['CEO'], 'uri'),
                "ceo1_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o1_dropdownoption_c_e_o1_129'), 'displayText', rail.result('foreach_user_to_process')['CEO_1'], 'uri'),
                "ceo2_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o2_dropdownoption_c_e_o2_130'), 'displayText', rail.result('foreach_user_to_process')['CEO_2'], 'uri'),
                "ceo3_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o3_dropdownoption_c_e_o3_131'), 'displayText', rail.result('foreach_user_to_process')['CEO_3'], 'uri'),
                "ceo4_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o4_dropdownoption_c_e_o4_132'), 'displayText', rail.result('foreach_user_to_process')['CEO_4'], 'uri'),
                "ceo5_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o5_dropdownoption_c_e_o5_133'), 'displayText', rail.result('foreach_user_to_process')['CEO_5'], 'uri'),
                "ceo6_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_c_e_o6_dropdownoption_c_e_o6_134'), 'displayText', rail.result('foreach_user_to_process')['CEO_6'], 'uri'),
                "groupleader_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionsgroupleader_dropdownoptiongroupleader_135'), 'displayText', rail.result('foreach_user_to_process')['Group_Head'], 'uri'),
                "businesleader_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_businessleader_dropdownoption_businessleader_136'), 'displayText', rail.result('foreach_user_to_process')['Business_Leader'], 'uri'),
                "contingentworkertype_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionscontigentworkertype_dropdownoptioncontigentworkertype_137'), 'displayText', rail.result('foreach_user_to_process')['Time_Type'], 'uri'),
                "workerstatus_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionsworkerstatus_dropdownoptionworkerstatus_138'), 'displayText', rail.result('foreach_user_to_process')['Worker_Status'], 'uri'),
                "lastdayofleave_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['lastdayofleave'],
                "country_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionscountry_dropdownoptionworkspace_139'), 'displayText', rail.result('foreach_user_to_process')['Country'], 'uri'),
                "payrule": rail.result('invoke_custom_ruby_code_145')['payrule'],
                "timezone": rail.result('invoke_custom_ruby_code_145')['timezone'],
                "teammanagerpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_get_all_permission_sets_51'), 'displayText', 'TEAM MANAGER', 'uri'),
                "timezoneuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_zones_get_all_time_zones_55'), 'displayText', rail.result('invoke_custom_ruby_code_145')['timezone'], 'uri'),
                "scheduledweeklyhours_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['scheduledweeklyhours'],
                "manager_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['ismanager'],
                "payrollid_udfuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['payrollid'],
                "manager_optionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_options_manager_dropdownoptionworkspace_140'), 'displayText', 'Yes' if rail.result('foreach_user_to_process')['Is_Manager'] == '1' else 'No', 'uri'),
                "scheduledweeklyhours": rail.result('foreach_user_to_process')['Scheduled_Weekly_Hours'],
                "payrollid": rail.result('foreach_user_to_process')['Payroll_ID'],
                "manager": 'Yes' if rail.result('foreach_user_to_process')['Is_Manager'] == '1' else 'No',
                "holiday_calendar_uri": get_holiday_calendar_uri(rail.result('foreach_user_to_process')['Country']),
            }

        trigger_dag_run_live_horizonmedia_child_add_user_v2_0async_151 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_child_add_user_v2_0async_151',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_add_user_child,
            execution_timeout=timedelta(days=14),
            conf=get_user_dag_run_conf
        )

        trigger_dag_run_live_horizonmedia_user_update_v2_0async_153 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_user_update_v2_0async_153',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_update_user_child,
            execution_timeout=timedelta(days=14),
            conf=get_user_dag_run_conf
        )

        insert_to_user_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list',
            append=True,
            name='{{ result("declare_list_dag_runs").name }}',
            value='{{(result("trigger_dag_run_live_horizonmedia_child_add_user_v2_0async_151") or result("trigger_dag_run_live_horizonmedia_user_update_v2_0async_153"))[0]}}'
        )

        foreach_user_to_process_end = rail.EmptyOperator(
            task_id='foreach_user_to_process_end',
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_user_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_user_process',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list").value | to_json if result("insert_to_user_dag_run_list") | is_truthy else [] }}'
        )

        can_use_conf_payload2 = rail.IfOperator(
            task_id='can_use_conf_payload2',
            test=lambda: Variable.get(
                config.can_use_conf_payload_var_name, default_var='false').lower() == 'true',
            yes_task='gather_supervisor_assignment',
            no_task='query_list_usersnotpresentininputfile_tobedisabled_156'
        )

        query_list_usersnotpresentininputfile_tobedisabled_156 = rail.QueryCollectionOperator(
            task_id='query_list_usersnotpresentininputfile_tobedisabled_156',
            query="""SELECT * FROM  userlist WHERE  userlist.Employee_ID NOT IN (SELECT  inputfilewithmd5.Employee_ID FROM  inputfilewithmd5) AND  userlist.User_Status != 'Disabled'""",
        )

        foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157 = rail.ForEachOperator(
            task_id='foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157',
            items="{{ result('query_list_usersnotpresentininputfile_tobedisabled_156') }}",
            start_task='if_foreach_employeetype_not_equals_to_agencytemp_158',
            end_task='foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end'
        )

        if_foreach_employeetype_not_equals_to_agencytemp_158 = rail.IfOperator(
            task_id='if_foreach_employeetype_not_equals_to_agencytemp_158',
            test='''{{ result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['Employee_Type__Current_'] != 'Agency Temp'  and result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['Login_Name'] != 'RepliconAdmin' }}''',
            yes_task="trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159",
            no_task="foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end",
        )

        def get_replicon_date(date):
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }

        trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159',
            retries=0,
            items=[1],
            trigger_dag_id=config.horizonmedia_user_import_disable_user_child,
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "log": rail.result('create_log'),
                "Employee_ID": rail.result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['Employee_ID'],
                "username": rail.result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['User_Name'],
                "Termination_Date": get_replicon_date(datetime.utcnow() - timedelta(days=3) if datetime.utcnow().weekday() == 1 else datetime.utcnow() - timedelta(days=1)),
                "Active": rail.result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['User_Status'],
                "useruri": rail.result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['UserUri'],
                "userid": rail.result('foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157')['Login_Name'],
                "wokerstatusuri": rail.result('invoke_custom_ruby_code_customfieldsuri_49')['workerstatus'],
                "terminatedstatusuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_field_drop_down_optionsworkerstatus_dropdownoptionworkerstatus_138'), 'displayText', 'Terminated', 'uri')
            }
        )

        insert_to_disabled_user_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_disabled_user_dag_run_list',
            append=True,
            name='disabled_user_dag_run_list ',
            value='{{result("trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159")[0]}}'
        )

        foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end = rail.EmptyOperator(
            task_id='foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end',
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_disabled_user_dag_run_list").value | to_json if  result("insert_to_disabled_user_dag_run_list") | is_truthy else [] }}'
        )

        gather_supervisor_assignment = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_assignment',
            dag_runs='{{ result("insert_to_user_dag_run_list").value | to_json if result("insert_to_user_dag_run_list") | is_truthy else [] }}',
            dagrun_task_id='queue_supervisor_assignment',
            flatten=True,
        )

        process_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_assignment',
            retries=0,
            items="{{ result('gather_supervisor_assignment') | to_json }}",
            trigger_dag_id=config.horizonmedia_user_import_supervisor_assignment_child,
            execution_timeout=timedelta(days=14),
            conf={
                "log": "{{ result('create_log') }}",
                "userid": "{{ item.userloginname }}",
                "username": "{{ item.username }}",
                "supervisorempid": "{{ item.supervisorempid }}",
                "employeeid": "{{ item.employeeid }}",
                "useruri": "{{ item.useruri }}",
                "action": "{{ item.action }}",
                "supervisorpermissionuri": "{{ result('get_all_permission_sets_get_all_permission_sets_51') | find_first_by_attr_and_get_attr('displayText','SUPERVISOR','uri') }}",
                "supeffectivedate": {
                    "day": "{{ item.effectivedate.day }}", "month": "{{ item.effectivedate.month }}", "year": "{{ item.effectivedate.year }}"
                },
                "teammanagerpermission":  "{{ result('get_all_permission_sets_get_all_permission_sets_51') | find_first_by_attr_and_get_attr('displayText','TEAM MANAGER','uri') }}",
            }
        )

        wait_for_process_supervisor_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor_assignment',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_supervisor_assignment") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("insert_to_user_dag_run_list").value | to_json if result("insert_to_user_dag_run_list") | is_truthy else [] }}',
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(map(lambda item: {
                "ecid": item[1][-1]['ecid'],
                "properties": {
                    "employeeid": item[1][-1]['properties']['employeeid'],
                    "username": item[1][-1]['properties']['username'],
                    "action": item[1][-1]['properties']['action'],
                    "status": item[1][-1]['properties']['status'],
                    "details": item[1][-1]['properties']['details'],
                }
            }, [(k, list(g)) for k, g in itertools.groupby(
                list(list(itertools.chain(
                    *list(map(rail.load_all_records, rail.result('gather_child_logs')+[rail.result('create_log')]))))), lambda x:x['properties']['employeeid'])]))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        get_logged_exceptions = rail.PythonOperator(
            task_id='get_logged_exceptions',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Exception')
        )

        get_logged_success = rail.PythonOperator(
            task_id='get_logged_success',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Success')
        )

        create_csv_lines_logs = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_logs',
            source="{{ result('format_logs') | to_json }}",
            header=['Employee ID',
                    'User Name',
                    'Action',
                    'Status',
                    'Details',
                    'Jobid'],
            row=[
                "{{ item.properties.employeeid }}",
                "{{ item.properties.username }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid }}",
            ]
        )

        upload_logs_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_sftp',
            # 'append': 'true' not supported
            content="{{ result('create_csv_lines_logs') }}",
            remote_filepath=config.logpath + \
            '/{{dag_run_ecid() | replace(":", "-") +"_UserImportLogs_" + result("log_formatteddateandtime_27")}}.csv',
        )

        catch_upload_error = rail.IfOperator(
            task_id='catch_upload_error',
            test=lambda: bool(list(filter(lambda x: x.state == 'failed' and x.task_id == 'upload_logs_sftp',
                                          rail.get_current_context()['dag_run'].get_task_instances()))),
            yes_task='send_mail_sendemailastheloguploadfailed_12',
            no_task='send_mail_16'
        )

        send_mail_sendemailastheloguploadfailed_12 = rail.EmailOperator(
            task_id='send_mail_sendemailastheloguploadfailed_12',
            to=config.alert_email,
            subject='''{{ get_company_key() }}| Replicon user import - Uploading Logs to SFTP failed {{ current_time() }}''',
            html_content='''<p>Hi Team,<br /> <br /> The Replicon user import job for {{ get_company_key() }} instance, created on {{ current_time() }} has been completed for file ref "{{ dag_run_ecid() }}", however, the log upload to sftp has failed. Attached is the log file for reference.</p>
            <ul>
            <li>Job ID: {{ dag_run_ecid() }} </li>
            </ul>
            <p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            files=[
                ('{{dag_run_ecid() | replace(":", "-") +"_UserImportLogs_" + result("log_formatteddateandtime_27")}}.csv',
                 "{{ result('create_csv_lines_logs') }}")
            ]
        )

        stop_and_fail = rail.FailOperator(
            task_id='stop_and_fail',
            message='Error'
        )

        send_mail_16 = rail.EmailOperator(
            task_id='send_mail_16',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors') -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon user import - " }} \
                {%- if result("get_logged_errors")-%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions")  -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon user import is \
                {%- set has_errors = result("get_logged_errors") | is_truthy -%}
                 {%- if result("get_logged_errors") -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions") -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " on " + current_time_in_specified_tz() }}. Please find the  log file details below for reference: <br /> <br />
                File path: {{ params.logpath }} <br />
                File name: {{dag_run_ecid() | replace(":", "-") +"_UserImportLogs_" + result("log_formatteddateandtime_27")}}.csv<br />
                </p>
                {%- if has_errors -%}
                {%- endif -%}
                For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc. </p> ''',
            params={'logpath': config.logpath},
        )

        rename_archivethereferncefile_174 = rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferncefile_174',
            existing_filename=config.sftp_ref_file_path,
            new_filename=config.sftp_archive_file_path +
            '/{{dag_run_ecid() | replace(":", "-") + "_Old_horizonmedia_reference.csv" }}'

        )

        upload_uploadnewreference_175 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreference_175',
            content="{{ result('create_csv_lines_create_inputfilewith_m_d5_r_a_a_sdata_10') }}",
            remote_filepath=config.sftp_ref_file_path,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> upload_logs_sftp
        can_run_batch_task >> rail.Label('No') >> can_use_conf_payload
        can_use_conf_payload >> rail.Label(
            'yes') >> get_conf_payload >> workdayreport_4
        can_use_conf_payload >> rail.Label(
            'no') >> get_workdayreport_http_payload >> workdayreport_4

        workdayreport_4 >> create_log >> if_first_employee_id_blank_1_5
        if_first_employee_id_blank_1_5 >> rail.Label(
            'Yes') >> send_mail_6 >> stop_7
        if_first_employee_id_blank_1_5 >> rail.Label('No') >> create_csv_lines_create_inputfilewith_m_d5_r_a_a_sdata_10 >> download_13 >> load_csv_create_list_from_csv_14 >> create_collection_create_list_from_csv_14 >> query_list_inputrecords_15 >> load_csv_create_list_from_csv_16 >> create_collection_create_list_from_csv_16 >> query_list_referencerecords_17 >> declare_list_18 >> query_list_identify_unchangedrecords_19 >> if_query_list_identify_unchangedrecords_19_rows_greater_than_0_20
        if_query_list_identify_unchangedrecords_19_rows_greater_than_0_20 >> rail.Label(
            'Yes') >> add_logs_ignored_records >> query_list_identify_changedrecords_22
        if_query_list_identify_unchangedrecords_19_rows_greater_than_0_20 >> rail.Label(
            'No') >> query_list_identify_changedrecords_22 >> create_list_23 >> query_list_changedrecordswithout_mandatoryfields_24 >> if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_25
        if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_25 >> rail.Label(
            'Yes') >> log_entry_missing_values >> log_formatteddateandtime_27
        if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_25 >> rail.Label(
            'No') >> log_formatteddateandtime_27 >> if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_28
        if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_28 >> rail.Label(
            'Yes') >> create_csv_lines_31 >> upload_32 >> query_list_changedrecordswith_mandatoryfields_33
        if_query_list_changedrecordswithout_mandatoryfields_24_rows_greater_than_0_28 >> rail.Label(
            'No') >> query_list_changedrecordswith_mandatoryfields_33 >> create_list_changedrecordswith_mandatoryfields_34 >> if_query_list_changedrecordswith_mandatoryfields_33_rows_greater_than_0_39
        if_query_list_changedrecordswith_mandatoryfields_33_rows_greater_than_0_39 >> rail.Label(
            'Yes') >> get_report_details >> generate_report_group >> if_generate_report_40_payload_starts_with_nodata_41
        if_generate_report_40_payload_starts_with_nodata_41 >> rail.Label(
            'Yes') >> stop_42
        if_generate_report_40_payload_starts_with_nodata_41 >> rail.Label(
            'No') >> if_generate_report_40_payload_not_starts_with_usernameloginnameemployeeiduseruriuserstatususerenddateemployeetypecurrent_43
        if_generate_report_40_payload_not_starts_with_usernameloginnameemployeeiduseruriuserstatususerenddateemployeetypecurrent_43 >> rail.Label(
            'Yes') >> stop_44
        if_generate_report_40_payload_not_starts_with_usernameloginnameemployeeiduseruriuserstatususerenddateemployeetypecurrent_43 >> rail.Label('No') >> load_user_report_csv_data >> create_list_46 >> get_all_custom_fields_47 >> get_all_time_zones_getalltimezones_48 \
            >> invoke_custom_ruby_code_customfieldsuri_49 >> get_all_permission_sets_get_all_permission_sets_51 >> get_enabled_activities_get_enabled_activities_52 >> get_all_scripts_timeoffvalidationscripts_timeoffvalidationscripts_53 >> get_all_scripts_time_off_balance_event_scripts_time_off_balance_event_scripts_54 \
            >> get_all_time_zones_get_all_time_zones_55 >> get_all_office_schedules_get_all_office_schedules_56 >> get_all_approval_paths_timesheet_get_all_approval_paths_timesheet_57 >> get_all_approval_paths_timeoff_get_all_approval_paths_timeoff_58 >> get_all_policy_sets_get_all_policy_sets_59 \
            >> get_all_holiday_calendars_get_all_holiday_calendars_60 >> get_data_timesheet_period_list_service1_timesheet_period_list_service1_61 >> invoke_custom_ruby_code_timesheet_period_list_62 >> query_list_getlocationsvalue_63 >> trigger_dag_run_live_horizonmedia_groups_check_child64 \
            >> query_list_get_departmentgroupsvalue_65 >> trigger_dag_run_live_horizonmedia_groups_check_child66 >> query_list_get_divisionvalue_67 >> trigger_dag_run_live_horizonmedia_groups_check_childdivision_68 >> query_list_get_servicecentervalue_69 \
            >> trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70 >> query_list_getemployeetypevalue_71 >> trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72 >> query_list_getcostcentergroupsvalue_73 >> trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childcostcentersgroups_74 >> query_list_getunique_c_e_ovalues_75 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child76 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child76 \
            >> query_list_getunique_c_e_o1values_77 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child78 >> query_list_getunique_c_e_o2values_79 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child80 >> query_list_getunique_c_e_o3values_81 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child82 \
            >> query_list_getunique_c_e_o4values_83 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child84 >> query_list_getunique_c_e_o5values_85 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child86 >> query_list_getunique_c_e_o6values_87 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child88 \
            >> query_list_getuniqueworkspacevalues_89 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child90 >> query_list_getuniquecostcentervalues_91 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child92 >> query_list_getuniquedepartmentcustomfields_93 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child94 \
            >> query_list_getuniqueprofitcentercustomfields_95 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child96 >> query_list_getuniqueprofitcentercustomfields_97 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child98 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child98 \
            >> query_list_getuniquecompanycustomfields_99 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child100 >> query_list_getuniquemgmtlevelcustomfields_101 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child102 >> query_list_getuniquehomestatecustomfields_103 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child104 \
            >> query_list_getuniquegroupheadcustomfields_105 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child106 >> query_list_getuniquebusinessleadercustomfields_107 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child108 >> query_list_getuniquecompanycustomfields_109 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child110 \
            >> query_list_getuniqueworkerstatuscustomfields_111 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child112 >> query_list_getuniquecountrycustomfields_113 >> trigger_dag_run_live_horizonmedia_process_custom_fields_child114 >> wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_child64 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_child66 >> wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childdivision_68 >> wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childservicecenter_70 >> wait_for_completion_trigger_dag_run_live_horizonmedia_groups_check_childemployeetype_72 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child78 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child80 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child82 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child84 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child86 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child88 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child90 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child92 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child94 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child96 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child100 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child102 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child104 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child106 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child108 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child110 \
            >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child112 >> wait_for_completion_trigger_dag_run_live_horizonmedia_process_custom_fields_child114 >> get_enabled_employee_type_groups_employeetypes_115 >> get_enabled_department_groups_departments_116 >> get_enabled_service_centers_servicecenters_117 \
            >> get_enabled_locations_locations_118 >> get_enabled_divisions_divisions_119 >> get_enabled_cost_centers_costcenters_120 >> get_all_custom_field_drop_down_optionsworkspace_dropdownoptionworkspace_121 >> get_all_custom_field_drop_down_optionscostcenter_dropdownoptioncostcenter_122 >> get_all_custom_field_drop_down_options_department_dropdownoptiondepartment_123 \
            >> get_all_custom_field_drop_down_optionsprofitcenter_dropdownoptionprofitcenter_124 >> get_all_custom_field_drop_down_optionscompany_dropdownoptioncompany_125 >> get_all_custom_field_drop_down_options_mgmtlevel_dropdownoption_mgmtlevel_126 >> get_all_custom_field_drop_down_options_residencestate_dropdownoption_residencestate_127 \
            >> get_all_custom_field_drop_down_options_c_e_o_dropdownoption_c_e_o_128 >> get_all_custom_field_drop_down_options_c_e_o1_dropdownoption_c_e_o1_129 >> get_all_custom_field_drop_down_options_c_e_o2_dropdownoption_c_e_o2_130 >> get_all_custom_field_drop_down_options_c_e_o3_dropdownoption_c_e_o3_131 >> get_all_custom_field_drop_down_options_c_e_o4_dropdownoption_c_e_o4_132 \
            >> get_all_custom_field_drop_down_options_c_e_o5_dropdownoption_c_e_o5_133 >> get_all_custom_field_drop_down_options_c_e_o6_dropdownoption_c_e_o6_134 >> get_all_custom_field_drop_down_optionsgroupleader_dropdownoptiongroupleader_135 >> get_all_custom_field_drop_down_options_businessleader_dropdownoption_businessleader_136 \
            >> get_all_custom_field_drop_down_optionscontigentworkertype_dropdownoptioncontigentworkertype_137 >> get_all_custom_field_drop_down_optionsworkerstatus_dropdownoptionworkerstatus_138 >> get_all_custom_field_drop_down_optionscountry_dropdownoptionworkspace_139 >> get_all_custom_field_drop_down_options_manager_dropdownoptionworkspace_140 \
            >> horizonmedia_activity_mapper_v3_0_search_entries_141 >> horizonmedia_user_import_master_mapper_search_entries_142 >> declare_list_dag_runs >> query_report_records_matching_with_delta_records >> load_filtered_report_records_as_per_changed_records >> foreach_user_to_process >> search_user_uri >> invoke_custom_ruby_code_145 >> if_output_useruri_blank_150
        if_output_useruri_blank_150 >> rail.Label(
            'Yes') >> trigger_dag_run_live_horizonmedia_child_add_user_v2_0async_151 >> insert_to_user_dag_run_list >> foreach_user_to_process_end
        if_output_useruri_blank_150 >> rail.Label(
            'No') >> trigger_dag_run_live_horizonmedia_user_update_v2_0async_153 >> insert_to_user_dag_run_list >> foreach_user_to_process_end
        foreach_user_to_process >> foreach_user_to_process_end
        foreach_user_to_process_end >> wait_for_completion_trigger_dag_run_live_horizonmedia_user_process >> can_use_conf_payload2
        can_use_conf_payload2 >> rail.Label(
            'yes') >> gather_supervisor_assignment
        can_use_conf_payload2 >> rail.Label(
            'no') >> query_list_usersnotpresentininputfile_tobedisabled_156
        query_list_usersnotpresentininputfile_tobedisabled_156 >> foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157 >> if_foreach_employeetype_not_equals_to_agencytemp_158
        if_foreach_employeetype_not_equals_to_agencytemp_158 >> rail.Label(
            'Yes') >> trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159 >> insert_to_disabled_user_dag_run_list >> foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end
        if_foreach_employeetype_not_equals_to_agencytemp_158 >> rail.Label(
            'No') >> foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end
        foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157 >> foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end
        foreach_query_list_usersnotpresentininputfile_tobedisabled_156_157_end >> wait_for_completion_trigger_dag_run_live_horizonmedia_child_disable_user_v2_0async_159 >> gather_supervisor_assignment
        gather_supervisor_assignment >> process_supervisor_assignment >> wait_for_process_supervisor_assignment >> gather_child_logs >> format_logs >> get_logged_errors >> get_logged_exceptions >> get_logged_success >> create_csv_lines_logs >> upload_logs_sftp >> catch_upload_error
        catch_upload_error >> rail.Label(
            'error') >> send_mail_sendemailastheloguploadfailed_12 >> stop_and_fail >> log_to_sumo
        catch_upload_error >> rail.Label(
            'success') >> send_mail_16 >> rename_archivethereferncefile_174 >> upload_uploadnewreference_175 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
