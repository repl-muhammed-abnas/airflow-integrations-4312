
from datetime import timedelta, datetime
import hashlib
import json
from airflow.models import Variable
import rail
from pendulum import datetime as dt

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_master_{config.instance}',
        description=f'MichaelKorsTnA UK User Sync Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_workday_report'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_workday_report',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_workday_report = rail.SimpleHttpOperator(
                task_id='get_workday_report',
                method='GET',
                http_conn_id=config.workday_http_conn_id,
                endpoint='/CR_INT065_Replicon_RaaS?format=json',
                headers={
                    "Content-Type": "application/json"
                },
                extra_options={
                    'verify': False
                }
            )

        def get_report_data(reportdata):
            for user in reportdata:
                for key, value in user.items():
                    if value is None:
                        user[key] = ""
            return reportdata


        get_report_5 = rail.PythonOperator(
            task_id='get_report_5',
            python_callable=lambda dag_run: get_report_data(dag_run.conf['report'] if 'report' in dag_run.conf 
                                                            else json.loads(rail.result('get_workday_report'))['Report_Entry'])
        )

        if_get_report_5_report_less_than_1_8 = rail.IfOperator(
            task_id='if_get_report_5_report_less_than_1_8',
            test=lambda: len(rail.result('get_report_5')) < 1,
            yes_task="trigger_child_disable_users",
            no_task="create_csv_lines_13",
        )

        trigger_child_disable_users = rail.TriggerDagRunOperator(
            task_id='trigger_child_disable_users',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_disable_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_child_disable_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_disable_users',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_disable_users") }}'
        )

        send_mail_send_emailfornorecordstoprocess_10 = rail.EmailOperator(
            task_id='send_mail_send_emailfornorecordstoprocess_10',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | ''' + config.jobtype + \
            ''' - no records to process - {{ current_time() }} ''',
            html_content='''templates/no_records_to_process_mail.html''',
            params={
                "jobtype": config.jobtype
            },
        )

        create_csv_lines_13 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_13',
            source=lambda: rail.result('get_report_5'),
            header=['Employee ID',
                    'FirstName',
                    'LastName',
                    'Hire_Date',
                    'Original_Hire_Date',
                    'businessTitle',
                    'Job_Profile',
                    'Job_Profile_Code',
                    'Job_Family',
                    'Job_Family_Group',
                    'Compensation_Grade',
                    'CostCenter_ID',
                    'CostCenter_Name',
                    'Cost_Center_Hierarchy',
                    'Business_Organization',
                    'Country',
                    'Location',
                    'Location_Type',
                    'Scheduled_Weekly_Hours',
                    'Default_Weekly_Hours',
                    'Employee_Type',
                    'Contract_Type',
                    'Contract_End_Date',
                    'Collective_Agreement',
                    'Manager_ID',
                    'Manager',
                    'Termination_Date',
                    'Last_Day_of_Work',
                    'Location_Address',
                    'Work_Email',
                    'Effective_Date',
                    'md5'],
            row=lambda item: [
                item['Employee_ID'],
                item['FirstName'],
                item['LastName'],
                item['Hire_Date'],
                item['Original_Hire_Date'],
                item['businessTitle'],
                item['Job_Profile'],
                item['Job_Profile_Code'],
                item['Job_Family'],
                item['Job_Family_Group'],
                item['Compensation_Grade'],
                item['CostCenter_ID'],
                item['CostCenter_Name'],
                item['Cost_Center_Hierarchy'],
                item['Business_Organization'],
                item['Country'],
                item['Location'],
                item['Location_Type'],
                item['Scheduled_Weekly_Hours'],
                item['Default_Weekly_Hours'],
                item['Employee_Type'],
                item['Contract_Type'],
                item['Contract_End_Date'],
                item['Collective_Agreement'],
                item['Manager_ID'],
                item['Manager'],
                item['Termination_Date'],
                item['Last_Day_of_Work'],
                item['Location_Address'],
                item['Work_Email'],
                item['Effective_Date'],
                hashlib.md5(str(str(item['Employee_ID']) + "_" + str(item['FirstName']) + "_" + str(item['LastName']) + "_" + str(item['Hire_Date']) + "_" +
                str(item['Original_Hire_Date']) + "_" + str(item['businessTitle']) + "_" + str(item['Job_Profile']) + "_" +
                str(item['Job_Profile_Code']) + "_" + str(item['Job_Family']) + "_" + str(item['Job_Family_Group']) + "_" +
                str(item['Compensation_Grade']) + "_" + str(item['CostCenter_ID']) + "_" + str(item['CostCenter_Name']) + "_" +
                str(item['Cost_Center_Hierarchy']) + "_" + str(item['Business_Organization']) + "_" + str(item['Country']) + "_" +
                str(item['Location']) + "_" + str(item['Location_Type']) + "_" + str(item['Location_Type']) + "_" + str(item['Scheduled_Weekly_Hours']) + "_" +
                str(item['Scheduled_Weekly_Hours']) + "_" + str(item['Employee_Type']) + "_" + str(item['Contract_Type']) + "_" +
                str(item['Contract_End_Date']) + "_" + str(item['Collective_Agreement']) + "_" + str(item['Manager_ID']) + "_" +
                str(item['Manager']) + "_" + str(item['Termination_Date']) + "_" + str(item['Last_Day_of_Work']) + "_" +
                str(item['Location_Address']) + "_" + str(item['Work_Email']) + "_" + str(item['Effective_Date'])).encode('utf-8')).hexdigest()
            ],
        )

        create_collection_create_list_from_csv_14 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_14',
            source="{{ result('create_csv_lines_13') }}",
            name="rawdata",
            columns={
                'Employee ID': 'Employee_ID',
                'FirstName': 'FirstName',
                'LastName': 'LastName',
                'Hire_Date': 'Hire_Date',
                'Original_Hire_Date': 'Original_Hire_Date',
                'businessTitle': 'businessTitle',
                'Job_Profile': 'Job_Profile',
                'Job_Profile_Code': 'Job_Profile_Code',
                'Job_Family': 'Job_Family',
                'Job_Family_Group': 'Job_Family_Group',
                'Compensation_Grade': 'Compensation_Grade',
                'CostCenter_ID': 'CostCenter_ID',
                'CostCenter_Name': 'CostCenter_Name',
                'Cost_Center_Hierarchy': 'Cost_Center_Hierarchy',
                'Business_Organization': 'Business_Organization',
                'Country': 'Country',
                'Location': 'Location',
                'Location_Type': 'Location_Type',
                'Scheduled_Weekly_Hours': 'Scheduled_Weekly_Hours',
                'Default_Weekly_Hours': 'Default_Weekly_Hours',
                'Employee_Type': 'Employee_Type',
                'Contract_Type': 'Contract_Type',
                'Contract_End_Date': 'Contract_End_Date',
                'Collective_Agreement': 'Collective_Agreement',
                'Manager_ID': 'Manager_ID',
                'Manager': 'Manager',
                'Termination_Date': 'Termination_Date',
                'Last_Day_of_Work': 'Last_Day_of_Work',
                'Location_Address': 'Location_Address',
                'Work_Email': 'Work_Email',
                'Effective_Date': 'Effective_Date',
                'md5': 'md5'
            }
        )

        download_16 = rail.SFTPDownloadFileOperator(
            task_id='download_16',
            remote_filepath=config.reference_filepath + 'mkusersync_reference.csv'
        )

        load_csv_create_list_from_csv_17 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_17",
            document="{{result('download_16')}}",
        )

        create_collection_create_list_from_csv_17 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_17',
            source="{{ result('load_csv_create_list_from_csv_17') }}",
            name="referencedata",
            columns={
                'Employee ID': 'Employee_ID',
                'FirstName': 'FirstName',
                'LastName': 'LastName',
                'Hire_Date': 'Hire_Date',
                'Original_Hire_Date': 'Original_Hire_Date',
                'businessTitle': 'businessTitle',
                'Job_Profile': 'Job_Profile',
                'Job_Profile_Code': 'Job_Profile_Code',
                'Job_Family': 'Job_Family',
                'Job_Family_Group': 'Job_Family_Group',
                'Compensation_Grade': 'Compensation_Grade',
                'CostCenter_ID': 'CostCenter_ID',
                'CostCenter_Name': 'CostCenter_Name',
                'Cost_Center_Hierarchy': 'Cost_Center_Hierarchy',
                'Business_Organization': 'Business_Organization',
                'Country': 'Country',
                'Location': 'Location',
                'Location_Type': 'Location_Type',
                'Scheduled_Weekly_Hours': 'Scheduled_Weekly_Hours',
                'Default_Weekly_Hours': 'Default_Weekly_Hours',
                'Employee_Type': 'Employee_Type',
                'Contract_Type': 'Contract_Type',
                'Contract_End_Date': 'Contract_End_Date',
                'Collective_Agreement': 'Collective_Agreement',
                'Manager_ID': 'Manager_ID',
                'Manager': 'Manager',
                'Termination_Date': 'Termination_Date',
                'Last_Day_of_Work': 'Last_Day_of_Work',
                'Location_Address': 'Location_Address',
                'Work_Email': 'Work_Email',
                'Effective_Date': 'Effective_Date',
                'md5': 'md5'
            }
        )

        query_list_unchangedrecords_18 = rail.QueryCollectionOperator(
            task_id='query_list_unchangedrecords_18',
            query="""SELECT * FROM  rawdata WHERE  rawdata.md5 IN (SELECT  referencedata.md5 FROM  referencedata)""",
        )

        query_list_changedrecords_19 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecords_19',
            name="changeddata",
            query="""SELECT * FROM  rawdata WHERE  rawdata.md5 NOT  IN (SELECT  referencedata.md5 FROM  referencedata)""",
        )

        if_query_list_changedrecords_19_rows_less_than_1_20 = rail.IfOperator(
            task_id='if_query_list_changedrecords_19_rows_less_than_1_20',
            test='''{{ result('query_list_changedrecords_19','length') < 1 }}''',
            yes_task="trigger_disable_users_child",
            no_task="query_list_getalltheusersfor_uk_25",
        )

        trigger_disable_users_child = rail.TriggerDagRunOperator(
            task_id='trigger_disable_users_child',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_disable_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_disable_users_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_users_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_disable_users_child") }}'
        )

        send_mail_send_emailfornorecordstoprocess_22 = rail.EmailOperator(
            task_id='send_mail_send_emailfornorecordstoprocess_22',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | ''' + config.jobtype + \
            ''' - no delta records to process - {{ current_time() }} ''',
            html_content='''templates/no_records_to_process_mail.html''',
            params={
                "jobtype": config.jobtype
            },
        )

        query_list_getalltheusersfor_uk_25 = rail.QueryCollectionOperator(
            task_id='query_list_getalltheusersfor_uk_25',
            query="""SELECT * FROM  changeddata WHERE  changeddata.Country = "United Kingdom" """,
        )

        if_first_employee_id_blank_26 = rail.IfOperator(
            task_id='if_first_employee_id_blank_26',
            test='''{{ result('query_list_getalltheusersfor_uk_25','length') < 1 }}''',
            yes_task="trigger_child_to_disable_users",
            no_task="if_query_list_changedrecords_19_rows_greater_than_0_30",
        )

        trigger_child_to_disable_users = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_disable_users',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_disable_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_child_to_disable_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_disable_users',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_disable_users") }}'
        )

        send_mail_send_emailfornorecordstoprocess_28 = rail.EmailOperator(
            task_id='send_mail_send_emailfornorecordstoprocess_28',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | ''' + config.jobtype + \
            ''' - no records to process - {{ current_time() }} ''',
            html_content='''templates/no_records_to_process_mail.html''',
            params={
                "jobtype": config.jobtype
            },
        )

        if_query_list_changedrecords_19_rows_greater_than_0_30 = rail.IfOperator(
            task_id='if_query_list_changedrecords_19_rows_greater_than_0_30',
            test='''{{ result('query_list_changedrecords_19','length') > 0 }}''',
            yes_task="trigger_groups_update_child",
            no_task="log_to_sumo",
        )

        trigger_groups_update_child = rail.TriggerDagRunOperator(
            task_id='trigger_groups_update_child',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_groups_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda :{
                #"report": "https://services1.myworkday.com/ccx/service/customreport2/capri/ISU_Replicon/CR_INT065_Replicon_RaaS?format=json",
                "report": rail.result('get_report_5'),
                "country": "United Kingdom"
            }
        )

        wait_for_groups_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_groups_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_groups_update_child") }}'
        )

        def get_group_list(response):
            groups = response['rows']
            return [{
                'groupname': group['cells'][0]['textValue'],
                'groupuri': group['cells'][0]['uri'],
                'fullpath': rail.smartjoin_by_delim([cell['textValue'] for cell in group['cells'][-1]['cellCollection']], '/')
            } for group in groups]

        get_cost_center_details_33 = rail.RepliconServiceOperator(
            task_id='get_cost_center_details_33',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_group_list
        )

        get_department_group_details_34 = rail.RepliconServiceOperator(
            task_id='get_department_group_details_34',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_group_list
        )

        get_location_details_35 = rail.RepliconServiceOperator(
            task_id='get_location_details_35',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_group_list
        )

        get_servicecenters_weekly_schedule_group_details_36 = rail.RepliconServiceOperator(
            task_id='get_servicecenters_weekly_schedule_group_details_36',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
        )

        _adhoc_http_action_42 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_42',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                "originalhiredate": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Original Hire Date', 'uri', '') if response and response[0]['displayText'] else '',
                "businesstitle": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Business Title', 'uri', '') if response and response[0]['displayText'] else '',
                "jobprofile": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Job Profile', 'uri', '') if response and response[0]['displayText'] else '',
                "jobprofilecode": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Job Profile Code', 'uri', '') if response and response[0]['displayText'] else '',
                "compensationgrade": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Compensation Grade', 'uri', '') if response and response[0]['displayText'] else '',
                "scheduledweeklyhours": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Scheduled Weekly Hours', 'uri', '') if response and response[0]['displayText'] else '',
                "defaultweeklyhours": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Default Weekly Hours', 'uri', '') if response and response[0]['displayText'] else '',
                "contracttype": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Contract Type', 'uri', '') if response and response[0]['displayText'] else '',
                "contractenddate": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Contract End Date', 'uri', '') if response and response[0]['displayText'] else '',
                "collectiveagreement": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Collective Agreement', 'uri', '') if response and response[0]['displayText'] else '',
                "lastdayofwork": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Last Day of Work', 'uri', '') if response and response[0]['displayText'] else '',
                "locationaddress": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Location Address', 'uri', '') if response and response[0]['displayText'] else '',
                "yearlyentitlement": rail.find_first_by_attr_and_get_attr(response, 'displayText',
                    'Yearly Entitlement', 'uri', '') if response and response[0]['displayText'] else '',
            }
        )

        create_user_import_logtable = rail.CreateLogOperator(
            task_id='create_user_import_logtable'
        )

        create_supervisor_assignment_lookup = rail.CreateLogOperator(
            task_id='create_supervisor_assignment_lookup'
        )

        create_child_triggered_list = rail.SetVariableOperator(
            task_id='create_child_triggered_list',
            name='childtriggered',
            append=False,
            value=[]
        )

        foreach_query_list_getalltheusersfor_uk_25_44 = rail.ForEachOperator(
            task_id='foreach_query_list_getalltheusersfor_uk_25_44',
            items="{{ result('query_list_getalltheusersfor_uk_25') }}",
            start_task='if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45',
            end_task='foreach_query_list_getalltheusersfor_uk_25_44_end'
        )

        if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45 = rail.IfOperator(
            task_id='if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45',
            test='''{{ result('foreach_query_list_getalltheusersfor_uk_25_44').Employee_ID | is_truthy }}''',
            yes_task="invoke_custom_ruby_code_47",
            no_task="michael_kors_gmbh_user_sync_logs_add_entry_60",
        )

        invoke_custom_ruby_code_47 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_47',
            python_callable=lambda: {
                "requireddepartment": rail.smartjoin_by_delim(("Michael Kors/" + str(rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['Job_Family_Group']) + "/" + str(rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['Job_Family'])).split("/"), "/"),
                "requiredcostcenter": rail.smartjoin_by_delim((str(rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['Cost_Center_Hierarchy']) + "/" + str(rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['CostCenter_ID'])).split("/"), "/"),
                "requiredlocation": rail.smartjoin_by_delim((str(rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['Business_Organization']) + "/" + str(rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['Location'])).split("/"), "/")
            }
        )

        def get_user_uri(response):
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == rail.result(
                    'foreach_query_list_getalltheusersfor_uk_25_44')['Employee_ID'], response['rows']))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else ''
            }

        search_users_51 = rail.RepliconServiceOperator(
            task_id='search_users_51',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": (rail.result('foreach_query_list_getalltheusersfor_uk_25_44')['Employee_ID']).strip()
                        }
                    }
                }
            },
            data_handler=get_user_uri
        )

        if_log_checkiftherequireduserisavailable_52_present_53 = rail.IfOperator(
            task_id='if_log_checkiftherequireduserisavailable_52_present_53',
            test='''{{ result('search_users_51').uri | is_truthy }}''',
            yes_task="trigger_user_update_child",
            no_task="trigger_add_user_child",
        )

        def get_add_update_user_payload(action):
            user = rail.result(
                'foreach_query_list_getalltheusersfor_uk_25_44')
            payload = {
                "employeeid": user['Employee_ID'].strip() if user['Employee_ID'] else '',
                "firstname": user['FirstName'].strip() if user['FirstName'] else '',
                "lastname": user['LastName'].strip() if user['LastName'] else '',
                "hiredate": user['Hire_Date'] if user['Hire_Date'] else '',
                "originalhiredate": user['Original_Hire_Date'] if user['Original_Hire_Date'] else '',
                "businesstitle": user['businessTitle'].strip() if user['businessTitle'] else '',
                "jobprofile": user['Job_Profile'].strip() if user['Job_Profile'] else '',
                "jobprofilecode": user['Job_Profile_Code'].strip() if user['Job_Profile_Code'] else '',
                "jobfamily": user['Job_Family'].strip() if user['Job_Family'] else '',
                "jobfamilygroup": user['Job_Family_Group'].strip() if user['Job_Family_Group'] else '',
                "compensationgrade": user['Compensation_Grade'].strip() if user['Compensation_Grade'] else '',
                "costcenterid": user['CostCenter_ID'].strip() if user['CostCenter_ID'] else '',
                "costcentername": user['CostCenter_Name'].strip() if user['CostCenter_Name'] else '',
                "costcenterhierarchy": user['Cost_Center_Hierarchy'].strip() if user['Cost_Center_Hierarchy'] else '',
                "businessorganization": user['Business_Organization'].strip() if user['Business_Organization'] else '',
                "country": user['Country'].strip() if user['Country'] else '',
                "location": user['Location'].strip() if user['Location'] else '',
                "locationtype": user['Location_Type'].strip() if user['Location_Type'] else '',
                "scheduledweeklyhours": user['Scheduled_Weekly_Hours'].strip() if user['Scheduled_Weekly_Hours'] else '',
                "defaultweeklyhours": user['Default_Weekly_Hours'].strip() if user['Default_Weekly_Hours'] else '',
                "employeetype": user['Employee_Type'].strip() if user['Employee_Type'] else '',
                "contracttype": user['Contract_Type'].strip() if user['Contract_Type'] else '',
                "contractenddate": user['Contract_End_Date'] if user['Contract_End_Date'] else '',
                "collectiveagreement": user['Collective_Agreement'].strip() if user['Collective_Agreement'] else '',
                "managerid": user['Manager_ID'].strip() if user['Manager_ID'] else '',
                "workersmanager": user['Manager'].strip() if user['Manager'] else '',
                "terminationdate": user['Termination_Date'] if user['Termination_Date'] else '',
                "lastdayofwork": user['Last_Day_of_Work'] if user['Last_Day_of_Work'] else '',
                "locationaddress": user['Location_Address'].strip() if user['Location_Address'] else '',
                "workemail": user['Work_Email'].strip() if user['Work_Email'] else '',
                "locationaddressuri": rail.result('_adhoc_http_action_42')['locationaddress'],
                "lastdayofworkuri": rail.result('_adhoc_http_action_42')['lastdayofwork'],
                "collectiveagreementuri": rail.result('_adhoc_http_action_42')['collectiveagreement'],
                "contractenddateuri": rail.result('_adhoc_http_action_42')['contractenddate'],
                "contracttypeuri": rail.result('_adhoc_http_action_42')['contracttype'],
                "defaultweeklyhoursuri": rail.result('_adhoc_http_action_42')['defaultweeklyhours'],
                "scheduledweeklyhoursuri": rail.result('_adhoc_http_action_42')['scheduledweeklyhours'],
                "compensationgradeuri": rail.result('_adhoc_http_action_42')['compensationgrade'],
                "jobprofilecodeuri": rail.result('_adhoc_http_action_42')['jobprofilecode'],
                "jobprofileuri": rail.result('_adhoc_http_action_42')['jobprofile'],
                "businesstitleuri": rail.result('_adhoc_http_action_42')['businesstitle'],
                "originalhiredateuri": rail.result('_adhoc_http_action_42')['originalhiredate'],
                "type": action,
                "departmenturi": (rail.find_first_by_attr_and_get_attr(rail.result('get_department_group_details_34'), 'fullpath', rail.result(
                    'invoke_custom_ruby_code_47')['requireddepartment'], 'groupuri', '') if rail.result(
                    'get_department_group_details_34') else '') if rail.result('invoke_custom_ruby_code_47')['requireddepartment'] else '',
                "costcenteruri": (rail.find_first_by_attr_and_get_attr(rail.result('get_cost_center_details_33'), 'fullpath', rail.result(
                    'invoke_custom_ruby_code_47')['requiredcostcenter'], 'groupuri', '') if rail.result(
                    'get_cost_center_details_33') else '') if rail.result('invoke_custom_ruby_code_47')['requiredcostcenter'] else '',
                "locationuri": (rail.find_first_by_attr_and_get_attr(rail.result('get_location_details_35'), 'fullpath', rail.result(
                    'invoke_custom_ruby_code_47')['requiredlocation'], 'groupuri', '') if rail.result(
                    'get_location_details_35') else '') if rail.result('invoke_custom_ruby_code_47')['requiredlocation'] else '',
                "weeklyscheduleeffectivedate": user['Effective_Date'] if user['Effective_Date'] else datetime.now().strftime('%Y-%m-%d'),
                "weeklyscheduleuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_servicecenters_weekly_schedule_group_details_36'), 'displayText', user['Scheduled_Weekly_Hours'], 'uri', '') if user[
                    'Scheduled_Weekly_Hours'] else '',
                "yearlyentitlementuri": rail.result('_adhoc_http_action_42')['yearlyentitlement'],
                "userimportlogtable": rail.result('create_user_import_logtable'),
                "supervisorlookup": rail.result('create_supervisor_assignment_lookup'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
            if action == 'Update':
                payload.update({'useruri': rail.result('search_users_51')['uri']})
            return payload

        trigger_user_update_child = rail.TriggerDagRunOperator(
            task_id='trigger_user_update_child',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_update_user_payload('Update')
        )

        insert_update_user_child_id_to_waitlist = rail.SetVariableOperator(
            task_id='insert_update_user_child_id_to_waitlist',
            name='childtriggered',
            append=True,
            value="{{result('trigger_user_update_child')}}"
        )

        trigger_add_user_child = rail.TriggerDagRunOperator(
            task_id='trigger_add_user_child',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_update_user_payload('Add')
        )

        insert_add_user_child_id_to_waitlist = rail.SetVariableOperator(
            task_id='insert_add_user_child_id_to_waitlist',
            name='childtriggered',
            append=True,
            value="{{result('trigger_add_user_child')}}"
        )

        if_error_in_validation = rail.IfOperator(
            task_id='if_error_in_validation',
            trigger_rule='all_done',
            #pylint: disable = line-too-long
            test='{{get_error_message() | is_truthy and get_task_state("if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45") == "success"}}',
            yes_task='michael_kors_gmbh_user_sync_logs_add_entry_58',
            no_task='foreach_query_list_getalltheusersfor_uk_25_44_end'
        )

        michael_kors_gmbh_user_sync_logs_add_entry_58 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_add_entry_58',
            log="{{ result('create_user_import_logtable') }}",
            message="na",
            severity="Error",
            properties={
                "loginname": "{{result('foreach_query_list_getalltheusersfor_uk_25_44').Employee_ID}}",
                "action": "Validation",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": '',
                #pylint: disable = line-too-long
                "username": "{{ result('foreach_query_list_getalltheusersfor_uk_25_44').FirstName }} {{ result('foreach_query_list_getalltheusersfor_uk_25_44').LastName }}"
            }
        )

        michael_kors_gmbh_user_sync_logs_add_entry_60 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_add_entry_60',
            log="{{  result('create_user_import_logtable') }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{result('foreach_query_list_getalltheusersfor_uk_25_44').Employee_ID}}",
                "action": "Validation",
                "status": "Skipped",
                "details": "Employee id is not present in workday",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": '',
                #pylint: disable = line-too-long
                "username": "{{ result('foreach_query_list_getalltheusersfor_uk_25_44').FirstName }} {{ result('foreach_query_list_getalltheusersfor_uk_25_44').LastName }}"
            }
        )

        foreach_query_list_getalltheusersfor_uk_25_44_end = rail.EmptyOperator(
            task_id='foreach_query_list_getalltheusersfor_uk_25_44_end',
        )

        if_child_triggered = rail.IfOperator(
            task_id='if_child_triggered',
            test=lambda: len(rail.get_dag_run_var('childtriggered')) > 0,
            yes_task='get_child_ids_to_wait_for',
            no_task='michael_kors_gmbh_supervisor_assignment_table_search_entries_66'
        )

        get_child_ids_to_wait_for = rail.PythonOperator(
            task_id = 'get_child_ids_to_wait_for',
            python_callable=lambda: rail.get_dag_run_var('childtriggered')
        )

        wait_for_add_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("get_child_ids_to_wait_for") | to_json }}'
        )

        michael_kors_gmbh_supervisor_assignment_table_search_entries_66 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_supervisor_assignment_table_search_entries_66',
            log="{{result('create_supervisor_assignment_lookup')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "status": "queued"
            }
        )

        load_supervisor_entries = rail.PythonOperator(
            task_id='load_supervisor_entries',
            python_callable=lambda: rail.load_all_records(rail.result(
                'michael_kors_gmbh_supervisor_assignment_table_search_entries_66'))
        )

        if_first_id_present_67 = rail.IfOperator(
            task_id='if_first_id_present_67',
            test='''{{ result('michael_kors_gmbh_supervisor_assignment_table_search_entries_66','length') > 0 }}''',
            yes_task="log_getalltheuniqsupervisor_68",
            no_task="log_file_name_6",
        )

        log_getalltheuniqsupervisor_68 = rail.PythonOperator(
            task_id='log_getalltheuniqsupervisor_68',
            python_callable=lambda: list(
                {entry['properties']['supervisorloginname'] for entry in rail.result('load_supervisor_entries')})
        )

        create_list_of_supervisor_child = rail.SetVariableOperator(
            task_id='create_list_of_supervisor_child',
            name='supervisorcreation',
            append=False,
            value=[]
        )

        create_error_list_for_supervisor = rail.SetVariableOperator(
            task_id='create_error_list_for_supervisor',
            name='Foreign_Supervisor_Creation_Error_List',
            append=False,
            value=[]
        )

        foreach_create_list_69_70 = rail.ForEachOperator(
            task_id='foreach_create_list_69_70',
            items=lambda: rail.result('log_getalltheuniqsupervisor_68'),
            start_task='log_required_supervisor_user_name_72',
            end_task='foreach_create_list_69_70_end'
        )

        log_required_supervisor_user_name_72 = rail.PythonOperator(
            task_id='log_required_supervisor_user_name_72',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'load_supervisor_entries'), 'properties.supervisorloginname', rail.result('foreach_create_list_69_70'), 'properties.supervisorusername', '')
        )

        trigger_child_add_foreign_supervisor = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_foreign_supervisor',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_foreign_supervisor_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "supervisorloginname": "{{ result('foreach_create_list_69_70') }}",
                "callerjobid": "{{ dag_run_ecid() }}",
                "supervisorname": "{{ result('log_required_supervisor_user_name_72') }}"
            }
        )

        insert_to_supervisor_waitlist = rail.SetVariableOperator(
            task_id='insert_to_supervisor_waitlist',
            name='supervisorcreation',
            append=True,
            value="{{result('trigger_child_add_foreign_supervisor')}}"
        )

        if_error_in_foreign_supervisor_creation = rail.IfOperator(
            task_id='if_error_in_foreign_supervisor_creation',
            trigger_rule='one_failed',
            test='{{ get_task_state("trigger_child_add_foreign_supervisor") == "failed"}}',
            yes_task='accumulate_list_items_76',
            no_task='foreach_create_list_69_70_end'
        )

        accumulate_list_items_76 = rail.SetVariableOperator(
            task_id='accumulate_list_items_76',
            name='Foreign_Supervisor_Creation_Error_List',
            append=True,
            value={
                "supervisorname": "{{ result('log_required_supervisor_user_name_72') }}",
                "supervisorid": "{{ result('foreach_create_list_69_70') }}",
                "error": "{{get_error_message()}}"
            }
        )

        foreach_create_list_69_70_end = rail.EmptyOperator(
            task_id='foreach_create_list_69_70_end',
        )

        if_supervisor_child_triggered = rail.IfOperator(
            task_id='if_supervisor_child_triggered',
            test=lambda: len(rail.get_dag_run_var('supervisorcreation')) > 0,
            yes_task='wait_for_add_foreign_supervisor',
            no_task='trigger_child_add_supervisor'
        )

        wait_for_add_foreign_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_foreign_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_to_supervisor_waitlist").value | to_json }}'

        )

        trigger_child_add_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_add_supervisor',
            retries=0,
            items="{{ result('michael_kors_gmbh_supervisor_assignment_table_search_entries_66') }}",
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_supervisor_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['properties']['username'],
                "supervisorloginname": item['properties']['supervisorloginname'],
                "callerjobid": item['properties']['jobid'],
                "childjobid": item['properties']['childjobid'],
                "useruri": item['properties']['useruri'],
                "action": item['properties']['action'],
                "supervisoreffectivedate": item['properties']['supervisoreffectivedate'],
                "supervisorusername": item['properties']['supervisorusername'],
                "country": item['properties']['country'],
                "userimportlogtable": rail.result('create_user_import_logtable'),
                "supervisorlookup": rail.result('create_supervisor_assignment_lookup'),
            }
        )

        wait_for_child_add_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_supervisor") }}'
        )

        log_file_name_6 = rail.PythonOperator(
            task_id='log_file_name_6',
            python_callable=lambda:  "logs_" + datetime.now().strftime("%H%M%S") + "_" +
            rail.smartjoin_by_delim((config.jobtype).split(" "), "") + ".csv"
        )

        michael_kors_gmbh_user_sync_logs_search_entries_7 = rail.FilterLogEntriesOperator(
            task_id='michael_kors_gmbh_user_sync_logs_search_entries_7',
            log="{{result('create_user_import_logtable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_request_inputlistsize_greater_than_0_8 = rail.IfOperator(
            task_id='if_request_inputlistsize_greater_than_0_8',
            #pylint: disable = line-too-long
            test="{{result('query_list_getalltheusersfor_uk_25','length') > 0 and result('michael_kors_gmbh_user_sync_logs_search_entries_7','length') == 0 }}",
            yes_task="stop_9",
            no_task="create_csv_lines_10",
        )

        stop_9 = rail.FailOperator(
            task_id='stop_9',
            message='''No record found in lookup table'''
        )

        create_csv_lines_10 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_10',
            source="{{ result('michael_kors_gmbh_user_sync_logs_search_entries_7') }}",
            header=['User Name',
                    'Login Name',
                    'Action',
                    'Status',
                    'Details',
                    'JobID'],
            row=[
                "{{ item.properties.username }}",
                "{{ item.properties.loginname }}",
                "{{ item.properties.action }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.properties.jobid }}|{{ item.properties.childjobid }}"
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('create_csv_lines_10') }}",
            output_file_name="{{result('log_file_name_6')}}",
            expires_in_seconds=7*24*60*60
        )

        def get_meta_for_logs():
            logs = rail.load_all_records(rail.result(
                'michael_kors_gmbh_user_sync_logs_search_entries_7'))
            errorPresent = rail.find_first_by_attr_and_get_attr(
                logs, 'properties.status', 'Error')
            exceptionPresent = rail.find_first_by_attr_and_get_attr(
                logs, 'properties.status', 'Exception')
            return {
                'error': errorPresent,
                'exception': exceptionPresent,
                'subject': "completed with errors" if errorPresent else ("completed with exceptions" if exceptionPresent else "completed successfully"),
                # pylint: disable = line-too-long
                'body': "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if errorPresent else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
            }

        get_meta_data_for_logs = rail.PythonOperator(
            task_id='get_meta_data_for_logs',
            python_callable=get_meta_for_logs
        )

        send_mail_with_cshare_21 = rail.EmailOperator(
            task_id='send_mail_with_cshare_21',
            to=config.tenant_email,
            bcc="{%- if result('get_meta_data_for_logs')['error'] -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject="{{ get_company_key()}}| " + config.jobtype + \
            " - {{ result('get_meta_data_for_logs').subject }} - {{ current_time('%m/%d/%YT%H:%M:%S') }}",
            html_content='''templates/success_mail.html''',
            params={
                "jobtype": config.jobtype
            },
        )

        trigger_disable_users_child_dag = rail.TriggerDagRunOperator(
            task_id = 'trigger_disable_users_child_dag',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_disable_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_disable_users_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_users_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_disable_users_child_dag") }}'
        )

        archive_old_referencefile = rail.SFTPMoveFileOperator(
            task_id = 'archive_old_referencefile',
            existing_filename=config.reference_filepath + 'mkusersync_reference.csv',
            new_filename=config.archive_filepath + "{{dag_run_ecid()}}_Old_mkusersync_reference.csv"
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id = 'upload_new_reference_file',
            content="{{result('create_csv_lines_13')}}",
            remote_filepath=config.reference_filepath + 'mkusersync_reference.csv'
        )

        if_first_supervisorname_present_90 = rail.IfOperator(
            task_id='if_first_supervisorname_present_90',
            test=lambda: len(rail.get_dag_run_var(
                'Foreign_Supervisor_Creation_Error_List')) > 0 if rail.result('create_error_list_for_supervisor') else False,
            yes_task="stop_91",
            no_task="log_to_sumo",
        )

        stop_91 = rail.FailOperator(
            task_id='stop_91',
            message='''Foreign supervisor creation failed for few users'''
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_workday_report >> get_report_5 >> if_get_report_5_report_less_than_1_8
        if_get_report_5_report_less_than_1_8 >> rail.Label(
            'Yes') >> trigger_child_disable_users >> wait_for_child_disable_users >> send_mail_send_emailfornorecordstoprocess_10 >> log_to_sumo
        if_get_report_5_report_less_than_1_8 >> rail.Label(
            'No') >> create_csv_lines_13 >> create_collection_create_list_from_csv_14 >> download_16 >> load_csv_create_list_from_csv_17
        load_csv_create_list_from_csv_17 >> create_collection_create_list_from_csv_17 >> query_list_unchangedrecords_18 >> query_list_changedrecords_19
        query_list_changedrecords_19 >> if_query_list_changedrecords_19_rows_less_than_1_20
        if_query_list_changedrecords_19_rows_less_than_1_20 >> rail.Label(
            'Yes') >> trigger_disable_users_child >> wait_for_disable_users_child >> send_mail_send_emailfornorecordstoprocess_22 >> log_to_sumo
        if_query_list_changedrecords_19_rows_less_than_1_20 >> rail.Label(
            'No') >> query_list_getalltheusersfor_uk_25 >> if_first_employee_id_blank_26
        if_first_employee_id_blank_26 >> rail.Label(
            'Yes') >> trigger_child_to_disable_users >> wait_for_child_to_disable_users >> send_mail_send_emailfornorecordstoprocess_28 >> log_to_sumo
        if_first_employee_id_blank_26 >> rail.Label(
            'No') >> if_query_list_changedrecords_19_rows_greater_than_0_30
        if_query_list_changedrecords_19_rows_greater_than_0_30 >> rail.Label(
            'Yes') >> trigger_groups_update_child >> wait_for_groups_update_child >> get_cost_center_details_33 >> get_department_group_details_34
        get_department_group_details_34 >> get_location_details_35 >> get_servicecenters_weekly_schedule_group_details_36 >> _adhoc_http_action_42
        _adhoc_http_action_42 >> create_user_import_logtable >> create_supervisor_assignment_lookup >> create_child_triggered_list
        create_child_triggered_list >> foreach_query_list_getalltheusersfor_uk_25_44
        foreach_query_list_getalltheusersfor_uk_25_44 >> if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45
        if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_47 >> search_users_51 >> if_log_checkiftherequireduserisavailable_52_present_53
        if_log_checkiftherequireduserisavailable_52_present_53 >> rail.Label(
            'Yes') >> trigger_user_update_child >> insert_update_user_child_id_to_waitlist >> if_error_in_validation
        if_log_checkiftherequireduserisavailable_52_present_53 >> rail.Label(
            'No') >> trigger_add_user_child >> insert_add_user_child_id_to_waitlist >> if_error_in_validation
        if_error_in_validation >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_add_entry_58 >> foreach_query_list_getalltheusersfor_uk_25_44_end
        if_error_in_validation >> rail.Label(
            'No') >> foreach_query_list_getalltheusersfor_uk_25_44_end
        if_foreach_query_list_getalltheusersfor_uk_25_44_employee_id_present_45 >> rail.Label(
            'No') >> michael_kors_gmbh_user_sync_logs_add_entry_60 >> foreach_query_list_getalltheusersfor_uk_25_44_end
        foreach_query_list_getalltheusersfor_uk_25_44 >> foreach_query_list_getalltheusersfor_uk_25_44_end >> if_child_triggered
        if_child_triggered >> rail.Label(
            'Yes') >> get_child_ids_to_wait_for >> wait_for_add_update_child >> michael_kors_gmbh_supervisor_assignment_table_search_entries_66
        if_child_triggered >> rail.Label(
            'No') >> michael_kors_gmbh_supervisor_assignment_table_search_entries_66 >> load_supervisor_entries >> if_first_id_present_67
        if_first_id_present_67 >> rail.Label('Yes') >> log_getalltheuniqsupervisor_68 >> create_list_of_supervisor_child
        create_list_of_supervisor_child >> create_error_list_for_supervisor >> foreach_create_list_69_70 >> log_required_supervisor_user_name_72
        log_required_supervisor_user_name_72 >> trigger_child_add_foreign_supervisor >> insert_to_supervisor_waitlist
        insert_to_supervisor_waitlist >> if_error_in_foreign_supervisor_creation
        if_error_in_foreign_supervisor_creation >> rail.Label(
            'Yes') >> accumulate_list_items_76 >> foreach_create_list_69_70_end
        if_error_in_foreign_supervisor_creation >> rail.Label(
            'No') >> foreach_create_list_69_70_end
        foreach_create_list_69_70 >> foreach_create_list_69_70_end >> if_supervisor_child_triggered
        if_supervisor_child_triggered >> rail.Label(
            'Yes') >> wait_for_add_foreign_supervisor >> trigger_child_add_supervisor
        if_supervisor_child_triggered >> rail.Label(
            'No') >> trigger_child_add_supervisor >> wait_for_child_add_supervisor
        wait_for_child_add_supervisor >> log_file_name_6 >> michael_kors_gmbh_user_sync_logs_search_entries_7 >> if_request_inputlistsize_greater_than_0_8
        if_request_inputlistsize_greater_than_0_8 >> rail.Label(
            'Yes') >> stop_9 >> log_to_sumo
        if_request_inputlistsize_greater_than_0_8 >> rail.Label(
            'No') >> create_csv_lines_10 >> generate_download_link >> get_meta_data_for_logs >> send_mail_with_cshare_21
        send_mail_with_cshare_21 >> trigger_disable_users_child_dag >> wait_for_disable_users_child_dag >> archive_old_referencefile
        archive_old_referencefile >> upload_new_reference_file >> if_first_supervisorname_present_90
        if_first_id_present_67 >> rail.Label('No') >> log_file_name_6
        if_first_supervisorname_present_90 >> rail.Label(
            'Yes') >> stop_91 >> log_to_sumo
        if_first_supervisorname_present_90 >> rail.Label('No') >> log_to_sumo
        if_query_list_changedrecords_19_rows_greater_than_0_30 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
