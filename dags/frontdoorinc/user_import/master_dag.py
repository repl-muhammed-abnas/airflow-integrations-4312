
from datetime import timedelta
import json
from pendulum import datetime as dt
from frontdoorinc.user_import.utils import custom_methods
from frontdoorinc.user_import.utils.response_filter import get_customfields, get_employee_grouplist, get_costcenterlist
from frontdoorinc.user_import.mappers import frontdoorinc_timezone_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_user_import_frontdoorinc_user_import_master_v1_0_{config.instance}',
        description=f'FrontdoorInc_User import - Master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        if_report_in_conf = rail.IfOperator(
            task_id='if_report_in_conf',
            test=lambda dag_run: 'report' in dag_run.conf,
            yes_task='report_data',
            no_task='get_workday_report',
        )

        get_workday_report = rail.SimpleHttpOperator(
            task_id='get_workday_report',
            method='GET',
            http_conn_id=config.workday_http_conn_id,
            endpoint='/CR_RaaS_INT170_Replicon_Engineering_Resource_Planning?format=json',
            headers={
                "Content-Type": "application/json"
            },
            extra_options={
                'verify': False
            }
        )

        report_data = rail.PythonOperator(
            task_id='report_data',
            python_callable=lambda dag_run: custom_methods.get_report_data(dag_run.conf['report'] if 'report' in dag_run.conf 
                else json.loads(rail.result('get_workday_report'))['Report_Entry'])
        )

        create_csv_lines_create_m_d5filefor_inputfile_4 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_create_m_d5filefor_inputfile_4',
            source=lambda: rail.result('report_data'),
            header=['lastname',
                    'company',
                    'hiredate',
                    'jobprofilecode',
                    'timetype',
                    'timezone',
                    'employeeid',
                    'managerid',
                    'terminationdate',
                    'firstname',
                    'emailaddress',
                    'jobprofilename',
                    'costcenterid',
                    'statelocation',
                    'costcentername',
                    'hourlyrate',
                    'md5'],
            row=lambda item: [
                item.get('lastName', ''),
                item.get('Company', ''),
                item.get('hireDate', ''),
                item.get('jobProfileCode', ''),
                item.get('timeType').lower() if item.get('timeType') else '',
                item.get('timeZone', ''),
                item.get('employeeID', ''),
                item.get('managerID', ''),
                item.get('terminationDate', ''),
                item.get('firstName', ''),
                item.get('emailAddress', ''),
                item.get('jobProfileName', ''),
                item.get('costCenterID', ''),
                item.get('stateLocation', ''),
                item.get('costCenterName', ''),
                item.get('hourlyRate', ''),
                custom_methods.get_formated_user_row(item)
            ]
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_file_path + 'frontdoorinc_reference.csv'
        )

        load_reference_file = rail.LoadCSVFileOperator(
            task_id="load_reference_file",
            document="{{result('download_reference_file')}}",
        )

        create_collection_create_list_from_csv_6 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_6',
            source="{{ result('create_csv_lines_create_m_d5filefor_inputfile_4') }}",
            name="inputfilewithmd5",
            columns={
                'lastname': 'lastname',
                'company': 'company',
                'hiredate': 'hiredate',
                'jobprofilecode': 'jobprofilecode',
                'timetype': 'timetype',
                'timezone': 'timezone',
                'employeeid': 'employeeid',
                'managerid': 'managerid',
                'terminationdate': 'terminationdate',
                'firstname': 'firstname',
                'emailaddress': 'emailaddress',
                'jobprofilename': 'jobprofilename',
                'costcenterid': 'costcenterid',
                'statelocation': 'statelocation',
                'costcentername': 'costcentername',
                'hourlyrate': 'hourlyrate',
                'md5': 'md5'
            }
        )

        create_collection_create_list_from_csv_7 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_7',
            source="{{ result('load_reference_file') }}",
            name="referencefilewithmd5",
            columns={
                'lastname': 'lastname',
                'company': 'company',
                'hiredate': 'hiredate',
                'jobprofilecode': 'jobprofilecode',
                'timetype': 'timetype',
                'timezone': 'timezone',
                'employeeid': 'employeeid',
                'managerid': 'managerid',
                'terminationdate': 'terminationdate',
                'firstname': 'firstname',
                'emailaddress': 'emailaddress',
                'jobprofilename': 'jobprofilename',
                'costcenterid': 'costcenterid',
                'statelocation': 'statelocation',
                'costcentername': 'costcentername',
                'hourlyrate': 'hourlyrate',
                'md5': 'md5'
            }
        )

        frontdoor_log_lookuptable = rail.CreateLogOperator(
            task_id='frontdoor_log_lookuptable'
        )

        query_list_identify_unchangedrecords_9 = rail.QueryCollectionOperator(
            task_id='query_list_identify_unchangedrecords_9',
            query="""SELECT inputfilewithmd5.* FROM inputfilewithmd5 
                    INNER JOIN referencefilewithmd5 ON inputfilewithmd5.md5 = referencefilewithmd5.md5""",
        )

        if_query_list_identify_unchangedrecords_9_rows_greater_than_0_10 = rail.IfOperator(
            task_id='if_query_list_identify_unchangedrecords_9_rows_greater_than_0_10',
            test='''{{ result('query_list_identify_unchangedrecords_9','length')> 0 }}''',
            yes_task="add_ignored_entries",
            no_task="query_list_identify_changedrecords_12",
        )

        add_ignored_entries = rail.WriteLogOperator(
            task_id='add_ignored_entries',
            log="{{ result('frontdoor_log_lookuptable')}}",
            items="{{ result('query_list_identify_unchangedrecords_9')}}",
            message="na",
            severity="ignored",
            properties=lambda item: {
                "employeeid": item['employeeid'],
                "username": item['firstname'] + " " + item['lastname'],
                "action": "pre-check",
                "details": "No changes in user records",
                "status": "ignored",
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "childjob": "",
            }
        )

        query_list_identify_changedrecords_12 = rail.QueryCollectionOperator(
            task_id='query_list_identify_changedrecords_12',
            query="""SELECT inputfilewithmd5.* FROM inputfilewithmd5
                    LEFT JOIN referencefilewithmd5 ON inputfilewithmd5.md5 = referencefilewithmd5.md5
                    WHERE referencefilewithmd5.md5 IS NULL""",
        )

        load_csv_create_list_from_csv_changedrecords_13 = rail.PythonOperator(
            task_id="load_csv_create_list_from_csv_changedrecords_13",
            python_callable=lambda: rail.load_all_records(
                rail.result('query_list_identify_changedrecords_12'))
        )

        create_collection_create_list_from_csv_changedrecords_13 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_changedrecords_13',
            source="{{ result('load_csv_create_list_from_csv_changedrecords_13')| to_json}}",
            name="changedrecordslist",
            columns={
                'lastname': 'lastname',
                'company': 'company',
                'hiredate': 'hiredate',
                'jobprofilecode': 'jobprofilecode',
                'timetype': 'timetype',
                'timezone': 'timezone',
                'employeeid': 'employeeid',
                'managerid': 'managerid',
                'terminationdate': 'terminationdate',
                'firstname': 'firstname',
                'emailaddress': 'emailaddress',
                'jobprofilename': 'jobprofilename',
                'costcenterid': 'costcenterid',
                'statelocation': 'statelocation',
                'costcentername': 'costcentername',
                'hourlyrate': 'hourlyrate',
                'md5': 'md5'
            }
        )

        query_list_changedrecordswithout_mandatoryfields_14 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswithout_mandatoryfields_14',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.firstname= "" OR  changedrecordslist.lastname= "" OR  changedrecordslist.company = "" OR  changedrecordslist.hiredate= "" OR  changedrecordslist.jobprofilecode= "" OR  changedrecordslist.timetype= "" OR  changedrecordslist.employeeid= "" OR  changedrecordslist.costcenterid= "" OR  changedrecordslist.firstname IS NULL OR  changedrecordslist.lastname IS NULL OR  changedrecordslist.company IS NULL OR  changedrecordslist.hiredate IS NULL OR  changedrecordslist.jobprofilecode IS NULL OR  changedrecordslist.timetype IS NULL OR  changedrecordslist.employeeid IS NULL OR  changedrecordslist.costcenterid IS NULL)""",
        )

        if_query_list_changedrecordswithout_mandatoryfields_14_rows_greater_than_0_15 = rail.IfOperator(
            task_id='if_query_list_changedrecordswithout_mandatoryfields_14_rows_greater_than_0_15',
            test='''{{ result('query_list_changedrecordswithout_mandatoryfields_14','length') > 0 }}''',
            yes_task="add_ignored_entries_not_mandatory_records",
            no_task="query_list_changedrecordswith_mandatoryfields_17",
        )

        add_ignored_entries_not_mandatory_records = rail.WriteLogOperator(
            task_id='add_ignored_entries_not_mandatory_records',
            log="{{ result('frontdoor_log_lookuptable')}}",
            items="{{ result('query_list_changedrecordswithout_mandatoryfields_14')}}",
            message="na",
            severity="ignored",
            properties=lambda item: {
                "employeeid": item['employeeid'],
                "username": item['firstname'] + " " + item['lastname'],
                "action": "pre-check",
                "details": "One or more mandatory fields are missing",
                "status": "ignored",
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "childjob": "",
            }
        )

        query_list_changedrecordswith_mandatoryfields_17 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswith_mandatoryfields_17',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.firstname != "" AND changedrecordslist.lastname != "" AND changedrecordslist.company != "" AND changedrecordslist.hiredate != "" AND changedrecordslist.jobprofilecode != "" AND changedrecordslist.timetype != "" AND changedrecordslist.employeeid != "" AND changedrecordslist.costcenterid != "")""",
        )

        if_query_list_changedrecordswith_mandatoryfields_17_rows_greater_than_0_18 = rail.IfOperator(
            task_id='if_query_list_changedrecordswith_mandatoryfields_17_rows_greater_than_0_18',
            test="{{result('query_list_changedrecordswith_mandatoryfields_17','length') > 0 }}",
            yes_task="get_report_details",
            no_task="filter_ignored_log_entries"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.enable_user_report_name,
        )

        generate_report = rail.run_report2(
            group_id='generate_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            }
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='{{result("generate_report.get_report_result", "has_data") | is_truthy}}',
            yes_task="if_payload_has_no_columns",
            no_task="stop_job"
        )

        stop_job = rail.FailOperator(
            task_id='stop_job',
            message="No Data in the base report"
        )

        if_payload_has_no_columns = rail.IfOperator(
            task_id='if_payload_has_no_columns',
            # pylint: disable=consider-using-f-string,line-too-long
            test="{{result('generate_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s')| is_falsy}}" % config.expected_report_columns,
            yes_task="stop_job_with_error_message",
            no_task="parse_csv_24",
        )

        stop_job_with_error_message = rail.FailOperator(
            task_id='stop_job_with_error_message',
            message="Base report column order doesn't match"
        )

        parse_csv_24 = rail.LoadCSVFileOperator(
            task_id='parse_csv_24',
            document="{{ result('generate_report.get_report_result').reportGenerationResults[0].payload}}",
        )

        get_all_custom_fields_25 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_25',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=get_customfields
        )

        query_list_all_supervisorlist_27 = rail.QueryCollectionOperator(
            task_id='query_list_all_supervisorlist_27',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.employeeid IN (SELECT DISTINCT  inputfilewithmd5.managerid FROM  inputfilewithmd5)""",
        )

        query_list_getallcostcenters_28 = rail.QueryCollectionOperator(
            task_id='query_list_getallcostcenters_28',
            query="""SELECT DISTINCT  inputfilewithmd5.costcentername as displaytext,  inputfilewithmd5.costcenterid as code FROM  inputfilewithmd5 WHERE ( inputfilewithmd5.costcentername != "" AND  inputfilewithmd5.costcenterid != "")""",
        )

        process_costcenter_child = rail.TriggerDagRunOperator(
            task_id='process_costcenter_child',
            trigger_dag_id=f'frontdoorinc_process_costcenter_groups_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "allcostcenters": rail.result('query_list_getallcostcenters_28')
            }
        )

        wait_for_process_costcenter_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_costcenter_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_costcenter_child") }}'
        )

        get_data_employee_type_group_list_service_30 = rail.RepliconServiceOperator(
            task_id='get_data_employee_type_group_list_service_30',
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:code",
                    "urn:replicon:employee-type-group-list-column:employee-type-group"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_employee_grouplist

        )

        get_data_cost_center_list_service1_32 = rail.RepliconServiceOperator(
            task_id='get_data_cost_center_list_service1_32',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:code",
                    "urn:replicon:cost-center-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_costcenterlist

        )

        get_data_location_list_service_34 = rail.RepliconServiceOperator(
            task_id='get_data_location_list_service_34',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:code",
                    "urn:replicon:location-list-column:location"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_costcenterlist

        )

        get_all_department_groups_36 = rail.RepliconServiceOperator(
            task_id='get_all_department_groups_36',
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups"
        )

        get_base_currency_37 = rail.RepliconServiceOperator(
            task_id='get_base_currency_37',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency"
        )

        frontdoorinc_timezone_mapper_search_entries_38 = rail.PythonOperator(
            task_id='frontdoorinc_timezone_mapper_search_entries_38',
            python_callable=lambda:  list(filter(
                lambda x: x["allowed"] == "yes", frontdoorinc_timezone_mapper.frontdoorinc_timezone_mapper))
        )

        declare_list_dag_runs = rail.SetVariableOperator(
            task_id='declare_list_dag_runs',
            name='user_process_dag_runs',
            value=[]
        )

        get_csv_data = rail.PythonOperator(
            task_id='get_csv_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_24'))
        )

        foreach_query_list_changedrecordswith_mandatoryfields_17_39 = rail.ForEachOperator(
            task_id='foreach_query_list_changedrecordswith_mandatoryfields_17_39',
            items="{{result('query_list_changedrecordswith_mandatoryfields_17')}}",
            start_task='invoke_custom_ruby_code_40',
            end_task='foreach_query_list_changedrecordswith_mandatoryfields_17_39_end'
        )

        invoke_custom_ruby_code_40 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_40',
            python_callable=lambda: {
                "user": rail.find_first_by_attr_and_get_attr(rail.result('get_csv_data'), 'Employee ID', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['employeeid'], 'User Name', ''),
                "useruri": rail.find_first_by_attr_and_get_attr(rail.result('get_csv_data'), 'Employee ID', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['employeeid'], 'UserUri', ''),
                "employeetype": ("full time employee" if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timetype'] == "Full time" else null) if rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39') and rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timetype'] else null
            }
        )

        def get_managerid():
            data = rail.load_all_records(rail.result(
                'query_list_all_supervisorlist_27'))
            for d in data:
                if d['employeeid'] == rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['managerid']:
                    return d
            return None

        invoke_custom_ruby_code_41 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_41',
            python_callable=get_managerid
        )

        if_output_user_blank_42 = rail.IfOperator(
            task_id='if_output_user_blank_42',
            test='''{{ result('invoke_custom_ruby_code_40').user | is_falsy }}''',
            yes_task="process_create_user_child",
            no_task="process_update_user_child",
        )

        process_create_user_child = rail.TriggerDagRunOperator(
            task_id='process_create_user_child',
            retries=0,
            trigger_dag_id=f'frontdoorinc_frontdoorinc_create_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "lastname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['lastname'],
                "firstname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['firstname'],
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_employee_type_group_list_service_30')['emp_grouplist'], 'Text value', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timetype'], 'URI', ""),
                "company": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['company'],
                "employeeid": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['employeeid'],
                "terminationdate": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['terminationdate'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_groups_36'), 'displayText', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['company'], 'uri', ""),
                "hiredate": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['hiredate'],
                "jobprofilecode": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['jobprofilecode'],
                "timetype": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timetype'],

                "timezone": rail.find_first_by_attr_and_get_attr(rail.result('frontdoorinc_timezone_mapper_search_entries_38'), 'workdaytimezone', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timezone'], 'iana_name', ""),

                "managerid": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['managerid'],
                "emailaddress": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['emailaddress'],
                "jobprofilename": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['jobprofilename'],

                "costcenterid": rail.find_first_by_attr_and_get_attr(rail.result('get_data_cost_center_list_service1_32')['costcenterlist'], 'Code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['costcenterid'], 'URI', ""),

                "statelocation": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['statelocation'], 
                "costcentername": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['costcentername'], 
                "hourlyrate": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['hourlyrate'],
                "customfielduri_jobprofilename": rail.result('get_all_custom_fields_25')['jobprofilename'],
                "customfielduri_jobprofilecode": rail.result('get_all_custom_fields_25')['jobprofilecode'],
                "customfielduri_adminmodified": rail.result('get_all_custom_fields_25')['adminmodified'],
                "mangaerdetails": {
                    "employeeid": rail.result('invoke_custom_ruby_code_41')['employeeid'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "firstname": rail.result('invoke_custom_ruby_code_41')['firstname'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "lastname": rail.result('invoke_custom_ruby_code_41')['lastname'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "company": rail.result('invoke_custom_ruby_code_41')['company'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_groups_36'), 'displayText', rail.result('invoke_custom_ruby_code_41')['company'], 'uri', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "hiredate": rail.result('invoke_custom_ruby_code_41')['hiredate'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "jobprofilecode": rail.result('invoke_custom_ruby_code_41')['jobprofilecode'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "timetype": rail.result('invoke_custom_ruby_code_41')['timetype'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_employee_type_group_list_service_30')['emp_grouplist'], 'Text value', rail.result('invoke_custom_ruby_code_41')['timetype'], 'URI', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "timezone": rail.find_first_by_attr_and_get_attr(rail.result('frontdoorinc_timezone_mapper_search_entries_38'), 'workdaytimezone', rail.result('invoke_custom_ruby_code_41')['timezone'], 'iana_name', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "managerid": rail.result('invoke_custom_ruby_code_41')['managerid'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "terminationdate": rail.result('invoke_custom_ruby_code_41')['terminationdate'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "emailaddress": rail.result('invoke_custom_ruby_code_41')['emailaddress'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "jobprofilename": rail.result('invoke_custom_ruby_code_41')['jobprofilename'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "costcenterid": rail.find_first_by_attr_and_get_attr(rail.result('get_data_cost_center_list_service1_32')['costcenterlist'], 'Code', rail.result('invoke_custom_ruby_code_41')['costcenterid'], 'URI', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "statelocation": rail.result('invoke_custom_ruby_code_41')['statelocation'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "costcentername": rail.result('invoke_custom_ruby_code_41')['costcentername'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "hourlyrate": rail.result('invoke_custom_ruby_code_41')['hourlyrate'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "customfielduri_jobprofilename": rail.result('get_all_custom_fields_25')['jobprofilename'], 
                    "customfielduri_jobprofilecode": rail.result('get_all_custom_fields_25')['jobprofilecode'], 
                    "customfielduri_adminmodified": rail.result('get_all_custom_fields_25')['adminmodified'],
                    "managerlocationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_location_list_service_34')
                                                                               ['costcenterlist'], 'Code', rail.result('invoke_custom_ruby_code_41')['statelocation'], 'URI', "") if rail.result('invoke_custom_ruby_code_41') else null,
                },
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_location_list_service_34')['costcenterlist'], 'Code',
                                                                    rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['statelocation'], 'URI', ""),
                "basecurrencyuri": rail.result('get_base_currency_37')['uri'],
                "lookuptable": rail.result('frontdoor_log_lookuptable'),
                "job_id": rail.render_template("{{dag_run_ecid()}}"),
            }

        )

        process_update_user_child = rail.TriggerDagRunOperator(
            task_id='process_update_user_child',
            retries=0,
            trigger_dag_id=f'frontdoorinc_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "lastname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['lastname'],
                "firstname": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['firstname'],
                "useruri": rail.result('invoke_custom_ruby_code_40')['useruri'],
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_employee_type_group_list_service_30')['emp_grouplist'], 'Text value', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timetype'], 'URI', ""),
                "company": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['company'],
                "employeeid": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['employeeid'],
                "terminationdate": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['terminationdate'],
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_groups_36'), 'displayText', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['company'], 'uri', ""),
                "hiredate": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['hiredate'],
                "jobprofilecode": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['jobprofilecode'],
                "timetype": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timetype'],

                "timezone": rail.find_first_by_attr_and_get_attr(rail.result('frontdoorinc_timezone_mapper_search_entries_38'), 'workdaytimezone', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['timezone'], 'iana_name', ""),

                "managerid": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['managerid'],
                "emailaddress": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['emailaddress'],
                "jobprofilename": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['jobprofilename'],

                "costcenterid": rail.find_first_by_attr_and_get_attr(rail.result('get_data_cost_center_list_service1_32')['costcenterlist'], 'Code', rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['costcenterid'], 'URI', ""),

                "statelocation": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['statelocation'], "costcentername": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['costcentername'], "hourlyrate": rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['hourlyrate'],
                "customfielduri_jobprofilename": rail.result('get_all_custom_fields_25')['jobprofilename'],
                "customfielduri_jobprofilecode": rail.result('get_all_custom_fields_25')['jobprofilecode'],
                "customfielduri_adminmodified": rail.result('get_all_custom_fields_25')['adminmodified'],
                "mangaerdetails": {
                    "employeeid": rail.result('invoke_custom_ruby_code_41')['employeeid'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "firstname": rail.result('invoke_custom_ruby_code_41')['firstname'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "lastname": rail.result('invoke_custom_ruby_code_41')['lastname'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "company": rail.result('invoke_custom_ruby_code_41')['company'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_groups_36'), 'displayText', rail.result('invoke_custom_ruby_code_41')['company'], 'uri', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "hiredate": rail.result('invoke_custom_ruby_code_41')['hiredate'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "jobprofilecode": rail.result('invoke_custom_ruby_code_41')['jobprofilecode'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "timetype": rail.result('invoke_custom_ruby_code_41')['timetype'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_employee_type_group_list_service_30')['emp_grouplist'], 'Text value', rail.result('invoke_custom_ruby_code_41')['timetype'], 'URI', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "timezone": rail.find_first_by_attr_and_get_attr(rail.result('frontdoorinc_timezone_mapper_search_entries_38'), 'workdaytimezone', rail.result('invoke_custom_ruby_code_41')['timezone'], 'iana_name', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "managerid": rail.result('invoke_custom_ruby_code_41')['managerid'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "terminationdate": rail.result('invoke_custom_ruby_code_41')['terminationdate'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "emailaddress": rail.result('invoke_custom_ruby_code_41')['emailaddress'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "jobprofilename": rail.result('invoke_custom_ruby_code_41')['jobprofilename'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "costcenterid": rail.find_first_by_attr_and_get_attr(rail.result('get_data_cost_center_list_service1_32')['costcenterlist'], 'Code', rail.result('invoke_custom_ruby_code_41')['costcenterid'], 'URI', "") if rail.result('invoke_custom_ruby_code_41') else null,
                    "statelocation": rail.result('invoke_custom_ruby_code_41')['statelocation'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "costcentername": rail.result('invoke_custom_ruby_code_41')['costcentername'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "hourlyrate": rail.result('invoke_custom_ruby_code_41')['hourlyrate'] if rail.result('invoke_custom_ruby_code_41') else null,
                    "customfielduri_jobprofilename": rail.result('get_all_custom_fields_25')['jobprofilename'], "customfielduri_jobprofilecode": rail.result('get_all_custom_fields_25')['jobprofilecode'], "customfielduri_adminmodified": rail.result('get_all_custom_fields_25')['adminmodified'],
                    "managerlocationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_location_list_service_34')
                                                                               ['costcenterlist'], 'Code', rail.result('invoke_custom_ruby_code_41')['statelocation'], 'URI', "") if rail.result('invoke_custom_ruby_code_41') else null,
                },
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_data_location_list_service_34')['costcenterlist'], 'Code',
                                                                    rail.result('foreach_query_list_changedrecordswith_mandatoryfields_17_39')['statelocation'], 'URI', ""),
                "basecurrencyuri": rail.result('get_base_currency_37')['uri'],
                "lookuptable": rail.result('frontdoor_log_lookuptable'),
                "job_id": rail.render_template("{{dag_run_ecid()}}"),
            }

        )

        insert_to_user_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list',
            append=True,
            name='{{ result("declare_list_dag_runs").name }}',
            value='{{result("process_create_user_child") or result("process_update_user_child")}}'
        )

        foreach_query_list_changedrecordswith_mandatoryfields_17_39_end = rail.EmptyOperator(
            task_id='foreach_query_list_changedrecordswith_mandatoryfields_17_39_end',
        )

        is_user_trigger_runs_avaialbale = rail.IfOperator(
            task_id='is_user_trigger_runs_avaialbale',
            test='''{{ result('insert_to_user_dag_run_list') | is_truthy }}''',
            yes_task="wait_for_completion_dags",
            no_task="log_to_sumo",
        )

        wait_for_completion_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dags',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list").value | to_json }}'
        )

        search_entries_frontdoor_log_lookuptable = rail.FilterLogEntriesOperator(
            task_id='search_entries_frontdoor_log_lookuptable',
            log="{{ result('frontdoor_log_lookuptable') }}",
            properties={
                'jobid': "{{dag_run_ecid()}}",
            }
        )

        get_failed_logs = rail.FilterLogEntriesOperator(
            task_id='get_failed_logs',
            log="{{result('frontdoor_log_lookuptable')}}",
            severity='failed'
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_logs',
            log="{{result('frontdoor_log_lookuptable')}}",
            severity='exception'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('frontdoor_log_lookuptable') }}",
            header=['Employeeid',
                    'Username',
                    'Action',
                    'Status',
                    'Details',
                    'Jobid'],
            row=lambda item: [
                item['properties']['employeeid'],
                item['properties']['username'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                str(item['properties']['jobid']) + "|" +
                str(item['properties']['childjob']),
            ],
        )

        # upload_file_to_sftp (SFTPUploadFileOperator) was removed — SFTP upload of the audit log
        # is not needed in Airflow; the presigned download link covers distribution.
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines')}}",
            output_file_name=f'userimportlogs_{{{{current_time_in_specified_tz("{config.time_zone}","%Y%m%dT%H%M%S")}}}}.csv',
            expires_in_seconds=7*24*60*60,
        )


        if_get_failed_logs_present = rail.IfOperator(
            task_id='if_get_failed_logs_present',
            test='''{{ result('get_failed_logs','length') > 0 }}''',
            yes_task="send_failure_mail",
            no_task="if_get_exception_logs_present",

        )

        send_failure_mail = rail.EmailOperator(
            task_id='send_failure_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon user import completed with failed records-{{current_time()}}''',
            html_content='templates/emails/failure_mail.html',
        )

        if_get_exception_logs_present = rail.IfOperator(
            task_id='if_get_exception_logs_present',
            test='''{{ result('get_exception_logs','length') > 0 }}''',
            yes_task="send_exception_mail",
            no_task="send_success_mail",

        )

        send_exception_mail = rail.EmailOperator(
            task_id='send_exception_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon user import completed with exceptions-{{current_time()}}''',
            html_content='templates/emails/exception_mail.html',
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon user import completed successfully -{{current_time()}}''',
            html_content='templates/emails/success_mail.html',
        )

        rename_archive_referencefile_54 = rail.SFTPMoveFileOperator(
            task_id='rename_archive_referencefile_54',
            new_filename=config.archive_filepath +
            '{{dag_run_ecid()}}_frontdoorinc_reference.csv',
            existing_filename=config.reference_file_path + 'frontdoorinc_reference.csv',
        )

        upload_newreference_file_55 = rail.SFTPUploadFileOperator(
            task_id='upload_newreference_file_55',
            content='''{{ result('create_csv_lines_create_m_d5filefor_inputfile_4') }}''',
            remote_filepath=config.reference_file_path + 'frontdoorinc_reference.csv',
        )

        filter_ignored_log_entries = rail.FilterLogEntriesOperator(
            task_id='filter_ignored_log_entries',
            log="{{ result('frontdoor_log_lookuptable') }}",
            severity='ignored',
        )

        if_output_loggers_greater_than_0_60 = rail.IfOperator(
            task_id='if_output_loggers_greater_than_0_60',
            test='''{{ result('filter_ignored_log_entries','length')> 0 }}''',
            yes_task="create_csv_lines_61",
            no_task="rename_archive_referencefile_54",
        )

        create_csv_lines_61 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_61',
            source="{{ result('filter_ignored_log_entries') }}",
            header=['employeeid',
                    'username',
                    'action',
                    'status',
                    'details',
                    'jobid'],
            row=lambda item: [
                item['properties']['employeeid'],
                item['properties']['username'],
                item['properties']['action'],
                item['properties']['status'],
                item['properties']['details'],
                str(item['properties']['jobid']) + "|" +
                str(item['properties']['childjob']),
            ],
        )

        # upload_62 (SFTPUploadFileOperator) was removed — SFTP upload of ignored-entries log
        # is not needed in Airflow; the presigned download link covers distribution.
        generate_download_link1 = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link1',
            artifact_name="{{ result('create_csv_lines_61')}}",
            output_file_name=f'userimportlogs_{{{{current_time_in_specified_tz("{config.time_zone}","%Y%m%dT%H%M%S")}}}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_success_mail = rail.EmailOperator(
            task_id='send_import_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon user import completed successfully -{{current_time()}}''',
            html_content='templates/emails/send_import_success_mail.html',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        if_report_in_conf >> rail.Label('Yes') >> report_data >> create_csv_lines_create_m_d5filefor_inputfile_4
        if_report_in_conf >> rail.Label('No') >> get_workday_report >> report_data
        create_csv_lines_create_m_d5filefor_inputfile_4 >> download_reference_file >> load_reference_file
        load_reference_file >> create_collection_create_list_from_csv_6 >> create_collection_create_list_from_csv_7
        create_collection_create_list_from_csv_7 >> frontdoor_log_lookuptable >> query_list_identify_unchangedrecords_9
        query_list_identify_unchangedrecords_9 >> if_query_list_identify_unchangedrecords_9_rows_greater_than_0_10
        if_query_list_identify_unchangedrecords_9_rows_greater_than_0_10 >> rail.Label(
            'Yes') >> add_ignored_entries >> query_list_identify_changedrecords_12
        if_query_list_identify_unchangedrecords_9_rows_greater_than_0_10 >> rail.Label(
            'No') >> query_list_identify_changedrecords_12 >> load_csv_create_list_from_csv_changedrecords_13
        load_csv_create_list_from_csv_changedrecords_13 >> create_collection_create_list_from_csv_changedrecords_13
        create_collection_create_list_from_csv_changedrecords_13 >> query_list_changedrecordswithout_mandatoryfields_14
        query_list_changedrecordswithout_mandatoryfields_14 >> if_query_list_changedrecordswithout_mandatoryfields_14_rows_greater_than_0_15
        if_query_list_changedrecordswithout_mandatoryfields_14_rows_greater_than_0_15 >> rail.Label(
            'Yes') >> add_ignored_entries_not_mandatory_records >> query_list_changedrecordswith_mandatoryfields_17
        if_query_list_changedrecordswithout_mandatoryfields_14_rows_greater_than_0_15 >> rail.Label(
            'No') >> query_list_changedrecordswith_mandatoryfields_17 >> if_query_list_changedrecordswith_mandatoryfields_17_rows_greater_than_0_18
        if_query_list_changedrecordswith_mandatoryfields_17_rows_greater_than_0_18 >> rail.Label(
            'Yes') >> get_report_details >> generate_report >> if_payload_has_data
        if_query_list_changedrecordswith_mandatoryfields_17_rows_greater_than_0_18 >> rail.Label(
            'No') >> filter_ignored_log_entries >> if_output_loggers_greater_than_0_60
        if_output_loggers_greater_than_0_60 >> rail.Label(
            'Yes') >> create_csv_lines_61 >> generate_download_link1 >> send_import_success_mail >> rename_archive_referencefile_54
        if_output_loggers_greater_than_0_60 >> rail.Label(
            'No') >> rename_archive_referencefile_54
        if_payload_has_data >> rail.Label(
            'Yes') >> if_payload_has_no_columns
        if_payload_has_data >> rail.Label(
            'No') >> stop_job >> log_to_sumo
        if_payload_has_no_columns >> rail.Label(
            'Yes') >> stop_job_with_error_message >> log_to_sumo
        if_payload_has_no_columns >> rail.Label(
            'No') >> parse_csv_24 >> get_all_custom_fields_25
        get_all_custom_fields_25 >> query_list_all_supervisorlist_27 >> query_list_getallcostcenters_28
        query_list_getallcostcenters_28 >> process_costcenter_child
        process_costcenter_child >> wait_for_process_costcenter_child >> get_data_employee_type_group_list_service_30
        get_data_employee_type_group_list_service_30 >> get_data_cost_center_list_service1_32
        get_data_cost_center_list_service1_32 >> get_data_location_list_service_34 >> get_all_department_groups_36 >> get_base_currency_37
        get_base_currency_37 >> frontdoorinc_timezone_mapper_search_entries_38 >> get_csv_data
        get_csv_data >> declare_list_dag_runs >> foreach_query_list_changedrecordswith_mandatoryfields_17_39
        foreach_query_list_changedrecordswith_mandatoryfields_17_39 >> invoke_custom_ruby_code_40
        invoke_custom_ruby_code_40 >> invoke_custom_ruby_code_41 >> if_output_user_blank_42
        if_output_user_blank_42 >> rail.Label(
            'Yes') >> process_create_user_child >> insert_to_user_dag_run_list
        insert_to_user_dag_run_list >> foreach_query_list_changedrecordswith_mandatoryfields_17_39_end
        if_output_user_blank_42 >> rail.Label(
            'No') >> process_update_user_child >> insert_to_user_dag_run_list
        insert_to_user_dag_run_list >> foreach_query_list_changedrecordswith_mandatoryfields_17_39_end
        foreach_query_list_changedrecordswith_mandatoryfields_17_39 >> foreach_query_list_changedrecordswith_mandatoryfields_17_39_end
        foreach_query_list_changedrecordswith_mandatoryfields_17_39_end >> is_user_trigger_runs_avaialbale
        is_user_trigger_runs_avaialbale >> rail.Label(
            'Yes') >> wait_for_completion_dags >> search_entries_frontdoor_log_lookuptable
        is_user_trigger_runs_avaialbale >> rail.Label(
            'No') >> log_to_sumo
        search_entries_frontdoor_log_lookuptable >> get_exception_logs >> get_failed_logs
        get_failed_logs >> create_csv_lines >> generate_download_link >> if_get_failed_logs_present
        if_get_failed_logs_present >> rail.Label(
            'Yes') >> send_failure_mail >> rename_archive_referencefile_54
        if_get_failed_logs_present >> rail.Label(
            'No') >> if_get_exception_logs_present
        if_get_exception_logs_present >> rail.Label(
            'Yes') >> send_exception_mail >> rename_archive_referencefile_54
        if_get_exception_logs_present >> rail.Label(
            'No') >> send_success_mail >> rename_archive_referencefile_54
        rename_archive_referencefile_54 >> upload_newreference_file_55 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
