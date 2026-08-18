from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_update_projects_file_dag_id,
        description=f'Audaxgroup process UPDATE projects file {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='log_timenow_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_timenow_3',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_timenow_3=rail.PythonOperator(
            task_id='log_timenow_3',
            python_callable= lambda: datetime.now().strftime('%m-%d-%Y %H:%M')
        )

        is_csv_4 = rail.IfOperator(
            task_id='is_csv_4',
            test="{{ dag_run.conf.fullpath | file_ext | lower == 'csv' }}",
            yes_task='audax_project_task_file_processing_add_entry_7',
            no_task='send_mail_5'
        )

        send_mail_5=rail.EmailOperator(
            task_id='send_mail_5',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=config.company_key + ''' | Project Import - Invalid file format {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Project Import job has not been processed because of invalid file format for the file - '{{ dag_run.conf.filename }}'.<br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
                        params=None,
                    )

        audax_project_task_file_processing_add_entry_7=rail.WriteLogOperator(
            task_id='audax_project_task_file_processing_add_entry_7',
            log="{{ dag_run.conf.audax_project_task_file_processing_lookup_table }}",
            message="na",
            severity="Info",
            properties={
                "recipeid": "{{dag_run.conf.parent_ecid}}",
                "filename": "{{ dag_run.conf.filename }}"
            }
        )

        audax_users_and_departments_lookup_table = rail.CreateLogOperator(
            task_id="audax_users_and_departments_lookup_table"
        )

        audax_project_task_import_logs = rail.CreateLogOperator(
            task_id="audax_project_task_import_logs"
        )

        download_9 = rail.SFTPDownloadFileOperator(
            task_id='download_9',
            remote_filepath="{{ dag_run.conf.fullpath }}",
        )

        parse_csv_10 = rail.LoadCSVFileOperator(
            task_id='parse_csv_10',
            document="{{ result('download_9') }}",
            delimiter=','
        )

        if_csv_has_data_present_11 = rail.IfOperator(
            task_id='if_csv_has_data_present_11',
            test="{{result('parse_csv_10') | load_all_records | length > 0 }}",
            yes_task="get_all_reports_16",
            no_task="send_mail_12",
        )

        send_mail_12=rail.EmailOperator(
            task_id='send_mail_12',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Project Task Import - No data in file {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Project Task Import job was not completed because there was no data in the file - '{{ dag_run.conf.filename }}'.<br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        audax_project_task_file_processing_truncate_13=rail.EmptyOperator(
            task_id='audax_project_task_file_processing_truncate_13',
        )

        get_all_reports_16=rail.RepliconServiceOperator(
            task_id='get_all_reports_16',
            endpoint="/services/reportService1.svc/GetAllReports",
            data=None
        )

        get_report_details_19 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_19',
            report_name='Department Report for Replicon Integration',
        )

        generate_reports_batch_19 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_19',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data={"reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details_19').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_generate_reports_batch_19 = rail.batch_execution(
            group_id='execute_execute_generate_reports_batch_19',
            creation_task_id='generate_reports_batch_19',
        )

        get_report_batch_results_21 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_21',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{ result('generate_reports_batch_19') }}"},
        )

        if_first_payload_not_contains_nodata_22 = rail.IfOperator(
            task_id='if_first_payload_not_contains_nodata_22',
            test='''{{ result('get_report_batch_results_21').reportGenerationResults | is_truthy and not result('get_report_batch_results_21').reportGenerationResults[0].payload |  matches('No Data') }}''',
            yes_task="parse_csv_departmentreport_25",
            no_task="finish",
        )

        parse_csv_departmentreport_25=rail.LoadCSVFileOperator(
            task_id='parse_csv_departmentreport_25',
            document="{{ result('get_report_batch_results_21').reportGenerationResults[0].payload }}",
        )

        compose_csv_26 = rail.WriteCSVFileOperator(
            task_id="compose_csv_26",
            source=lambda: rail.result('parse_csv_departmentreport_25'),
            header=[
                "Department Name",
                "Department Code",
                "DepartmentUri",
                "Parent Department Name",
                "Department Full Name",
            ],
            row=lambda item:[
                item['Department Name'],
                item['Department Code'],
                item['DepartmentUri'],
                item['Parent Department Name'],
                '/'.join(item['Department Full Name'].split(" / ")) if (item['Department Full Name'] and '/' in item['Department Full Name']) else ''
            ],

        )

        parse_csv_formatted_departmentreport_27 = rail.LoadCSVFileOperator(
            task_id='parse_csv_formatted_departmentreport_27',
            document="{{ result('compose_csv_26') }}",
        )


        audaxgroup_users_and_departments_add_batch_of_entries_28= rail.WriteLogOperator(
            task_id='audaxgroup_users_and_departments_add_batch_of_entries_28',
            log="{{result('audax_users_and_departments_lookup_table')}}",
            items="{{result('parse_csv_formatted_departmentreport_27')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "name": item["Department Full Name"],
                "uri": item["DepartmentUri"],
            }
        )

        get_report_details_33 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_33',
            report_name='Project Task Report for Replicon Integration',
        )

        generate_reports_batch_33 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_33',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data={"reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details_33').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_generate_reports_batch_33 = rail.batch_execution(
            group_id='execute_execute_generate_reports_batch_33',
            creation_task_id='generate_reports_batch_33',
        )

        get_report_batch_results_35 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_35',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{ result('generate_reports_batch_33') }}"},
        )

        if_first_payload_not_contains_nodata_36 = rail.IfOperator(
            task_id='if_first_payload_not_contains_nodata_36',
            test='''{{ result('get_report_batch_results_35').reportGenerationResults | is_truthy and not result('get_report_batch_results_35').reportGenerationResults[0].payload |  matches('No Data') }}''',
            yes_task="get_all_custom_fields_for_project_40",
            no_task="finish",
        )

        get_all_custom_fields_for_project_40 = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_for_project_40",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                    "objectUri": "urn:replicon:object-type:project"
            }
        )

        log_udf_program_uri_41=rail.PythonOperator(
            task_id='log_udf_program_uri_41',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_for_project_40'), 'displayText', "Program",'uri', '')
        )

        get_all_custom_field_drop_down_options_42=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_42',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_udf_program_uri_41') }}"
                }
        )

        log_udf_deal_opportunity_id_project_uri_43=rail.PythonOperator(
            task_id='log_udf_deal_opportunity_id_project_uri_43',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_for_project_40'),'displayText', "Deal Opportunity ID (Project)",'uri', '')
        )


        log_udf_project_department_uri_44=rail.PythonOperator(
            task_id='log_udf_project_department_uri_44',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_for_project_40'),'displayText', "Project Department",'uri', '')
        )


        get_all_custom_field_drop_down_options_45=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_45',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_udf_project_department_uri_44') }}"
                }
        )

        log_udf_project_business_unit_uri_46=rail.PythonOperator(
            task_id='log_udf_project_business_unit_uri_46',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_for_project_40'),'displayText', "Project Business Unit", 'uri', '')
        )

        get_all_custom_field_drop_down_options_47=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_47',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
            "customFieldUri": "{{ result('log_udf_project_business_unit_uri_46') }}"
            }
        )

        create_collection_create_list_from_csv_52 = rail.CreateCollectionOperator(
            task_id="create_collection_create_list_from_csv_52",
            source='{{result("parse_csv_10")}}',
            columns={
                    "Project Name":"projectname",
                    "Billing Type":"billingtype",
                    "Time Entry Billing Type":"timeentrybillingtype",
                    "Time Entry Start Date":"timeentrystartdate",
                    "Time Entry End Date":"timeentryenddate",
                    "Time Entry Allowed":"timeentryallowed",
                    "Project Status":"projectstatus",
                    "Project Description":"projectdescription",
                    "Program Name":"programname",
                    "Project Leader Login Name":"projectleaderloginname",
                    "ProjectInfo1":"projectinfo1",
                    "ProjectInfo2":"projectinfo2",
                    "ProjectInfo3":"projectinfo3",
                    "Project Team - Users":"projectteamusers",
                    "Project Team - Departments":"projectteamdepartments"
            },
            name="inputfile"
        )

        load_csv_create_list_from_csv_53=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_53",
            document='{{result("get_report_batch_results_35").reportGenerationResults[0].payload }}}',
        )

        create_collection_create_list_from_csv_53 = rail.CreateCollectionOperator(
            task_id="create_collection_create_list_from_csv_53",
            source='{{ result("load_csv_create_list_from_csv_53")}}',
            columns={
                    "Project Name": "existingprojectname",
                    "ProjectUri": "projecturi",
                    "Project Status":"status"
            },
            name="allprojectstasks"
        )

        query_list_new_projects_54=rail.QueryCollectionOperator(
            task_id='query_list_new_projects_54',
            query="""SELECT DISTINCT inputfile.projectname, inputfile.billingtype, inputfile.timeentrybillingtype, inputfile.timeentrystartdate, inputfile.timeentryenddate, inputfile.timeentryallowed, inputfile.projectstatus, inputfile.projectdescription, inputfile.programname, inputfile.projectleaderloginname, inputfile.projectinfo1, inputfile.projectinfo2, inputfile.projectinfo3, inputfile.projectteamusers, inputfile.projectteamdepartments FROM inputfile WHERE inputfile.projectname NOT IN (SELECT DISTINCT allprojectstasks.existingprojectname FROM allprojectstasks) AND NULLIF(inputfile.projectname, '') IS NOT NULL""",
        )

        query_list_existing_projects_55=rail.QueryCollectionOperator(
            task_id='query_list_existing_projects_55',
            query="""SELECT DISTINCT  allprojectstasks.projecturi, inputfile.projectname, inputfile.billingtype, inputfile.timeentrybillingtype, inputfile.timeentrystartdate, inputfile.timeentryenddate, inputfile.timeentryallowed, inputfile.projectstatus, inputfile.projectdescription, inputfile.programname, inputfile.projectleaderloginname, inputfile.projectinfo1, inputfile.projectinfo2, inputfile.projectinfo3, inputfile.projectteamusers, inputfile.projectteamdepartments FROM  allprojectstasks INNER JOIN  inputfile ON  allprojectstasks.existingprojectname= inputfile.projectname""",
        )

        query_list_invalid_projectsblankprojectnames_56=rail.QueryCollectionOperator(
            task_id='query_list_invalid_projectsblankprojectnames_56',
            query="""SELECT * FROM  inputfile WHERE NULLIF(inputfile.projectname, '') IS NULL""",
        )

        audaxgroup_project_task_import_logs_add_batch_of_entries_57 = rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_batch_of_entries_57',
            log="{{result('audax_project_task_import_logs')}}",
            items="{{result('query_list_invalid_projectsblankprojectnames_56')}}",
            message="na",
            severity="Ignored",
            properties=lambda item: {
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "projectname": item["projectname"],
                "taskname": "",
                "taskcode": "",
                "status": "Exception",
                "details": "Project name not provided",
                "childjobid": "",
            }
        )

        if_query_list_new_projects_54_rows_greater_than_0_60=rail.IfOperator(
            task_id='if_query_list_new_projects_54_rows_greater_than_0_60',
            test="{{result('query_list_new_projects_54', 'length') > 0 }}",
            yes_task="declare_child_triggered_list",
            no_task="if_query_list_existing_projects_55_rows_greater_than_0_65",
        )

        declare_child_triggered_list = rail.SetVariableOperator(
            task_id='declare_child_triggered_list',
            name='childtriggered',
            append=False,
            value=0
        )

        declare_dagrun_list = rail.SetVariableOperator(
            task_id='declare_dagrun_list',
            append=False,
            name='dagrunlist1',
            value=[]
        )

        foreach_query_list_new_projects_54_61=rail.ForEachOperator(
            task_id='foreach_query_list_new_projects_54_61',
            items="{{ result('query_list_new_projects_54') }}",
            start_task = 'trigger_dag_run_add_update_projectsasync_62',
            end_task = 'foreach_query_list_new_projects_54_61_end'
        )

        trigger_dag_run_add_update_projectsasync_62=rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_add_update_projectsasync_62',
            retries=0,
            trigger_dag_id=config.add_update_projects_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "projectname": rail.result('foreach_query_list_new_projects_54_61')['projectname'],
                "billingtype": rail.result('foreach_query_list_new_projects_54_61')['billingtype'],
                "timeentrybillingtype": rail.result('foreach_query_list_new_projects_54_61')['timeentrybillingtype'],
                "timeentrystartdate": rail.result('foreach_query_list_new_projects_54_61')['timeentrystartdate'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('timeentrystartdate') else '',
                "timeentryenddate": rail.result('foreach_query_list_new_projects_54_61')['timeentryenddate'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('timeentryenddate') else '',
                "timeentryallowed": rail.result('foreach_query_list_new_projects_54_61')['timeentryallowed'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('timeentryallowed') else '',
                "projectstatus": rail.result('foreach_query_list_new_projects_54_61')['projectstatus'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectstatus') else '',
                "projectdescription": rail.result('foreach_query_list_new_projects_54_61')['projectdescription'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectdescription') else '',
                "programname": rail.result('foreach_query_list_new_projects_54_61')['programname'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('programname') else '',
                "projectleaderloginname": rail.result('foreach_query_list_new_projects_54_61')['projectleaderloginname'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectleaderloginname') else '',
                "projectinfo1": rail.result('foreach_query_list_new_projects_54_61')['projectinfo1'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectinfo1') else '',
                "projectinfo2": rail.result('foreach_query_list_new_projects_54_61')['projectinfo2'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectinfo2') else '',
                "projectinfo3": rail.result('foreach_query_list_new_projects_54_61')['projectinfo3'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectinfo3') else '',
                "projectteamusers": rail.result('foreach_query_list_new_projects_54_61')['projectteamusers'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectteamusers') else '',
                "projectteamdepartments": rail.result('foreach_query_list_new_projects_54_61')['projectteamdepartments'].strip() if rail.result('foreach_query_list_new_projects_54_61').get('projectteamdepartments') else '',
                "type": "add",
                "originalfilename": dag_run.conf['filename'],
                "fullpath": dag_run.conf['fullpath'] ,
                "tenanturl": rail.get_tenant_slug(),
                "companykey": config.company_key,
                "emailaddress": config.tenant_email,
                "Program_OptionURI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_42'),'displayText', rail.result('foreach_query_list_new_projects_54_61')["programname"],'uri', '') if "programname" in rail.result('foreach_query_list_new_projects_54_61') else '',
                "ProjectDepartment_OptionURI":rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_45'),'displayText', rail.result('foreach_query_list_new_projects_54_61')["projectinfo3"],'uri', '') if "projectinfo3" in rail.result('foreach_query_list_new_projects_54_61') else '',
                "ProjectBusinessUnit_OptionURI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_47'),'displayText', rail.result('foreach_query_list_new_projects_54_61')["projectinfo2"],'uri', '') if "projectinfo2" in rail.result('foreach_query_list_new_projects_54_61') else '',
                "UDFProgramURI": rail.result('log_udf_program_uri_41'),
                "UDFDealOpportunityIDProjectURI": rail.result('log_udf_deal_opportunity_id_project_uri_43'),
                "UDFProjectDepartmentURI": rail.result('log_udf_project_department_uri_44'),
                "UDFProjectBusinessUnitURI": rail.result('log_udf_project_business_unit_uri_46'),
                "audax_project_task_import_logs": rail.result('audax_project_task_import_logs'),
                "audax_users_and_departments_lookup_table": rail.result('audax_users_and_departments_lookup_table'),
                "islastitem": "yes" if (rail.result('declare_child_triggered_list')['value'] + 1) == rail.result('query_list_new_projects_54', 'length') else "no",
                "parent_ecid": rail.render_template('{{dag_run_ecid()}}')
            }
        )
        insert_to_list_add = rail.SetVariableOperator(
            task_id='insert_to_list_add',
            append=True,
            name='{{ result("declare_dagrun_list").name }}',
            value='{{result("trigger_dag_run_add_update_projectsasync_62")}}'
        )
        insert_child_to_triggered_list = rail.SetVariableOperator(
            task_id='insert_child_to_triggered_list',
            name="{{result('declare_child_triggered_list').name}}",
            append=True,
            value=lambda: rail.result('declare_child_triggered_list')[
                'value'] + 1
        )

        foreach_query_list_new_projects_54_61_end=rail.EmptyOperator(
            task_id='foreach_query_list_new_projects_54_61_end',
        )

        wait_for_completion_dag_add_update_projectsasync1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_add_update_projectsasync1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ dag_run_var(result('declare_dagrun_list').name) | to_json }}"
        )

        log_itemsprocessed_63=rail.PythonOperator(
            task_id='log_itemsprocessed_63',
            python_callable= lambda:  rail.result('query_list_new_projects_54', 'length') * 3
        )

        if_query_list_existing_projects_55_rows_greater_than_0_65=rail.IfOperator(
            task_id='if_query_list_existing_projects_55_rows_greater_than_0_65',
            test="{{result('query_list_existing_projects_55', 'length') > 0 }}",
            yes_task="declare_child_triggered_list_1",
            no_task="audaxgroup_project_task_import_logs_search_entries_70",
        )

        declare_child_triggered_list_1 = rail.SetVariableOperator(
            task_id='declare_child_triggered_list_1',
            name='childtriggered1',
            append=False,
            value=0
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='dagrunlist',
            value=[]
        )
        foreach_query_list_existing_projects_55_66=rail.ForEachOperator(
            task_id='foreach_query_list_existing_projects_55_66',
            items="{{ result('query_list_existing_projects_55') }}",
            start_task = 'trigger_dag_add_update_projectsasync_67',
            end_task = 'foreach_query_list_existing_projects_55_66_end'
        )

        trigger_dag_add_update_projectsasync_67=rail.TriggerDagRunOperator(
            task_id='trigger_dag_add_update_projectsasync_67',
            retries=0,
            trigger_dag_id=config.add_update_projects_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "projectname": rail.result('foreach_query_list_existing_projects_55_66')['projectname'],
                "billingtype": rail.result('foreach_query_list_existing_projects_55_66')['billingtype'],
                "timeentrybillingtype": rail.result('foreach_query_list_existing_projects_55_66')['timeentrybillingtype'],
                "timeentrystartdate": rail.result('foreach_query_list_existing_projects_55_66')['timeentrystartdate'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('timeentrystartdate') else '',
                "timeentryenddate": rail.result('foreach_query_list_existing_projects_55_66')['timeentryenddate'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('timeentryenddate') else '',
                "timeentryallowed": rail.result('foreach_query_list_existing_projects_55_66')['timeentryallowed'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('timeentryallowed') else '',
                "projectstatus": rail.result('foreach_query_list_existing_projects_55_66')['projectstatus'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectstatus') else '',
                "projectdescription": rail.result('foreach_query_list_existing_projects_55_66')['projectdescription'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectdescription') else '',
                "programname": rail.result('foreach_query_list_existing_projects_55_66')['programname'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('programname') else '',
                "projectleaderloginname": rail.result('foreach_query_list_existing_projects_55_66')['projectleaderloginname'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectleaderloginname') else '',
                "projectinfo1": rail.result('foreach_query_list_existing_projects_55_66')['projectinfo1'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectinfo1') else '',
                "projectinfo2": rail.result('foreach_query_list_existing_projects_55_66')['projectinfo2'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectinfo2') else '',
                "projectinfo3": rail.result('foreach_query_list_existing_projects_55_66')['projectinfo3'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectinfo3') else '',
                "projectteamusers": rail.result('foreach_query_list_existing_projects_55_66')['projectteamusers'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectteamusers') else '',
                "projectteamdepartments": rail.result('foreach_query_list_existing_projects_55_66')['projectteamdepartments'].strip() if rail.result('foreach_query_list_existing_projects_55_66').get('projectteamdepartments') else '',
                "type": "update",
                "originalfilename": dag_run.conf['filename'],
                "fullpath": dag_run.conf['fullpath'],
                "tenanturl": rail.get_tenant_slug(),
                "companykey": config.company_key,
                "emailaddress": config.tenant_email,
                "projecturi": rail.result('foreach_query_list_existing_projects_55_66')['projecturi'],
                "Program_OptionURI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_42'),'displayText', rail.result('foreach_query_list_existing_projects_55_66')["programname"],'uri', '') if "programname" in rail.result('foreach_query_list_existing_projects_55_66') else '',
                "ProjectDepartment_OptionURI":rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_45'),'displayText', rail.result('foreach_query_list_existing_projects_55_66')["projectinfo3"],'uri', '') if "projectinfo3" in rail.result('foreach_query_list_existing_projects_55_66') else '',
                "ProjectBusinessUnit_OptionURI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_47'),'displayText', rail.result('foreach_query_list_existing_projects_55_66')["projectinfo2"],'uri', '') if "projectinfo2" in rail.result('foreach_query_list_existing_projects_55_66') else '',
                "UDFProgramURI": rail.result('log_udf_program_uri_41'),
                "UDFDealOpportunityIDProjectURI": rail.result('log_udf_deal_opportunity_id_project_uri_43'),
                "UDFProjectDepartmentURI": rail.result('log_udf_project_department_uri_44'),
                "UDFProjectBusinessUnitURI": rail.result('log_udf_project_business_unit_uri_46'),
                "audax_users_and_departments_lookup_table": rail.result('audax_users_and_departments_lookup_table'),
                "audax_project_task_import_logs": rail.result('audax_project_task_import_logs'),
                "islastitem": "yes" if (rail.result('declare_child_triggered_list_1')['value'] + 1) == rail.result('query_list_existing_projects_55', 'length') else "no",
                "parent_ecid": rail.render_template('{{dag_run_ecid()}}')
            }
        )

        insert_to_list_update = rail.SetVariableOperator(
            task_id='insert_to_list_update',
            append=True,
            name='{{ result("declare_list").name }}',
            value='{{result("trigger_dag_add_update_projectsasync_67")}}'
        )

        insert_child_to_triggered_list_1 = rail.SetVariableOperator(
            task_id='insert_child_to_triggered_list_1',
            name="{{result('declare_child_triggered_list_1').name}}",
            append=True,
            value=lambda: rail.result('declare_child_triggered_list_1')[
                'value'] + 1
        )

        foreach_query_list_existing_projects_55_66_end=rail.EmptyOperator(
            task_id='foreach_query_list_existing_projects_55_66_end',
        )

        wait_for_completion_dag_add_update_projectsasync = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_add_update_projectsasync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ dag_run_var(result('declare_list').name) | to_json }}"
        )
        log_itemsprocessed_68=rail.PythonOperator(
            task_id='log_itemsprocessed_68',
            python_callable= lambda:  rail.result('query_list_existing_projects_55', 'length') * 3
        )


        audaxgroup_project_task_import_logs_search_entries_70=rail.FilterLogEntriesOperator(
            task_id='audaxgroup_project_task_import_logs_search_entries_70',
            log="{{result('audax_project_task_import_logs')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_first_id_present_71=rail.IfOperator(
            task_id='if_first_id_present_71',
            test="{{result('audaxgroup_project_task_import_logs_search_entries_70') | length > 0 | is_truthy }}",
            yes_task="create_csv_lines_72",
            no_task="rename_93",
        )

        create_csv_lines_72=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_72',
            source="{{result('audaxgroup_project_task_import_logs_search_entries_70')}}",
            header=[
                    'Projectname',
                    'Status',
                    'Details',
                    'JobID'
                    ],
            row= [
                "{{ item.properties.projectname }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}",
                "{{ item.ecid}}"
            ],
        )

        upload_74=rail.SFTPUploadFileOperator(
            task_id='upload_74',
            content='''{{ result('create_csv_lines_72') }}''',
            remote_filepath= config.log_filepath +'''/Project_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename }}''',
        )

        log_checkforerrors_79 = rail.PythonOperator(
            task_id='log_checkforerrors_79',
            python_callable=lambda: next(
                (item['properties']['status']
                 for item in rail.load_all_records(rail.result('audaxgroup_project_task_import_logs_search_entries_70'))
                 if item.get('properties', {}).get('status') == 'Error'),
                None)
        )

        if_log_checkforerrors_79_blank_80=rail.IfOperator(
            task_id='if_log_checkforerrors_79_blank_80',
            test='''{{ result('log_checkforerrors_79') | is_falsy }}''',
            yes_task="log_checkfor_exceptions_81",
            no_task="send_mail_completedwitherrors_87",
        )


        log_checkfor_exceptions_81=rail.PythonOperator(
            task_id='log_checkfor_exceptions_81',
            python_callable=lambda: next(
                (item['properties']['status']
                 for item in rail.load_all_records(rail.result('audaxgroup_project_task_import_logs_search_entries_70'))
                 if item.get('properties', {}).get('status') == 'Exception'),
                None)
        )


        if_log_checkfor_exceptions_81_blank_82=rail.IfOperator(
            task_id='if_log_checkfor_exceptions_81_blank_82',
            test='''{{ result('log_checkfor_exceptions_81') | is_falsy }}''',
            yes_task="send_mail_completedsuccessfully_83",
            no_task="send_mail_completedwith_exceptions_85",
        )


        send_mail_completedsuccessfully_83=rail.EmailOperator(
            task_id='send_mail_completedsuccessfully_83',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Project Import - Completed Successfully {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Project Import job is completed successfully based on the file - '{{ dag_run.conf.filename }}'. Please find the Project import logs placed in the below SFTP location for reference. <br />
            <br />
            Logs: {{params.log_filepath}}/Project_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename}} <br />
            <br />
            <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
            ''',
            params={'log_filepath': config.log_filepath},
        )


        send_mail_completedwith_exceptions_85=rail.EmailOperator(
            task_id='send_mail_completedwith_exceptions_85',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Project Import - Completed with Exceptions {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
                <br />Hello, <br />
                <br />
                The Project Import job is completed with exceptions based on the file - '{{ dag_run.conf.filename }}'. Please find the Project import logs placed in the below SFTP location for reference. <br />
                <br />
                Logs: {{params.log_filepath}}/Project_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename}} <br />
                <br />
                <p><br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
                ''',
            params={'log_filepath': config.log_filepath},
        )


        send_mail_completedwitherrors_87=rail.EmailOperator(
            task_id='send_mail_completedwitherrors_87',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Project Import - Completed with Errors {{ result('log_timenow_3') }} ''',
            html_content= '''<strong>This is an automated mail, please don't reply.</strong><br />
            <br />Hello, <br />
            <br />
            The Project Import job is completed with Errors based on the file - '{{ dag_run.conf.filename }}'. Please find the Project import logs placed in the below SFTP location for reference. <br />
            <br />
            Logs: {{params.log_filepath}}/Project_Logs_{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename}} <br />
            <br />
            <br />
            For any queries, please contact our support team at https://support.deltek.com <br />
            <br />
            Regards, <br />
            Deltek Inc. ''',
            params={'log_filepath': config.log_filepath},
        )

        rename_93=rail.SFTPMoveFileOperator(
            task_id='rename_93',
            new_filename=config.archive_filepath+'''/{{ dag_run_ecid() | replace(":", "-") }}_{{ dag_run.conf.filename }}''',
            existing_filename= '''{{ dag_run.conf.fullpath }}''',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.audax_project_task_file_processing_lookup_table }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "childjobid": "{{ dag_run_ecid() }}"
            },
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> log_timenow_3
        send_mail_5 >> finish
        log_timenow_3 >> is_csv_4
        is_csv_4 >> rail.Label('Yes') >> audax_project_task_file_processing_add_entry_7 >> audax_users_and_departments_lookup_table >> audax_project_task_import_logs >> download_9 \
        >> parse_csv_10 >> if_csv_has_data_present_11
        if_csv_has_data_present_11 >> rail.Label('Yes') >> get_all_reports_16 >> get_report_details_19 >> generate_reports_batch_19 >> execute_generate_reports_batch_19[
            0] >>  execute_generate_reports_batch_19[1] \
        >> get_report_batch_results_21 >> if_first_payload_not_contains_nodata_22
        if_first_payload_not_contains_nodata_22 >> rail.Label('Yes') >> parse_csv_departmentreport_25 >> compose_csv_26 >> parse_csv_formatted_departmentreport_27 \
        >> audaxgroup_users_and_departments_add_batch_of_entries_28 >> get_report_details_33 >> generate_reports_batch_33 >> execute_generate_reports_batch_33[
            0] >> execute_generate_reports_batch_33[1] \
        >> get_report_batch_results_35 >> if_first_payload_not_contains_nodata_36
        if_first_payload_not_contains_nodata_36 >> rail.Label('Yes') >> get_all_custom_fields_for_project_40 >> log_udf_program_uri_41 >> get_all_custom_field_drop_down_options_42 \
        >> log_udf_deal_opportunity_id_project_uri_43 >> log_udf_project_department_uri_44 >> get_all_custom_field_drop_down_options_45 >> log_udf_project_business_unit_uri_46 \
        >> get_all_custom_field_drop_down_options_47 >> create_collection_create_list_from_csv_52 >> load_csv_create_list_from_csv_53 >> create_collection_create_list_from_csv_53 \
        >> query_list_new_projects_54 >> query_list_existing_projects_55 >> query_list_invalid_projectsblankprojectnames_56 >> audaxgroup_project_task_import_logs_add_batch_of_entries_57 \
        >> if_query_list_new_projects_54_rows_greater_than_0_60
        if_query_list_new_projects_54_rows_greater_than_0_60 >> rail.Label('Yes') >> declare_child_triggered_list >> declare_dagrun_list >> foreach_query_list_new_projects_54_61 \
        >> trigger_dag_run_add_update_projectsasync_62 >> insert_to_list_add >> insert_child_to_triggered_list >> foreach_query_list_new_projects_54_61_end >> wait_for_completion_dag_add_update_projectsasync1\
        >> log_itemsprocessed_63 >> if_query_list_existing_projects_55_rows_greater_than_0_65
        foreach_query_list_new_projects_54_61 >> foreach_query_list_new_projects_54_61_end
        if_query_list_new_projects_54_rows_greater_than_0_60 >> rail.Label('No') >> if_query_list_existing_projects_55_rows_greater_than_0_65
        if_query_list_existing_projects_55_rows_greater_than_0_65 >> rail.Label('Yes') >> declare_child_triggered_list_1 >> declare_list >> foreach_query_list_existing_projects_55_66 \
        >> trigger_dag_add_update_projectsasync_67 >> insert_to_list_update >> insert_child_to_triggered_list_1 >> foreach_query_list_existing_projects_55_66_end >> wait_for_completion_dag_add_update_projectsasync >> log_itemsprocessed_68 \
        >> audaxgroup_project_task_import_logs_search_entries_70 >> if_first_id_present_71
        foreach_query_list_existing_projects_55_66 >> foreach_query_list_existing_projects_55_66_end
        if_first_id_present_71 >> rail.Label('Yes') >> create_csv_lines_72 >> upload_74 >> log_checkforerrors_79 >> if_log_checkforerrors_79_blank_80
        if_log_checkforerrors_79_blank_80 >> rail.Label('Yes') >> log_checkfor_exceptions_81 >> if_log_checkfor_exceptions_81_blank_82
        if_log_checkfor_exceptions_81_blank_82 >> rail.Label('Yes') >> send_mail_completedsuccessfully_83 >> rename_93
        if_log_checkfor_exceptions_81_blank_82 >> rail.Label('No') >> send_mail_completedwith_exceptions_85 >> rename_93
        if_log_checkforerrors_79_blank_80 >> rail.Label('No') >> send_mail_completedwitherrors_87 >> rename_93
        if_first_id_present_71 >> rail.Label('No') >> rename_93 >> finish
        if_query_list_existing_projects_55_rows_greater_than_0_65 >> rail.Label('No') >> audaxgroup_project_task_import_logs_search_entries_70
        if_first_payload_not_contains_nodata_36 >> rail.Label('No') >> finish
        if_first_payload_not_contains_nodata_22 >> rail.Label('No') >> finish
        if_csv_has_data_present_11 >> rail.Label('No') >> send_mail_12 >> audax_project_task_file_processing_truncate_13 >> finish
        is_csv_4 >> rail.Label('No') >> send_mail_5 >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
