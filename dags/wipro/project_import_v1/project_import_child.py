from datetime import timedelta,datetime as dt
import rail
from wipro.project_import_v1.utils import custom_methods,response_filter

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.projects_child_dag_id,
        description='Wipro Process Each Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_tenant_wide_log = rail.CreateLogOperator(
            task_id = 'create_tenant_wide_log',
            tenant_wide_name= config.get_project_tenant_log,
            existing_log_mode="append"
        )

        create_project_log = rail.CreateLogOperator(
             task_id='create_project_log'
        )

        create_collection_input_data = rail.CreateCollectionOperator(
            task_id = 'create_collection_input_data',
            source=lambda dag_run: dag_run.conf['project_data'],
            name= 'inputdata',
            columns= {
                "EMPLOYEE": "empid",
                "PROJECT_ID": "projectcode",
                "PROJTEXT": "projectname",
                "ASNMT_START_DATE": "userstartdate",
                "ASNMT_END_DATE": "userenddate",
                "PM_ID": "pm_empid",
                "PM_ADID": "pm_loginname",
                "PM_NAME": "pm_name",
                "PM_MAIL": "pm_email",
                "TASK_ID": "taskcode",
                "TASK_DESCRIPTION": "taskname",
                "TASK_START_DATE": "taskstartdate",
                "TASK_END_DATE": "taskenddate"
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_input_data', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check'
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM inputdata WHERE (NULLIF(empid,'') IS NULL OR NULLIF(projectcode,'') IS NULL OR
                NULLIF(taskname,'') IS NULL)"""
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
            log= "{{ result('create_project_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=custom_methods.get_invalid_logs_property_conf
        )

        query_valid_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_data_from_rawdata',
            query="""SELECT * FROM inputdata WHERE (NULLIF(empid,'') IS NOT NULL AND NULLIF(projectcode,'') IS NOT NULL AND
                NULLIF(taskname,'') IS NOT NULL)""",
            name= 'final_data'
        )

        has_valid_projects = rail.IfOperator(
            task_id='has_valid_projects',
            test="{{ result('query_valid_data_from_rawdata', 'length') > 0 }}",
            yes_task='get_user_report_details',
            no_task= 'process_child_dag_for_exception_log'
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_base_report_name
        )

        generate_user_report = rail.run_report2(
            group_id="generate_base_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri":  rail.result('get_user_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test=lambda: rail.result(
                "generate_base_report.get_report_result", "has_data"),
            yes_task='report_has_expected_columns',
            no_task="fail_no_data_in_report"
        )

        fail_no_data_in_report = rail.FailOperator(
            task_id="fail_no_data_in_report",
            message="No Data in the Base report"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_user_report_columns,
            yes_task="load_report_data",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id="create_report_collection",
            source="{{ result('load_report_data') }}",
            name="user_report_collection"
        )

        filter_required_users_from_report = rail.QueryCollectionOperator(
            task_id="filter_required_users_from_report",
            query="""SELECT * FROM user_report_collection WHERE Employee_ID IN (SELECT DISTINCT empid FROM final_data)""",
            name="available_user_details_report"
        )

        query_user_not_available = rail.QueryCollectionOperator(
            task_id="query_user_not_available",
            query="""SELECT * FROM final_data WHERE empid NOT IN (SELECT Employee_ID FROM user_report_collection) """,
            name="report_disabled_users"
        )

        has_any_disabled_users = rail.IfOperator(
            task_id="has_any_disabled_users",
            test="{{ result('query_user_not_available', 'length') > 0 }}",
            yes_task="log_user_not_available",
            no_task="query_records_to_process"
        )

        log_user_not_available = rail.WriteLogOperator(
            task_id="log_user_not_available",
            items="{{result('query_user_not_available')}}",
            log="{{result('create_project_log')}}",
            severity="Exception",
            message="User not available/disabled in Replicon",
            properties=lambda item: custom_methods.get_log_message_per_item(item,
                                                                            status="Skipped",
                                                                            action="Validation",
                                                                            details="User not available/disabled in Replicon")

        )

        query_records_to_process = rail.QueryCollectionOperator(
            task_id="query_records_to_process",
            query="""SELECT * FROM final_data WHERE empid IN (SELECT DISTINCT Employee_ID FROM user_report_collection) """,
            name="final_valid_users"
        )

        has_any_valid_users = rail.IfOperator(
            task_id="has_any_valid_users",
            test="{{ result('query_records_to_process', 'length') > 0 }}",
            yes_task="map_user_details_with_feed",
            no_task= 'process_child_dag_for_exception_log'
        )

        map_user_details_with_feed = rail.PythonOperator(
            task_id="map_user_details_with_feed",
            python_callable=custom_methods.map_user_details_with_feed_callable
        )

        create_final_valid_data_collection = rail.CreateCollectionOperator(
            task_id="create_final_valid_data_collection",
            source="{{ result('map_user_details_with_feed') | to_json }}",
            name="final_collection"
        )

        query_distict_projects = rail.QueryCollectionOperator(
            task_id='query_distict_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT projectcode from final_collection"""
        )

        get_project_last_modified_date_udf = rail.RepliconServiceOperator(
            task_id="get_project_last_modified_date_udf",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Integration_last_modified_date', 'uri')
        )

        get_project_export_type_oef_details = rail.RepliconServiceOperator(
            task_id = 'get_project_export_type_oef_details',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:project"
                },
            data_handler= response_filter.get_project_type_oef_uri
        )

        get_oef_drop_down_project_export_type_values = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_project_export_type_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_project_export_type_oef_details")[0]['uri'],
            },
            data_handler= lambda resp: {
                'it_proj_details_dropdown_uri': rail.find_first_by_attr_and_get_attr(resp['tags'],'name', 'IT_PROJ_DETAILS', 'uri')
            }
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(resp, 'displayText', 'Foreign Managers', 'uri')
        )

        process_each_projects = rail.trigger_parallel_dagrun(
            task_id = 'process_each_projects',
            items= '{{ result("query_distict_projects") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id= config.process_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'projectcode': item['projectcode'],
                'exception_log': rail.result("create_project_log"),
                'employeetypeuri': rail.result("get_all_employeetypes"),
                'can_process_project': 'Yes',
                'last_modified_date_udf_uri': rail.result("get_project_last_modified_date_udf"),
                'project_export_type_oef': rail.result("get_project_export_type_oef_details")[0]['uri'],
                'it_proj_details_dropdown_uri': rail.result("get_oef_drop_down_project_export_type_values")['it_proj_details_dropdown_uri']
            }
        )

        log_project_details = rail.WriteLogOperator(
            task_id = "log_project_details",
            log= "{{result('create_tenant_wide_log')}}",
            items= '{{ result("map_user_details_with_feed") | to_json }}',
            severity="project_data",
            message="Project data added to log",
            properties=lambda item: {
                "employee_id" : item['empid'],
                "project_id": item['projectcode'],
                "project_name": item['projectname'],
                "asn_start_date": item['userstartdate'],
                "asn_end_date": item['userenddate'],
                "task_id": item['taskcode'],
                "record_deletion_date": (dt.now() + timedelta(days=30)).strftime("%y-%m-%d")
            }
        )

        process_child_dag_for_exception_log = rail.TriggerDagRunOperator(
            task_id = 'process_child_dag_for_exception_log',
            trigger_dag_id= config.process_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'can_process_project': 'No',
                'exception_log': rail.result("create_project_log")
            }
        )

        create_tenant_wide_log >> create_project_log >> create_collection_input_data >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_any_blankmandatory_check >> has_any_blank_mandatory_field

        has_any_blank_mandatory_field >> rail.Label(
            "Yes") >> write_blankmandatory_field_log >> query_valid_data_from_rawdata

        has_any_blank_mandatory_field >> rail.Label(
            "No") >> query_valid_data_from_rawdata >> has_valid_projects

        has_valid_projects >> rail.Label(
            "Yes") >> get_user_report_details >> generate_user_report

        generate_user_report >> report_has_data >> rail.Label(
            "No") >> fail_no_data_in_report

        report_has_data >> rail.Label("Yes") >> report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

        report_has_expected_columns >> rail.Label(
            "Yes") >> load_report_data >> create_report_collection >> filter_required_users_from_report >> \
                query_user_not_available >> has_any_disabled_users

        has_any_disabled_users >> rail.Label(
            "Yes") >> log_user_not_available >> query_records_to_process

        has_any_disabled_users >> rail.Label(
            "No") >> query_records_to_process >> has_any_valid_users

        has_any_valid_users >> rail.Label(
            "Yes") >> map_user_details_with_feed >> create_final_valid_data_collection >> query_distict_projects >>\
                get_project_last_modified_date_udf >> get_project_export_type_oef_details >> get_oef_drop_down_project_export_type_values >>\
                    get_all_employeetypes >> process_each_projects >> log_project_details

        has_any_valid_users >> rail.Label(
            "No") >> process_child_dag_for_exception_log

        has_valid_projects >> rail.Label(
            "No") >> process_child_dag_for_exception_log

    return dag

rail.for_each_instance(create_child_dag_wbs)
