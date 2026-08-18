from datetime import timedelta
import itertools
import rail
from rail.lib.ecid import get_dagrun_ecid
from mammoet.time_data_import_v2.utils import custom_methods, response_filter

#pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.timedata_child_dag_id,
        description="mammoet timedata Import Child",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_main_log = rail.CreateLogOperator(
            task_id="create_main_log"
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            source=lambda dag_run: dag_run.conf['time_data']['timedata'],
            columns={
                "sap_counter_id": "counter",
                "entry_date": "workdate",
                "employeeid": "employeenumber",
                "activity_type": "acttype",
                "project_code": "wbselement",
                "task_id": "sub_activity",
                "time_entry_type": "abs_att_type",
                "in_time": "starttime",
                "out_time": "endtime",
                "entry_id": "extdocumentno",
                "source_system": "sourcesystem",
                "crane_capacity": "cranecapacity",
                "ref_counter_id": "ref_counter_id",
                "account_indicator": "account_indicator"
            },
            name="raw_input_data"
        )

        has_any_data = rail.IfOperator(
            task_id = "has_any_data",
            test="{{ result('create_input_collection','length') > 0}}",
            yes_task="query_invalid_records"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM raw_input_data
                    WHERE NULLIF(workdate, '') IS NULL OR NULLIF(employeenumber, '') IS NULL OR
                    (NULLIF(wbselement, '') IS NULL AND abs_att_type != "BRK") OR NULLIF(starttime, '') IS NULL OR
                    NULLIF(endtime, '') IS NULL OR
                    NULLIF(sourcesystem, '') IS NULL""",
            name="mandatory_fields_missing"
        )

        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_invalid_records",
            test="{{ result('query_invalid_records','length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id="log_invalid_records",
            log="{{result('create_main_log')}}",
            items="{{ result('query_invalid_records') }}",
            message="mandatory field are missing",
            properties=lambda item: custom_methods.get_log_message_per_item(item,
                                                                            status="Skipped",
                                                                            action="Validation",
                                                                            details=custom_methods.get_missing_field_message(item))
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM raw_input_data
                    WHERE (NULLIF(workdate, '') IS NOT NULL AND NULLIF(employeenumber, '') IS NOT NULL AND
                    NULLIF(starttime, '') IS NOT NULL AND NULLIF(endtime, '') IS NOT NULL AND NULLIF(sourcesystem, '') IS NOT NULL) AND
                    ((NULLIF(wbselement, '') IS NOT NULL AND abs_att_type != "BRK") OR (NULLIF(wbselement, '') IS NULL AND abs_att_type == "BRK")) """,
            name="valid_input_records"
        )

        has_any_valid_records = rail.IfOperator(
            task_id="has_any_valid_records",
            test="{{ result('query_valid_records','length') > 0}}",
            yes_task=["query_unique_users_from_input",
                      "query_unique_projects_from_input"],
            no_task="format_logs"
        )

        query_unique_users_from_input = rail.QueryCollectionOperator(
            task_id="query_unique_users_from_input",
            query="""SELECT DISTINCT employeenumber FROM valid_input_records""",
            name="feed_unique_users"
        )

        query_unique_projects_from_input = rail.QueryCollectionOperator(
            task_id="query_unique_projects_from_input",
            query="""SELECT DISTINCT wbselement FROM valid_input_records""",
            name="feed_unique_projects"
        )

        get_all_project_details = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_all_project_details",
            items="{{result('query_unique_projects_from_input')}}",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            # unique identifier is project_code
            data=lambda item: {
                "projects": [
                    {
                        "code": item['wbselement']
                    }
                ]
            },
            data_handler=lambda res: res[0].get('projectDetails')
        )

        has_any_project_found = rail.IfOperator(
            task_id = "has_any_project_found",
            test=lambda: len(list(filter(None, rail.result("get_all_project_details")))) > 0,
            yes_task="get_timesync_project_report_details",
            no_task="log_project_not_available_in_replicon"
        )

        get_timesync_project_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_timesync_project_report_details",
            report_name=config.project_task_base_report_name
        )

        def get_project_report_params():
            project_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result("get_timesync_project_report_details")[
                'filterConfiguration']['enabledFilters'], "displayText", "ProjectFilter", 'uri')
            filter_values = []
            for item in rail.result("get_all_project_details"):
                if not item:
                    continue
                filter_values.append({
                    "reportFilterUri": project_filter_uri,
                    "value": item['uri'].split(":")[-1]
                })
            return {
                "reportParameters": [
                    {
                        "reportUri": rail.result("get_timesync_project_report_details")['uri'],
                        "filterValues": filter_values,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

        generate_project_task_base_report = rail.run_report2(
            group_id="generate_project_task_base_report",
            report_params=get_project_report_params,
            replicon_conn_id=config.replicon_conn_id
        )

        project_report_has_data = rail.IfOperator(
            task_id="project_report_has_data",
            test=lambda: rail.result(
                "generate_project_task_base_report.get_report_result", "has_data"),
            yes_task='project_report_has_expected_columns',
            no_task="log_project_not_available_in_replicon"
        )

        log_project_not_available_in_replicon = rail.WriteLogOperator(
            task_id="log_project_not_available_in_replicon",
            log="{{result('create_main_log')}}",
            items="{{ result('query_valid_records')}}",
            message="Project not available in Replicon",
            severity="Exception",
            properties=lambda item: custom_methods.get_log_message_per_item(
                item=item,
                status="Skipped",
                action="Validation",
                details="Project not available or disabled in Replicon"
            )
        )

        project_report_has_expected_columns = rail.IfOperator(
            task_id="project_report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('generate_project_task_base_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_project_report_columns,
            yes_task="load_project_report_data",
            no_task="fail_invalid_project_report_columns"
        )

        fail_invalid_project_report_columns = rail.FailOperator(
            task_id="fail_invalid_project_report_columns",
            message="Base report column does not match"
        )

        load_project_report_data = rail.LoadCSVFileOperator(
            task_id='load_project_report_data',
            document="{{ result('generate_project_task_base_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=[
                'project_uri', 'Project Name', 'Project Code', 'Project Status',
                'Task Code', 'Task Name (Full Path)', 'Task Status', 'Task_uri',
                'Task Time & Expense Entry Type'
                    ]
        )

        create_project_report_collection = rail.CreateCollectionOperator(
            task_id="create_project_report_collection",
            source="{{ result('load_project_report_data') }}",
            name="project_task_report_collection"
        )

        get_timesync_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_timesync_user_report_details",
            report_name=config.user_base_report_name
        )

        generate_user_report = rail.run_report2(
            group_id="generate_base_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri":  rail.result('get_timesync_user_report_details')['uri'],
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

        query_get_disabled_users = rail.QueryCollectionOperator(
            task_id="query_get_disabled_users",
            query="""SELECT * FROM user_report_collection WHERE User_Status == 'Disabled'""",
            name="report_disabled_users"
        )

        has_any_disabled_users = rail.IfOperator(
            task_id="has_any_disabled_users",
            test="{{ result('query_get_disabled_users', 'length') > 0 }}",
            yes_task="fail_report_has_modified",
            no_task="dummy_can_process_records"
        )

        fail_report_has_modified = rail.FailOperator(
            task_id="fail_report_has_modified",
            message=f"Base Report {config.user_base_report_name} has been modified; Contains Disabled users"
        )

        dummy_can_process_records = rail.EmptyOperator(
            task_id = "dummy_can_process_records"
        )

        can_process_records = rail.IfOperator(
            task_id = "can_process_records",
            test=lambda: bool(rail.result('create_project_report_collection')),
            yes_task= "filter_required_users_from_report",
            no_task="format_logs"
        )

        filter_required_users_from_report = rail.QueryCollectionOperator(
            task_id="filter_required_users_from_report",
            query="""SELECT * FROM user_report_collection WHERE Employee_ID IN (SELECT employee_id FROM feed_unique_users)""",
            name="available_user_details_report"
        )

        load_available_users = rail.PythonOperator(
            task_id="load_available_users",
            python_callable=lambda: rail.write_json_artifact(
                rail.load_all_records(rail.result("filter_required_users_from_report")))
        )

        query_user_not_available = rail.QueryCollectionOperator(
            task_id="query_user_not_available",
            query="""SELECT * FROM valid_input_records WHERE employeenumber NOT IN (SELECT Employee_ID FROM user_report_collection)"""
        )

        log_user_not_available = rail.WriteLogOperator(
            task_id="log_user_not_available",
            items="{{result('query_user_not_available')}}",
            log="{{result('create_main_log')}}",
            severity="Exception",
            message="User not available/disabled in Replicon",
            properties=lambda item: custom_methods.get_log_message_per_item(item,
                                                                            status="Skipped",
                                                                            action="Validation",
                                                                            details="User not available/disabled in Replicon")

        )

        query_records_to_process = rail.QueryCollectionOperator(
            task_id="query_records_to_process",
            query="""SELECT * FROM valid_input_records WHERE employeenumber IN (SELECT Employee_ID FROM user_report_collection)""",
            name="records_to_process"
        )

        has_any_valid_records_to_process = rail.IfOperator(
            task_id="has_any_valid_records_to_process",
            test="{{ result('query_records_to_process','length') > 0}}",
            yes_task="get_all_activities",
            no_task="format_logs"
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint="/services/ActivityService1.svc/GetAllActivities"
        )

        get_all_breaks = rail.RepliconServiceOperator(
            task_id="get_all_breaks",
            endpoint="/services/BreakTypeService1.svc/GetAllBreakTypes"
        )

        map_user_details_with_feed = rail.PythonOperator(
            task_id="map_user_details_with_feed",
            python_callable=custom_methods.map_user_details_with_feed_callable
        )

        create_final_data_collection = rail.CreateCollectionOperator(
            task_id="create_final_data_collection",
            source="{{ result('map_user_details_with_feed')}}",
            name="raw_final_data"
        )

        query_invalid_final_data = rail.QueryCollectionOperator(
            task_id="query_invalid_final_data",
            query="""SELECT * FROM raw_final_data WHERE
                        is_valid_dates == 0 OR ((NULLIF(project_uri, '') IS NULL
                        OR LOWER(project_status) != 'in progress') AND abs_att_type != "BRK") """
        )

        def get_final_data_validation_msg(item):
            if item['is_valid_dates'] == "0":
                return "Entry Date outside of User's Start/End Date"
            if item['project_status'] and item['project_status'].lower() != "in progress":
                return "Project status is not In-Progress"
            return "Project not available in Replicon"

        log_invalid_final_data_records = rail.WriteLogOperator(
            task_id="log_invalid_final_data_records",
            items="{{result('query_invalid_final_data')}}",
            log="{{result('create_main_log')}}",
            message="Validation Failed",
            severity="Skipped",
            properties=lambda item: custom_methods.get_log_message_per_item(item,
                                                                            status="Skipped",
                                                                            action="Validation",
                                                                            details=get_final_data_validation_msg(item))
        )

        query_valid_final_data = rail.QueryCollectionOperator(
            task_id="query_valid_final_data",
            query="""SELECT * FROM raw_final_data WHERE
                        is_valid_dates == 1 AND ((NULLIF(project_uri, '') IS NOT NULL
                        AND LOWER(project_status) == 'in progress') OR abs_att_type == "BRK") """,
            name="final_data"
        )

        query_unique_users_from_final_data = rail.QueryCollectionOperator(
            task_id="query_unique_users_from_final_data",
            query="SELECT DISTINCT employeenumber, user_uri FROM final_data"
        )

        get_entryid_oef_uris = rail.RepliconServiceOperator(
            task_id="get_entryid_oef_uris",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"},
            data_handler=lambda res: {
                'sap_counter_id': rail.find_first_by_attr_and_get_attr(res, 'name', 'SAP Counter ID', 'uri'),
                'replicon_id': rail.find_first_by_attr_and_get_attr(res, 'name', 'Replicon ID', 'uri'),
                'attendence_type_nl': rail.find_first_by_attr_and_get_attr(res, 'name', 'Attendance Type (NL)', 'uri'),
                'attendence_type_be': rail.find_first_by_attr_and_get_attr(res, 'name', 'Attendance Type (BE)', 'uri'),
                'time_entry_type': rail.find_first_by_attr_and_get_attr(res, 'name', 'Time Entry Type', 'uri'),
                'account_indicator': rail.find_first_by_attr_and_get_attr(res, 'name', 'Account Indicator', 'uri')
            }
        )

        get_time_entry_all_columns = rail.RepliconServiceOperator(
            task_id='get_time_entry_all_columns',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllColumns',
            data_handler=response_filter.get_timeentry_column_uri
        )

        get_time_entry_all_filter_definitions = rail.RepliconServiceOperator(
            task_id='get_time_entry_all_filter_definitions',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllFilterDefinitions',
            data_handler=response_filter.get_timeentry_filter_definition_uri
        )

        get_oef_drop_down_values_attendence_type_nl = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_attendence_type_nl",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_entryid_oef_uris').attendence_type_nl }}"},
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(resp['tags'],'name','On site','uri')
        )

        get_oef_drop_down_values_attendence_type_be = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_attendence_type_be",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_entryid_oef_uris').attendence_type_be }}"},
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(resp['tags'],'name','On site','uri')
        )

        process_each_user_record = rail.trigger_parallel_dagrun(
            task_id="process_each_user_record",
            items=lambda: rail.result("query_unique_users_from_final_data"),
            trigger_dag_id=config.process_each_user_dagid,
            parallel_count=config.parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                **{
                    "sap_id_oef_uri": rail.result("get_entryid_oef_uris")['sap_counter_id'],
                    "replicon_id_oef_uri": rail.result("get_entryid_oef_uris")['replicon_id'],
                    "attendence_type_nl_oef_uri": rail.result("get_entryid_oef_uris")['attendence_type_nl'],
                    "attendence_type_be_oef_uri": rail.result("get_entryid_oef_uris")['attendence_type_be'],
                    "time_entry_type_oef_uri": rail.result("get_entryid_oef_uris")['time_entry_type'],
                    "account_indicator_oef_uri": rail.result("get_entryid_oef_uris")['account_indicator'],
                    'sapid_column_uri': rail.result('get_time_entry_all_columns')['sap_column_uri'],
                    'repliconid_column_uri': rail.result('get_time_entry_all_columns')['replicon_column_uri'],
                    'sapid_filter_definition_uri': rail.result('get_time_entry_all_filter_definitions')['sap_filter_uri'],
                    'repliconid_filter_definition_uri': rail.result('get_time_entry_all_filter_definitions')['replicon_filter_uri'],
                    "logs": rail.result("create_main_log"),
                    "attendence_type_nl_dropdown_value": rail.result("get_oef_drop_down_values_attendence_type_nl"),
                    "attendence_type_be_dropdown_value": rail.result("get_oef_drop_down_values_attendence_type_be")
                }
            }
        )

        format_logs = rail.EmptyOperator(
            task_id="format_logs"
        )

        def get_all__child_dag_run_ids_callable():
            dagrun_ids = list(filter(None,map(lambda x: rail.result(
                    f'process_each_user_record_{x+1}'), range(config.parallel_count))))
            if not dagrun_ids:
                return []
            return list(itertools.chain(*dagrun_ids))

        get_all__child_dag_run_ids = rail.PythonOperator(
            task_id="get_all__child_dag_run_ids",
            python_callable=get_all__child_dag_run_ids_callable,
        )

        gather_all_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_all_logs",
            dag_runs="{{ result('get_all__child_dag_run_ids')}}",
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.execution_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id="process_log_generation",
            trigger_dag_id=config.process_log_generation,
            conf=lambda dag_run: {
                "logs": rail.result('gather_all_logs'),
                "main_log": rail.result('create_main_log'),
                'log_filename': f'''log_{ get_dagrun_ecid(dag_run).replace(":", "-")}.csv'''
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run: {
                "valid_user_records": rail.result("query_valid_final_data", "length"),
                "invalid_user_records": rail.result("query_invalid_final_data", "length"),
                "master_ecid": dag_run.conf['master_ecid']
            }
        )

        create_main_log >> create_input_collection >> has_any_data >> rail.Label("Yes") >> query_invalid_records
        query_invalid_records >> has_any_invalid_records >> rail.Label(
            "Yes") >> log_invalid_records >> query_valid_records
        has_any_invalid_records >> rail.Label("No") >> query_valid_records >> has_any_valid_records >> rail.Label(
            "Yes") >> [query_unique_users_from_input, query_unique_projects_from_input]
        has_any_valid_records >> rail.Label("No") >> format_logs

        query_unique_projects_from_input >> get_all_project_details >> has_any_project_found
        has_any_project_found >> rail.Label("No") >> log_project_not_available_in_replicon
        has_any_project_found  >> rail.Label("Yes") >> get_timesync_project_report_details\
            >> generate_project_task_base_report >> project_report_has_data
        project_report_has_data >> rail.Label("Yes") >> project_report_has_expected_columns >> rail.Label(
            "Yes") >> load_project_report_data >> create_project_report_collection
        create_project_report_collection >> can_process_records
        project_report_has_data >> rail.Label(
            "No") >> log_project_not_available_in_replicon >> format_logs
        project_report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_project_report_columns

        query_unique_users_from_input >> get_timesync_user_report_details >> generate_user_report
        generate_user_report >> report_has_data >> rail.Label(
            "No") >> fail_no_data_in_report
        report_has_data >> rail.Label("Yes") >> report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns
        report_has_expected_columns >> rail.Label(
            "Yes") >> load_report_data >> create_report_collection

        create_report_collection >> query_get_disabled_users >> has_any_disabled_users
        has_any_disabled_users >> rail.Label(
            "Yes") >> dummy_can_process_records >> can_process_records >> rail.Label("Yes") \
                >> filter_required_users_from_report >> load_available_users >> query_user_not_available
        has_any_disabled_users >> rail.Label("No") >> fail_report_has_modified
        can_process_records >> rail.Label("No") >> format_logs
        query_user_not_available >> log_user_not_available >> query_records_to_process >> has_any_valid_records_to_process
        has_any_valid_records_to_process >> rail.Label("No") >> format_logs
        has_any_valid_records_to_process >> rail.Label("Yes") >> get_all_activities
        get_all_activities >> get_all_breaks >> map_user_details_with_feed >> create_final_data_collection >> query_invalid_final_data
        query_invalid_final_data >> log_invalid_final_data_records >> query_valid_final_data >> query_unique_users_from_final_data
        query_unique_users_from_final_data >> get_entryid_oef_uris >> get_time_entry_all_columns >> get_time_entry_all_filter_definitions
        get_time_entry_all_filter_definitions >> get_oef_drop_down_values_attendence_type_nl >>\
            get_oef_drop_down_values_attendence_type_be >> process_each_user_record
        process_each_user_record >> format_logs >> get_all__child_dag_run_ids >> gather_all_logs >> process_log_generation >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
