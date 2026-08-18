import rail
from technicolorg3.time_export_to_ceta.utils import custom_methods
from technicolorg3.time_export_to_ceta.utils.request_payload import get_create_time_data_download_batch_payload
from technicolorg3.time_export_to_ceta.tasks.send_export_data_internal import create_send_export_data_internal
from technicolorg3.time_export_to_ceta.tasks.time_export import time_export_task

# config:  https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/time_export_to_ceta/config.py

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"technicolorg3_time_data_to_ceta_{config.instance}",
        description=f"TechnicolorG3 Time Data to CETA {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_interval,
        max_active_runs=config.master_dag_max_active_run
    ) as dag:

        get_ceta_export_script = rail.RepliconServiceOperator(
            task_id="get_ceta_export_script",
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", config.ceta_export_script_name)
        )

        is_script_exists = rail.IfOperator(
            task_id="is_script_exists",
            test="{{result('get_ceta_export_script') | is_truthy}}",
            yes_task=["get_all_employee_type_groups", "get_all_columns"],
            no_task="fail_file_format_not_found",
        )

        fail_file_format_not_found = rail.FailOperator(
            task_id="fail_file_format_not_found",
            message="Required file format is not available"
        )

        get_all_employee_type_groups = rail.RepliconServiceOperator(
            task_id="get_all_employee_type_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/TimeDataExportService1.svc/GetAllColumns"
        )

        get_required_details = rail.PythonOperator(
            task_id="get_required_details",
            python_callable=lambda: custom_methods.get_required_details(config)
        )

        create_time_data_item_batch = rail.RepliconServiceOperator(
            task_id="create_time_data_item_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataItemDataBatch",
            data=get_create_time_data_download_batch_payload
        )
        wait_for_batch_execution_start, wait_for_batch_execution_end = rail.batch_execution(
            group_id="wait_for_batch_execution",
            creation_task_id=create_time_data_item_batch.task_id,
            replicon_conn_id=config.replicon_conn_id
        )
        get_time_data_item_batch_result = rail.RepliconServiceOperator(
            task_id="get_time_data_item_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataItemDataBatchResults",
            data={
                "timeDataItemDataBatchUri": "{{result('create_time_data_item_batch')}}"
            }
        )
        has_timeexport_errors = rail.IfOperator(
            task_id="has_timeexport_errors",
            test="{{result('get_time_data_item_batch_result').error | is_truthy}}",
            yes_task="fail_timeexport_has_error",
            no_task="timeexport_has_data"
        )
        fail_timeexport_has_error = rail.FailOperator(
            task_id="fail_timeexport_has_error",
            message="{{result('get_time_data_item_batch_result').error}}"
        )

        timeexport_has_data = rail.IfOperator(
            task_id="timeexport_has_data",
            test="{{result('get_time_data_item_batch_result').listData.rows | is_truthy}}",
            yes_task= "get_report_details",
            no_task="delete_this_dagrun"
        )
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.location_report_name
        )

        run_user_location_report_start, run_user_location_report_end = rail.run_report(
            group_id="run_user_location_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )
        timeexport_batch_start, timeexport_batch_end = time_export_task(export_file_name=lambda: rail.result(
            "get_required_details")['export_name'], file_format_uri=lambda: rail.result("get_required_details")['file_format'])

        has_report_failed = rail.IfOperator(
            task_id = "has_report_failed",
            test="{{ result('run_user_location_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task= "report_has_errors",
            no_task= timeexport_batch_start.task_id
        )
        report_has_errors = rail.FailOperator(
            task_id = "report_has_errors",
            message= "{{ result('run_user_location_report.get_report_result').reportGenerationResults[0].error}}"
        )
        export_data = rail.CreateCollectionOperator(
            task_id="export_data",
            name="export_raw_data",
            source="{{result('load_export')}}",
            columns={
                "Employee ID": "employeeid",
                "Country&Work Locations Name": "work_location_name",
                "Entry Date": "entrydate",
                "In Time": "intime",
                "Out Time": "outtime",
                "Hours": "hours",
                "Project Code": "projectcode",
                "Project Name": "projectname",
                "Task Name": "taskname",
                "Task Name (Full Path)": "taskname_fullpath",
                "Project Mill / MPC": "project_mill_mpc",
                "Project Jira": "project_jira",
                "RSSID": "rssid",
                "Time Entry ID": "time_entry_id",
                "Time Off Type Name": "timeoff_type_name"
            }
        )

        can_send_downstream = rail.IfOperator(
            task_id="can_send_downstream",
            test=lambda: rail.result("get_required_details")[
                'can_send_downstream_airflow_var'].lower() == 'true',
            yes_task="load_report_data"
        )
        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_user_location_report.get_report_result').reportGenerationResults[0].payload}}"
        )
        create_report_collection = rail.CreateCollectionOperator(
            task_id="create_report_collection",
            name="report_data",
            source="{{result('load_report_data')}}",
            columns={
                "Employee ID": "employeeid",
                "Country&Work Locations (Current) (Full Path)": "work_location_full_path"
            }
        )
        get_mill_records = rail.QueryCollectionOperator(
            task_id="get_mill_records",
            name = "mill_records",
            query="""SELECT *  FROM export_raw_data WHERE 'mill'== LOWER(project_mill_mpc)
	AND NULLIF (hours, '') IS NOT NULL AND LOWER(projectname) NOT IN ("business operation","absence") AND project_jira != 'Yes'"""
        )

        process_mill_data = rail.PythonOperator(
            task_id="process_mill_data",
            python_callable=lambda: custom_methods.get_processed_data(data=custom_methods.has_data(rail.result("get_mill_records")), database='mill')
        )
        send_export_data_internal_mill = create_send_export_data_internal(
            config, "mill")

        post_data_to_endpoint_mill = rail.HTTPUploadFileOperator(
            task_id="post_data_to_endpoint_mill",
            http_conn_id=config.technicolor_timeexport_to_ceta_endpoint_mill,
            content_type= "application/json",
            content="{{result('process_mill_data')}}",
            retries=0
        )

        get_mpc_records = rail.QueryCollectionOperator(
            task_id="get_mpc_records",
            name = "mpc_records",
            query="""SELECT *  FROM export_raw_data WHERE 'mpc'== LOWER(project_mill_mpc)
	AND NULLIF (hours, '') IS NOT NULL AND LOWER(projectname) NOT IN ("business operation","absence") AND project_jira != 'Yes'"""
        )
        process_mpc_data = rail.PythonOperator(
            task_id="process_mpc_data",
            python_callable=lambda: custom_methods.get_processed_data(data=custom_methods.has_data(rail.result("get_mpc_records")), database='mpc'),
        )

        send_export_data_internal_mpc = create_send_export_data_internal(
            config, "mpc")

        post_data_to_endpoint_mpc = rail.HTTPUploadFileOperator(
            task_id="post_data_to_endpoint_mpc",
            content_type= "application/json",
            http_conn_id=config.technicolor_timeexport_to_ceta_endpoint_mpc,
            content="{{result('process_mpc_data')}}",
            retries=0
        )

        get_skipped_records = rail.QueryCollectionOperator(
            task_id="get_skipped_records",
            name = "skipped_records",
            query="""SELECT * FROM export_raw_data erd WHERE NULLIF(rssid, '') IS NULL"""
        )

        process_skipped_data = rail.DataAdaptorOperator(
            task_id="process_skipped_data",
            source="{{result('get_skipped_records')}}",
            columns=['global_id', 'starttime', 'endtime', 'projectname',
                     'taskname_fullpath', 'hours', 'timeentryid'],
            data=custom_methods.get_skipped_data
        )
        send_export_data_internal_skipped = create_send_export_data_internal(
            config, "skipped")

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule='all_done',
            test="{{result('get_time_data_item_batch_result').listData.rows | is_truthy}}",
            yes_task="log_to_sumo"
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task= "fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id = "fail_dagrun",
            message='{{ get_error_message() }}'
        )

        def custom_len(data):
            if not data:
                return 0
            return len(data)

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda: {
                "time_export_name": rail.result('get_required_details')['export_name'],
                "mill_records": custom_len(rail.result("process_mill_data")),
                "mpc_records": custom_len(rail.result("process_mpc_data")),
                "skipped_records": rail.result("process_skipped_data",'length'),
                # No records, processed, skipped(records present but not posted)
                "status": custom_methods.get_status()
            }
        )

        get_ceta_export_script >> is_script_exists >> rail.Label(
            "No") >> fail_file_format_not_found

        is_script_exists >> rail.Label("Yes") >> [get_all_employee_type_groups, get_all_columns] >> get_required_details >> \
            create_time_data_item_batch >> wait_for_batch_execution_start
        wait_for_batch_execution_end >> get_time_data_item_batch_result >> has_timeexport_errors >> rail.Label(
            "Yes") >> fail_timeexport_has_error
        has_timeexport_errors >> rail.Label(
            "No") >> timeexport_has_data >> rail.Label("Yes") >> get_report_details

        get_report_details >> run_user_location_report_start
        run_user_location_report_end >> has_report_failed >> rail.Label("No") >> timeexport_batch_start
        timeexport_batch_end >> export_data >> can_send_downstream >> rail.Label("Yes") >> load_report_data >> create_report_collection >> [
            get_mill_records, get_mpc_records, get_skipped_records]
        has_report_failed >> rail.Label("Yes") >> report_has_errors
        get_mill_records >> process_mill_data
        get_mpc_records >> process_mpc_data
        get_skipped_records >> process_skipped_data

        process_mill_data >> send_export_data_internal_mill >> post_data_to_endpoint_mill >> can_log_to_sumo
        process_mpc_data >> send_export_data_internal_mpc >> post_data_to_endpoint_mpc >> can_log_to_sumo
        process_skipped_data >> send_export_data_internal_skipped >> can_log_to_sumo

        can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo >> can_fail_dag
        timeexport_has_data >> rail.Label("No") >> delete_this_dagrun

        can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
