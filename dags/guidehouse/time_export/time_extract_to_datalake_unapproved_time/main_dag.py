from datetime import timedelta
from guidehouse.time_export.time_extract_to_datalake_unapproved_time.utils import custom_methods, request_payload, response_filter

import rail
import pendulum


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"GuideHouse Unapproved Time Export to Datalake {config.country} - Master DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2026, 5, 5, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master,
        default_args={
            "execution_timeout": timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        get_todays_date = rail.PythonOperator(
            task_id="get_current_date",
            python_callable=lambda dag_run: dag_run.conf.get("date") if dag_run.conf.get("date") else pendulum.now(config.timezone).to_date_string(),
        )

        get_current_prior_and_history_week_date_range = rail.PythonOperator(
            task_id="get_current_prior_and_history_week_date_range",
            python_callable=lambda: custom_methods.get_current_prior_and_history_week_date_range(
                rail.result("get_current_date")
            ),
        )

        create_company_code_date_range_pairs = rail.PythonOperator(
            task_id="create_company_code_date_range_pairs",
            python_callable=lambda: custom_methods.get_company_code_date_range_pairs(
                rail.result("get_current_prior_and_history_week_date_range"),
                config.COMPANY_CODE,
            ),
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id="get_all_reports",
            endpoint="/services/ReportService1.svc/GetAllReports",
        )

        get_unapproved_time_extract_report_uri = rail.PythonOperator(
            task_id="get_unapproved_time_extract_report_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_reports"), "displayText", config.REPLICON_UNAPPROVED_TIME_REPORT_NAME, "uri"
            )
        )

        if_report_not_present = rail.IfOperator(
            task_id="if_report_not_present",
            test=lambda: not rail.result("get_unapproved_time_extract_report_uri"),
            yes_task="fail_with_report_not_found_error",
            no_task="get_unapproved_time_extract_report_details",
        )

        fail_with_report_not_found_error = rail.FailOperator(
            task_id="fail_with_report_not_found_error",
            message=f'Report "{config.REPLICON_UNAPPROVED_TIME_REPORT_NAME}" not found in Replicon.',
        )

        get_unapproved_time_extract_report_details = rail.RepliconServiceOperator(
            task_id="get_unapproved_time_extract_report_details",
            endpoint="/services/ReportService1.svc/GetReportDetails2",
            data=lambda: {
                "reportUri": rail.result("get_unapproved_time_extract_report_uri")
            }
        )

        get_unapproved_time_extract_report_filter_uri = rail.PythonOperator(
            task_id="get_unapproved_time_extract_report_filter_uri",
            python_callable=lambda: custom_methods.get_unapproved_time_extract_report_filter_uri(
                report_details=rail.result("get_unapproved_time_extract_report_details")
            )
        )

        get_all_level_1_location = rail.RepliconServiceOperator(
            task_id="get_all_level_1_location",
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=lambda: request_payload.get_level_1_locations_payload(),
        )

        get_level_1_location_code_pair = rail.PythonOperator(
            task_id="get_level_1_location_code_pair",
            python_callable=lambda: response_filter.get_location_name_and_code_pairs(
                response=rail.result("get_all_level_1_location")
            )
        )

        get_usa_location_uri = rail.PythonOperator(
            task_id="get_usa_location_uri",
            python_callable=lambda: response_filter.get_usa_location_uri(
                rail.result("get_all_level_1_location")
            )
        )

        get_usa_level_1_location_code_pair = rail.RepliconServiceOperator(
            task_id="get_usa_level_1_location_code_pair",
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=lambda: request_payload.get_level_1_locations_payload(
                parent_location_uri=rail.result("get_usa_location_uri")
            ),
            data_handler=lambda response: response_filter.get_location_name_and_code_pairs(response)
        )

        # Cost Center URI is needed in the report filter
        get_all_cost_centers = rail.RepliconServicePageOperator(
            task_id="get_all_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetHierarchyData",
            data=request_payload.get_all_cost_centers_payload,
            page_handler=custom_methods.page_handler,
            all_result_data_handler=response_filter.extract_cost_centers
        )

        get_level_1_cost_center_name_and_uri_pairs = rail.PythonOperator(
            task_id="get_level_1_cost_center_name_and_uri_pairs",
            python_callable=lambda: custom_methods.get_level_1_cost_center_name_and_uri_pairs(
                cost_centers=rail.result("get_all_cost_centers"), hierarchy_level=0
            )
        )


        trigger_process_time_extract_child = rail.trigger_parallel_dagrun(
            task_id="trigger_process_time_extract_child",
            trigger_dag_id=config.process_time_extract_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result("create_company_code_date_range_pairs"),
            parallel_count=config.parallel_count,
            conf=lambda item: {
                **item,
                "report_uri": rail.result("get_unapproved_time_extract_report_uri"),
                "report_filter_uri": rail.result("get_unapproved_time_extract_report_filter_uri"),
                "level_1_location_code_pairs": rail.result("get_level_1_location_code_pair"),
                "usa_level_1_location_code_pair": rail.result("get_usa_level_1_location_code_pair"),
                "level_1_cost_center_name_and_uri_pairs": rail.result("get_level_1_cost_center_name_and_uri_pairs"),
                "timezone": config.timezone
            },
        )

        get_todays_date >> get_current_prior_and_history_week_date_range >> create_company_code_date_range_pairs >>\
            get_all_reports >> get_unapproved_time_extract_report_uri >> if_report_not_present
        if_report_not_present >> rail.Label("Yes") >> fail_with_report_not_found_error
        if_report_not_present >> rail.Label("No") >> get_unapproved_time_extract_report_details >> get_unapproved_time_extract_report_filter_uri >>\
        get_all_level_1_location >> get_level_1_location_code_pair >> get_usa_location_uri >> get_usa_level_1_location_code_pair >> get_all_cost_centers >> get_level_1_cost_center_name_and_uri_pairs >> trigger_process_time_extract_child

    return dag

rail.for_each_instance(create_master_dag)