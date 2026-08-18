from datetime import datetime as dt, timedelta
from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id = config.project_dates_update,
        description=f"crl process project start and enddates {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        schedule_interval= config.project_dates_schedule,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        get_task_assignment_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_task_assignment_report_details",
            report_name=config.task_assignment_report_name
        )

        def get_filter_payload():
            filter_uri_expr = rail.find_first_by_attr_and_get_attr(rail.result('get_task_assignment_report_details')[
                'filterConfiguration']['enabledFilters'],'displayText', config.task_assignment_udf_filter_name, 'uri')
            return [
                    {
                        "reportFilterUri": filter_uri_expr,
                        "value": None
                    },
                    {
                        "reportFilterUri": filter_uri_expr,
                        "value": (dt.now() - timedelta(days= 1)).strftime("%Y/%m/%d")
                    },
                    {
                        "reportFilterUri": filter_uri_expr,
                        "value": (dt.now() - timedelta(days= 1)).strftime("%Y/%m/%d")
                    },
                ]

        generate_task_assignment_report = rail.run_report2(
            group_id="generate_base_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri":  rail.result('get_task_assignment_report_details')['uri'],
                        "filterValues": get_filter_payload(),
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
            no_task="finish"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_task_assignment_report_columns,
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
            name="task_assignment_report_collection"
        )

        has_collection_data = rail.IfOperator(
            task_id = 'has_collection_data',
            test= '{{ result("create_report_collection", "length") > 0 }}',
            yes_task= 'query_distinct_projects',
            no_task= 'finish'
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id = "query_distinct_projects",
            query="""SELECT DISTINCT Project_Code FROM task_assignment_report_collection""",
            name="distinct_projects"
        )

        process_each_project = rail.trigger_parallel_dagrun(
            task_id="process_each_project",
            items='{{ result("query_distinct_projects") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id= config.project_dates_update_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'project_code': '{{ item.Project_Code }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        get_task_assignment_report_details >> generate_task_assignment_report >> report_has_data >> rail.Label(
            "No") >> finish

        report_has_data >> rail.Label("Yes") >> report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

        report_has_expected_columns >> rail.Label(
            "Yes") >> load_report_data >> create_report_collection >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_distinct_projects >> process_each_project

        has_collection_data >> rail.Label(
            "No") >> finish

        process_each_project >> finish


    return dag

rail.for_each_instance(create_main_dag)
