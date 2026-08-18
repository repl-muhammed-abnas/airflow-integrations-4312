from datetime import timedelta
from datetime import datetime as dt

from pendulum import datetime as pdt
from wipro.user_import_spain_v3.utils import custom_methods
from wipro.user_import_spain_v3.utils import request_payload
import rail
null = None


def create_airflow_master_dag(config):
    # cntry = config.country.lower().replace(" ", "_")
    with rail.create_airflow_dag(
        dag_id=f"wipro_terminationbalance_user_{config.instance}_v3",
        description="spain termination balance user",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.disable_schedule_interval,
        start_date=pdt(2023, 12, 18, tz=config.time_zone),
        max_active_runs=config.master_max_active_run,
    ) as dag:

        create_spain_terminationbalance_user_log = rail.CreateLogOperator(
            task_id=f"create_spain_disable_user_log"
        )
        
        
        integration_run_date = rail.PythonOperator(
            task_id="integration_run_date",
            python_callable=lambda: custom_methods.get_integration_run_date(
            config)
        )


        get_country_uri = rail.RepliconServiceOperator(
            task_id="get_country_uri",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                config.country,
                "uri"
            )
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_report_details,
        )

        run_report_start, run_report_end = rail.run_report(
            group_id="terminationbalance_user_report",
            report_params=custom_methods.get_report_params
        )

        report_end = rail.EmptyOperator(task_id="report_end")

        if_error_in_report_run = rail.IfOperator(
            task_id="if_error_in_report_run",
            test="{{result('terminationbalance_user_report.get_report_result')['reportGenerationResults'][0].error | is_truthy}}",
            yes_task="fail_report_run",
            no_task="report_has_data"
        )

        fail_report_run = rail.FailOperator(
            task_id="fail_report_run",
            message="Base report run error"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test="{{ result('terminationbalance_user_report.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task="fail_no_data"
        )

        fail_no_data = rail.FailOperator(
            task_id="fail_no_data",
            message="Base report has no data"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id='report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('terminationbalance_user_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='load_report_data',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id = 'load_report_data',
            document = "{{ result('terminationbalance_user_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=["user_name","user_first_name",
                     "user_last_name","user_end_date","employee_id","login_name","country","onsite_direct_recruit","onsite_end_date","user_status","user_uri","esp_annual_termination_policy","user_start_date",
                     "onsite_start_date","acquired_company","company_code"]
        )

        create_all_user_collection = rail.CreateCollectionOperator(
            task_id="create_all_user_collection",
            source='{{result("load_report_data")}}',
            name="all_user_records"
        )

        query_all_users_with_enddate = rail.QueryCollectionOperator(
            task_id="query_all_users_with_enddate",
            query="""SELECT * 
                FROM all_user_records 
                WHERE
                NULLIF(employee_id, "") IS NOT NULL 
                AND user_status = 'Enabled' 
                AND (
                    (onsite_direct_recruit = 'ASSIGNEE' 
                    AND NULLIF(onsite_end_date, "") IS NOT NULL 
                    AND REPLACE(TRIM(onsite_end_date), '/', '-') = '{{ result("integration_run_date")["date"] }}')
                    OR
                    (onsite_direct_recruit = 'LOCAL_HIRE'
                    AND NULLIF(user_end_date, "") IS NOT NULL
                    AND REPLACE(TRIM(user_end_date), '/', '-') = '{{ result("integration_run_date")["date"] }}')
                )""",
                name="users_with_enddate"
        )
        
        get_all_active_users_with_enddate = rail.PythonOperator(
            task_id="get_all_active_users_with_enddate",
            python_callable=custom_methods.get_all_users_with_enddate_data
        )

        is_user_with_enddate_present = rail.IfOperator(
            task_id="is_user_with_enddate_present",
            test='{{result("query_all_users_with_enddate", "length") > 0}}',
            yes_task="get_all_enabled_timeoff_types",
            no_task="end_disable_user"
        )

        end_terminationbalance_for_user = rail.EmptyOperator(task_id="end_disable_user")
        
        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_all_enabled_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                "ESP - Vacaciones anuales (Annual leave)",
                "uri"
            )
        )
        
        get_termination_oef_field = rail.RepliconServiceOperator(
            task_id="get_termination_oef_field",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:user"},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,"name", config.OEF_TERMINATION_FIELD_NAME, "uri")
        )

        get_termination_processed_tag = rail.RepliconServiceOperator(
            task_id="get_termination_processed_tag",
            endpoint="/services/ObjectExtensionTagListService1.svc/GetData",
            data=lambda: {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": rail.result("get_termination_oef_field"),
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null,
                        "numberRange": null
                    },
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
                },
        )


        trigger_child_dag = rail.trigger_parallel_dagrun(
            task_id="for_each_user_termination_process",
            trigger_dag_id=f"wipro_terminationbalance_user_child_{config.instance}_v3",
            items='{{result("get_all_active_users_with_enddate")|to_json}}',
            execution_timeout=timedelta(config.execution_timeout),
            parallel_count=config.max_active_run_child,
            conf=lambda item:{**item, 
                "annual_accrual_uri": rail.result("get_all_enabled_timeoff_types"),
                "get_termination_processed_tag": rail.result("get_termination_processed_tag"),
                "disable_log": rail.result("create_spain_disable_user_log")
                }
        )

        process_logs_for_spain = rail.TriggerDagRunOperator(
            task_id="process_logs_for_spain",
            trigger_dag_id=config.log_schedule_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run:{
                "parent_run_id":dag_run.id,
                "disable_user": True,
                "disable_log": rail.result("create_spain_disable_user_log")
            }
        )
       

        create_spain_terminationbalance_user_log >>\
        integration_run_date >> get_country_uri >> get_user_report_details >>\
        run_report_start >>\
        run_report_end>> report_end >>\
        if_error_in_report_run >> rail.Label("Yes") >> fail_report_run
        if_error_in_report_run >> rail.Label("No") >>\
        report_has_data >> rail.Label("No") >> fail_no_data
        report_has_data >> rail.Label("Yes") >>\
        report_has_expected_columns >> rail.Label("No") >>\
        fail_no_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >>\
        load_report_data >> create_all_user_collection >>\
        query_all_users_with_enddate >> get_all_active_users_with_enddate >>\
        is_user_with_enddate_present
        is_user_with_enddate_present >> rail.Label("No") >> end_terminationbalance_for_user
        is_user_with_enddate_present >> rail.Label("Yes")>> get_enabled_timeoff_types >>\
        get_termination_oef_field >> get_termination_processed_tag >>\
        trigger_child_dag >> process_logs_for_spain
        
    return dag


rail.for_each_instance(create_airflow_master_dag)
