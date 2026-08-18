import json
from datetime import timedelta
from pendulum import datetime, now
from repliconinc.timeoff_sync_to_polaris.utils import custom_methods, request_payload
import rail

null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f"Timeoff_Sync_to_Polaris",
        company_key=config.company_key,
        # timezone aware datetime
        start_date=datetime(2025, 1, 1, tz=config.timezone),
        max_active_runs=config.max_active_runs,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        default_args={
            "replicon_conn_id": config.replicon_conn_id_polaris,
        }
    ) as dag:

        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            replicon_conn_id=config.replicon_conn_id_repliconinc,
            report_name=config.report_name_repliconinc,
        )

        load_report = rail.run_report(
            group_id='load_report',
            replicon_conn_id=config.replicon_conn_id_repliconinc,
            report_params=lambda: request_payload.get_run_report_payload()
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{"No Data" not in result("load_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task='report_has_expected_columns',
            no_task='finish_export'
        )

        finish_export = rail.EmptyOperator(
                    task_id='finish_export'
                )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            # pylint: disable=line-too-long
            test="{{ result('load_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.column_order_replicon_report,
            no_task='fail_invalid_report_columns',
            yes_task='report_payload_to_csv',
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{result("load_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id="report_data_collection",
            name="report_data",
            source='{{result("report_payload_to_csv")}}',
            columns = {
                "User Name": "username",
                "User Email": "useremail",
                "Login Name": "loginname",
                "Time Off Date": "timeoffdate",
                "Time Off Hrs": "timeoffhrs",
                "Units": "units",
                "Time Off Type": "timeofftype",
                "Booking Start Date/Time": "bookingstartdatetime",
                "Time Off Comments": "Comment",
                "Approval Status": "approvalstatus",
                "Approval Date": "approvaldate",
                "Time Off Days": "timeoffdays",
                "TimeOffBookingUri": "timeoffbookinguri",
                "Employee ID": "empid",
                "Modified On": "modifiedon",
            }
        )

        data1 = rail.QueryCollectionOperator(
            task_id="data1",
            query='''SELECT 
                        username,
                        useremail,
                        loginname,
                        timeoffdate,
                        timeoffhrs,
                        units,
                        timeofftype,
                        bookingstartdatetime,
                        Comment,
                        approvalstatus,
                        approvaldate,
                        timeoffdays,
                        timeoffbookinguri,
                        empid,
                        modifiedon
                FROM report_data
                WHERE
                    NULLIF(TRIM(username), '') IS NOT NULL AND
                    NULLIF(TRIM(useremail), '') IS NOT NULL AND
                    NULLIF(TRIM(loginname), '') IS NOT NULL AND
                    NULLIF(TRIM(timeoffdate), '') IS NOT NULL AND
                    NULLIF(TRIM(timeoffhrs), '') IS NOT NULL AND
                    NULLIF(TRIM(units), '') IS NOT NULL AND
                    NULLIF(TRIM(bookingstartdatetime), '') IS NOT NULL AND
                    NULLIF(TRIM(approvalstatus), '') IS NOT NULL AND
                    NULLIF(TRIM(timeoffdays), '') IS NOT NULL AND
                    CAST(timeoffhrs AS DECIMAL(10,2)) > 0.000
                '''
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        capture_run_start_time = rail.PythonOperator(
            task_id="capture_run_start_time",
            python_callable=custom_methods.capture_run_start_time
        )

        filter_by_modifiedon = rail.PythonOperator(
            task_id="filter_by_modifiedon",
            python_callable=custom_methods.filter_records_by_modifiedon
        )

        data1_filtered = rail.CreateCollectionOperator(
            task_id="data1_filtered",
            name="data1_filtered",
            source='{{result("filter_by_modifiedon")}}',
            columns={
                "username": "username",
                "useremail": "useremail",
                "loginname": "loginname",
                "timeoffdate": "timeoffdate",
                "timeoffhrs": "timeoffhrs",
                "units": "units",
                "timeofftype": "timeofftype",
                "bookingstartdatetime": "bookingstartdatetime",
                "Comment": "Comment",
                "approvalstatus": "approvalstatus",
                "modifiedon": "modifiedon",
                "timeoffdays": "timeoffdays",
                "timeoffbookinguri": "timeoffbookinguri",
                "empid": "empid",
            }
        )

        get_specific_report_details1 = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details1',
            report_name=config.report_name_polaris,
            replicon_conn_id=config.replicon_conn_id_polaris
        )

        load_report1 = rail.run_report(
            group_id='load_report1',
            replicon_conn_id=config.replicon_conn_id_polaris,
            report_params=request_payload.get_run_report_payload1
        )
        report_has_expected_columns1 = rail.IfOperator(
            task_id="report_has_expected_columns1",
            # pylint: disable=consider-using-f-string
            # pylint: disable=line-too-long
            test="{{ result('load_report1.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.column_order_polaris_report,
            no_task='fail_invalid_report_columns1',
            yes_task='report_payload_to_csv1',
        )

        report_payload_to_csv1 = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv1",
            document='{{result("load_report1.get_report_result").reportGenerationResults[0].payload}}'
        )

        report_data_collection1 = rail.CreateCollectionOperator(
            task_id="report_data_collection1",
            name="report_data1",
            source='{{result("report_payload_to_csv1")}}',
            columns={
                "User Name": "username",
                "User Email": "useremail",
                "Login Name": "loginname",
                "UserUri": "useruri",
                "Employee ID": "empid"
            }

        )

        data2 = rail.QueryCollectionOperator(
            task_id="data2",
            query='''SELECT
                        username,
                        useremail,
                        loginname,
                        useruri,
                        empid
                    FROM report_data1
                    WHERE
                        NULLIF(TRIM(username), '') IS NOT NULL AND
                        NULLIF(TRIM(useremail), '') IS NOT NULL AND
                        NULLIF(TRIM(loginname), '') IS NOT NULL
                    '''
        )

        fail_invalid_report_columns1 = rail.FailOperator(
            task_id="fail_invalid_report_columns1",
            message="Base report column does not match"
        )
        has_data_in_data1 = rail.IfOperator(
            task_id="has_data_in_data1",
            test=lambda: bool(json.loads(rail.result("filter_by_modifiedon"))),
            yes_task="get_enabled_time_off_types",
            no_task="finish"
        )

        get_enabled_time_off_types = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types',
            replicon_conn_id=config.replicon_conn_id_repliconinc,
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        final_data = rail.QueryCollectionOperator(
            task_id="final_data",
            query='''SELECT
                    d1.username,
                    d1.useremail,
                    d1.loginname,
                    d1.timeoffdate,
                    d1.timeoffhrs,
                    d1.units,
                    d1.timeofftype,
                    d1.bookingstartdatetime,
                    d1.Comment,
                    d1.approvalstatus,
                    d1.timeoffdays,
                    d1.timeoffbookinguri,
                    d1.empid,
                    d1.modifiedon,
                    d2.useruri
                FROM data1_filtered AS d1
                LEFT JOIN report_data1 AS d2
                ON d1.empid = d2.empid
                WHERE
                    NULLIF(TRIM(d1.username), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.useremail), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.loginname), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.timeoffdate), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.timeoffhrs), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.units), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.timeofftype), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.bookingstartdatetime), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.approvalstatus), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.modifiedon), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.timeoffdays), '') IS NOT NULL AND
                    CAST(d1.timeoffhrs AS DECIMAL(10,2)) > 0.000 AND
                    NULLIF(TRIM(d2.username), '') IS NOT NULL AND
                    NULLIF(TRIM(d2.useremail), '') IS NOT NULL AND
                    NULLIF(TRIM(d2.loginname), '') IS NOT NULL
                '''
        )

        process_each_timeoffentries_in_polaris = rail.trigger_parallel_dagrun(
            task_id="Process_each_timeoffentries_in_polaris",
            trigger_dag_id=config.push_timeoffentries_to_polaris,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_count,
            items="{{ result('final_data') }}",
            conf=lambda item: {
                "username": item['username'],
                "useruri": item['useruri'],
                "useremail": item['useremail'],
                "loginname": item['loginname'],
                "timeoffdate": item['timeoffdate'],
                "timeoffhrs": item['timeoffhrs'],
                "units": item['units'],
                "timeofftypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'name', item['timeofftype'], 'uri'),
                "timeofftype": item['timeofftype'],
                "bookingstartdatetime": item['bookingstartdatetime'],
                "Comment": item['Comment'],
                "approvalstatus": item['approvalstatus'],
                "modifiedon": item['modifiedon'],
                "timeoffdays": item['timeoffdays'],
                "timeoffbookinguri": item['timeoffbookinguri'],
                "empid": item['empid'],
                "log": rail.result("create_log")
            }
        )
        update_last_run_time = rail.PythonOperator(
            task_id="update_last_run_time",
            python_callable=custom_methods.update_last_run_var
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

    get_specific_report_details >> load_report >> has_data
    has_data >> rail.Label("No") >> finish_export
    has_data >> rail.Label("Yes") >> report_has_expected_columns
    report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_columns
    report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv
    report_payload_to_csv >> report_data_collection >> data1
    [data1, capture_run_start_time] >> filter_by_modifiedon >> data1_filtered
    get_specific_report_details1 >> load_report1 >> report_has_expected_columns1
    report_has_expected_columns1 >> rail.Label("No") >> fail_invalid_report_columns1
    report_has_expected_columns1 >> rail.Label("Yes") >> report_payload_to_csv1 >> report_data_collection1 >> data2
    data1_filtered >> data2 >> has_data_in_data1
    has_data_in_data1 >> rail.Label("Yes") >> get_enabled_time_off_types >> final_data >> process_each_timeoffentries_in_polaris >> update_last_run_time >> finish
    has_data_in_data1 >> rail.Label("No") >> finish
    
    return dag

rail.for_each_instance(create_main_dag)
