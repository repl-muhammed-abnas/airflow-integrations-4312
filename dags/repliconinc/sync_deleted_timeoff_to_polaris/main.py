from datetime import timedelta
from pendulum import datetime, now
from repliconinc.sync_deleted_timeoff_to_polaris.utils import request_payload
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f"Deleted_Timeoff_Sync_to_Polaris",
        company_key=config.company_key,
        start_date=datetime(2025, 1, 1, tz=config.timezone),
        max_active_runs=config.max_active_runs,
        multi_tenant=True,
        schedule_interval=config.schedule_interval,
        default_args={
            "replicon_conn_id": config.replicon_conn_id_repliconinc,
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
            report_params=lambda: request_payload.get_run_report_payload(config.instance)
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ "No Data" not in result("load_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task='report_has_expected_columns',
            no_task='finish_export'
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
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
                "Login Name": "loginname",
                "Current Start Date": "currentstartdate",
                "Action": "action",
                "Modified By": "modifiedby",
                "Modified On": "modifiedon",
                "Field": "field",
                "Original Value": "originalvalue",
                "New Value": "newvalue",
                "Department (Current)": "department",
                "departmentcheck": "departmentcheck",
                "TimeOffId": "timeoffid",
                "Employee ID": "empid"
            }
        )

        data1 = rail.QueryCollectionOperator(
            task_id="data1",
            query='''SELECT 
                        username,
                        loginname,
                        currentstartdate,
                        action,
                        modifiedby,
                        modifiedon,
                        field,
                        originalvalue,
                        newvalue,
                        department,
                        departmentcheck,
                        timeoffid,
                        empid
                FROM report_data
                WHERE
                    action = 'Deleted'
                '''
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
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
                        NULLIF(TRIM(useruri), '') IS NOT NULL AND
                        NULLIF(TRIM(loginname), '') IS NOT NULL
                    '''
        )

        fail_invalid_report_columns1 = rail.FailOperator(
            task_id="fail_invalid_report_columns1",
            message="Base report column does not match"
        )
        
        has_data_in_data1 = rail.IfOperator(
            task_id="has_data_in_data1",
            test=lambda: bool(rail.result("data1")),
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
                    d1.loginname,
                    d1.currentstartdate,
                    d1.action,
                    d1.modifiedby,
                    d1.modifiedon,
                    d1.field,
                    d1.originalvalue,
                    d1.newvalue,
                    d1.department,
                    d1.departmentcheck,
                    d1.timeoffid,
                    d1.empid,
                    d2.useruri
                FROM data1 AS d1
                LEFT JOIN data2 AS d2
                ON d1.empid = d2.empid
                WHERE
                    NULLIF(TRIM(d1.username), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.loginname), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.action), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.departmentcheck), '') IS NOT NULL AND
                    NULLIF(TRIM(d1.timeoffid), '') IS NOT NULL AND
                    NULLIF(TRIM(d2.useruri), '') IS NOT NULL
                '''
        )

        final_data_filtered = rail.QueryCollectionOperator(
            task_id="final_data_filtered",
            query='''SELECT DISTINCT
                    username,
                    loginname,
                    currentstartdate,
                    action,
                    modifiedby,
                    modifiedon,
                    field,
                    originalvalue,
                    newvalue,
                    department,
                    departmentcheck,
                    timeoffid,
                    empid,
                    useruri
                FROM final_data
                WHERE
                    departmentcheck = 'True'
                    AND action = 'Deleted'
                    AND field = 'Time Off Type'
                    AND NULLIF(TRIM(originalvalue), '') IS NOT NULL
            '''
        )

        process_each_timeentries_in_polaris = rail.trigger_parallel_dagrun(
            task_id="process_each_timeentries_in_polaris",
            trigger_dag_id=config.delete_timeoff_entries_from_polaris_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.parallel_count,
            items="{{ result('final_data_filtered') }}",
            conf=lambda item: {
                "username": item['username'],
                "useruri": item.get('useruri'),
                "loginname": item['loginname'],
                "timeoffid": item["timeoffid"],
                "currentstartdate": item["currentstartdate"],
                "action": item["action"],
                "field": item["field"],
                "timeofftypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'name', item['originalvalue'], 'uri'),
                "originalvalue": item["originalvalue"],
                "newvalue": item["newvalue"],
                "modifiedby": item["modifiedby"],
                "modifiedon": item["modifiedon"],
                "department": item.get("department"),
                "departmentcheck": item.get("departmentcheck"),
                "empid": item.get("empid"),
            }
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
    get_specific_report_details1 >> load_report1 >> report_has_expected_columns1
    report_has_expected_columns1 >> rail.Label("No") >> fail_invalid_report_columns1
    report_has_expected_columns1 >> rail.Label("Yes") >> report_payload_to_csv1 >> report_data_collection1 >> data2
    data1 >> data2 >> has_data_in_data1
    has_data_in_data1 >> rail.Label("Yes") >> get_enabled_time_off_types >> final_data >> final_data_filtered >> process_each_timeentries_in_polaris >> finish
    has_data_in_data1 >> rail.Label("No") >> finish
    return dag


rail.for_each_instance(create_main_dag)