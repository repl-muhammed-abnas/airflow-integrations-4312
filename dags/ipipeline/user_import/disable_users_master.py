from datetime import datetime as dt, timedelta

from pendulum import datetime
from ipipeline.user_import.utils import request_payload
from airflow.models import Variable
import rail

def create_disable_user_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_users_master_dag_id,
        description="iPipeline Disable User Master DAG - Disable users in Replicon",
        start_date=datetime(2025, 8, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_disable_master,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_master_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_report_details"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_report_details",
            end_task="finish"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.user_details_report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_parameters,
            target='artifact'
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='finish'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='process_report_data',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        process_report_data = rail.EmptyOperator(
            task_id='process_report_data'
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        create_users_data_collection = rail.CreateCollectionOperator(
            task_id='create_users_data_collection',
            source='{{ result("load_csv") }}',
            columns={
                "User Name": "username",
                "Employee ID": "employeeid",
                "UserUri": "useruri",
                "Login Name": "loginname",
                "User Email": "useremail",
                "User End Date": "userenddate",
                'User Status': 'status',
                'Day Diff': 'daydiff'
            },
            name="users_data"
        )

        query_users_to_disable = rail.QueryCollectionOperator(
            task_id='query_users_to_disable',
            query=f"""SELECT * FROM users_data WHERE NULLIF(userenddate, "") IS NOT NULL
                    AND daydiff < 0 AND status = 'Enabled'"""
        )

        trigger_disable_user_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_disable_user_child',
            trigger_dag_id=config.disable_users_child_dag_id,
            items='{{ result("query_users_to_disable") }}',
            conf=lambda item: {
                "employeeid": item["employeeid"],
                "useruri": item["useruri"],
                "userenddate": dt.strptime(item["userenddate"], "%b %d, %Y").strftime("%m/%d/%Y"),
                "loginname": item["loginname"]
            }
        )

        wait_for_disable_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_user_child',
            dag_runs='{{ result("trigger_disable_user_child") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_disable_user_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disable_user_errors',
            dag_runs="{{ result('trigger_disable_user_child') }}",
            dagrun_task_id='catch_disable_user_error',
            flatten=True
        )

        is_disable_user_error = rail.IfOperator(
            task_id='is_disable_user_error',
            test="{{ result('gather_disable_user_errors') | map_to_attr('useruri') | length > 0 }}",
            yes_task='fail_disable_user_error',
            no_task='finish'
        )

        fail_disable_user_error = rail.FailOperator(
            task_id='fail_disable_user_error',
            message='Errors noticed while disabling few users'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> get_report_details

        get_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> finish
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> finish

        is_report_has_expected_columns >> rail.Label("Yes") >> process_report_data >> load_csv \
            >> create_users_data_collection >> query_users_to_disable >> trigger_disable_user_child \
                >> wait_for_disable_user_child >> gather_disable_user_errors >> is_disable_user_error
        
        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> finish
        
        is_disable_user_error >> rail.Label("Yes") >> fail_disable_user_error
        is_disable_user_error >> rail.Label("No") >> finish

        return dag

rail.for_each_instance(create_disable_user_master_dag)
