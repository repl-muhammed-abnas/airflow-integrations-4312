from datetime import timedelta
import itertools
from pendulum import datetime
import rail
from dxctechnology.workday_user_import.disable_user.utils.request_payload \
    import disable_users_report_generation_params, get_process_disable_user_conf, get_trigger_id
from dxctechnology.workday_user_import.disable_user.utils.data_handler \
    import get_starting_balance_script_data_handler, get_prevent_balance_overdraw_script_data_handler


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_user_master_dag_id,
        description="dxctechnology workday user sync disable users master",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2024, 2, 1),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_run_master
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.report_name
        )

        run_report_start = rail.run_report2(
            group_id="run_disable_report",
            report_params=disable_users_report_generation_params
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_disable_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_disable_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_disable_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task="fail_no_data_in_report"
        )

        fail_no_data_in_report = rail.FailOperator(
            task_id="fail_no_data_in_report",
            message="No Data for Contractors"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_disable_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_starting_balance_script = rail.RepliconServiceOperator(
            task_id="get_starting_balance_script",
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=get_starting_balance_script_data_handler
        )

        get_prevent_balance_overdraw_script = rail.RepliconServiceOperator(
            task_id="get_prevent_balance_overdraw_script",
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=get_prevent_balance_overdraw_script_data_handler
        )

        create_report_data_collection = rail.CreateCollectionOperator(
            task_id="create_report_data_collection",
            source="{{ result('load_report_data') }}",
            columns={
                'User Name': 'user_name',
                'User Status': 'user_status',
                'employeetype': 'employee_type',
                'UserUri': 'user_uri',
                'User End Date': 'user_end_date',
                'daydiff': 'day_diff',
                'Location (Current) (Full Path)': 'current_location',
                'Company Code (Current) (Full Path)': 'current_company_code',
                'Login Name': 'login_name'
            },
            name="report_user_data"
        )

        query_users_to_disable = rail.QueryCollectionOperator(
            task_id="query_users_to_disable",
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id, rud.*
                    FROM report_user_data rud
                    WHERE NULLIF(rud.user_end_date, '') IS NOT NULL 
                    AND CAST(rud.day_diff as decimal) < 0 
                    AND rud.user_status='Enabled' 
                    AND rud.employee_type != "Agency Contractor"
                    AND rud.employee_type != "SOW Contractor"
                    AND rud.employee_type != "Contractor" """,
            name="user_to_disable"
        )

        has_any_user_to_disable = rail.IfOperator(
            task_id="has_any_user_to_disable",
            test="{{result('query_users_to_disable', 'length') > 0}}",
            yes_task="dummy_process_disable_user"
        )

        dummy_process_disable_user = rail.EmptyOperator(
            task_id="dummy_process_disable_user"
        )

        process_disable_user = rail.trigger_parallel_dagrun(
            task_id="process_disable_user",
            items="{{ result('query_users_to_disable') }}",
            trigger_dag_id=lambda item: get_trigger_id(config, item),
            parallel_count=config.parallel_dag_run_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_process_disable_user_conf
        )

        dummy_process_disable_user_complete = rail.EmptyOperator(
            task_id="dummy_process_disable_user_complete"
        )

        get_dag_run_ids = rail.PythonOperator(
            task_id="get_dag_run_ids",
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_disable_user_{x+1}') if rail.result(
                    f'process_disable_user_{x+1}') else []), range(config.parallel_dag_run_count)))))
        )

        gather_failures = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_failures",
            dag_runs="{{result('get_dag_run_ids')}}",
            dagrun_task_id="catch_errors",
            flatten=True
        )

        gather_failures2 = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_failures2",
            dag_runs="{{result('get_dag_run_ids')}}",
            dagrun_task_id="gather_failures",
            flatten=True
        )

        has_any_failures = rail.IfOperator(
            task_id="has_any_failures",
            test=lambda: bool(rail.result("gather_failures")) or bool(
                rail.result("gather_failures2")),
            yes_task="fail_disable_run"
        )

        fail_disable_run = rail.FailOperator(
            task_id="fail_disable_run",
            message="""{{result("gather_failures") or result("gather_failures2")}}"""
        )

        get_report_details >> run_report_start
        run_report_start >> is_report_failed >> rail.Label(
            "No") >> report_has_data >> rail.Label("Yes") >> load_report_data
        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        report_has_data >> rail.Label("No") >> fail_no_data_in_report

        load_report_data >> get_starting_balance_script >> get_prevent_balance_overdraw_script >> create_report_data_collection \
            >> query_users_to_disable >> has_any_user_to_disable >> rail.Label("Yes") >> dummy_process_disable_user

        dummy_process_disable_user >> process_disable_user >> dummy_process_disable_user_complete >> get_dag_run_ids \
            >> gather_failures >> gather_failures2 >> has_any_failures >> rail.Label("Yes") >> fail_disable_run

    return dag


rail.for_each_instance(create_main_dag)
