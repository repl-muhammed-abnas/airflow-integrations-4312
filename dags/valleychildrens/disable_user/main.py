from datetime import timedelta
import rail
from pendulum import datetime
from valleychildrens.disable_user.utils.custom_methods import logging_details


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'valleychildrens_disbale_users_master_{config.instance}',
        description='Valleychildrens Disable Users Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.time_zone]
        )

        get_hourly_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_hourly_report_details',
            report_name=config.extract_user_report,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_hourly_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("get_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('get_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('get_report_details.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='no_data',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}"
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            source="{{ result('load_report_data') }}",
            name="user_data_table"
        )

        query_valid_input_records = rail.QueryCollectionOperator(
            task_id='query_valid_input_records',
            query="""SELECT * FROM user_data_table WHERE CAST(DayDiff AS INT) < -1 and Login_Name !='admin'"""
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('query_valid_input_records', 'length') > 0 }}",
            yes_task='process_records',
            no_task='no_records'
        )

        no_records = rail.EmptyOperator(
            task_id="no_records"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        process_disable_user_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_disable_user_records",
            items="{{result('query_valid_input_records')}}",
            trigger_dag_id=f"valleychildrens_disable_users_child_{config.instance}",
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_disable_user_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_disable_user_records",
            dag_runs="{{result('process_disable_user_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_disable_user_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disable_user_errors',
            dag_runs="{{ result('process_disable_user_records') }}",
            dagrun_task_id='catch_disable_user_error',
            flatten=True
        )

        is_disable_user_error = rail.IfOperator(
            task_id='is_disable_user_error',
            test="{{ result('gather_disable_user_errors') | length > 0 }}",
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

        get_logging_details >> get_hourly_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data\
            >> create_user_collection >> query_valid_input_records >> has_any_records

        has_any_records >> process_records >> process_disable_user_records >> wait_process_disable_user_records\
           >> gather_disable_user_errors >> is_disable_user_error

        is_disable_user_error >> rail.Label(
            'Yes') >> fail_disable_user_error

        is_disable_user_error >> rail.Label(
            'No') >> finish

        has_any_records >> rail.Label("No") >> no_records

        report_has_data >> rail.Label("No") >> no_data

    return dag


rail.for_each_instance(create_main_airflow_dag)
