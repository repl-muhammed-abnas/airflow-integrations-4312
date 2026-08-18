from datetime import timedelta
from pendulum import datetime
from capgemini.france_sellback_leaves_export_v3.utils import custom_methods
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Capgemini France Sell Back Leaves Export Master {config.instance} V3',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2026, 1, 1),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunScheduleOperator(task_id="view_dagrun_schedule")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='logging_details',
            end_task='dagrun_log_to_sumo',
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone, config.filename_prefix]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name='\
                {%- if dag_run.conf | is_truthy and dag_run.conf.adhoc_report_name | is_truthy -%} \
                    {{ dag_run.conf.adhoc_report_name }} \
                {%- else -%}'
                    + config.report_name +
                '{%- endif -%}'
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda dag_run: custom_methods.get_report_parameters(dag_run, config.time_zone),
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
            no_task='trigger_sellback_export'
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
            document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=','
        )

        create_sellback_leaves_collection = rail.CreateCollectionOperator(
            task_id='create_sellback_leaves_collection',
            source='{{ result("load_csv") }}',
            columns={
                "Employee ID": "employeeid",
                "User Name": "username",
                "UserUri": "useruri",
                "Time Off Type": "timeofftype",
                "Units": "units",
                "Date": "date",
                "Event Type": "eventtype",
                "Amount": "amount"
            },
            name="sellback_leaves_data"
        )

        query_valid_empid_data = rail.QueryCollectionOperator(
            task_id='query_valid_empid_data',
            query="SELECT * FROM sellback_leaves_data WHERE eventtype = 'Sell Back' AND NULLIF(employeeid, '') IS NOT NULL",
            name='valid_sellbacks_data'
        )

        sl_and_pm_timeoff_types = rail.PythonOperator(
            task_id='sl_and_pm_timeoff_types',
            python_callable=custom_methods.get_sl_and_pm_timeoff_types,
            op_args=[config.codes_to_export_mapper]
        )

        trigger_sellback_export = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_sellback_export',
            items=["SL", "PM"],
            trigger_dag_id=config.export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "adj_type": item,
                "export_filename": rail.result("logging_details")["sl_export_filename"] if item == "SL"
                    else rail.result("logging_details")["pm_export_filename"],
                "timeoff_types": (rail.result("sl_and_pm_timeoff_types")["sl_timeoff_types"] if item == "SL"
                    else rail.result("sl_and_pm_timeoff_types")["pm_timeoff_types"])
                        if rail.result("sl_and_pm_timeoff_types") else null,
                "has_data": "yes" if rail.result("query_valid_empid_data") and
                    rail.result("query_valid_empid_data", key="length") > 0 else "no"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> logging_details

        logging_details >> get_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> trigger_sellback_export

        is_report_has_expected_columns >> rail.Label("Yes") >> process_report_data >> load_csv

        load_csv >> create_sellback_leaves_collection >> query_valid_empid_data \
            >> sl_and_pm_timeoff_types >> trigger_sellback_export >> dagrun_log_to_sumo

        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_dag)
