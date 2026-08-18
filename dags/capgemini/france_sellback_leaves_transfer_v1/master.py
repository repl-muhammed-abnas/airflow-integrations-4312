from datetime import timedelta
import itertools
from pendulum import datetime
from capgemini.france_sellback_leaves_transfer_v1.utils import request_payload
from capgemini.france_sellback_leaves_transfer_v1.tasks.send_logs import get_send_logs
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Capgemini France Sellback Leaves Transfer Master {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 12, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
    ) as dag:

        rail.ViewDagRunScheduleOperator(task_id="view_dagrun_schedule")

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda dag_run: request_payload.get_report_parameters(dag_run, config.time_zone),
            target='artifact'
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test='{{ (result("run_report.get_report_result") | load_json_artifact).reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('run_report.get_report_result', 'has_data') }}",
            yes_task='is_report_has_expected_columns',
            no_task='send_empty_data_email'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='load_csv',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
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

        query_sellback_transfer_leaves = rail.QueryCollectionOperator(
            task_id='query_sellback_transfer_leaves',
            query="SELECT * FROM sellback_leaves_data WHERE eventtype = 'Sell Back'"
        )

        is_sellback_leaves_exists = rail.IfOperator(
            task_id='is_sellback_leaves_exists',
            test='{{ result("query_sellback_transfer_leaves", "length") > 0 }}',
            yes_task='get_all_timeoffs_script_uris',
            no_task='send_empty_data_email'
        )

        send_empty_data_email = rail.EmailOperator(
            task_id="send_empty_data_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon Sell Back Leaves Transfer for France'
                + ' - No records to export {{ " - " + current_time_in_specified_tz("' + config.time_zone +'") }}',
            html_content="/templates/emails/no_data.html",
            params={
                "time_zone": config.time_zone
            }
        )

        get_all_timeoffs_script_uris = rail.RepliconServiceOperator(
            task_id='get_all_timeoffs_script_uris',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts"
        )

        trigger_assign_policy_to_user = rail.trigger_parallel_dagrun(
            task_id='trigger_assign_policy_to_user',
            items='{{ result("query_sellback_transfer_leaves") }}',
            parallel_count=config.assign_policy_parallel_count,
            trigger_dag_id=config.assign_policy_child_dagid,
            conf=lambda item: {
                "sellback_leaves_details": item,
                "transfer_bal_to_timeoff": config.timeoff_types_mapper[item["timeofftype"]],
                "starting_balance_set_to_script_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_timeoffs_script_uris"), 'displayText', 'Starting Balance Set To', 'uri', ''),
                "yearly_reset_script_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_timeoffs_script_uris"), 'displayText', 'Yearly Reset', 'uri', '')
            },
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_assign_policy_dag_ids =rail.PythonOperator(
            task_id= 'get_assign_policy_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'trigger_assign_policy_to_user_{x+1}') if rail.result(
                    f'trigger_assign_policy_to_user_{x+1}') else []), range(config.assign_policy_parallel_count))))),
            show_return_value_in_logs= False
        )

        gather_policy_assignment_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_policy_assignment_logs',
            dag_runs='{{ result("get_assign_policy_dag_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        send_logs_enter, send_logs_exit = get_send_logs(config)

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info={
                "total_processed_records": '{{ result("create_sellback_leaves_collection", "length") }}',
                "success_records": '{{ result("format_logs", "success_record_count") }}',
                "exception_records": '{{ result("format_logs", "exception_record_count") }}',
                "failed_records": '{{ result("format_logs", "error_record_count") }}',
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_sellback_transfer'
        )

        fail_sellback_transfer = rail.FailOperator(
            task_id='fail_sellback_transfer',
            message='{{ get_error_message() }}'
        )

        get_report_details>> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> send_empty_data_email

        is_report_has_expected_columns >> rail.Label("Yes") >> load_csv \
            >> create_sellback_leaves_collection >> query_sellback_transfer_leaves >> is_sellback_leaves_exists
        is_sellback_leaves_exists >> rail.Label("Yes") >> get_all_timeoffs_script_uris
        is_sellback_leaves_exists >> rail.Label("No") >> send_empty_data_email

        get_all_timeoffs_script_uris >> trigger_assign_policy_to_user >> get_assign_policy_dag_ids \
            >> gather_policy_assignment_logs >> send_logs_enter

        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo

        send_empty_data_email >> dagrun_log_to_sumo
        send_logs_exit >> dagrun_log_to_sumo
        dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_sellback_transfer

    return dag

rail.for_each_instance(create_dag)
