# pylint: disable=line-too-long wildcard-import unused-wildcard-import, too-many-statements line-too-long
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pendulum
import rail
from cie_darkmattertechnologies.ts_submit_utility.utils import data_formatting, request_payload


def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    location = f'{config.location}_' if config.location else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_timesheet_submission_{location}master_v2{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}timehseet_submission_Master{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # runs at everytime as per configure minutes
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_master_run,
        default_args={
        },
    ) as dag:
        

        get_all_variables = rail.PythonOperator(
            task_id='get_all_variables',
            python_callable=request_payload.get_environment_variables,
            op_args=[config]
        )
        
        config_params = request_payload.get_environment_variables(config)
        date_format = config_params.get("date_format", "%m/%d/%Y")
        execution_timeout_days = config_params.get("execution_timeout_days", 14)
    
        entry_report_name = config.te_report_name
        timesheet_report_name = config.ts_report_name
        period_in_months = config_params.get("period_in_months", 3)
        now = pendulum.now(config.timezone)
        startDate = (now - relativedelta(months=period_in_months)
                     ).strftime(date_format)

        endDate = now.strftime(date_format)
        

        get_entry_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_entry_report_details',
            report_name=entry_report_name,
        )

        def entry_approval_filter_uri(filter_name):
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_entry_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

        def entry_date_filter_uri(filter_name):
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_entry_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

        get_report_approval_filter_uri = rail.PythonOperator(
            task_id='get_report_approval_filter_uri',
            python_callable=entry_approval_filter_uri,
            op_args=["TimeEntryStatusFilter"]
        )
        get_report_filter = rail.PythonOperator(
            task_id='get_report_filter',
            python_callable=entry_date_filter_uri,
            op_args=["TimesheetPeriodFilter"]
        )
        run_report_for_entry = rail.run_report2(
            group_id='run_report_for_entry',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_entry_report_details').get('uri')}}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": startDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_report_filter')}}",
                                "value": endDate,
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        has_report_entry_data = rail.IfOperator(
            task_id='has_report_entry_data',
            test="{{ result('run_report_for_entry.get_report_result','has_data')}}",
            yes_task='get_timesheet_report_details',
            no_task='finish'
        )

        
        get_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_report_details',
            report_name=timesheet_report_name,
        )

        def timesheet_date_filter_uri(filter_name):
            return rail.find_first_by_attr_and_get_attr(
                rail.result('get_timesheet_report_details')['filterConfiguration']['enabledFilters'], 'displayText', filter_name, 'uri')

        get_timesheet_status_filter_uri = rail.PythonOperator(
            task_id='get_timesheet_status_filter_uri',
            python_callable=timesheet_date_filter_uri,
            op_args=["ApprovalStatusFilter"]
        )
        get_timesheet_report_filter = rail.PythonOperator(
            task_id='get_timesheet_report_filter',
            python_callable=timesheet_date_filter_uri,
            op_args=["TimesheetPeriodFilter"]
        )
        run_report_for_timesheet = rail.run_report2(
            group_id='run_report_for_timesheet',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_timesheet_report_details').get('uri')}}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{result('get_timesheet_report_filter')}}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_report_filter')}}",
                                "value": startDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_report_filter')}}",
                                "value": endDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_status_filter_uri')}}",
                                "value": "0",
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        has_report_timesheet_data = rail.IfOperator(
            task_id='has_report_timesheet_data',
            test="{{ result('run_report_for_timesheet.get_report_result','has_data')}}",
            yes_task='get_timesheet_waiting_for_approval',
            no_task='finish'
        )

        get_timesheet_waiting_for_approval = rail.PythonOperator(
            task_id="get_timesheet_waiting_for_approval",
            python_callable=data_formatting.get_formated_timesheet_data,
            op_args=[config]
        )

        process_timesheet_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_child',
            items=lambda: rail.result('get_timesheet_waiting_for_approval'),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{location}child_v2{dag_id_postfix}'.lower(
            ),
            execution_timeout=timedelta(days=execution_timeout_days),
            conf=lambda item: {
                "date_format": rail.result("get_all_variables").get("date_format"),
                "period_in_months": rail.result("get_all_variables").get("period_in_months"),
                "chunk_size": rail.result("get_all_variables").get("chunk_size"),
                "report_date_format": rail.result("get_all_variables").get("report_date_format"),
                "max_child_run": rail.result("get_all_variables").get("max_child_run"),
                "execution_timeout_days": rail.result("get_all_variables").get("execution_timeout_days"),
                "timesheet_submit_remarks": rail.result("get_all_variables").get("timesheet_submit_remarks"),
                "timesheet_uris": item
                },
            retries=0,
        )
        wait_for_process_timesheet_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_child',
            dag_runs='{{ result("process_timesheet_child") }}',
            execution_timeout=timedelta(days=14),
        )

        gather_ts_child_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_ts_child_data',
            dag_runs="{{ result('process_timesheet_child') }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        get_merged_ts_logs = rail.PythonOperator(
            task_id='get_merged_ts_logs',
            python_callable=data_formatting.get_ts_errror_logs
        )


        finish = rail.EmptyOperator(
            task_id="finish"
        )

        
        send_entry_task_completion_email = rail.EmailOperator(
            task_id='send_entry_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Time Sheet Automation Status For - {{ current_time_in_specified_tz() }}',
            html_content="templates/success_email.html",
        )

        
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Timesheet Submission - failed to Submit Timehseet - {{ current_time_in_specified_tz() }}",
            html_content="templates/failure_email.html",
            params={
                'dag_id': f'{config.company_key}_timesheet_approval_master{dag_id_postfix}'.lower()
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        def final_status(**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status,
        )
        
        get_all_variables >> get_entry_report_details >> get_report_approval_filter_uri >> get_report_filter >> run_report_for_entry >> has_report_entry_data
        has_report_entry_data >> rail.Label(
            'No') >> finish
        has_report_entry_data >> rail.Label(
            'Yes') >> \
        get_timesheet_report_details >> get_timesheet_status_filter_uri >> get_timesheet_report_filter >> run_report_for_timesheet >> has_report_timesheet_data
        has_report_timesheet_data >> rail.Label(
            'No') >> finish
        has_report_timesheet_data >> rail.Label(
            'Yes') >> get_timesheet_waiting_for_approval >> process_timesheet_child >> wait_for_process_timesheet_child >> gather_ts_child_data >> get_merged_ts_logs 
        get_merged_ts_logs >> finish >> send_entry_task_completion_email >> send_task_failure_email >> log_to_sumo >> final_status
        
    return dag


rail.for_each_instance(create_main_dag)
