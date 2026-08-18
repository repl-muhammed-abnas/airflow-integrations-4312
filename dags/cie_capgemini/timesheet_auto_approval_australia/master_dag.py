# pylint: disable=line-too-long wildcard-import unused-wildcard-import, too-many-statements
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pendulum
import rail
from cie_capgemini.timesheet_auto_approval_australia.utils import python_callable


def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_{config.country}_timesheet_approval_master{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}timehseet_approval_Master{dag_id_postfix} - V1.0',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=timedelta(minutes=120),
        # runs at everytime as per configure minutes
        start_date=datetime(2022, 6, 1),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
        },
    ) as dag:

        now = pendulum.now(config.time_zone)
        startDate = (now - relativedelta(months=config.previous_period_in_months)
                     ).strftime(config.dateFormatTSReport)
        endDate = (now + relativedelta(months=config.future_period_in_months)
                   ).strftime(config.dateFormatTSReport)

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: python_callable.findItemByDisplayText(
                response, config.timesheet_report_name, config.timeoff_report_name)
        )
        has_all_report = rail.IfOperator(
            task_id='has_all_report',
            test="{{ result('get_all_report') | is_truthy}}",
            yes_task='get_entry_report_details',
            no_task='finish'
        )

        get_entry_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_entry_report_details',
            report_name=config.timesheet_report_name,
        )

        get_report_filter = rail.PythonOperator(
            task_id='get_report_filter',
            python_callable=python_callable.date_filter_uri,
            op_args=["TimesheetPeriodFilter"]
        )
        run_report_for_timsheet = rail.run_report2(
            group_id='run_report_for_timsheet',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_all_report').get('timesheet_report_uri')}}",
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
            test="{{ result('run_report_for_timsheet.get_report_result','has_data')}}",
            yes_task='get_eligible_timesheet_details',
            no_task='finish'
        )

        get_eligible_timesheet_details = rail.PythonOperator(
            task_id="get_eligible_timesheet_details",
            python_callable=python_callable.get_filtered_timesheet_data,
            # op_args=[
            #     "{{ result('run_report_for_timsheet.get_report_result') }}", config]
        )
        has_eligible_timesheet_details = rail.IfOperator(
            task_id='has_eligible_timesheet_details',
            test="{{ result('get_eligible_timesheet_details') | length > 0}}",
            yes_task='create_timedata_collection',
            no_task='get_subject_line'
        )
        create_timedata_collection = rail.CreateCollectionOperator(
            task_id='create_timedata_collection',
            source="{{ result('get_eligible_timesheet_details') | to_json }}",
            name="timedata",
            # todo update this map from actual csv header for key name
            columns={
                "Scheduled Hrs": "Scheduled Hrs",
                "Hours Worked": "Hours Worked",
                "Time Off Hrs": "Time Off Hrs",
                "TimesheetURI": "TimesheetURI",
                "Match": "Match",
                "UserUri": "UserUri",
                "Timesheet Start Date": "Timesheet Start Date",
                "Timesheet End Date": "Timesheet End Date",
                "Timesheet Period": "Timesheet Period",
            }
        )

        query_unique_timesheet_period = rail.QueryCollectionOperator(
            task_id='query_unique_timesheet_period',
            query="""SELECT DISTINCT Timesheet_Start_Date,Timesheet_End_Date  FROM timedata""",
        )

        min_max_date = rail.PythonOperator(
            task_id='min_max_date',
            python_callable=python_callable.getmin_max,
            op_args=[config]
        )
        get_timeoff_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timeoff_report_details',
            report_name=config.timeoff_report_name,
        )

        get_timeoff_report_filter = rail.PythonOperator(
            task_id='get_timeoff_report_filter',
            python_callable=python_callable.timeoff_date_filter_uri,
            op_args=["DateRangeFilter", 'get_timeoff_report_details']
        )

        run_report_for_timeoff = rail.run_report2(
            group_id='run_report_for_timeoff',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_all_report').get('timeoff_report_uri') }}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{ result('get_timeoff_report_filter') }}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{ result('get_timeoff_report_filter') }}",
                                "value": "{{ result('min_max_date').start_date }}",
                            },
                            {
                                "reportFilterUri": "{{ result('get_timeoff_report_filter') }}",
                                "value": "{{ result('min_max_date').end_date }}",
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )
        get_timesheet_waiting_for_approval = rail.PythonOperator(
            task_id="get_timesheet_waiting_for_approval",
            python_callable=python_callable.get_timesheet_uri_data,
            op_args=[config]

        )
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )
        process_timesheet_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_child',
            items=lambda: rail.result('get_timesheet_waiting_for_approval'),
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_{config.country}_process_timesheet_chunk_child{dag_id_postfix}'.lower(
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item: {
                'item': item,
                'logid': rail.result('create_log'),

            }
        )
        wait_for_process_timesheet_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_child',
            dag_runs='{{ result("process_timesheet_child") }}',
            execution_timeout=timedelta(days=14),
        )
        get_logs = rail.PythonOperator(
            task_id='get_logs',
            python_callable=python_callable.get_error_logs
        )
        create_log_csv = rail.WriteCSVFileOperator(
            task_id='create_log_csv',
            source=lambda: rail.result('get_logs'),
            header=['Timesheet Uris Batch',
                    'Status',
                    'Job Details'],
            row=lambda item: [
                item['timesheet_batch'],
                item['status'],
                item['childjobid'],
            ],
        )
        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_log_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}_logs.csv',
            expires_in_seconds=7*24*60*60,
        )
        get_subject_line = rail.PythonOperator(
            task_id='get_subject_line',
            python_callable=python_callable.get_subject_line
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Time Entry and Timesheet Approval - {{ result("get_subject_line") }} - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/email_for_success_format.html",
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Timesheet Approval - failed to Approve Timehseet/TimeEntry - {{ current_time_in_specified_tz() }}",
            html_content="templates/email/failure_email.html",
            params={
                'dag_id': f'{config.company_key}_timesheet_approval_master{dag_id_postfix}'.lower()
            }
        )
        finish = rail.EmptyOperator(
            task_id="finish"
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

        get_all_report >> has_all_report
        has_all_report >> rail.Label(
            'Yes') >> get_entry_report_details >> get_report_filter >> run_report_for_timsheet >> has_report_entry_data
        has_all_report >> rail.Label(
            'No') >> finish
        has_report_entry_data >> rail.Label(
            'No') >> finish
        has_report_entry_data >> rail.Label(
            'Yes') >> get_eligible_timesheet_details >> has_eligible_timesheet_details
        has_eligible_timesheet_details >> rail.Label(
            'No') >> get_subject_line
        has_eligible_timesheet_details >> rail.Label(
            'Yes') >> create_timedata_collection >> query_unique_timesheet_period >> min_max_date >> get_timeoff_report_details >> get_timeoff_report_filter >> run_report_for_timeoff >> get_timesheet_waiting_for_approval >> create_log >> process_timesheet_child >> wait_for_process_timesheet_child >> get_logs >> create_log_csv >> generate_download_link >> get_subject_line
        get_subject_line >> send_task_completion_email >> finish
        finish >> send_task_failure_email >> final_status
    return dag


rail.for_each_instance(create_main_dag)
