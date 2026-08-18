# pylint: disable=line-too-long wildcard-import unused-wildcard-import, too-many-statements line-too-long
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pendulum
import rail
from cie_epiq.ts_approval_utility.utils import data_formatting

def create_main_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    run_type = f'{config.run_type}_' if config.run_type else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_timesheet_approval_{run_type}master{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}epiq_timehseet_approval_Master{dag_id_postfix} - V1.0',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # runs at everytime as per configure minutes
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_master_run,
        default_args={
        },
    ) as dag:

        timesheet_report_name = config.infosys_config['timesheet_report_name']
        now = pendulum.now(config.timezone)
        weekStartDate = (now - relativedelta(days=7)
                     ).strftime('%b, %d, %Y')
        weekEndDate = (now - relativedelta(days=1)
                     ).strftime('%b, %d, %Y')

        month_startDate = now.subtract(months=1).start_of('month').strftime('%b, %d, %Y')
        month_endDate = now.subtract(months=1).end_of('month').strftime('%b, %d, %Y')

        startDate = month_startDate if config.run_type=="Monthly" else weekStartDate
        endDate = month_endDate if config.run_type=="Monthly" else weekEndDate

        ts_notification_period_details = startDate+" - "+endDate

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: data_formatting.findItemByDisplayText(
                response, timesheet_report_name)
        )

        has_all_report = rail.IfOperator(
            task_id='has_all_report',
            test="{{ result('get_all_report') | is_truthy}}",
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

        get_timesheet_period_filter_uri = rail.PythonOperator(
            task_id='get_timesheet_period_filter_uri',
            python_callable=timesheet_date_filter_uri,
            op_args=["TimesheetPeriodFilter"]
        )

        run_report_for_timesheet = rail.run_report2(
            group_id='run_report_for_timesheet',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_all_report').get('timesheet_report_uri')}}",
                        "filterValues": [

                            {
                                "reportFilterUri": "{{result('get_timesheet_period_filter_uri')}}",
                                "value": None,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_period_filter_uri')}}",
                                "value": startDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_period_filter_uri')}}",
                                "value": endDate,
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_status_filter_uri')}}",
                                "value": "0",
                            },
                            {
                                "reportFilterUri": "{{result('get_timesheet_status_filter_uri')}}",
                                "value": "1",
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
            trigger_dag_id=f'{dag_id_prefix}{config.company_key}_process_timesheet_chunk_{run_type}child{dag_id_postfix}'.lower(),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_timesheet_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_child',
            dag_runs='{{ result("process_timesheet_child") }}',
            execution_timeout=timedelta(days=14),
        )

        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Timesheet Approval '+config.run_type+' Process - Run Successfully For The Period: '+ ts_notification_period_details,
            html_content="templates/emails/email_for_success_format.html",
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Timesheet Approval "+config.run_type+" Process - failed to Approve Timehseet For The Period: "+ ts_notification_period_details,
            html_content="templates/emails/failure_email.html",
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

        get_all_report >> has_all_report
        has_all_report >> rail.Label(
            'Yes') >> get_timesheet_report_details >> get_timesheet_status_filter_uri >> get_timesheet_period_filter_uri >> run_report_for_timesheet >> has_report_timesheet_data
        has_all_report >> rail.Label(
            'No') >> finish
        has_report_timesheet_data >> rail.Label(
            'Yes') >> get_timesheet_waiting_for_approval >> process_timesheet_child >> wait_for_process_timesheet_child >> send_task_completion_email >> finish
        has_report_timesheet_data >> rail.Label(
            'No') >> finish
        finish >> send_task_failure_email >> log_to_sumo >> final_status

    return dag


rail.for_each_instance(create_main_dag)
