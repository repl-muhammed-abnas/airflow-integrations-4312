# pylint: disable=unnecessary-lambda,line-too-long,too-many-statements
# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dags/cie_wipro/efforts_notification/config.py
from datetime import timedelta
import pendulum
import rail
from cie_wipro.KSA_Defaulter_Report.utils import python_callable


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_defaulter_report_{config.country}{dag_id_postfix}_master_v1'.lower(),
        description=f'Defaulter User Report - {dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=timedelta(minutes=5),
        start_date=pendulum.datetime(2022, 10, 10,  tz=config.instance_tz),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        # current_datetime = python_callable.get_eastern_timenow(config)
        # current_date = current_datetime.strftime('%b %d, %Y')
        # start_datetime = current_datetime - timedelta(days=30)
        start_date, end_date = python_callable.get_start_end_dateformat(config)

        get_all_report = rail.RepliconServiceOperator(
            task_id="get_all_report",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: python_callable.findItemByDisplayText(
                response, config.report_name)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )
        get_timeoff_booking_report_in_batch = rail.run_report2(
            group_id='get_timeoff_booking_report_in_batch',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details').get('uri'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "DateRangeFilter", 'uri'),
                                "value": None,
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "DateRangeFilter", 'uri'),
                                "value": start_date,
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "DateRangeFilter", 'uri'),
                                "value": end_date,
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        get_defaulter_user_data = rail.PythonOperator(
            task_id='get_defaulter_user_data',
            python_callable=python_callable.get_defaulter_user_data,
            op_args=[config]
        )
        has_data = rail.IfOperator(
            task_id="has_data",
            test="{{ result('get_defaulter_user_data') | is_truthy}}",
            yes_task="write_defaulter_user_to_csv",
            no_task="send_task_completion_nodata_email"
        )
        write_defaulter_user_to_csv = rail.WriteCSVFileOperator(
            task_id="write_defaulter_user_to_csv",
            source=lambda: rail.result('get_defaulter_user_data'),
            header=["Employee ID", "Employee Name", "Company Code", "Name of EE grp", "Name of EE subgroup",
                    "Country", "Start Date", "End date", "Reminder to Emp","Reminder to Manager", "Reminder to HR", "Status"],

        )
        send_task_completion_nodata_email = rail.EmailOperator(
            task_id='send_task_completion_nodata_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Defualter Report Automation - No Data Found - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content="templates/emails/nodata_email_complete.html",
        )
        send_task_completion_email = rail.EmailOperator(
            task_id='send_task_completion_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Defualter Report Automation - Created File Successfully - {{ current_time_in_specified_tz("America/New_York","%m_%d_%Y") }}',
            html_content="templates/emails/email_reminder_complete.html",
            files=[
                ('DefaulterReport_' + (pendulum.now()).strftime("%m%d%Y%H%M%S")+'.csv', "{{ result('write_defaulter_user_to_csv') }}")]
        )
        send_task_failure_email = rail.EmailOperator(
            task_id='send_task_failure_email',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject="{{ get_company_key() }} | Defualter Report Automation - Failed to Create File - {{ current_time_in_specified_tz() }}",
            html_content='templates/emails/failure_email.html',
            params={
                'dag_id': f'{dag_id_prefix}{config.company_key}efforts_custom_email_notification_{config.country}{dag_id_postfix}_master_v1'.lower()
            }
        )

        def final_status_func(config, msg, **kwargs):
            if config.send_failure_alerts:
                python_callable.send_failure_alert(config, msg)
            for task_instance in kwargs['dag_run'].get_task_instances():
                if task_instance.current_state() == "failed" and \
                        task_instance.task_id != kwargs['task_instance'].task_id:
                    raise Exception(
                        f"Task {task_instance.task_id} failed. Failing this DAG run")

        final_status = rail.PythonOperator(
            task_id='final_status',
            python_callable=final_status_func,
            op_args=[config, "Airflow Dag: " + dag_id_prefix+config.company_key +
                     dag_id_postfix + " has failed with the Error - {{ get_error_message() }}"],
            retries=0,
        )

        get_all_report >> get_report_details >> get_timeoff_booking_report_in_batch >> get_defaulter_user_data >> has_data
        has_data >> rail.Label(
            "Yes") >> write_defaulter_user_to_csv >> send_task_completion_email >> send_task_failure_email
        has_data >> rail.Label(
            "No") >> send_task_completion_nodata_email >> send_task_failure_email
        send_task_failure_email >> final_status
    return dag


rail.for_each_instance(create_dag)
