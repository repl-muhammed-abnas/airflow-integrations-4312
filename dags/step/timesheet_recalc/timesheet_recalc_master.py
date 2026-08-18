from datetime import timedelta, datetime as dt
import rail
from pendulum import datetime
import pendulum
from airflow.models import Variable
from step.timesheet_recalc.utils.custom_methods import logging_details
from step.timesheet_recalc.utils import request_payload


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'step_timesheet_recalc_master_{config.instance}',
        description='step Timesheet Recalc Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:
        
        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            return ((dt.strptime(current_date,"%d-%m-%Y") - dt.strptime(Variable.get(config.run_date_var),"%d-%m-%Y")).days == 14)

        can_process_run = rail.IfOperator(
            task_id = "can_process_run",
            test=can_process_run_test,
            yes_task="get_logging_details"
        )

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.time_zone]
        )

        get_hourly_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_hourly_report_details',
            report_name=config.extract_timesheet_recalc_report,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_report_details',
            report_params=request_payload.get_slug
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

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.extract_user_report,
        )

        report_user_group_entry, report_user_group_exit = rail.run_report(
            group_id='get_users_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_user_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_user_report_failed = rail.IfOperator(
            task_id="is_user_report_failed",
            test='{{result("get_users_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_user_report_generation",
            no_task="user_report_has_data"
        )

        fail_user_report_generation = rail.FailOperator(
            task_id="fail_user_report_generation",
            message="{{result('get_users_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        user_report_has_data = rail.IfOperator(
            task_id="user_report_has_data",
            test="{{ result('get_users_report_details.get_report_result', 'has_data') }}",
            yes_task='load_user_report_data',
            no_task='no_data',
        )

        load_user_report_data = rail.LoadCSVFileOperator(
            task_id='load_user_report_data',
            document="{{ result('get_users_report_details.get_report_result').reportGenerationResults[0].payload }}"
        )

        no_data = rail.EmptyOperator(
            task_id='no_data'
        )

        create_timesheet_data_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_data_collection',
            source="{{ result('load_report_data') }}",
            name="timesheet_data_table",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
                'TimesheetPeriodUri': 'timesheetperioduri'
            }
        )

        create_user_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_data_collection',
            source="{{ result('load_user_report_data') }}",
            name="user_data_table",
            columns={
                'Login Name': 'loginname',
                'Timesheet Template': 'timesheettemplate'
            }
        )

        query_valid_input_records = rail.QueryCollectionOperator(
            task_id='query_valid_input_records',
            query="SELECT user_data_table.timesheettemplate, timesheet_data_table.* FROM user_data_table" +
            " INNER JOIN timesheet_data_table ON user_data_table.loginname=timesheet_data_table.loginname AND user_data_table.timesheettemplate='Field Hourly'"
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('query_valid_input_records', 'length') > 0 }}",
            yes_task='send_started_mail',
            no_task='no_records'
        )

        no_records = rail.EmptyOperator(
            task_id="no_records"
        )

        send_started_mail = rail.EmailOperator(
            task_id='send_started_mail',
            to=config.tenant_email,
            bcc=config.tenant_cc,
            cc=config.tenant_bcc,
            subject='{{ get_company_key() }} - Timesheet Recalculation job started',
            html_content="templates/dag_started_mail.html"
        )

        process_records = rail.EmptyOperator(
            task_id="process_records"
        )

        process_time_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_time_records",
            items="{{result('query_valid_input_records')}}",
            batch_size=50,
            trigger_dag_id=f"step_timesheet_data_process_each_record_child_{config.instance}",
            conf=lambda item : {
                "timesheetdetails": item
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_time_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=['JobID', 'User', 'Loginname',
                    'Startdate', 'Enddate', 'TimesheetURI', 'Timesheettemplate'],
            row=['{{ item.properties.childecid }}', '{{ item.properties.username }}',
                 '{{ item.properties.loginname }}', '{{ item.properties.timesheetstartdate }}', '{{ item.properties.timesheetenddate }}',
                 '{{ item.properties.timesheeturi }}',
                 '{{ item.properties.timesheettemplate }}'],
        )

        filter_master_log = rail.FilterLogEntriesOperator(
            task_id='filter_master_log',
            severity='Success',
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.tenant_cc,
            cc=config.tenant_bcc,
            subject='{{ get_company_key() }} - Timesheet Recalculation completed',
            html_content="templates/mail_completion.html",
            files=[
                ("RecalculatedTimesheets_" + "{{dag_run_ecid()}}.csv", "{{ result('render_logs_csv') }}")]
        )

        set_date_var = rail.PythonOperator(
            task_id='set_date_var',
            python_callable=lambda dag_run: Variable.set(config.run_date_var, value=pendulum.now(config.time_zone).strftime("%d-%m-%Y"))
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_process_run >> rail.Label("Yes") >> get_logging_details >> get_hourly_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> get_user_report_details >> report_user_group_entry, report_user_group_exit\
            >> is_user_report_failed >> rail.Label("No") >> user_report_has_data >> rail.Label("Yes") >> load_user_report_data\
            >> create_timesheet_data_collection >> create_user_data_collection >> query_valid_input_records >> has_any_records

        has_any_records >> rail.Label("Yes") >> send_started_mail >> process_records >> process_time_records >> wait_process_time_records >> render_logs_csv\
            >> filter_master_log >> send_completion_mail >> set_date_var >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

        has_any_records >> rail.Label("No") >> no_records

        report_has_data >> rail.Label("No") >> no_data

        user_report_has_data >> rail.Label("No") >> no_data

        is_user_report_failed >> rail.Label(
            "Yes") >> fail_user_report_generation

    return dag


rail.for_each_instance(create_main_airflow_dag)
