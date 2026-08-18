from datetime import timedelta
from pendulum import datetime
import rail
from pwcglobal.timesheet_auto_submission_v5.task.timeoff_hours_specific_scenarios import get_specific_scenarios

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import/config.py


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.timeoff_hour_logic_master_dag_id,
        description=config.timeoff_hour_logic_master_dag_id,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 12, 15, tz=config.time_zone),
        # runs at 9 30 PM on every day
        schedule_interval=config.timeoff_schedule_interval,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timesheet_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_report_data',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_loa_users_collection = rail.CreateCollectionOperator(
            task_id='create_loa_users_collection',
            name='loa_users',
            source="{{ result('load_report_data') }}",
        )

        query_user = rail.QueryCollectionOperator(
            task_id='query_user',
            query='''SELECT * FROM loa_users
                     WHERE (
                        Validation_Message="" OR Validation_Message IS NULL OR Validation_Message = "Null" OR
                       Validation_Message=?)
            ''',
            query_params=[config.validation_message]
        )

        get_timeoff_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timeoff_report_details',
            report_name=config.timesheet_with_timeoffhours_report_name,
        )

        run_timeoff_report_group_entry, run_timeoff_report_group_exit = rail.run_report(
            group_id='run_timeoff_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_timeoff_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_timeoff_data = rail.IfOperator(
            task_id="report_has_timeoff_data",
            test="{{ result('run_timeoff_report.get_report_result','has_data')}}",
            yes_task='load_timeoff_report_data',
        )

        load_timeoff_report_data = rail.LoadCSVFileOperator(
            task_id='load_timeoff_report_data',
            document="{{ result('run_timeoff_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_timeoff_collection = rail.CreateCollectionOperator(
            task_id='create_timeoff_collection',
            name='timeoff_user',
            source="{{ result('load_timeoff_report_data') }}",
        )

        query_timeoff_user = rail.QueryCollectionOperator(
            task_id='query_timeoff_user',
            query='''SELECT * FROM timeoff_user
                        WHERE Timesheeturi IN (
                            SELECT DISTINCT Timesheeturi FROM query_user
                            )
                    '''
        )

        query_distinct_statistical_record = rail.QueryCollectionOperator(
            task_id='query_distinct_statistical_record',
            name='Statisticaltimesheets',
            query='''SELECT DISTINCT
                        timesheeturi,totalhours,Scheduled_Hrs__In_Period_,projecttype,useruri,daydiff,Timesheet_Start_Date,Timesheet_End_Date,User_Name,Country__Current_
                     FROM query_timeoff_user
                     WHERE projecttype='Statistical'
                     AND NOT LOWER(Country__Current_) IN ('japan')
                     AND (CAST(totalhours as decimal) > CAST(Scheduled_Hrs__In_Period_ as decimal) OR CAST(totalhours as decimal) = CAST(Scheduled_Hrs__In_Period_ as decimal))
                     AND CAST(daydiff as decimal) < 1
                    '''
        )

        query_distinct_nonstatistical_record = rail.QueryCollectionOperator(
            task_id='query_distinct_nonstatistical_record',
            name='nonStatisticaltimesheets',
            query='''SELECT DISTINCT
                        timesheeturi,totalhours,Scheduled_Hrs__In_Period_,projecttype,useruri,daydiff,Timesheet_Start_Date,Timesheet_End_Date,User_Name,Country__Current_
                     FROM query_timeoff_user
                     WHERE projecttype='NonStatistical'
                     AND NOT LOWER(Country__Current_) IN ('japan')
                     AND (CAST(totalhours as decimal) > CAST(Scheduled_Hrs__In_Period_ as decimal) OR CAST(totalhours as decimal) = CAST(Scheduled_Hrs__In_Period_ as decimal))
                     AND TimeoffType='Null' AND CAST(daydiff as decimal) < 1
                    '''
        )

        query_distinct_wholeweek_timeoff_record = rail.QueryCollectionOperator(
            task_id='query_distinct_wholeweek_timeoff_record',
            name='wholeweek_timeoff_timesheet',
            query=f'''SELECT
                t.timesheeturi,
                t.useruri,
                t.User_Name,
                t.Timesheet_Start_Date,
                t.Timesheet_End_Date,
                MAX(t.totalhours)         AS totalhours,       -- same for the group
                SUM(CAST(t.Time_Off_Hrs AS DECIMAL)) AS sum_timeoff_hours,
                MAX(t.daydiff)            AS daydiff,
                MAX(t.Country__Current_)  AS Country__Current_
            FROM query_timeoff_user t
            WHERE LOWER(t.Country__Current_) = 'japan'
            AND CAST(t.totalhours AS DECIMAL) >= CAST(t.Scheduled_Hrs__In_Period_ AS DECIMAL)
            AND CAST(t.daydiff AS DECIMAL) < 1
            GROUP BY
                t.timesheeturi,
                t.useruri,
                t.User_Name,
                t.Timesheet_Start_Date,
                t.Timesheet_End_Date
            HAVING
                MAX(CAST(t.totalhours AS DECIMAL))
                = SUM(CAST(t.Time_Off_Hrs AS DECIMAL))'''
        )

        query_distinct_valid_timesheet = rail.QueryCollectionOperator(
            task_id='query_distinct_valid_timesheet',
            name='distinct_valid_timesheet',
            query='''SELECT DISTINCT
                        timesheeturi,useruri,Timesheet_Start_Date,Timesheet_End_Date,User_Name
                     FROM Statisticaltimesheets
                     WHERE timesheeturi NOT IN (
                        SELECT timesheeturi FROM nonStatisticaltimesheets)
                    '''
        )

        process_timesheet = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet',
            retries=0,
            items=lambda: rail.result('query_distinct_valid_timesheet'),
            batch_size=25,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'{config.timeoff_hour_logic_child_dag_id}',
        )

        wait_for_process_timesheet = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet',
            dag_runs='{{ result("process_timesheet") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_timesheet_for_wholeweek_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_for_wholeweek_timeoff',
            retries=0,
            items=lambda: rail.result('query_distinct_wholeweek_timeoff_record'),
            batch_size=25,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'{config.timeoff_hour_logic_child_dag_id}',
        )

        wait_for_process_timesheet_for_wholeweek_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_for_wholeweek_timeoff',
            dag_runs='{{ result("process_timesheet_for_wholeweek_timeoff") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        specific_scenarios = get_specific_scenarios(config)

        has_log_data = rail.IfOperator(
            task_id="has_log_data",
            # pylint: disable=line-too-long
            test='{{ (result("query_distinct_valid_timesheet", "length") | is_truthy and result("query_distinct_valid_timesheet", "length") > 0) or ((result("query_final_data", "length") | is_truthy and result("query_final_data", "length")) > 0) or (result("query_distinct_wholeweek_timeoff_record", "length") | is_truthy and result("query_distinct_wholeweek_timeoff_record", "length") > 0) }}',
            yes_task='get_errored_logs',
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'User',
                'Timesheeturi',
                'Timesheetperiod',
                'Status',
                'Details',
                'Job ID'],
            row=[
                '{{ item.properties | attr_or_default("User_Name", "") }}',
                '{{ item.properties | attr_or_default("timesheeturi", "") }}',
                '{{ item.properties | attr_or_default("timesheetperiod", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Timesheet autosubmission/approval run - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/emails/import_complete_timeoff.html",
        )

        get_report_details >> run_report_group_entry >> run_report_group_exit >> report_has_data

        report_has_data >> rail.Label("Yes") >> \
            load_report_data >> create_loa_users_collection >> query_user >> get_timeoff_report_details >>\
            run_timeoff_report_group_entry >> run_timeoff_report_group_exit >> \
            report_has_timeoff_data >> rail.Label("Yes") >> load_timeoff_report_data >> create_timeoff_collection >> \
            query_timeoff_user >> query_distinct_statistical_record >> query_distinct_nonstatistical_record >> \
                query_distinct_wholeweek_timeoff_record >> query_distinct_valid_timesheet
        query_distinct_valid_timesheet >> process_timesheet >> wait_for_process_timesheet >> process_timesheet_for_wholeweek_timeoff >> \
            wait_for_process_timesheet_for_wholeweek_timeoff >> specific_scenarios >> has_log_data

        has_log_data >> rail.Label(
            'Yes') >> get_errored_logs >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_import_complete_email

    return dag


rail.for_each_instance(create_dag)
