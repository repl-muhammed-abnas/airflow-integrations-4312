from datetime import timedelta
import rail

from dxctechnology.timesheet_autosubmission_loa.utils.request_payload import get_filters
from dxctechnology.timesheet_autosubmission_loa.utils.custom_methods import do_format_logs

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.c1_chid_dag_id,
        description=f'DxcTechnology TimeSheet Auto Submission LOA Each Month C1 Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_each_month_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        generate_report_data = rail.run_report2(
            group_id="generate_report_data",
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": dag_run.conf['report_uri'],
                        "filterValues": get_filters(dag_run,'c1_company_code_uri_values'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            }
        )

        is_report_generation_failed = rail.IfOperator(
            task_id ="is_report_generation_failed",
            test="{{ (result('generate_report_data.get_report_result')).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('generate_report_data.get_report_result')).reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{result('generate_report_data.get_report_result','has_data')}}",
            yes_task='load_report_data',
            no_task='send_no_data_email',
        )

        send_no_data_email = rail.EmailOperator(
            task_id="send_no_data_email",
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key()}} | Replicon Timesheet Auto-Submission(LOA) completed for {{dag_run.conf.erp}} for the daterange {{dag_run.conf.report_start_date}} - {{dag_run.conf.report_end_date}} - No timesheet to process on {{current_time_in_specified_tz()}}',
            html_content="templates/blank_email.html"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('generate_report_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_report_data') }}",
            name='timesheet_report_data',
            columns={
                    'Employee ID': 'employee_id',
                    'User Uri': 'user_uri',
                    'Timesheet Period': 'timesheet_period',
                    'Timesheet Period Uri': 'timesheet_period_uri',
                    'Scheduled Hrs (In Period)': 'scheduled_hours_for_week',
                    'Validation Messages': 'validation_messages',
                    'Total TimeOff Hrs (In Period)': 'timeoff_hours_for_week',
                    'WBS / SO Hrs (In Period)': 'project_hours_for_week',
                    'User End Date': 'user_end_date',
                    'Timesheet End Date': 'timesheet_end_date',
                    'User Status': 'user_status',
            }
        )

        query_eligible_timesheets = rail.QueryCollectionOperator(
            task_id='query_eligible_timesheets',
            query="""SELECT * FROM timesheet_report_data
                        WHERE NULLIF(employee_id,'') IS NOT NULL
                        AND NULLIF(validation_messages,'') IS NULL
                        AND NUllIF(timeoff_hours_for_week ,'') IS NOT NUll
                        AND timeoff_hours_for_week  != "0.00"
                        AND scheduled_hours_for_week != "0.00"
                        AND (project_hours_for_week == "0.00" OR NULLIF(project_hours_for_week,'') IS NULL)
                        AND scheduled_hours_for_week <= timeoff_hours_for_week
                        AND ( LOWER(user_status) == "enabled" OR 
                                ( LOWER(user_status) == "disabled" AND
                                    ( NULLIF(user_end_date,'') IS NOT NULL AND (
                                    DATE(
                                        SUBSTR(user_end_date, -4) || '-' ||
                                        CASE
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'January' THEN '01'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'February' THEN '02'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'March' THEN '03'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'April' THEN '04'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'May' THEN '05'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'June' THEN '06'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'July' THEN '07'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'August' THEN '08'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'September' THEN '09'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'October' THEN '10'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'November' THEN '11'
                                            WHEN SUBSTR(user_end_date, INSTR(user_end_date, ' ') + 1, LENGTH(user_end_date) - INSTR(user_end_date, ' ') - 5) = 'December' THEN '12'
                                        END || '-' || SUBSTR(user_end_date, 1, INSTR(user_end_date, ' ') - 1)
                                    )
                                    <= 
                                    DATE(
                                        SUBSTR(timesheet_end_date, -4) || '-' ||
                                        CASE
                                            WHEN SUBSTR(timesheet_end_date , INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'January' THEN '01'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'February' THEN '02'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'March' THEN '03'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'April' THEN '04'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'May' THEN '05'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'June' THEN '06'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'July' THEN '07'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'August' THEN '08'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'September' THEN '09'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'October' THEN '10'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'November' THEN '11'
                                            WHEN SUBSTR( timesheet_end_date, INSTR( timesheet_end_date, ' ') + 1, LENGTH( timesheet_end_date) - INSTR( timesheet_end_date, ' ') - 5) = 'December' THEN '12'
                                        END || '-' || SUBSTR( timesheet_end_date, 1, INSTR( timesheet_end_date, ' ') - 1)
                                    )
                                    )
                                    )
                                )
                            )"""
        )

        has_eligible_timesheets = rail.IfOperator(
            task_id='has_eligible_timesheets',
            test="{{ result('query_eligible_timesheets', key='length') > 0 }}",
            yes_task='process_timesheet_autosubmission_child',
            no_task='send_no_eligible_timesheets_email'
        )

        send_no_eligible_timesheets_email = rail.EmailOperator(
            task_id="send_no_eligible_timesheets_email",
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key()}} | Replicon Timesheet Auto-Submission(LOA) completed for {{dag_run.conf.erp}} for the daterange {{dag_run.conf.report_start_date}} - {{dag_run.conf.report_end_date}} - No eligible timesheet to process on {{current_time_in_specified_tz()}}',
            html_content="templates/blank_email.html"
        )
        
        process_timesheet_autosubmission_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_autosubmission_child',
            items=lambda: rail.result('query_eligible_timesheets'),
            trigger_dag_id=config.process_c1_timesheet_dag_id,
            batch_size=config.batch_size,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_process_timesheet_autosubmission = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet_autosubmission',
            dag_runs='{{ result("process_timesheet_autosubmission_child") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("process_timesheet_autosubmission_child") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Employee ID',
                'Timesheet Period',
                'Status',
                'Details',
                'Jobid'],
            row=[
                '{{ item.employee_id }}',
                '{{ item.timesheet_period}}',
                '{{ item.status}}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_upload_path +
            'logs_{{ dag_run_ecid()  | replace(":", "-") }}_timesheet_auto_submission_loa_{{dag_run.conf.erp}}' +
            '.csv'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + "| Replicon Timesheet Auto-Submission(LOA) "}} \
                    {%- if result("format_logs", key="error_record_count")  > 0 -%} \
                        completed with errors for   \
                    {%- else -%} \
                        completed successfully for {%- endif -%}'
                    + '{{" "+ dag_run.conf.erp}} for the daterange {{dag_run.conf.report_start_date}} - {{dag_run.conf.report_end_date}} on ' +
                    '{{current_time_in_specified_tz()}}',
                    html_content="templates/email_import_complete.html",
                    params={
                        'log_filepath': config.sftp_upload_path
                    }
        )

        generate_report_data >> is_report_generation_failed >> rail.Label('Yes') >> fail_report_generation
        is_report_generation_failed >> rail.Label('No') >> report_has_data
        report_has_data >> rail.Label('Yes') >> load_report_data >> create_report_collection >> query_eligible_timesheets 
        query_eligible_timesheets >> has_eligible_timesheets >> rail.Label('Yes') >> process_timesheet_autosubmission_child
        has_eligible_timesheets >> rail.Label('No') >> send_no_eligible_timesheets_email
        process_timesheet_autosubmission_child >> wait_for_process_timesheet_autosubmission
        report_has_data >> rail.Label('No') >> send_no_data_email
        wait_for_process_timesheet_autosubmission >>  gather_logs >> format_logs
        format_logs >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email
        
    return dag  


rail.for_each_instance(create_dag)