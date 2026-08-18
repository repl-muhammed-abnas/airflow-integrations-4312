from datetime import timedelta
import rail
from crjg3.payroll_export.utils import python_callable
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"crj_custom_payroll_report_child_{config.instance}",
        description=f"CRJ_Custom Payroll Report V2.0 child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_username_and_time_now'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_username_and_time_now',
            end_task='finish',
        )

        get_username_and_time_now = rail.PythonOperator(
            task_id='get_username_and_time_now',
            python_callable=python_callable.get_username_and_time_now
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": dag_run.conf['reportUri'],
                        "filterValues": python_callable.report_filter_for_payroll_data(dag_run.conf['dagran'], dag_run.conf['enabledFilters']),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="finish",
            no_task="has_report_data"
        )

        has_report_data = rail.IfOperator(
            task_id='has_report_data',
            test='{{ result("run_report.get_report_result", "has_data") }}',
            yes_task="create_final_data_per_week_lookup_table",
            no_task="send_nodata_mail",
        )

        send_nodata_mail = rail.EmailOperator(
            task_id='send_nodata_mail',
            to="{{dag_run.conf.email}}",
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Payroll report extract  - {{ result("get_username_and_time_now").timenow }}',
            html_content="templates/emails/no_data_mail.html"
        )

        create_final_data_per_week_lookup_table = rail.CreateLogOperator(
            task_id='create_final_data_per_week_lookup_table'
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=[
                'Funders Name', 'Project Name', 'Time Type Name', 'Time Type Code', 'Timesheet Period', 'Entry Date', 'Replace',
                'Hours Worked', 'Work Location', 'Labor Metrics', 'Week (Entry Date)', 'Normalization Required?', 'Employee ID',
                'Employee No.', 'Login Name', 'Employee Category',
            ]
        )

        create_raw_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_raw_payroll_data_collection',
            source="{{ result('parse_csv') }}",
            name="rawpayrolldata",
            columns={
                'Funders Name': 'clientname',
                'Project Name': 'projectname',
                'Time Type Name': 'taskname',
                'Time Type Code': 'taskcode',
                'Timesheet Period': 'timesheetperiod',
                'Entry Date': 'entrydate',
                'Replace': 'replace',
                'Hours Worked': 'hoursworked',
                'Work Location': 'worklocation',
                'Labor Metrics': 'labormetrics',
                'Week (Entry Date)': 'weekentrydate',
                'Normalization Required?': 'normalizationrequired',
                'Employee ID': 'employeeid',
                'Employee No.': 'employeeno',
                'Login Name': 'loginname',
                'Employee Category': 'employeecategory',
            }
        )

        get_all_data = rail.QueryCollectionOperator(
            task_id='get_all_data',
            query="""SELECT * FROM rawpayrolldata""",
            name='allpayrolldata'
        )

        compose_csv_with_headers = rail.WriteCSVFileOperator(
            task_id='compose_csv_with_headers',
            source="{{ result('get_all_data') | load_all_records() | to_json }}",
            header=[
                'clientname', 'projectname', 'taskname', 'taskcode', 'timesheetperiod', 'entrydate', 'replace', 'hoursworked', 'worklocation',
                'labormetrics', 'weekentrydate', 'normalizationrequired', 'employeeid', 'employeeno', 'loginname', 'employeecategory'
            ],
            row=python_callable.get_csv_row_data
        )

        create_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payroll_data_collection',
            source="{{ result('compose_csv_with_headers') }}",
            name="payrolldata",
            columns=[
                'clientname', 'projectname', 'taskname', 'taskcode', 'timesheetperiod', 'entrydate', 'replace', 'hoursworked', 'worklocation',
                'labormetrics', 'weekentrydate', 'normalizationrequired', 'employeeid', 'employeeno', 'loginname', 'employeecategory'
            ]
        )

        get_distinct_weeks_and_login_names_data = rail.QueryCollectionOperator(
            task_id='get_distinct_weeks_and_login_names_data',
            query="""SELECT DISTINCT weekentrydate as week, loginname, employeecategory FROM payrolldata""",
            name='distinctweeksandloginnames'
        )

        process_each_report_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_report_records',
            items="{{ result('get_distinct_weeks_and_login_names_data') | load_all_records() | to_json }}",
            trigger_dag_id=f"crj_custom_payroll_report_extract_data_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index: {
                "lookup_table": rail.result('create_final_data_per_week_lookup_table'),
                "week": item['week'],
                "loginname": item['loginname'],
                "employeecategory": item['employeecategory']
            }
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_each_report_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        create_final_data_lookup_table = rail.CreateLogOperator(
            task_id='create_final_data_lookup_table'
        )

        get_final_data_to_export = rail.QueryCollectionOperator(
            task_id='get_final_data_to_export',
            query="""SELECT DISTINCT taskcode,replace,worklocation,labormetrics,employeeno FROM payrolldata ORDER BY employeeno ASC""",
            name='getfinaldatatoexport'
        )

        add_final_data_to_export_lookup_table = rail.WriteLogOperator(
            task_id='add_final_data_to_export_lookup_table',
            log="{{result('create_final_data_lookup_table')}}",
            items="{{ result('get_final_data_to_export') | load_all_records() | to_json }}",
            message="na",
            severity="Success",
            properties=python_callable.get_final_data_to_export
        )

        create_final_extract_data_csv = rail.WriteCSVFileOperator(
            task_id='create_final_extract_data_csv',
            source=lambda: rail.result(
                'create_final_data_lookup_table'),
            header=[
                'Employee No.',
                'Replace',
                'Code',
                'Hrs.',
                'Work Location',
                'Labor Metrics'
            ],
            row=lambda item: [
                item['properties']['employeenumber'],
                item['properties']['replace'],
                item['properties']['code'],
                item['properties']['hrs'],
                item['properties']['worklocation'],
                item['properties']['labormetrics']
            ],
            thread_pool_size=2
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_final_extract_data_csv')}}",
            output_file_name='Custom CJI - Payroll Report_{{ dag_run.conf.userid }}_{{ current_time_in_specified_tz(fmt="%d%m%Y%H%M%S") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_extract_file_to_user_mail = rail.EmailOperator(
            task_id='send_extract_file_to_user_mail',
            to="{{dag_run.conf.email}}",
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Payroll report extract - {{ result("get_username_and_time_now").timenow }}',
            html_content="templates/emails/send_extract_file_to_user_mail.html"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_username_and_time_now

        get_username_and_time_now >> run_report_entry
        run_report_exit >> is_report_failed >> rail.Label("Yes") >> finish
        is_report_failed >> rail.Label("No") >> has_report_data >> rail.Label(
            "No") >> send_nodata_mail >> finish
        has_report_data >> rail.Label("Yes") >> create_final_data_per_week_lookup_table >> parse_csv >> create_raw_payroll_data_collection >> \
        get_all_data >> compose_csv_with_headers >> create_payroll_data_collection >> \
        get_distinct_weeks_and_login_names_data >> process_each_report_records >> wait_process_time_records >> create_final_data_lookup_table >> \
        get_final_data_to_export >> add_final_data_to_export_lookup_table >> create_final_extract_data_csv >> generate_download_link
        generate_download_link >> send_extract_file_to_user_mail >> finish

    return dag


rail.for_each_instance(create_child_dag)
