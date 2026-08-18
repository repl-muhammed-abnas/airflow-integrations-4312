from datetime import timedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid

from necau.auto_shift_assignment.utils.python_callable_method import do_format_logs

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'necau_auto_shift_assignment_master_daily_{config.instance}',
        description=f'NECAU - auto shift assignment_Master_Daily_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval_daily,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_shift_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
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
            no_task='send_no_data_email'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_shift_collection = rail.CreateCollectionOperator(
            task_id='create_user_shift_collection',
            name='userdata',
            source="{{ result('load_report_data') }}",
            columns={
                'Login Name': 'Loginname',
                'Auto schedule assignment - yes/no': 'Auto_schedule_assignment_yes_no',
                'Auto schedule assignment - shift': 'Shiftname',
                'Auto schedule assignment - days Wk1': 'Wk1pattern',
                'Auto schedule assignment - start date Wk1': 'Startdate',
                'Schedule Name (Current)': 'currentschedule',
                'useruri': 'Useruri',
                'User Name': 'Username',
                'Auto schedule assignment - days Wk2': 'Wk2pattern'}
        )

        def get_user_shift_info(dag_run, item):
            return {
                "Loginname": item["Loginname"],
                "Shiftname": item["Shiftname"],
                "Startdate": item["Startdate"],
                "Wk1pattern": item["Wk1pattern"],
                "Useruri": item["Useruri"],
                "Username": item["Username"],
                "Wk2pattern": item["Wk2pattern"],
                "master_ecid": get_dagrun_ecid(dag_run)
            }

        query_userdata = rail.QueryCollectionOperator(
            task_id='query_userdata',
            query='SELECT * FROM userdata Where currentschedule = "Shift Schedule"'
        )

        query_user_has_data = rail.IfOperator(
            task_id="query_user_has_data",
            test="{{ result('query_userdata','length') > 0 }}",
            yes_task='process_shifts',
            no_task='send_no_data_email'
        )

        process_shifts = rail.TriggerDagRunForEachItemOperator(
            task_id='process_shifts',
            retries=0,
            items="{{ result('query_userdata') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'necau_auto_shift_assignment_child_{config.instance}',
            conf=get_user_shift_info
        )

        wait_for_process_shifts = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_shifts',
            dag_runs='{{ result("process_shifts") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("process_shifts") }}',
            dagrun_task_id='create_child_log',
            execution_timeout=timedelta(hours=2),
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
                'User_name',
                'shiftname',
                'status',
                'reason',
                'jobid'],
            row=[
                '{{ item | attr_or_default("User_name", "") }}',
                '{{ item | attr_or_default("shiftname", "") }}',
                '{{ item | attr_or_default("status", "")}}',
                '{{ item | attr_or_default("reason", "") }}',
                '{{ item | attr_or_default("jobid", "") }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Users Shift assignment run - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/email/email_import_complete.html",
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() + " |  Weekly Auto-Shift Assignment - No data found - " }} \
                {{ current_time() }}',
            html_content="templates/email/email_no_data.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Recordcount ': '{{ result("query_userdata","length")}}'
            }
        )

        get_report_details >> run_report_group_entry
        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label("Yes") >> \
            load_report_data >> create_user_shift_collection >> query_userdata >> query_user_has_data
        query_user_has_data >> rail.Label('yes') >> process_shifts
        process_shifts >> wait_for_process_shifts >> gather_child_logs
        gather_child_logs >> format_logs >> render_logs_csv
        render_logs_csv >> generate_download_link >> send_import_complete_email >> log_to_sumo
        report_has_data >> rail.Label("No") >> \
            send_no_data_email >> log_to_sumo
        query_user_has_data >> rail.Label("No") >> \
            send_no_data_email >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
