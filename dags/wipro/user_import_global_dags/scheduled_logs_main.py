from datetime import timedelta
from pendulum import datetime
from wipro.user_import_global_dags.utils import custom_methods
import rail


def create_airflow_master_dag(config):
    cntry = config.country.lower().replace(" ", "_")
    with rail.create_airflow_dag(
        dag_id=f"wipro_user_import_logs_{cntry}_master_{config.instance}",
        description="wipro user import scheduled logs",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        start_date=datetime(2023, 12, 18, tz=config.time_zone)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        if_user_import_logs = rail.IfOperator(
            task_id="if_user_import_logs",
            test=lambda dag_run:dag_run.conf.get("parent_run_id","") and not dag_run.conf.get("disable_user",""),
            yes_task=f'get_user_import_logs_{cntry}',
            no_task=f'get_disable_user_import_logs_{cntry}'
        )

        get_user_import_logs = rail.GatherResultsFromDagRunsOperator(
            task_id=f'get_user_import_logs_{cntry}',
            execution_timeout=timedelta(days=config.execution_timeout),
            dag_runs='{{dag_run.conf.parent_run_id}}',
            dagrun_task_id='create_log_for_user_import_global',
            flatten=True
        )

        get_disable_user_import_logs = rail.GatherResultsFromDagRunsOperator(
            task_id=f'get_disable_user_import_logs_{cntry}',
            execution_timeout=timedelta(days=config.execution_timeout),
            dag_runs='{{dag_run.conf.parent_run_id}}',
            dagrun_task_id=f"create_{cntry}_disable_user_log",
            flatten=True
        )

        load_all_logs = rail.PythonOperator(
            task_id=f'load_all_logs_{cntry}',
            python_callable=lambda cntry=cntry: custom_methods.format_logs(
                cntry)
        )

        create_user_import_master_log = rail.CreateLogOperator(
            task_id=f"create_user_import_master_log_{cntry}"
        )

        write_user_import_master_log = rail.WriteLogOperator(
            task_id=f'write_user_import_master_log_{cntry}',
            log='{{result("create_user_import_master_log_'+cntry+'")}}',
            items='{{result("load_all_logs_'+cntry+'")|to_json}}',
            message="aggregating log",
            severity=lambda item: item['status'],
            properties={
                "timestamp": '{{item.timestamp}}',
                "employee_id": '{{ item| attr_or_default("employee_id", "") }}',
                "employee_first_name": '{{item| attr_or_default("employee_first_name","")}}',
                "employee_last_name": '{{ item | attr_or_default("employee_last_name","")}}',
                "country": '{{ item | attr_or_default("country","")}}',
                "company_code": '{{item | attr_or_default("company_code", "")}}',
                "status": '{{item | attr_or_default("status","")}}',
                "action": '{{item | attr_or_default("action","")}}',
                "details": '{{item | attr_or_default("details","")}}',
                "ecid": '{{item | attr_or_default("ecid","")}}'
            }
        )

        filter_user_import_exception = rail.FilterLogEntriesOperator(
            task_id=f'filter_user_import_exception_{cntry}',
            log='{{result("create_user_import_master_log_'+cntry+'")}}',
            severity="Exception"
        )

        filter_user_import_failures = rail.FilterLogEntriesOperator(
            task_id=f'filter_user_import_failures_{cntry}',
            log='{{result("create_user_import_master_log_'+cntry+'")}}',
            severity="Error"
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id=f'write_logs_to_csv_{cntry}',
            source='{{result("write_user_import_master_log_'+cntry+'")}}',
            header=['EmployeeID', 'EmployeeFirstName', 'EmployeeLastName', 'Country',
                    'CompanyCode', 'Status', 'Action','Details', "ECID"],
            row=[
                 '{{ item.properties | attr_or_default("employee_id", "") }}',
                 '{{item.properties | attr_or_default("employee_first_name","")}}',
                 '{{ item.properties | attr_or_default("employee_last_name","")}}',
                 '{{ item.properties | attr_or_default("country","")}}',
                 '{{item.properties | attr_or_default("company_code", "")}}',
                 '{{item.properties | attr_or_default("status","")}}',
                 '{{item.properties | attr_or_default("action","")}}',
                 '{{item.properties | attr_or_default("details","")}}',
                 '{{item.properties | attr_or_default("ecid","")}}']
        )

        generate_pre_signed_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id=f'generate_pre_signed_download_url_{cntry}',
            artifact_name='{{result("write_logs_to_csv_'+cntry+'")}}',
            output_file_name='{{get_company_key()}}'+ f"_UserImport_Logs_{config.country}_" +
            '{{ current_time_in_specified_tz(fmt="%d%m%Y_%H%M%S", tz="Etc/UTC") }}'+".csv",
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id=f"send_import_complete_mail_{cntry}",
            to=config.tenant_email,
            bcc="{%- if result('filter_user_import_failures_"+cntry+"', 'length') == 0  -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alerts_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | User Import '+ config.country +' {{"-"}} \
                {%- if result("filter_user_import_failures_'+cntry+'", key="length") > 0 -%} \
                     {{" "}}Completed with errors  \
                {%- elif result("filter_user_import_exception_'+cntry+'", key="length") > 0 -%}\
                     {{" "}}Completed with exceptions \
                {%- else -%} \
                    {{" "}}Completed Successfully - \
                {%- endif -%} \
                {{ " " + current_time("%m-%d-%Y-%H-%M-%S.%f%z") }}',
            html_content=config.template_path
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id=f'log_to_sumo_{cntry}',
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id=f'can_fail_dag_{cntry}',
            test='{{get_error_message()|is_truthy}}',
            yes_task=f"fail_dagrun_{cntry}"
        )

        fail_dagrun = rail.FailOperator(
            task_id=f'fail_dagrun_{cntry}',
            message='{{get_error_message()}}'
        )

        if_user_import_logs >> rail.Label("No") >>\
        get_disable_user_import_logs
        if_user_import_logs >> rail.Label("Yes") >>\
        get_user_import_logs >> load_all_logs
        get_disable_user_import_logs >>\
        load_all_logs >> create_user_import_master_log >> write_user_import_master_log >>\
        filter_user_import_exception >> filter_user_import_failures >>\
        write_logs_to_csv >> generate_pre_signed_download_url >>\
        send_import_complete_email >> log_to_sumo >> can_fail_dag >> fail_dagrun
    return dag


rail.for_each_instance(create_airflow_master_dag)
