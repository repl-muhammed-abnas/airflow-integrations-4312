from datetime import timedelta
import re
import rail
from airflow.models import Variable
from rei.invoice_export.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"{config.company_key}_quickbooks_online_invoice_export_report_extract_child_dag_{config.instance}",
        description=f'QuickBooks Online {config.region} Invoice Export Report Extract Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_timesheet_start_end_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_timesheet_start_end_present',
            end_task='can_fail_dag',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        if_timesheet_start_end_present = rail.IfOperator(
            task_id='if_timesheet_start_end_present',
            test=lambda dag_run: bool(dag_run.conf['timesheetperiodstartdate'] and dag_run.conf['timesheetperiodenddate']),
            yes_task='get_report_details',
            no_task='can_fail_dag'
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        get_all_projects = rail.RepliconServiceOperator(
            task_id="get_all_projects",
            endpoint="/services/ProjectService1.svc/GetAllProjects",
        )

        create_report_generation_batch = rail.RepliconServiceOperator(
            task_id='create_report_generation_batch',
            endpoint="/services/reportService1.svc/CreateReportGenerationBatch",
            data=request_payload.get_generate_report_batch_param
        )

        report_generation_batch_entry, report_generation_batch_exit = rail.batch_execution(
            'execute_report_generation_batch', create_report_generation_batch.task_id
        )

        get_report_generation_batch_results = rail.RepliconServiceOperator(
            task_id='get_report_generation_batch_results',
            endpoint="/services/reportService1.svc/GetReportGenerationBatchResults",
            data={
                "reportGenerationBatchUri": "{{ result('create_report_generation_batch') }}"
            }
        )

        has_empty_report_data = rail.IfOperator(
            task_id='has_empty_report_data',
            test='''{{ result('get_report_generation_batch_results').reportGenerationResults[0].payload |  starts_with('No Data') }}''',
            yes_task="can_fail_dag",
            no_task="load_report_data",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_report_generation_batch_results').reportGenerationResults[0].payload }}"
        )

        upload_report_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_report_to_sftp',
            content="{{ result('load_report_data') }}",
            remote_filepath=config.report_file_path + '/Timesheet_Hours_by_Day_Report_{{ dag_run.conf.invoicenumber }}.csv'
        )

        send_invoice_synced_mail = rail.EmailOperator(
            task_id='send_invoice_synced_mail',
            to=config.tenant_email,
            subject='{{ get_company_key() }}  | Invoice Sync and Report Extract For Invoice# - {{ dag_run.conf.invoicenumber }} Is completed',
            html_content="templates/emails/invoice_sync_completed.html",
            params={
                'invoice_number': "{{ dag_run.conf.invoicenumber }}"
            }
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

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> can_fail_dag

        can_run_batch_task >> rail.Label(
            'No') >> if_timesheet_start_end_present
        
        if_timesheet_start_end_present >> rail.Label('Yes') >> get_report_details
        if_timesheet_start_end_present >> rail.Label('No') >> can_fail_dag

        get_report_details >> get_all_projects >> create_report_generation_batch >> report_generation_batch_entry
        report_generation_batch_exit >> get_report_generation_batch_results >> has_empty_report_data

        has_empty_report_data >> rail.Label('Yes') >> can_fail_dag
        has_empty_report_data >> rail.Label('No') >> load_report_data >> upload_report_to_sftp >> send_invoice_synced_mail >> can_fail_dag

        can_fail_dag >> rail.Label('On Error') >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag)
