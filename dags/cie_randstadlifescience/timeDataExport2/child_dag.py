# pylint: disable=line-too-long, too-many-statements trailing-whitespace
from datetime import timedelta
import json
from airflow.models import Variable
import rail
from cie_randstadlifescience.timeDataExport2.utils import download_from_s3, python_callable

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dags/cie_randstadlifescience/timeDataExport/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    dag_id_prefix = f'{config.team_id}_' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{dag_id_prefix}{config.company_key}_process_user_chunk_child_v2{dag_id_postfix}'.lower(),
        description=f'{dag_id_prefix}_process_user_chunk_child{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_child_run,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='chunk_min_max_date'
        )
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='chunk_min_max_date',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        chunk_min_max_date = rail.PythonOperator(
            task_id='chunk_min_max_date',
            python_callable=python_callable.get_users_min_max_date,
        )
        get_timesheetaudit_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheetaudit_report_details',
            report_name=config.audit_report_name,
        )
        get_user_audit_filter_details = rail.PythonOperator(
            task_id='get_user_audit_filter_details',
            python_callable=python_callable.get_user_chunck_filter
        )

        generate_report_2_in_batch = rail.run_report2(
            group_id='generate_report_2_in_batch',
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timesheetaudit_report_details').get('uri'),
                        "filterValues": json.loads(json.dumps(rail.result('get_user_audit_filter_details'))),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )
        report_has_timesheet_audit_data = rail.IfOperator(
            task_id='report_has_timesheet_audit_data',
            test="{{ result('generate_report_2_in_batch.get_report_result','has_data')}}",
            yes_task='get_timeentry_report_details',
            no_task='finish',
        )

        get_timeentry_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timeentry_report_details',
            report_name=config.entrydata_report_name,
        )
        get_user_entry_filter_details = rail.PythonOperator(
            task_id='get_user_entry_filter_details',
            python_callable=python_callable.get_user_chunck_filter_entries
        )

        generate_base_time_entry_data_report_in_batch = rail.run_report2(
            group_id='generate_base_time_entry_data_report_in_batch',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_timeentry_report_details').get('uri'),
                        "filterValues": json.loads(json.dumps(rail.result('get_user_entry_filter_details'))),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_entrydata = rail.IfOperator(
            task_id='report_has_entrydata',
            test="{{ result('generate_base_time_entry_data_report_in_batch.get_report_result','has_data')}}",
            yes_task='get_processed_TimesheetUris',
            no_task='finish',
        )

        # get processed TimesheetUris
        get_processed_TimesheetUris = download_from_s3.DownloadCsvOperator(
            task_id='get_processed_TimesheetUris',
            file_path=config.file_path,
            file_name=config.file_name,
            bucket_name=config.bucket_name,
            expires_in_seconds=7*24*60*60,
        )

        generate_data_for_final_report = rail.PythonOperator(
            task_id='generate_data_for_final_report',
            execution_timeout=timedelta(days=14),
            python_callable=lambda dag_run: python_callable.get_filtered_data(
                dag_run, config),
        )

        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test="{{ result('generate_data_for_final_report').get('processed_data') | length > 0}}",
            yes_task='write_semi_processed_data_to_csv',
            no_task='is_excluded_data_available',
        )
        col_names = ["SOURCE", "RNA_RPL_IMP_ID", "SEQNBR", "RNA_RPT_PRD_ID", "RNA_TASK_TSH_ID", "RNA_TSH_ENTRY_ID", "RNA_RPL_EMPLID", "EMPLID", "FIRST_NAME",
                     "LAST_NAME", "PAY_END_DT", "DATE_WRK", "TL_QUANTITY", "EXPENSE_TYPE", "RNA_EXPENSE_DATE", "RNA_EXP_PAY_AMT", "SP_EXP_APPROVER", "RNA_RPL_PAY_CODE", "RNA_RPL_ACTIVITY",
                     "RNA_RPL_TASKID", "APPROVAL_STATUS", "RNA_TASK_BILLABLE", "RNA_TSH_BILLABLE", "DTTIME_ADDED", "DTTM_EXPORT", "RNA_RPL_PROJ_ID", "RNA_RPL_TASK_NAME",
                     "RNA_RPL_TASK_CODE", "RNA_RPL_UNITID", "RNA_CLIENT_CODE", "RNA_CLIENT_NAME", "RNA_RPL_NEW_TIME", "VENDOR_ID", "PAY_RATE", "RUN_DTTM", "PROCESS_STATUS",
                     "RECORD_IDENTIFIER", "DTTM_IMPORTED", "EMPLID2", "FIRST_NAME_SRCH", "LAST_NAME_SRCH", "RNA_APPROVER_DTTM", "timesheetUriUniqueCode"]

        write_semi_processed_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_semi_processed_data_to_csv',
            source="{{ result('generate_data_for_final_report').get('processed_data') }}",
            # delimiter="|",
            header=col_names,
            row=['{{ item["SOURCE"] }}', '{{ item["RNA_RPL_IMP_ID"] }}', '{{ item["SEQNBR"] }}', '{{ item["RNA_RPT_PRD_ID"] }}', '{{ item["RNA_TASK_TSH_ID"] }}',
                    '{{ item["RNA_TSH_ENTRY_ID"] }}', '{{ item["RNA_RPL_EMPLID"] }}',
                    '{{ item["EMPLID"] }}', '{{ item["FIRST_NAME"] }}', '{{ item["LAST_NAME"] }}', '{{ item["PAY_END_DT"] }}', '{{ item["DATE_WRK"] }}', '{{ item["TL_QUANTITY"] }}',
                    '{{ item["EXPENSE_TYPE"] }}', '{{ item["RNA_EXPENSE_DATE"] }}', '{{ item["RNA_EXP_PAY_AMT"] }}', '{{ item["SP_EXP_APPROVER"] }}', '{{ item["RNA_RPL_PAY_CODE"] }}',
                    '{{ item["RNA_RPL_ACTIVITY"] }}', '{{ item["RNA_RPL_TASKID"] }}', '{{ item["APPROVAL_STATUS"] }}', '{{ item["RNA_TASK_BILLABLE"] }}',
                   '{{ item["RNA_TSH_BILLABLE"] }}', '{{ item["DTTIME_ADDED"] }}', '{{ item["DTTM_EXPORT"] }}', '{{ item["RNA_RPL_PROJ_ID"] }}',
                    '{{ item["RNA_RPL_TASK_NAME"] }}', '{{ item["RNA_RPL_TASK_CODE"] }}', '{{ item["RNA_RPL_UNITID"] }}', '{{ item["RNA_CLIENT_CODE"] }}',
                   '{{ item["RNA_CLIENT_NAME"] }}', '{{ item["RNA_RPL_NEW_TIME"] }}', '{{ item["VENDOR_ID"] }}', '{{ item["PAY_RATE"] }}',
                    '{{ item["RUN_DTTM"] }}', '{{ item["PROCESS_STATUS"] }}', '{{ item["RECORD_IDENTIFIER"] }}', '{{ item["DTTM_IMPORTED"] }}',
                 '{{ item["EMPLID2"] }}', '{{ item["FIRST_NAME_SRCH"] }}', '{{ item["LAST_NAME_SRCH"] }}', '{{ item["RNA_APPROVER_DTTM"] }}', '{{ item["timesheetUriUniqueCode"] }}'],
        )

        is_excluded_data_available = rail.IfOperator(
            task_id='is_excluded_data_available',
            test="{{ result('generate_data_for_final_report').get('excluded_data') | length > 0}}",
            yes_task='excluded_data_to_csv',
            no_task='finish',
        )
        excluded_data_to_csv = rail.WriteCSVFileOperator(
            task_id='excluded_data_to_csv',
            source=lambda: rail.result(
                'generate_data_for_final_report').get('excluded_data'),
            header=["timesheet_uri", "end_date", "approval_date"],
            row=['{{ item["timesheet_uri"] }}',
                 '{{ item["end_date"] }}',
                 '{{ item["approval_date"] }}'],
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> chunk_min_max_date
        chunk_min_max_date >> get_timesheetaudit_report_details >> get_user_audit_filter_details >> generate_report_2_in_batch >> report_has_timesheet_audit_data
        report_has_timesheet_audit_data >> rail.Label(
            'No') >> finish
        report_has_timesheet_audit_data >> rail.Label(
            'Yes') >> get_timeentry_report_details >> get_user_entry_filter_details >> generate_base_time_entry_data_report_in_batch >> report_has_entrydata
        report_has_entrydata >> rail.Label(
            'No') >> finish
        report_has_entrydata >> rail.Label(
            'Yes') >> get_processed_TimesheetUris >> generate_data_for_final_report >> is_data_available
        is_data_available >> rail.Label(
            'No') >> is_excluded_data_available
        is_data_available >> rail.Label(
            'Yes') >> write_semi_processed_data_to_csv >> is_excluded_data_available
        is_excluded_data_available >> rail.Label(
            'No') >> finish
        is_excluded_data_available >> rail.Label(
            'Yes') >> excluded_data_to_csv >> finish

    return dag


rail.for_each_instance(create_dag)
