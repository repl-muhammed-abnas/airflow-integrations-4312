from datetime import timedelta
from json import loads
from math import ceil
from pendulum import datetime, now
import rail
from mammoet.payroll_export_italy.utils import request_payload
from mammoet.payroll_export_italy.utils.custom_methods import get_logging_details_callable, EXPORT_DATE_FORMAT, get_log_to_sumo_extra_info, get_can_process_task
from mammoet.payroll_export_italy.utils.response_filters import get_payroll_file_format_details_for_country, get_location_uris
from mammoet.payroll_export_italy.tasks.payroll_export_task import payroll_data_export


OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.payroll_export_process_payroll,
        description="Mammoet payroll Export child Dag",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_process_tasks = rail.IfOperator(
            task_id='can_process_tasks',
            test=lambda dag_run: get_can_process_task(dag_run, config),
            yes_task='get_logging_details'
        )

        get_all_location_for_parent = rail.RepliconServiceOperator(
            task_id="get_all_location_for_parent",
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=request_payload.get_all_locations_for_parent,
            data_handler=get_location_uris
        )

        get_payroll_file_format_for_country = rail.RepliconServiceOperator(
            task_id="get_payroll_file_format_for_country",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: get_payroll_file_format_details_for_country(
                response, config)
        )

        get_logging_details = rail.PythonOperator(
            task_id="get_logging_details",
            python_callable=get_logging_details_callable,
            op_args=[config]
        )

        create_payroll, load_payroll_data = payroll_data_export(
            group_id="payroll_export",
            generate_request=request_payload.get_create_payroll_batch_payload,
            get_export_name="{{result('get_logging_details').payroll_export_name}}",
            file_script_uri="get_payroll_file_format_for_country",
            retries=0
        )

        create_payroll_collection = rail.CreateCollectionOperator(
            task_id="create_payroll_collection",
            source="{{result('payroll_export.load_export')}}",
            columns={
                'Employee ID': 'employee_id',
                'Timesheet Period': 'timesheet_period',
                'Pay Code Name': 'pay_code_name',
                'Pay Code Code': 'pay_code_code',
                'Pay Code Hours': 'pay_code_hours',
                'Pay Code Pay': 'pay_code_pay'
            },
            name="raw_payroll_data"
        )

        is_payroll_has_data = rail.IfOperator(
            task_id="is_payroll_has_data",
            test="{{ result('create_payroll_collection', 'length') > 0}}",
            yes_task="query_blank_employee_id_records",
            no_task="empty_is_payroll_has_data_no_task"
        )

        empty_is_payroll_has_data_no_task = rail.EmptyOperator(
            task_id = "empty_is_payroll_has_data_no_task"
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Payroll Export - {{dag_run.conf.payroll_location_name}} - No records to export - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_empty_export.html"
        )

        update_export_name_with_nodata = rail.RepliconServiceOperator(
            task_id="update_export_name_with_nodata",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('payroll_export.get_export_uri')}}"
                },
                "name": "{{ result('get_logging_details').no_data_payroll_export_name }}"
            },
        )

        query_blank_employee_id_records = rail.QueryCollectionOperator(
            task_id="query_blank_employee_id_records",
            query="""SELECT * FROM raw_payroll_data rpd WHERE NULLIF(rpd.employee_id, '') IS NULL"""
        )

        has_any_blank_emp_id = rail.IfOperator(
            task_id="has_any_blank_emp_id",
            test="{{ result('query_blank_employee_id_records', 'length') > 0}}",
            yes_task="revert_to_draft",
            no_task="filter_payroll_data"
        )

        revert_to_draft = rail.RepliconServiceOperator(
            task_id='revert_to_draft',
            endpoint='/services/PayRunService1.svc/MarkPayRunAsDraft',
            data=lambda: request_payload.get_revert_draft_or_cancel_payroll_export_payload(
                'payroll_export')
        )

        cancel_export = rail.RepliconServiceOperator(
            task_id='cancel_export',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=lambda: request_payload.get_revert_draft_or_cancel_payroll_export_payload(
                'payroll_export')
        )

        send_cancelled_export_email = rail.EmailOperator(
            task_id='send_cancelled_export_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Payroll Export - {{dag_run.conf.payroll_location_name}} - Invalid records found - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_invalid_records.html"
        )

        filter_payroll_data = rail.QueryCollectionOperator(
            task_id="filter_payroll_data",
            query="""SELECT * FROM raw_payroll_data rpd WHERE rpd.Pay_Code_Code IN
            ('{{result('get_logging_details').paycodes}}')""",
            name="filtered_raw_data"
        )

        has_any_records_to_process = rail.IfOperator(
            task_id="has_any_records_to_process",
            test="{{ result('filter_payroll_data', 'length') > 0 }}",
            yes_task="add_index_to_raw_data",
            no_task="empty_has_any_records_to_process_no_task"
        )

        empty_has_any_records_to_process_no_task = rail.EmptyOperator(
            task_id = "empty_has_any_records_to_process_no_task"
        )

        add_index_to_raw_data = rail.QueryCollectionOperator(
            task_id="add_index_to_raw_data",
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id, frd.* FROM filtered_raw_data frd""",
            name="final_raw_data"
        )

        # !IF you pass any headers in the below request it will fail with the 401 Unauthorized ERROR
        get_access_token = rail.SimpleHttpOperator(
            task_id='get_access_token',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='OAuthService/GenerateToken',
            data={
                "grant_type": "client_credentials",
                "client_id": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_id {CLOSE_BRACKETS}",
                "client_secret": f"{OPEN_BRACKETS}var.json.{config.client_id_secret_variable_name}.client_secret {CLOSE_BRACKETS}",
            }
        )

        post_payroll_data = rail.TriggerDagRunForEachItemOperator(
            task_id="post_payroll_data",
            items=lambda: list(range(ceil(rail.result(
                'filter_payroll_data', 'length')/config.API_JSON_PAYLOAD_LIMIT))),
            trigger_dag_id=config.payroll_export_post_export_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item, index: {
                "todays_date": now(tz=config.time_zone).strftime(EXPORT_DATE_FORMAT),
                "timezone": config.time_zone,
                "export_name": rail.result('get_logging_details')['payroll_export_name'],
                "access_token_to_use": loads(rail.result('get_access_token'))['access_token'],
                "record_start_index": (item*config.API_JSON_PAYLOAD_LIMIT)+1,
                "record_end_index": (item+1)*config.API_JSON_PAYLOAD_LIMIT,
                "index": index+1,
                "payroll_location_name": config.PAYROLL_LOCATION_NAME,
            }
        )

        wait_for_post_payroll_data = rail.WaitForDagRunsSensor(
            task_id="wait_for_post_payroll_data",
            dag_runs="{{ result('post_payroll_data') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        payroll_data_processing_complete = rail.EmptyOperator(
            task_id="time_data_processing_complete"
        )

        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Replicon Payroll Export - {{dag_run.conf.payroll_location_name}} is completed successfully on {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/email_export_success.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=lambda dag_run: get_log_to_sumo_extra_info(
                dag_run, config)
        )

        can_process_tasks >> rail.Label("Yes") >> get_logging_details >> get_all_location_for_parent >> get_payroll_file_format_for_country >> create_payroll
        load_payroll_data >> create_payroll_collection >> is_payroll_has_data >> rail.Label(
            "Yes") >> query_blank_employee_id_records
        is_payroll_has_data >> rail.Label(
            "No") >> empty_is_payroll_has_data_no_task >> send_no_data_email >> update_export_name_with_nodata
        query_blank_employee_id_records >> has_any_blank_emp_id
        has_any_blank_emp_id >> rail.Label(
            "No") >> filter_payroll_data >> has_any_records_to_process >> rail.Label("No") >> empty_has_any_records_to_process_no_task >> send_no_data_email
        has_any_blank_emp_id >> rail.Label(
            "Yes") >> revert_to_draft >> cancel_export >> send_cancelled_export_email
        has_any_records_to_process >> rail.Label(
            "Yes") >> add_index_to_raw_data >> get_access_token >> post_payroll_data >> wait_for_post_payroll_data >> payroll_data_processing_complete\
            >> send_success_email >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
