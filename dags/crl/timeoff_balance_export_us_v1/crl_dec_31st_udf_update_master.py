from datetime import timedelta
from pendulum import datetime
import rail
from crl.timeoff_balance_export_us_v1.utils import python_callable, response_filter

# pylint: disable=too-many-statements line-too-long
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.crl_dec_31st_udf_update_master,
        description=f"CRL Payout Export USA Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.dec_31st_schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timeoff_report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": rail.result('get_report_details')['uri']
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_has_data',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_users_report_data',
            no_task= 'finish_export_no_payout_data'
        )

        finish_export_no_payout_data = rail.EmptyOperator(
            task_id='finish_export_no_payout_data'
        )

        send_email_for_no_payout_data = rail.EmailOperator(
            task_id='send_email_for_no_payout_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | ADP Payout Export Notification',
            html_content="/templates/email/blank_export.html"
        )

        load_users_report_data = rail.LoadCSVFileOperator(
            task_id='load_users_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        timeoff_report_data_collection = rail.CreateCollectionOperator(
            task_id='timeoff_report_data_collection',
            source="{{ result('load_users_report_data') }}",
            columns={
                "Employee ID": "empid",
                "Login Name": "loginname",
                "useruri": "useruri",
                "Time Off Type": "timeoff_type",
                "Time Off Balance": "timeoff_balance",
                "Sick Payout Eligible": "sick_eligible",
                "User Start Date": "user_start_date",
                "User End Date": "user_end_date",
                "Business Unit (Current)": "business_unit",
                "Employee Type (Current)": "employee_type"
            },
            name="sickandbanedtimeoffdetails"
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        query_report_data = rail.QueryCollectionOperator(
            task_id='query_report_data',
            query="""SELECT * FROM sickandbanedtimeoffdetails WHERE (sick_eligible='No') AND 
            (NULLIF(empid, '') IS NOT NULL OR empid!="") AND CAST(timeoff_balance AS DECIMAL(10,2)) > 0 AND
            ((employee_type LIKE 'Salaried%' AND business_unit == "NA05") OR
            employee_type NOT LIKE 'Salaried%')"""
        )

        get_timeoff_values = rail.PythonOperator(
            task_id="get_timeoff_values",
            python_callable=python_callable.get_timeoff_values,
        )

        get_exported_custom_field = rail.RepliconServiceOperator(
            task_id="get_exported_custom_field",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=response_filter.get_custom_field_uris
        )

        get_sick_payout_udf_option_uris = rail.RepliconServiceOperator(
            task_id = 'get_sick_payout_udf_option_uris',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_exported_custom_field')["sick_payout_eligible"]
            },
            data_handler=response_filter.get_sick_custom_field_dropdown_uris
        )

        process_child_udf_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child_udf_update',
            retries=0,
            items="{{ result('get_timeoff_values') | to_json }}",
            trigger_dag_id=config.process_udf_update_child_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'user_uri': item['useruri'],
                'sick_payout_eligible': rail.result('get_exported_custom_field')["sick_payout_eligible"],
                'set_sick_payout': rail.result('get_sick_payout_udf_option_uris')["yes"],
                'update_spo_udf':item['update_spo_udf']
            }
        )

        wait_process_child_udf_update = rail.WaitForDagRunsSensor(
            task_id="wait_process_child_udf_update",
            dag_runs="{{result('process_child_udf_update')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        send_email_for_update_udf_success = rail.EmailOperator(
            task_id='send_email_for_update_udf_success',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Payout Eligible UDF updates successfull',
            html_content="/templates/email/update_udf_success_email.html"
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        process_start_time >> get_report_details >> run_report_entry
        run_report_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation >> log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_expected_columns >> rail.Label("Yes") >> report_has_data
        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums >> log_to_sumo
        report_has_data >> rail.Label("Yes") >> load_users_report_data >> timeoff_report_data_collection >> create_log >> query_report_data
        query_report_data >> get_timeoff_values >> get_exported_custom_field >> get_sick_payout_udf_option_uris >> \
        process_child_udf_update >> wait_process_child_udf_update >> \
        send_email_for_update_udf_success >> log_to_sumo
        report_has_data >> rail.Label('No') >> finish_export_no_payout_data >> send_email_for_no_payout_data >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
