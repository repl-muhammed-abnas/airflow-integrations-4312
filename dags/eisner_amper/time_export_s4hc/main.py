from datetime import datetime as timedelta
from datetime import timedelta as td
from pendulum import datetime
import rail
from eisner_amper.time_export_s4hc.utils.custom_methods import logging_details
from eisner_amper.time_export_s4hc.utils import request_payload, custom_methods
import json
null = None

# pylint: disable=too-many-statements


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'eisner_amper_time_export_master_s4hc_{config.instance}',
        description='Eisner Amper Time Export Master S4HC',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_internal_id
        }
    ) as dag:

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_details,
            data_handler=custom_methods.get_user_timezone
        )

        is_timezone_eastern = rail.IfOperator(
            task_id="is_timezone_eastern",
            test=lambda: rail.result('get_user_details')['time_zone'] == "(UTC-5:00) Eastern Standard Time",
            yes_task="get_logging_details",
            no_task="update_user_timezone"
        )

        update_user_timezone = rail.RepliconServiceOperator(
            task_id="update_user_timezone",
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data=request_payload.update_user_timezone
        )

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.instance, config.time_zone]
        )

        get_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_report_details',
            report_name=config.extract_time_entry_report,
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
            no_task='send_no_data_mail',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_report_details.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_time_data_collection',
            source="{{ result('load_report_data') }}",
            name="timesheet_data"
        )

        get_cost_center_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_cost_center_report_details',
            report_name=config.extract_cost_and_roles_report,
        )

        report_cost_group_entry, report_cost_group_exit = rail.run_report(
            group_id='get_cost_report_details',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_cost_center_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_cost_report_failed = rail.IfOperator(
            task_id="is_cost_report_failed",
            test='{{result("get_cost_report_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_cost_report_generation",
            no_task="report_has_cost_data"
        )

        fail_cost_report_generation = rail.FailOperator(
            task_id="fail_cost_report_generation",
            message="{{result('get_cost_report_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_cost_data = rail.IfOperator(
            task_id="report_has_cost_data",
            test="{{ result('get_cost_report_details.get_report_result', 'has_data') }}",
            yes_task='load_cost_report_data',
            no_task='send_no_data_mail',
        )

        load_cost_report_data = rail.LoadCSVFileOperator(
            task_id='load_cost_report_data',
            document="{{ result('get_cost_report_details.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_cost_data_collection = rail.CreateCollectionOperator(
            task_id='create_cost_data_collection',
            source="{{ result('load_cost_report_data') }}",
            name="cost_data"
        )

        query_valid_time_entry_records = rail.QueryCollectionOperator(
            task_id='query_valid_time_entry_records',
            query="""SELECT * FROM timesheet_data WHERE Project_Profile IS NOT NULL AND Project_Profile != ''
            AND Company_Code_Code__Current_ IS NOT NULL AND Company_Code_Code__Current_ != ''
            AND Roles_Code__Current_ IS NOT NULL AND Roles_Code__Current_ != ''
            AND Work_Item_Code IS NOT NULL AND Work_Item_Code != ''"""
        )

        has_no_valid_data = rail.IfOperator(
            task_id="has_no_valid_data",
            test="{{ result('query_valid_time_entry_records', 'length') < 1}}",
            yes_task='send_no_data_mail',
            no_task='compose_csv_data',
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon Time Data extract for SAP HANA - No Data to Export ' + \
            (timedelta.now()).strftime("%Y%m%d%M%S"),
            html_content="template/no_data.html",
            params={
                'Created_time': (timedelta.now()).strftime("%Y%m%d%M%S")
            }
        )

        compose_csv_data = rail.WriteCSVFileOperator(
            task_id="compose_csv_data",
            source="{{ result('query_valid_time_entry_records') }}",
            row=request_payload.get_formated_row,
            thread_pool_size=config.thread_pool_size_write_csv,
            header=[
                'EmployeeID',
                'CompanyCode',
                'SubmittedOn',
                'SAPEmployeeID',
                'ProjectProfile',
                'TaskLevel1Code',
                'WorkPackageWorkItemCode',
                'Comments',
                'HoursWorked',
                'Location',
                'TimeEntryCode',
                'TimesheetPeriodUri',
                'CostCenterCode',
                'Roles',
                'ServiceLine',
                'WorkLocation',
                'Useruri',
                'Process'
            ],
            execution_timeout=td(days=config.execution_timeout_days)
        )

        create_valid_data_collection = rail.CreateCollectionOperator(
            task_id='create_valid_data_collection',
            source="{{ result('compose_csv_data') }}",
            name="valid_data"
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM valid_data WHERE ProjectProfile IS NOT NULL AND ProjectProfile != ''
            AND CompanyCode IS NOT NULL AND CompanyCode != ''
            AND Roles IS NOT NULL AND Roles != ''
            AND WorkPackageWorkItemCode IS NOT NULL AND WorkPackageWorkItemCode != ''
            AND CostCenterCode IS NOT NULL
            AND Process='Yes' """
        )

        process_time_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_time_export',
            retries=0,
            trigger_dag_id=f'eisner_amper_time_export_child_s4hc_{config.instance}',
            items="{{ result('query_valid_records') }}",
            conf=lambda item, **context: {"postingsdata": item, "index": context['index']},
            batch_size=config.child_dag_batch_size
        )

        query_zero_records = rail.QueryCollectionOperator(
            task_id='query_zero_records',
            query="""SELECT * FROM valid_data WHERE Process='No' """
        )

        has_zero_data = rail.IfOperator(
            task_id="has_zero_data",
            test="{{ result('query_zero_records', 'length') > 0}}",
            yes_task='compose_zero_data',
            no_task='log_to_sumo',
        )

        compose_zero_data = rail.WriteCSVFileOperator(
            task_id="compose_zero_data",
            source="{{ result('query_zero_records') }}",
            thread_pool_size=config.thread_pool_size_write_csv,
            header=[
                'Employee ID', 'Company code', 'Submitted on', 'SAP employee ID', 'Project profile',
                'Task level 1 code', 'Work package work item code', 'Comments', 'Hours worked', 'Time entry code', 'Timesheet period URI', 'Cost center code',
                'Roles', 'Work location', 'Useruri'
            ],
            row=lambda item: [
                item["EmployeeID"] if item["EmployeeID"] else '""',
                item["CompanyCode"] if item["CompanyCode"] else '""',
                item["SubmittedOn"] if item["SubmittedOn"] else '""',
                item['SAPEmployeeID'] if item['SAPEmployeeID'] else '""',
                item["ProjectProfile"] if item["ProjectProfile"] else '""',
                item["TaskLevel1Code"] if item["TaskLevel1Code"] else '""',
                item["WorkPackageWorkItemCode"] if item["WorkPackageWorkItemCode"] else '""',
                item["Comments"] if item["Comments"] else '""',
                item["HoursWorked"] if item["HoursWorked"] else '0',
                item["TimeEntryCode"] if item["TimeEntryCode"] else '""',
                item["TimesheetPeriodUri"] if item["TimesheetPeriodUri"] else '""',
                item["CostCenterCode"] if item["CostCenterCode"] else '""',
                item["Roles"] if item["Roles"] else '""',
                item['WorkLocation'] if item['WorkLocation'] else '""',
                item['Useruri'] if item['Useruri'] else '""'
            ],
            execution_timeout=td(days=config.execution_timeout_days)
        )

        def fix_zero_csv_empty_value_quotes():
            """Fix empty value quotes in zero hours CSV"""
            from rail.lib.artifact import existing_artifact, new_artifact
            csv_artifact_name = rail.result('compose_zero_data')
            with existing_artifact(csv_artifact_name, mode='r', encoding='utf-8') as input_artifact:
                csv_content = input_artifact.file.read()
            fixed_content = csv_content.replace('""""""', '""')
            with new_artifact(mode='w', encoding='utf-8') as output_artifact:
                output_artifact.file.write(fixed_content)
                output_artifact.set_attribute('type', 'csv')
                return output_artifact.name

        csv_data_update = rail.PythonOperator(
            task_id="csv_data_update",
            python_callable=fix_zero_csv_empty_value_quotes
        )

        upload_zero_data_internal_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_zero_data_internal_sftp',
            content="{{ result('csv_data_update') }}",
            remote_filepath=config.invalid_data_export_path + 'Error_Timesheet_Ohours' +
            '{{ result("get_logging_details")["current_date"] }}' + '.csv',
            sftp_conn_id=config.sftp_conn_internal_id
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

        get_user_details >> is_timezone_eastern >> rail.Label("No") >> get_logging_details >> get_timesheet_report_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> create_time_data_collection >> get_cost_center_report_details >> report_cost_group_entry
        report_cost_group_exit >> is_cost_report_failed >> rail.Label(
            "Yes") >> fail_cost_report_generation
        is_cost_report_failed >> rail.Label("No") >> report_has_cost_data >> rail.Label(
            "Yes") >> load_cost_report_data >> create_cost_data_collection >> query_valid_time_entry_records >> \
                has_no_valid_data >> rail.Label("Yes") >> send_no_data_mail
        has_no_valid_data >> rail.Label(
            "No") >> compose_csv_data >> create_valid_data_collection >> query_valid_records >> process_time_export >> query_zero_records >>\
                has_zero_data >> rail.Label("Yes") >> compose_zero_data >>\
                    csv_data_update >> upload_zero_data_internal_sftp >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun
        has_zero_data >> rail.Label(
            "No") >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

        is_timezone_eastern >> rail.Label("Yes") >> update_user_timezone >> get_logging_details

        report_has_data >> rail.Label("No") >> send_no_data_mail

        report_has_cost_data >> rail.Label("No") >> send_no_data_mail

    return dag


rail.for_each_instance(create_main_airflow_dag)
