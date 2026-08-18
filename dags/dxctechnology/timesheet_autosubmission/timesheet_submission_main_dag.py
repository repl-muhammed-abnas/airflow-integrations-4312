from datetime import timedelta
import rail
from dxctechnology.timesheet_autosubmission import request_payload
from dxctechnology.timesheet_autosubmission import response_filter


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_timesheet_submission_master_{config.instance}_{config.country}_{config.entity}{config.identifier_dagname}',
        description=f'DXC - Timesheet Auto-submission - V1.0 - {config.instance}_{config.country}_{config.entity}{config.identifier_dagname}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule,
        max_active_runs=2,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.extract_report_name,
        )

        get_enabled_divisions_company_codes = rail.RepliconServiceOperator(
            task_id="get_enabled_divisions_company_codes",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_enabled_divisions_company_codes_payload,
            response_filter=response_filter.map_list_data_to_companycode_list
        )

        get_enabled_employeetypes = rail.RepliconServiceOperator(
            task_id="get_enabled_employeetypes",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            response_filter=response_filter.map_list_data_to_employeetype_list
        )

        get_mappped_companycodes = rail.PythonOperator(
            task_id='get_mappped_companycodes',
            python_callable=lambda: request_payload.companycode_from_mapper(
                config.entity, config.country, config.employee_type)
        )

        get_report_filters = rail.PythonOperator(
            task_id="get_report_filters",
            python_callable=request_payload.get_report_filter_uris,
            op_args=[
                'get_enabled_divisions_company_codes',
                'get_mappped_companycodes',
                config,
                'get_report_details',
                'get_enabled_employeetypes'
            ]
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='run_timesheet_autosubmission',
            report_params="{{result('get_report_filters')}}",
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_nodata = rail.IfOperator(
            task_id="report_has_nodata",
            test="{{ result('run_timesheet_autosubmission.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task='send_no_data_email',
            no_task='load_report_data',
        )

        send_no_data_email = rail.EmailOperator(
            task_id="send_no_data_email",
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject=f'{ config.company_key } | Automatic Timesheet Submission completed for { config.country } ({config.identifier_filename}) - No timesheet to process - '+ '{{current_time("%Y-%m-%dT%H:%M:%S")}}',
            html_content="blank_email.html",
            params={
                'country': config.country,
                'entity': config.identifier_filename,
            },
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_timesheet_autosubmission.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_report_data') }}",
            name='timesheet',
            columns={
                    'Timesheet Period': 'timesheetperiod',
                    'User Name': 'username',
                    'Employee ID': 'employeeid',
                    'Validation Messages': 'validationmessages',
                    'Approval Status': 'approvalstatus',
                    'TimesheetUri': 'timesheeturi',
                    'Timesheet Start Date': 'timesheetstartdate',
                    'Timesheet End Date': 'timesheetenddate',
                    'UserUri': 'useruri',
                    'Employee Type (Current)': 'employeetype',
                    'Employee type group': 'employeetypegroup'

            }
        )

        query_eligible_timesheets = rail.QueryCollectionOperator(
            task_id='query_eligible_timesheets',
            query="""SELECT * FROM timesheet
                        WHERE validationmessages = 'Null'
                        AND employeetypegroup != 'Contractor'""",
        )

        process_timesheet_c1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_c1',
            items=lambda: rail.result('query_eligible_timesheets'),
            trigger_dag_id=f'dxctechnology_timesheet_autosubmission_child_{config.instance}_{config.country}_{config.entity}{config.identifier_dagname}',
            batch_size=config.batch_size,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_process_timesheet = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheet',
            dag_runs='{{ result("process_timesheet_c1") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'Employee ID',
                'User Name',
                'Timesheet Period',
                'Date Time',
                'Status',
                'Remarks',
                'Country-Type',
                'Jobid'],
            row=[
                '{{ item.properties | attr_or_default("employeeid", "") }}',
                '{{ item.properties | attr_or_default("username", "") }}',
                '{{ item.properties | attr_or_default("timesheetperiod", "")}}',
                '{{ data_interval_start }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.message }}',
                '{{ item.properties | attr_or_default("country_type", "") }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.sftp_upload_path +
            f'Timesheet_auto_submission_{config.country}_{config.identifier_filename}' +
            'logs_{{ dag_run_ecid()  | replace(":", "-") }}.csv'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,

            subject=f'{config.company_key} | Automatic Timesheet Submission' +
                    '{% if result("get_errored_logs", key="length") > 0 %} \
                        completed with errors for   \
                    {% else %} \
                    completed successfully for   {% endif %}'
                    + f'{ config.country} ({config.identifier_filename}) - ' +
                    '{{current_time("%Y-%m-%dT%H:%M:%S")}}',
                    html_content="email_import_complete.html",
                    params={
                        'log_filepath': config.sftp_upload_path,
                        'country': config.country,
                        'entity': config.identifier_filename
                    }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> get_enabled_divisions_company_codes >> get_mappped_companycodes >> get_enabled_employeetypes >> \
            get_report_filters >> report_group_entry >> report_group_exit >> report_has_nodata
        report_has_nodata >> rail.Label("No") >> load_report_data >> create_report_collection >> query_eligible_timesheets >> process_timesheet_c1 >> \
            wait_for_process_timesheet >> get_errored_logs >> render_logs_csv >> upload_log_to_sftp >> send_import_complete_email >> finish

        report_has_nodata >> rail.Label("Yes") >> send_no_data_email >> finish
    return dag


rail.for_each_instance(create_dag)
