from datetime import timedelta
import pendulum
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'DTNA_DataMart_Export_ENG_No Cost Center V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=pendulum.datetime(2026, 7, 1, tz=config.time_zone),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_start_time',
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_start_time = rail.PythonOperator(
            task_id='process_start_time',
            python_callable=lambda: pendulum.now(config.time_zone).strftime("%Y%m%d%H%M%S")
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        generate_report = rail.run_report2(
            group_id='generate_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        parse_csv_batch_results = rail.LoadCSVFileOperator(
            task_id='parse_csv_batch_results',
            document="{{ (result('generate_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        create_collection_worker_data = rail.CreateCollectionOperator(
            task_id='create_collection_worker_data',
            source="{{ result('parse_csv_batch_results') }}",
            name="rawdata",
            columns={
                'Employee ID': 'employeeid',
                'HIRING_MANAGER_ID': 'hiringmanagerid',
                'COST_CENTER_NAME (Current)': 'costcenternamecurrent',
                'User Start Date': 'userstartdate',
                'User End Date': 'userenddate',
                'Login Name': 'loginname',
                'CLNT_WRKR_ID': 'clntwrkrid',
                'Employee Type': 'employeetype',
                'User First Name': 'userfirstname',
                'User Last Name': 'userlastname',
                'User Email': 'useremail',
                'User Supervisor Name (Current)': 'usersupervisornamecurrent',
                'Initials - ENG': 'initialseng',
                'Manager - ENG (Current)': 'managerengcurrent',
                'WRKR_ID': 'wrkrid',
                'JOB_CODE': 'jobcode',
                'APPR_ID': 'apprid',
                'SUPPLIER_ID': 'supplierid',
                'User Status': 'userstatus',
                'User Department Name': 'userdepartmentname'
            }
        )

        query_no_costcenter = rail.QueryCollectionOperator(
            task_id='query_no_costcenter',
            query="""SELECT * FROM rawdata WHERE NULLIF(rawdata.costcenternamecurrent, '') IS NULL""",
        )

        if_no_costcenter_records = rail.IfOperator(
            task_id='if_no_costcenter_records',
            test='''{{ result('query_no_costcenter') | length > 0 }}''',
            yes_task="create_csv_lines",
            no_task="finish",
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('query_no_costcenter') }}",
            header=['Replicon Worker ID',
                    'Hiring Manager ID',
                    'Cost Center',
                    'Cost Center Effective Date',
                    'Active Date',
                    'Termination Date',
                    'Status',
                    'Replicon Login Name',
                    'Client Worker ID',
                    'Worker Type',
                    'Worker First Name',
                    'Worker Last Name',
                    'Worker Email address',
                    'Approver ID',
                    'Initials - ENG',
                    'Manager - ENG',
                    'Reason'],
            row=lambda item: [
                item['employeeid'],
                item['hiringmanagerid'],
                item['costcenternamecurrent'],
                '',
                item['userstartdate'],
                item['userenddate'],
                'Inactive',
                item['loginname'],
                item['clntwrkrid'],
                item['employeetype'],
                item['userfirstname'],
                item['userlastname'],
                item['useremail'],
                item['usersupervisornamecurrent'],
                item['initialseng'],
                item['managerengcurrent'],
                'Cost center is not assigned to the user in Replicon profile'
            ],
        )

        dir_rejected_files = rail.SFTPListFilesOperator(
            task_id='dir_rejected_files',
            paths=[config.input_filepath]
        )

        if_rejected_files_present = rail.IfOperator(
            task_id='if_rejected_files_present',
            test='''{{ result('dir_rejected_files').values() | is_truthy }}''',
            yes_task="foreach_rejected_files",
            no_task="upload_rejected_file",
        )

        foreach_rejected_files = rail.ForEachOperator(
            task_id='foreach_rejected_files',
            items=lambda: list(rail.result('dir_rejected_files').values())[0],
            start_task='if_name_contains_costcenter',
            end_task='foreach_rejected_files_end'
        )

        if_name_contains_costcenter = rail.IfOperator(
            task_id='if_name_contains_costcenter',
            test='''{{ result('foreach_rejected_files').name | matches('CostCenter') }}''',
            yes_task="rename_to_archive",
            no_task="foreach_rejected_files_end",
        )

        rename_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_to_archive',
            existing_filename=config.input_filepath +
            "/" + "{{ result('foreach_rejected_files').name }}",
            new_filename=config.archive_filepath +
            "/" + "{{ result('foreach_rejected_files').name }}",
        )

        foreach_rejected_files_end = rail.EmptyOperator(
            task_id='foreach_rejected_files_end',
        )

        upload_rejected_file = rail.SFTPUploadFileOperator(
            task_id='upload_rejected_file',
            content="{{ result('create_csv_lines') }}",
            remote_filepath=config.input_filepath + "/" +
            config.rejected_file_name_prefix +
            "{{ result('process_start_time') }}.csv"
        )

        send_mail_export_complete = rail.EmailOperator(
            task_id='send_mail_export_complete',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Datamart worker data with no cost center export for ENG Department completed - {{ current_time_in_specified_tz(params.time_zone) }}''',
            html_content="templates/emails/export_complete.html",
            params={
                'file_name_prefix': config.rejected_file_name_prefix,
                'rejected_files_directory': config.input_filepath,
                'time_zone': config.time_zone
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> process_start_time
        process_start_time >> get_report_details >> generate_report >> parse_csv_batch_results >> create_collection_worker_data >> query_no_costcenter >> if_no_costcenter_records
        if_no_costcenter_records >> rail.Label(
            'Yes') >> create_csv_lines >> dir_rejected_files >> if_rejected_files_present
        if_no_costcenter_records >> rail.Label('No') >> finish
        if_rejected_files_present >> rail.Label(
            'Yes') >> foreach_rejected_files >> if_name_contains_costcenter
        if_name_contains_costcenter >> rail.Label(
            'Yes') >> rename_to_archive >> foreach_rejected_files_end
        if_name_contains_costcenter >> rail.Label(
            'No') >> foreach_rejected_files_end
        foreach_rejected_files >> foreach_rejected_files_end >> upload_rejected_file
        if_rejected_files_present >> rail.Label('No') >> upload_rejected_file
        upload_rejected_file >> send_mail_export_complete >> finish

    return dag


rail.for_each_instance(create_dag)
