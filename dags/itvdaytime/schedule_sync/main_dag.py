from datetime import timedelta
import math
import rail
from itvdaytime.schedule_sync.utils import request_payload, custom_methods


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_schedule_sync_from_replicon_to_oracle_master_{config.instance}",
        description=f"iTV DayTime Schedule Sync from Replicon to Oracle master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_schedule,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_required_details = rail.PythonOperator(
            task_id="get_required_details",
            python_callable=custom_methods.get_required_details
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.schedule_sync_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id="report_generation",
            report_params=request_payload.get_generate_report_payload,
            replicon_conn_id=config.replicon_conn_id
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("report_generation.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_no_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('report_generation.get_report_result').reportGenerationResults[0].error}}"
        )

        has_no_data = rail.IfOperator(
            task_id="has_no_data",
            test='{{"No Data" in result("report_generation.get_report_result").reportGenerationResults[0].payload}}',
            yes_task="send_blank_mail",
            no_task='report_has_expected_columns',
        )

        send_blank_mail = rail.EmailOperator(
            task_id="send_blank_mail",
            subject='{{ get_company_key() }} | Schedule Sync from Replicon to Oracle is Skipped on - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            to=config.tenant_email,
            html_content="/templates/emails/no_data_email.html",
            params={
                "export_upload_filepath": config.upload_filepath
            },
        )

        #pylint: disable=line-too-long
        expected_report_columns = """Employee ID,Entry Date,User Name,Scheduled Hrs,Hours Worked,Schedule Variance,Assignment ID,Shift Name,Shift Work Hours,Shift Break Hours,Shift Total Hours,Shift Start Time,Shift End Time,1.Shift Type,useruri"""

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('report_generation.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            yes_task="report_payload_to_csv",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{result("report_generation.get_report_result").reportGenerationResults[0].payload}}'
        )

        export_raw_data = rail.CreateCollectionOperator(
            task_id="export_raw_data",
            name="raw_data",
            source="{{result('report_payload_to_csv')}}",
            columns={
                'Entry Date': 'entry_date',
                'User Name': 'user_name',
                'Scheduled Hrs': 'scheduled_hrs',
                'Hours Worked': 'hrs_worked',
                'Schedule Variance': 'scheduled_variance',
                'Assignment ID': 'assignment_id',
                'Shift Work Hours': 'shift_work_hours',
                'Shift Break Hours': 'shift_break_time',
                'Shift Total Hours': 'shift_total_hours',
                'Shift Start Time': 'shift_start_time',
                'Shift End Time': 'shift_end_time',
                '1.Shift Type': 'shift_type',
                'Employee ID': 'employee_id',
                'Shift Name': 'shift_name',
                'useruri': 'useruri',
                'Shift Code': "shift_code"
            }
        )

        unique_raw_data = rail.QueryCollectionOperator(
            task_id="unique_raw_data",
            name="unique_raw_data",
            query="""SELECT DISTINCT useruri, entry_date FROM raw_data"""
        )

        unique_raw_data_with_index = rail.QueryCollectionOperator(
            task_id="unique_raw_data_with_index",
            name="unique_raw_data_with_index",
            query="""SELECT  ROW_NUMBER() OVER (ORDER BY useruri , entry_date ) AS ROW_NUM, * FROM unique_raw_data"""
        )

        get_batch_list = rail.PythonOperator(
            task_id="get_batch_list",
            python_callable=lambda: list(
                range(0, math.ceil(rail.result('unique_raw_data_with_index', 'length')/config.BATCH_SIZE)))
        )

        process_records_by_batch = rail.TriggerDagRunForEachItemOperator(
            task_id="process_records_by_batch",
            items=lambda: rail.result('get_batch_list'),
            trigger_dag_id=f"itvdaytime_schedule_sync_process_data_by_batch_child_{config.instance}",
            conf=lambda item, index: {
                "export_file_name": rail.result("get_required_details")['export_file_name'],
                "record_start_index": (item*config.BATCH_SIZE)+1,
                "record_end_index": (item+1)*config.BATCH_SIZE,
                "index": index,
                "get_required_details": rail.result("get_required_details")
            },
            retries=0,
            execution_timeout=timedelta(hours=10)
        )

        wait_for_process_records_by_batch = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_records_by_batch",
            dag_runs="{{result('process_records_by_batch')}}",
            execution_timeout=timedelta(hours=10)
        )

        gather_all_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_all_logs",
            dag_runs='{{result("process_records_by_batch")}}',
            dagrun_task_id='create_log',
            flatten=True,
        )

        def combine_all_logs():
            master_data = []
            for log in (rail.result('gather_all_logs') or []):
                log_records = rail.load_all_records(log)
                if log_records:
                    master_data.extend(log_records)
            if not master_data:
                raise Exception("No Data found")
            return master_data

        load_all_records = rail.PythonOperator(
            task_id="load_all_records",
            python_callable=combine_all_logs
        )

        final_export_data = rail.WriteCSVFileOperator(
            task_id="final_export_data",
            source="{{ result('load_all_records') | to_json }}",
            header=['Resource Reference Type', 'Period Start Date', 'Period End Date', 'Publish', 'Shift Number (Shift Assignment Number)',
                    'Shift Action', 'Reference Day', 'Shift Start Time', 'Shift End Time', 'Shift Duration', 'Shift Time Not worked', 'Shift Code',
                    'Shift Category', 'Shift Type', 'Allow Edits'],
            delimiter="|",
            row=[
                "{{item.properties.resource_reference_type}}",
                "{{item.properties.period_start_date}}",
                "{{item.properties.period_end_date}}",
                "{{item.properties.publish}}",
                "{{item.properties.shift_number}}",
                "{{item.properties.shift_actions}}",
                "{{item.properties.reference_day}}",
                "{{item.properties.shift_start_time}}",
                "{{item.properties.shift_end_time}}",
                "{{item.properties.shift_duration}}",
                "{{item.properties.shift_time_not_worked}}",
                "{{item.properties.shift_code}}",
                "{{item.properties.shift_category}}",
                "{{item.properties.shift_type}}",
                "{{item.properties.allow_shift}}",
            ]
        )

        encrypt_export_file = rail.PGPEncryptionOperator(
            task_id="encrypt_export_file",
            pgp_conn_id=config.pgp_connection_id,
            source="{{ result('final_export_data') }}"
        )

        update_export_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='update_export_file_to_sftp',
            content="{{ result('encrypt_export_file') }}",
            remote_filepath=config.upload_filepath +
            '/{{result("get_required_details").export_file_name}}',
        )

        send_complete_email = rail.EmailOperator(
            task_id="send_complete_email",
            subject='{{ get_company_key() }} | Schedule Sync from Replicon to Oracle is completed on - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            to=config.tenant_email,
            html_content="/templates/emails/successful_email.html",
            params={
                "export_upload_filepath": config.upload_filepath
            },
        )

        get_required_details >> get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> has_no_data >> rail.Label("Yes") >> report_has_expected_columns >> rail.Label(
            "Yes") >> report_payload_to_csv >> export_raw_data >> unique_raw_data >> unique_raw_data_with_index >> get_batch_list\
            >> process_records_by_batch >> wait_for_process_records_by_batch >> gather_all_logs >> load_all_records >> final_export_data\
            >> encrypt_export_file >> update_export_file_to_sftp >> send_complete_email
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns
        has_no_data >> rail.Label("Yes") >> send_blank_mail
    return dag


rail.for_each_instance(create_main_dag)
