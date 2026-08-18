
from datetime import timedelta
from airflow.models import Variable
import csv
import rail
from rail.lib.artifact import existing_artifact, new_artifact

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ingenta_custom_report_extract_send_file_child{config.instance}',
        description=f'Ingenta_custom_report_extract_send_file_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_jobid_present_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_jobid_present_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_jobid_present_3 = rail.IfOperator(
            task_id='if_request_jobid_present_3',
            test='''{{ dag_run.conf.jobid | is_truthy }}''',
            yes_task="ingenta_report_data_search_entries_4",
            no_task="log_to_sumo",
        )

        ingenta_report_data_search_entries_4 = rail.FilterLogEntriesOperator(
            task_id='ingenta_report_data_search_entries_4',
            log="{{ dag_run.conf.lookuptable }}",
            properties={
                'jobid': "{{ dag_run.conf.jobid }}",
            }
        )

        if_first_id_blank_5 = rail.IfOperator(
            task_id='if_first_id_blank_5',
            test='''{{ result('ingenta_report_data_search_entries_4','length') < 0 }}''',
            yes_task="stop_6",
            no_task="create_csv_lines_7",
        )

        stop_6 = rail.FailOperator(
            task_id='stop_6',
            message='''Export failed'''
        )

        create_csv_lines_7 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_7',
            source="{{ dag_run.conf.lookuptable}}",
            header=['User Name',
                    'User Department Name',
                    'User Dept for TRANS',
                    'Project Dept for TRANS',
                    'Client Name',
                    'Project Name',
                    'Time Off Type',
                    'Month (Allocation Date)',
                    'Timeoff Days',
                    'Contract Days',
                    'Actual Days',
                    'Allocated Days',
                    'Available Days',
                    'Actual v Planned'],
            row=lambda item: [
                item['properties']['username'] if  item['properties']['username'] != 'None' else '',

                item['properties']['userdepartment_name|userdepttrans'].split("|")[
                    0] if item['properties']['userdepartment_name|userdepttrans'].split("|")[
                    0] != 'None' else '',

                item['properties']['userdepartment_name|userdepttrans'].split(
                    "|")[-1] if  item['properties']['userdepartment_name|userdepttrans'].split(
                    "|")[-1] != 'None' else '',

                item['properties']['projectdept_for_trans'] if  item['properties']['projectdept_for_trans'] != 'None' and item['properties']['projectdept_for_trans'] != '' else '\n',

                item['properties']['clientname|projectname'].split(
                    "|")[0] if item['properties']['clientname|projectname'].split(
                    "|")[0] != 'None' else '',

                item['properties']['clientname|projectname'].split(
                    "|")[-1] if  item['properties']['clientname|projectname'].split(
                    "|")[-1] != 'None' else '',

                item['properties']['timeofftype'] if  item['properties']['timeofftype'] != 'None' and item['properties']['timeofftype'] != '' else '\n',

                item['properties']['month|time_off_days'].split(
                    "|")[0] if  item['properties']['month|time_off_days'].split(
                    "|")[0] != 'None' else '',

                item['properties']['month|time_off_days'].split(
                    "|")[-1] if item['properties']['month|time_off_days'].split(
                    "|")[-1] != 'None' else '',

                item['properties']['contractdays|actualdays'].split(
                    "|")[0] if  item['properties']['contractdays|actualdays'].split(
                    "|")[0] != 'None'  else '',

                item['properties']['contractdays|actualdays'].split(
                    "|")[-1] if  item['properties']['contractdays|actualdays'].split(
                    "|")[-1] != 'None' else '',

                item['properties']['allocateddays|availbledays'].split(
                    "|")[0] if  item['properties']['allocateddays|availbledays'].split(
                    "|")[0] != 'None'  else '',

                item['properties']['allocateddays|availbledays'].split(
                    "|")[-1] if item['properties']['allocateddays|availbledays'].split(
                    "|")[-1] != 'None' else '',

                item['properties']['actual_vs_planned'] if item['properties']['actual_vs_planned'] != 'None' else '',
            ],
            quoting=csv.QUOTE_MINIMAL
        )

        def fix_empty_field_quoting_callable(**context):
            """Replace quoted newlines with empty quotes for selective quoting on specific fields"""

            # Read the CSV artifact
            csv_artifact_name = rail.result("create_csv_lines_7")

            # Read content as string
            with existing_artifact(csv_artifact_name, mode='r', encoding='utf-8') as input_artifact:
                content = input_artifact.file.read()

            # Replace quoted newlines with empty quotes
            fixed_content = content.replace('"\n"', '""')

            # Write to new artifact
            with new_artifact(mode='w', encoding='utf-8') as output_artifact:
                output_artifact.file.write(fixed_content)
                output_artifact.set_attribute('type', 'csv')
                return output_artifact.name

        fix_empty_field_quoting_8 = rail.PythonOperator(
            task_id='fix_empty_field_quoting_8',
            python_callable=fix_empty_field_quoting_callable
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('fix_empty_field_quoting_8')}}",
            output_file_name="Custom Project Report" +" "+
            "{{current_time('%Y-%m-%d-%H-%M-%S')}}"+".csv",
            expires_in_seconds=7*24*60*60,
        )

        upload_12 = rail.SFTPUploadFileOperator(
            task_id='upload_12',
            content='''{{ result('fix_empty_field_quoting_8') }}''',
            remote_filepath=config.log_filepath + "/Custom Project Report" +" "+
            "{{current_time('%Y-%m-%d-%H-%M-%S')}}"+".csv",
        )

        send_mail_with_cshare_16 = rail.EmailOperator(
            task_id='send_mail_with_cshare_16',
            to="{{ dag_run.conf.email }}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Report Export - Project Report - Completed successfully - {{ current_time("%m-%d-%Y %H:%M:%S") }}''',
            html_content="templates/emails/success_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_request_jobid_present_3
        if_request_jobid_present_3 >> rail.Label(
            'Yes') >> ingenta_report_data_search_entries_4 >> if_first_id_blank_5
        if_request_jobid_present_3 >> rail.Label(
            'No') >> log_to_sumo
        if_first_id_blank_5 >> rail.Label(
            'Yes') >> stop_6 >> log_to_sumo
        if_first_id_blank_5 >> rail.Label(
            'No') >> create_csv_lines_7 >> fix_empty_field_quoting_8 >> generate_download_link >> upload_12 >> send_mail_with_cshare_16
        send_mail_with_cshare_16 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
